#include <Processors/Transforms/ShuffleTransform.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Processors/Port.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <Common/Exception.h>
#include <Common/RadixSort.h>
#include <Common/randomSeed.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <numeric>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// The maximum number of buckets of `ShuffleTransform`. Every bucket has at least about `max_block_size` rows,
/// so a bucket fits in the CPU cache in most cases, and the number of small pieces created while scattering is bounded.
constexpr size_t max_num_buckets = 256;

/// A random number in [0, n) using multiply-high reduction, see https://arxiv.org/abs/1805.10941
/// The bias is at most n / 2^64, which is negligible.
inline UInt64 randomBelow(pcg64 & rng, UInt64 n)
{
    return static_cast<UInt64>((static_cast<unsigned __int128>(rng()) * n) >> 64);
}

/// A uniformly distributed random number in (0, 1).
inline double randomUnitOpen(pcg64 & rng)
{
    return (static_cast<double>(rng() >> 11) + 0.5) / 9007199254740992.0;
}

/// A uniformly random permutation of [0, n): the Fisher-Yates shuffle.
IColumn::Permutation randomPermutation(pcg64 & rng, size_t n)
{
    IColumn::Permutation permutation(n);
    std::iota(permutation.begin(), permutation.end(), 0);
    for (size_t i = n; i > 1; --i)
        std::swap(permutation[i - 1], permutation[randomBelow(rng, i)]);
    return permutation;
}

/// The columns to accumulate: nullptr for constant columns (they are restored from the header) and for `skip_position`.
Columns makeEmptyColumns(const Block & header, std::optional<size_t> skip_position)
{
    Columns columns(header.columns());
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const auto & column = header.getByPosition(i).column;
        if ((!skip_position || i != *skip_position) && !isColumnConst(*column))
            columns[i] = removeSpecialRepresentations(column)->cloneEmpty();
    }
    return columns;
}

/// The columns of a chunk in the form to accumulate: nullptr where `empty_columns` has nullptr, full columns otherwise.
Columns prepareColumns(Chunk & chunk, const Columns & empty_columns)
{
    removeSpecialColumnRepresentations(chunk);
    Columns columns = chunk.detachColumns();
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (empty_columns[i])
            columns[i] = columns[i]->convertToFullColumnIfConst();
        else
            columns[i] = nullptr;
    }
    return columns;
}

UInt64 getPopulation(const Chunk & chunk)
{
    auto info = chunk.getChunkInfos().get<ShuffledChunkInfo>();
    if (!info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A chunk of a shuffled stream has no ShuffledChunkInfo");
    return info->population;
}

}


ShuffleKeyGenerator::ShuffleKeyGenerator(const DataTypePtr & type, int direction_)
    : direction(direction_)
{
    WhichDataType which(type);
    if (which.isUInt32())
        is_uint32 = true;
    else if (!which.isUInt64())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Shuffle key column must be UInt64 or UInt32, got {}", type->getName());
}

ColumnPtr ShuffleKeyGenerator::makeColumn(const PaddedPODArray<UInt64> & keys) const
{
    /// The keys are 64-bit. For a `UInt32` key, take the high bits, which is also uniformly distributed and keeps the order.
    /// For descending order, invert the bits, which keeps the distribution and reverses the order.
    auto make = [&]<typename T>()
    {
        auto column = ColumnVector<T>::create(keys.size());
        auto & data = column->getData();
        constexpr size_t shift = (sizeof(UInt64) - sizeof(T)) * 8;
        for (size_t i = 0; i < keys.size(); ++i)
        {
            T value = static_cast<T>(keys[i] >> shift);
            data[i] = direction < 0 ? static_cast<T>(~value) : value;
        }
        return column;
    };

    if (is_uint32)
        return make.template operator()<UInt32>();
    return make.template operator()<UInt64>();
}

ColumnPtr ShuffleKeyGenerator::generateInRange(pcg64 & rng, size_t num_rows, size_t index, size_t count) const
{
    PaddedPODArray<UInt64> keys(num_rows);
    if (count == 1)
    {
        for (auto & key : keys)
            key = rng();
    }
    else
    {
        const unsigned __int128 begin_wide = (static_cast<unsigned __int128>(index) << 64) / count;
        const unsigned __int128 end_wide = (static_cast<unsigned __int128>(index + 1) << 64) / count;
        const UInt64 begin = static_cast<UInt64>(begin_wide);
        /// `end_wide` is 2^64 for the last range, so the width is computed in the wider type to avoid a wrap-around.
        const UInt64 width = static_cast<UInt64>(end_wide - begin_wide);
        for (auto & key : keys)
            key = begin + randomBelow(rng, width);
    }
    RadixSort<RadixSortUIntTraits<UInt64>>::executeLSD(keys.data(), keys.size());
    return makeColumn(keys);
}

ColumnPtr ShuffleKeyGenerator::generateNextOrderStatistics(pcg64 & rng, size_t num_rows, UInt64 population)
{
    /// The sorted keys of n independent uniformly random numbers in [0, 1) are generated sequentially: if `u` is the
    /// previous key, and m keys are not generated yet, the next key is 1 - (1 - u) * V^(1/m), where V is uniform in (0, 1).
    /// `remaining_fraction` keeps log(1 - u) for precision.
    PaddedPODArray<UInt64> keys(num_rows);
    for (auto & key : keys)
    {
        if (num_generated >= population)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot generate more than {} shuffle keys", population);

        remaining_fraction += std::log(randomUnitOpen(rng)) / static_cast<double>(population - num_generated);
        ++num_generated;

        const double fraction = -std::expm1(remaining_fraction);
        const double scaled = fraction * 18446744073709551616.0;
        key = scaled >= 18446744073709551615.0 ? std::numeric_limits<UInt64>::max() : static_cast<UInt64>(scaled);
    }
    return makeColumn(keys);
}


ShuffleTransform::ShuffleTransform(
    SharedHeader input_header,
    SharedHeader output_header,
    std::optional<ShuffleKeyDescription> key_,
    size_t max_block_size_,
    size_t range_index_,
    size_t num_ranges_)
    : IAccumulatingTransform(input_header, output_header)
    , key(key_)
    , max_block_size(std::max<size_t>(max_block_size_, 1))
    , range_index(range_index_)
    , num_ranges(std::max<size_t>(num_ranges_, 1))
    , rng(randomSeed())
{
    std::optional<size_t> skip_position;
    if (key)
    {
        key_generator.emplace(output_header->getByPosition(key->position).type, key->direction);
        if (key->replace)
            skip_position = key->position;
    }
    empty_columns = makeEmptyColumns(*input_header, skip_position);
}

void ShuffleTransform::consume(Chunk chunk)
{
    const size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    chunks.push_back(prepareColumns(chunk, empty_columns));
    bucket_rows.push_back(num_rows); /// Until the scatter: the number of rows of every chunk.
    accumulated_rows += num_rows;
}

void ShuffleTransform::scatterIntoBuckets()
{
    VectorWithMemoryTracking<size_t> chunk_rows;
    chunk_rows.swap(bucket_rows);

    if (accumulated_rows == 0)
        return;

    num_buckets = std::clamp<size_t>(accumulated_rows / max_block_size, 1, max_num_buckets);

    buckets.resize(num_buckets);
    for (auto & bucket : buckets)
    {
        bucket.resize(empty_columns.size());
        for (size_t i = 0; i < empty_columns.size(); ++i)
            if (empty_columns[i])
                bucket[i] = empty_columns[i]->cloneEmpty();
    }
    bucket_rows.assign(num_buckets, 0);

    /// Reserve the exact size of every bucket, so that the columns do not grow by doubling (which would take up to
    /// twice the memory of the data). The bucket numbers are not stored: they are generated twice from the same state
    /// of the random generator, the first time only to count the rows.
    {
        VectorWithMemoryTracking<size_t> expected_rows(num_buckets, 0);
        if (num_buckets == 1)
        {
            expected_rows[0] = accumulated_rows;
        }
        else
        {
            const pcg64 saved_rng = rng;
            for (const size_t num_rows : chunk_rows)
                for (size_t row = 0; row < num_rows; ++row)
                    ++expected_rows[randomBelow(rng, num_buckets)];
            rng = saved_rng;
        }

        for (size_t bucket = 0; bucket < num_buckets; ++bucket)
            for (auto & column : buckets[bucket])
                if (column)
                    column->reserve(expected_rows[bucket]);
    }

    IColumn::Selector selector;
    for (size_t chunk_index = 0; chunk_index < chunks.size(); ++chunk_index)
    {
        auto & columns = chunks[chunk_index];
        const size_t num_rows = chunk_rows[chunk_index];

        if (num_buckets == 1)
        {
            for (size_t i = 0; i < columns.size(); ++i)
                if (columns[i])
                    buckets[0][i]->insertRangeFrom(*columns[i], 0, num_rows);
            bucket_rows[0] += num_rows;
            columns.clear();
            continue;
        }

        /// Every row goes into a random bucket.
        selector.resize(num_rows);
        for (auto & bucket : selector)
        {
            bucket = randomBelow(rng, num_buckets);
            ++bucket_rows[bucket];
        }

        for (size_t i = 0; i < columns.size(); ++i)
        {
            if (!columns[i])
                continue;

            auto parts = columns[i]->scatter(num_buckets, selector);
            for (size_t bucket = 0; bucket < num_buckets; ++bucket)
                buckets[bucket][i]->insertRangeFrom(*parts[bucket], 0, parts[bucket]->size());
        }

        /// Release the memory of the chunk.
        columns.clear();
    }
    chunks.clear();
}

bool ShuffleTransform::prepareNextBucket()
{
    while (next_bucket < num_buckets && bucket_rows[next_bucket] == 0)
        ++next_bucket;

    if (next_bucket >= num_buckets)
        return false;

    const size_t bucket = next_bucket;
    ++next_bucket;

    part_rows = bucket_rows[bucket];
    const auto permutation = randomPermutation(rng, part_rows);

    part_columns.assign(empty_columns.size(), nullptr);
    for (size_t i = 0; i < empty_columns.size(); ++i)
        if (buckets[bucket][i])
            part_columns[i] = buckets[bucket][i]->permute(permutation, 0);
    MutableColumns().swap(buckets[bucket]);

    /// The range and the bucket number are the high part of the key: the buckets of all ranges together are
    /// equal ranges of keys (every range can have its own number of buckets).
    if (key)
        part_key = key_generator->generateInRange(rng, part_rows, range_index * num_buckets + bucket, num_ranges * num_buckets);

    part_offset = 0;
    return true;
}

Chunk ShuffleTransform::generate()
{
    if (!buckets_prepared)
    {
        scatterIntoBuckets();
        buckets_prepared = true;
    }

    if (part_offset >= part_rows)
    {
        if (!prepareNextBucket())
        {
            part_columns.clear();
            part_key = nullptr;
            return {};
        }
    }

    const size_t length = std::min(max_block_size, part_rows - part_offset);
    const bool whole_part = part_offset == 0 && length == part_rows;

    const auto & input_header = getInputPort().getHeader();
    const auto & output_header = getOutputPort().getHeader();
    Columns columns(output_header.columns());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (key && i == key->position)
            columns[i] = whole_part ? std::move(part_key) : part_key->cut(part_offset, length);
        else if (part_columns[i])
            columns[i] = whole_part ? std::move(part_columns[i]) : part_columns[i]->cut(part_offset, length);
        else
            columns[i] = input_header.getByPosition(i).column->cloneResized(length);
    }

    part_offset += length;
    return Chunk(std::move(columns), length);
}


ReservoirSampleTransform::ReservoirSampleTransform(SharedHeader header, UInt64 limit_, size_t max_block_size_)
    : IAccumulatingTransform(header, header)
    , limit(limit_)
    , max_block_size(std::max<size_t>(max_block_size_, 1))
    , rng(randomSeed())
{
    if (limit == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ReservoirSampleTransform requires a limit");

    empty_columns = makeEmptyColumns(*header, std::nullopt);
    data.resize(empty_columns.size());
    for (size_t i = 0; i < empty_columns.size(); ++i)
        if (empty_columns[i])
            data[i] = empty_columns[i]->cloneEmpty();
}

double ReservoirSampleTransform::randomUnit()
{
    return randomUnitOpen(rng);
}

void ReservoirSampleTransform::advance()
{
    /// Algorithm L: the number of rows to skip has the geometric distribution with the probability `w` of success.
    const double skip = std::floor(std::log(randomUnit()) / std::log1p(-w));
    const UInt64 max_index = std::numeric_limits<UInt64>::max();
    if (!(skip < 1.8e19) || static_cast<UInt64>(skip) >= max_index - next_index - 1)
        next_index = max_index;
    else
        next_index += static_cast<UInt64>(skip) + 1;
}

void ReservoirSampleTransform::compact()
{
    auto indices = ColumnUInt64::create(slots.size());
    auto & indices_data = indices->getData();
    for (size_t i = 0; i < slots.size(); ++i)
        indices_data[i] = slots[i];

    for (auto & column : data)
        if (column)
            column = IColumn::mutate(column->index(*indices, 0));

    std::iota(slots.begin(), slots.end(), 0);
    data_rows = slots.size();
}

void ReservoirSampleTransform::consume(Chunk chunk)
{
    const size_t num_rows = chunk.getNumRows();
    if (read_rows)
        read_rows->add(num_rows);

    if (num_rows == 0)
        return;

    Columns columns = prepareColumns(chunk, empty_columns);

    size_t first_row = 0;
    if (slots.size() < limit)
    {
        /// Fill the reservoir with the first rows.
        const size_t take = static_cast<size_t>(std::min<UInt64>(num_rows, limit - slots.size()));
        for (size_t i = 0; i < data.size(); ++i)
            if (data[i])
                data[i]->insertRangeFrom(*columns[i], 0, take);
        for (size_t row = 0; row < take; ++row)
            slots.push_back(data_rows + row);
        data_rows += take;
        first_row = take;

        if (slots.size() == limit)
        {
            w = std::exp(std::log(randomUnit()) / static_cast<double>(limit));
            next_index = num_seen + take - 1;
            advance();
        }
    }

    if (slots.size() == limit)
    {
        /// The rows that replace random rows of the sample; the rows in between are skipped.
        auto indices = ColumnUInt64::create();
        auto & indices_data = indices->getData();
        PaddedPODArray<UInt64> replaced_slots;

        while (next_index < num_seen + num_rows)
        {
            const UInt64 row = next_index - num_seen;
            if (row < first_row)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Reservoir sampling selected a row that was already taken");
            indices_data.push_back(row);
            replaced_slots.push_back(randomBelow(rng, limit));

            w *= std::exp(std::log(randomUnit()) / static_cast<double>(limit));
            advance();
        }

        if (!indices_data.empty())
        {
            for (size_t i = 0; i < data.size(); ++i)
            {
                if (data[i])
                {
                    auto selected = columns[i]->index(*indices, 0);
                    data[i]->insertRangeFrom(*selected, 0, selected->size());
                }
            }

            /// Later replacements of the same slot win, as in the sequential algorithm.
            for (size_t i = 0; i < replaced_slots.size(); ++i)
                slots[replaced_slots[i]] = data_rows + i;
            data_rows += indices_data.size();

            if (data_rows - slots.size() >= std::max<UInt64>(limit, max_block_size))
                compact();
        }
    }

    num_seen += num_rows;
}

Chunk ReservoirSampleTransform::generate()
{
    if (!generated_prepared)
    {
        generated_prepared = true;
        /// The order of the rows in the reservoir is not random: shuffle it.
        for (size_t i = slots.size(); i > 1; --i)
            std::swap(slots[i - 1], slots[randomBelow(rng, i)]);
    }

    if (output_offset >= slots.size())
    {
        data.clear();
        PaddedPODArray<UInt64>().swap(slots);
        return {};
    }

    const size_t length = std::min(max_block_size, slots.size() - output_offset);
    auto indices = ColumnUInt64::create(length);
    auto & indices_data = indices->getData();
    for (size_t i = 0; i < length; ++i)
        indices_data[i] = slots[output_offset + i];
    output_offset += length;

    const auto & header = getOutputPort().getHeader();
    Columns columns(header.columns());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (data[i])
            columns[i] = data[i]->index(*indices, 0);
        else
            columns[i] = header.getByPosition(i).column->cloneResized(length);
    }

    Chunk result(std::move(columns), length);
    result.getChunkInfos().add(std::make_shared<ShuffledChunkInfo>(num_seen));
    return result;
}


MergingShuffledTransform::MergingShuffledTransform(SharedHeader header, size_t num_inputs, UInt64 limit_, size_t max_block_size_)
    : IProcessor(InputPorts(num_inputs, header), OutputPorts{header})
    , limit(limit_)
    , max_block_size(std::max<size_t>(max_block_size_, 1))
    , rng(randomSeed())
    , inputs_state(num_inputs)
{
}

IProcessor::Status MergingShuffledTransform::prepare()
{
    auto & output = outputs.front();

    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    if (output_chunk)
    {
        output.push(std::move(output_chunk));
        return Status::PortFull;
    }

    /// Read the inputs: the first chunk of every input to know its population, then as much as needed for the next block.
    size_t input_index = 0;
    for (auto & input : inputs)
    {
        auto & state = inputs_state[input_index++];
        if (state.finished)
            continue;

        if (input.isFinished())
        {
            state.finished = true;
            if (!state.population)
                state.population = 0;
            continue;
        }

        const bool want_data = !state.population || state.buffered_rows < std::max<size_t>(state.to_take, max_block_size);
        if (!want_data)
        {
            input.setNotNeeded();
            continue;
        }

        input.setNeeded();
        if (!input.hasData())
            continue;

        Chunk chunk = input.pull();
        if (!state.population)
            state.population = getPopulation(chunk);
        if (chunk.getNumRows())
        {
            state.buffered_rows += chunk.getNumRows();
            state.chunks.push_back(std::move(chunk));
        }
    }

    if (!populations_known)
    {
        for (const auto & state : inputs_state)
            if (!state.population)
                return Status::NeedData;

        populations_known = true;
        for (auto & state : inputs_state)
        {
            state.remaining_population = *state.population;
            total_population += *state.population;
        }
        remaining_population = total_population;
        remaining_output = limit ? std::min(limit, total_population) : total_population;
    }

    if (!counts_drawn)
    {
        if (remaining_output == 0)
        {
            output.finish();
            for (auto & input : inputs)
                input.close();
            return Status::Finished;
        }
        return Status::Ready;
    }

    for (const auto & state : inputs_state)
    {
        if (state.buffered_rows < state.to_take)
        {
            if (state.finished)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A shuffled stream has fewer rows than its population requires");
            return Status::NeedData;
        }
    }

    return Status::Ready;
}

void MergingShuffledTransform::work()
{
    if (!counts_drawn)
    {
        drawCounts();
        counts_drawn = true;
        return;
    }

    output_chunk = buildBlock();
    counts_drawn = false;
}

void MergingShuffledTransform::drawCounts()
{
    /// Sampling without replacement from the populations of the streams: every row of the block comes from a stream
    /// with the probability proportional to the number of its rows not taken yet.
    const size_t block_size = static_cast<size_t>(std::min<UInt64>(max_block_size, remaining_output));
    for (auto & state : inputs_state)
        state.to_take = 0;

    for (size_t row = 0; row < block_size; ++row)
    {
        UInt64 value = randomBelow(rng, remaining_population);
        for (auto & state : inputs_state)
        {
            if (value < state.remaining_population)
            {
                ++state.to_take;
                --state.remaining_population;
                break;
            }
            value -= state.remaining_population;
        }
        --remaining_population;
    }

    remaining_output -= block_size;
}

Chunk MergingShuffledTransform::buildBlock()
{
    const auto & header = outputs.front().getHeader();
    MutableColumns columns(header.columns());
    for (size_t i = 0; i < columns.size(); ++i)
        if (!isColumnConst(*header.getByPosition(i).column))
            columns[i] = removeSpecialRepresentations(header.getByPosition(i).column)->cloneEmpty();

    /// The rows of every stream are in a random order, so taking the next rows is a random choice.
    size_t num_rows = 0;
    for (auto & state : inputs_state)
    {
        size_t need = state.to_take;
        while (need)
        {
            auto & chunk = state.chunks.front();
            const size_t available = chunk.getNumRows() - state.offset;
            const size_t take = std::min(need, available);
            const auto & chunk_columns = chunk.getColumns();
            for (size_t i = 0; i < columns.size(); ++i)
                if (columns[i])
                    columns[i]->insertRangeFrom(*chunk_columns[i], state.offset, take);

            state.offset += take;
            need -= take;
            if (state.offset == chunk.getNumRows())
            {
                state.chunks.pop_front();
                state.offset = 0;
            }
        }
        state.buffered_rows -= state.to_take;
        num_rows += state.to_take;
        state.to_take = 0;
    }

    /// The rows taken from different streams are concatenated: shuffle them.
    const auto permutation = randomPermutation(rng, num_rows);
    Columns result(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (columns[i])
            result[i] = columns[i]->permute(permutation, 0);
        else
            result[i] = header.getByPosition(i).column->cloneResized(num_rows);
    }

    Chunk chunk(std::move(result), num_rows);
    chunk.getChunkInfos().add(std::make_shared<ShuffledChunkInfo>(total_population));
    return chunk;
}


ShuffleConcatTransform::ShuffleConcatTransform(SharedHeader header, size_t num_inputs)
    : IProcessor(InputPorts(num_inputs, header), OutputPorts{header})
    , queues(num_inputs)
{
}

IProcessor::Status ShuffleConcatTransform::prepare()
{
    auto & output = outputs.front();

    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    /// Read all inputs at the same time; the chunks of the inputs after the current one wait in the queues.
    size_t input_index = 0;
    for (auto & input : inputs)
    {
        auto & queue = queues[input_index++];
        if (input.isFinished())
            continue;
        input.setNeeded();
        if (input.hasData())
            queue.push_back(input.pull());
    }

    if (!output.canPush())
        return Status::PortFull;

    auto input_it = std::next(inputs.begin(), current_input);
    while (current_input < queues.size())
    {
        auto & queue = queues[current_input];
        if (!queue.empty())
        {
            output.push(std::move(queue.front()));
            queue.pop_front();
            return Status::PortFull;
        }

        if (!input_it->isFinished())
            return Status::NeedData;

        ++current_input;
        ++input_it;
    }

    output.finish();
    return Status::Finished;
}


FillShuffleKeyTransform::FillShuffleKeyTransform(SharedHeader input_header, SharedHeader output_header, ShuffleKeyDescription key_)
    : ISimpleTransform(input_header, output_header, /*skip_empty_chunks=*/ true)
    , key(key_)
    , key_generator(output_header->getByPosition(key.position).type, key.direction)
    , rng(randomSeed())
{
}

void FillShuffleKeyTransform::transform(Chunk & chunk)
{
    const size_t num_rows = chunk.getNumRows();
    const UInt64 population = getPopulation(chunk);

    auto columns = chunk.detachColumns();
    auto key_column = key_generator.generateNextOrderStatistics(rng, num_rows, population);
    if (key.replace)
        columns[key.position] = std::move(key_column);
    else
        columns.insert(columns.begin() + key.position, std::move(key_column));

    chunk.setColumns(std::move(columns), num_rows);
}


AppendShuffleKeyTransform::AppendShuffleKeyTransform(SharedHeader input_header, SharedHeader output_header)
    : ISimpleTransform(input_header, output_header, /*skip_empty_chunks=*/ false)
{
}

void AppendShuffleKeyTransform::transform(Chunk & chunk)
{
    const size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    columns.push_back(getOutputPort().getHeader().getByPosition(columns.size()).column->cloneResized(num_rows));
    chunk.setColumns(std::move(columns), num_rows);
}

}
