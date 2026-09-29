#include <Processors/Transforms/PartitionAggregateTransform.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/HashTable/Prefetching.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/memory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/AggregationCommon.h>
#include <base/getL2CacheSize.h>

#include <array>

namespace DB
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
}

PartitionAggregateTransform::PartitionAggregateTransform(
    SharedHeader input_header,
    SharedHeader output_header,
    ColumnNumbers key_positions_,
    std::vector<WindowFunctionDescription> functions_,
    SpillSettings spill_settings_)
    : IAccumulatingTransform(input_header, output_header)
    , key_positions(std::move(key_positions_))
    , functions(std::move(functions_))
    , spill_settings(std::move(spill_settings_))
{
    for (const auto & function : functions)
    {
        ColumnNumbers positions;
        for (const auto & name : function.argument_names)
            positions.push_back(input_header->getPositionByName(name));
        argument_positions.push_back(std::move(positions));

        const auto & aggregate_function = *function.aggregate_function;
        has_states_owning_memory = has_states_owning_memory || !aggregate_function.hasTrivialDestructor();
        const size_t alignment = aggregate_function.alignOfData();
        state_stride = ::Memory::alignUp(state_stride, alignment);
        state_offsets.push_back(state_stride);
        state_stride += aggregate_function.sizeOfData();
        state_alignment = std::max(state_alignment, alignment);
    }
    state_stride = ::Memory::alignUp(state_stride, state_alignment);

    size_t keys_bytes = 0;
    for (auto position : key_positions)
    {
        /// `getKeyColumns` removes `LowCardinality` from the keys packed here.
        const auto type = recursiveRemoveLowCardinality(input_header->getByPosition(position).type);
        if (!type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
            break;
        key_sizes.push_back(type->getSizeOfValueInMemory());
        keys_bytes += key_sizes.back();
    }
    if (key_sizes.size() != key_positions.size() || keys_bytes > sizeof(UInt256))
    {
        key_sizes.clear();
        single_string_key = key_positions.size() == 1
            && isString(recursiveRemoveLowCardinality(input_header->getByPosition(key_positions[0]).type));
        grouping.emplace<Grouping<SerializedKeyToGroup>>();
    }
    else if (keys_bytes > sizeof(UInt128))
        grouping.emplace<Grouping<FixedKeyToGroup<UInt256>>>();
    else if (keys_bytes > sizeof(UInt64))
        grouping.emplace<Grouping<FixedKeyToGroup<UInt128>>>();

    for (const auto & column : input_header->getColumnsWithTypeAndName())
        is_const_column.push_back(column.column && isColumnConst(*column.column));
    /// The group of each row, appended to the buffered chunks.
    is_const_column.push_back(false);
}

PartitionAggregateTransform::~PartitionAggregateTransform()
{
    for (auto * place : places)
        destroyStates(place);
}

void PartitionAggregateTransform::consume(Chunk chunk)
{
    const size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    num_input_rows += num_rows;
    chunks.push_back(std::move(chunk));
    chunks_bytes += chunks.back().allocatedBytes();

    if (defer_grouping)
    {
        std::visit([&](auto & state) { deferChunk(state, chunks.back()); }, grouping);
    }
    else
    {
        groupChunks(/*last=*/ false);
        defer_grouping = std::visit([](const auto & state) { return state.map.size(); }, grouping) > max_groups_to_group_eagerly;
    }

    /// The keys and the aggregate states cannot be spilled, but they count towards the threshold, so that
    /// the buffered rows are spilled earlier when the partitions take most of the memory.
    const size_t footprint = chunks_bytes + arena.allocatedBytes() + places.allocated_bytes() + row_places.allocated_bytes()
        + deferred_buckets.allocated_bytes() + std::visit([](const auto & state) { return state.map.getBufferSizeInBytes(); }, grouping);
    if (spill_settings.max_bytes_before_external && footprint > spill_settings.max_bytes_before_external
        && (!spill_settings.max_query_bytes_before_external
            || getCurrentQueryMemoryUsage() > static_cast<Int64>(spill_settings.max_query_bytes_before_external)))
    {
        groupChunks(/*last=*/ false);
        spill();
    }
}

ColumnRawPtrs PartitionAggregateTransform::getKeyColumns(const Columns & columns, Columns & holders) const
{
    ColumnRawPtrs key_columns;
    for (auto position : key_positions)
    {
        /// Only the packed keys need plain columns: the serialized keys read `LowCardinality` from its dictionary, so
        /// the keys are not materialized for all the buffered rows in `groupDeferred`.
        auto column = columns[position]->convertToFullIfWrapped();
        holders.push_back(key_sizes.empty() ? std::move(column) : recursiveRemoveLowCardinality(column));
        key_columns.push_back(holders.back().get());
    }
    return key_columns;
}

void PartitionAggregateTransform::groupChunks(bool last)
{
    if (num_grouped_chunks == chunks.size())
        return;

    if (defer_grouping)
    {
        std::visit([&](auto & state) { groupDeferred(state, last); }, grouping);
        return;
    }

    for (; num_grouped_chunks < chunks.size(); ++num_grouped_chunks)
    {
        auto & chunk = chunks[num_grouped_chunks];
        const size_t num_rows = chunk.getNumRows();
        Columns holders;
        const auto key_columns = getKeyColumns(chunk.getColumns(), holders);
        auto groups = ColumnUInt32::create(num_rows);
        std::visit([&](auto & state) { addGroups(state.map, num_rows, key_columns, groups->getData().data()); }, grouping);
        aggregateChunk(chunk, std::move(groups));
    }
}

void PartitionAggregateTransform::aggregateChunk(Chunk & chunk, MutableColumnPtr groups)
{
    auto columns = chunk.detachColumns();
    const auto & all_group_data = assert_cast<const ColumnUInt32 &>(*groups).getData();

    /// The rows of partitions of one row are aggregated when they are output.
    IColumn::Filter in_groups;
    ColumnPtr filtered_groups;
    if (total_single_row_groups && std::find(all_group_data.begin(), all_group_data.end(), single_row_group) != all_group_data.end())
    {
        in_groups.resize(all_group_data.size());
        for (size_t row = 0; row < all_group_data.size(); ++row)
            in_groups[row] = all_group_data[row] != single_row_group;
        filtered_groups = groups->filter(in_groups, -1);
    }
    const auto & group_data = assert_cast<const ColumnUInt32 &>(filtered_groups ? *filtered_groups : *groups).getData();
    const size_t num_rows = group_data.size();

    /// With many rows in each of not many partitions, the states that own memory can be large, and slow to take
    /// the rows one by one, in turn with other states: then the rows are put in the order of their partitions, and
    /// each state takes its rows at once, like in `WindowTransform`.
    const bool by_partitions = has_states_owning_memory && places.size() <= max_groups_to_aggregate_by_partitions
        && num_input_rows >= min_rows_per_group_to_aggregate_by_partitions * places.size();
    IColumn::Permutation permutation;
    /// The rows of the partition `group` are from `partition_offsets[group]` to `partition_offsets[group + 1]`.
    PaddedPODArray<size_t> partition_offsets;
    if (by_partitions)
    {
        partition_offsets.resize_fill(places.size() + 1, 0);
        for (size_t row = 0; row < num_rows; ++row)
            ++partition_offsets[group_data[row] + 1];
        for (size_t group = 0; group < places.size(); ++group)
            partition_offsets[group + 1] += partition_offsets[group];
        permutation.resize(num_rows);
        PaddedPODArray<size_t> positions(partition_offsets.begin(), partition_offsets.end());
        for (size_t row = 0; row < num_rows; ++row)
            permutation[positions[group_data[row]]++] = row;
    }
    else
    {
        row_places.resize(num_rows);
        for (size_t row = 0; row < num_rows; ++row)
            row_places[row] = places[group_data[row]];
    }

    /// Until a partition has more than two rows, the number of rows of each is counted, up to three.
    if (!has_group_of_more_than_two_rows)
    {
        group_num_rows.resize_fill(places.size(), 0);
        for (size_t row = 0; row < num_rows; ++row)
        {
            auto & group_rows = group_num_rows[group_data[row]];
            group_rows += group_rows < 3;
            has_group_of_more_than_two_rows |= group_rows == 3;
        }
        if (has_group_of_more_than_two_rows)
            PaddedPODArray<UInt8>().swap(group_num_rows);
    }

    Columns argument_holders;
    auto arguments = getArguments(columns, in_groups, num_rows, permutation, argument_holders);
    for (size_t i = 0; i < functions.size(); ++i)
    {
        auto * argument_columns = arguments[i].data();
        const auto & aggregate_function = *functions[i].aggregate_function;
        if (by_partitions)
        {
            for (size_t group = 0; group < places.size(); ++group)
                if (partition_offsets[group] != partition_offsets[group + 1])
                    aggregate_function.addBatchSinglePlace(
                        partition_offsets[group], partition_offsets[group + 1], places[group] + state_offsets[i], argument_columns, &arena);
        }
        else
        {
            aggregate_function.addBatch(0, num_rows, row_places.data(), state_offsets[i], argument_columns, &arena);
        }
    }

    chunks_bytes += groups->allocatedBytes();
    const size_t num_chunk_rows = groups->size();
    columns.push_back(std::move(groups));
    chunk.setColumns(std::move(columns), num_chunk_rows);
}

std::vector<ColumnRawPtrs> PartitionAggregateTransform::getArguments(
    const Columns & columns, const IColumn::Filter & filter, size_t filtered_size, const IColumn::Permutation & permutation,
    Columns & holders) const
{
    holders.assign(columns.size(), nullptr);
    std::vector<ColumnRawPtrs> arguments(functions.size());
    for (size_t i = 0; i < functions.size(); ++i)
    {
        for (auto position : argument_positions[i])
        {
            auto & argument = holders[position];
            if (!argument)
            {
                argument = recursiveRemoveLowCardinality(columns[position]->convertToFullIfWrapped());
                if (!filter.empty())
                    argument = argument->filter(filter, filtered_size);
                if (!permutation.empty())
                    argument = argument->permute(permutation, 0);
            }
            arguments[i].push_back(argument.get());
        }
    }
    return arguments;
}

size_t PartitionAggregateTransform::getBucket(size_t hash)
{
    /// Not the bits of the hash that select the cell in the table, as the table of a bucket takes its keys.
    return intHash64(hash) >> (64 - num_buckets_bits);
}

template <typename Map>
void PartitionAggregateTransform::deferChunk(Grouping<Map> & state, const Chunk & chunk)
{
    const size_t num_rows = chunk.getNumRows();
    Columns holders;
    const auto key_columns = getKeyColumns(chunk.getColumns(), holders);

    auto & buckets = deferred_buckets;
    const size_t offset = buckets.size();
    buckets.resize(offset + num_rows);
    if constexpr (is_serialized<Map>)
    {
        for (size_t row = 0; row < num_rows; ++row)
        {
            auto key = getSerializedKey(row, key_columns, arena);
            buckets[offset + row] = static_cast<UInt8>(getBucket(state.map.hash(key)));
            if (!single_string_key)
                arena.rollback(key.size());
        }
    }
    else
    {
        PaddedPODArray<typename Map::key_type> keys(num_rows);
        packKeys(key_columns, num_rows, keys.data());
        for (size_t row = 0; row < num_rows; ++row)
            buckets[offset + row] = static_cast<UInt8>(getBucket(state.map.hash(keys[row])));
    }
}

template <typename Map>
void PartitionAggregateTransform::groupDeferred(Grouping<Map> & state, bool last)
{
    using Key = typename Map::key_type;
    static constexpr size_t num_buckets = 1 << num_buckets_bits;
    const auto & buckets = deferred_buckets;

    /// The deferred rows are partitioned by the buckets: `offsets[bucket]` is where the rows of a bucket begin.
    std::array<size_t, num_buckets + 1> offsets{};
    for (auto bucket : buckets)
        ++offsets[bucket + 1];
    for (size_t bucket = 0; bucket < num_buckets; ++bucket)
        offsets[bucket + 1] += offsets[bucket];

    /// The serialized keys of the deferred rows. They point into the buffered chunks, kept in `key_holders`, if
    /// `single_string_key`.
    auto keys_arena = std::make_unique<Arena>();
    std::vector<Columns> key_holders;
    PaddedPODArray<Key> keys(buckets.size());
    /// The hashes of the serialized keys, taken while their bytes are in the cache: the keys of a bucket are
    /// scattered in memory.
    PaddedPODArray<size_t> key_hashes(is_serialized<Map> ? buckets.size() : 0);
    {
        auto positions = offsets;
        size_t deferred_row = 0;
        PaddedPODArray<Key> chunk_keys;
        for (size_t i = num_grouped_chunks; i < chunks.size(); ++i)
        {
            if (last && isCancelled())
                return;
            const size_t num_rows = chunks[i].getNumRows();
            Columns chunk_key_holders;
            const auto key_columns = getKeyColumns(chunks[i].getColumns(), single_string_key ? key_holders.emplace_back() : chunk_key_holders);
            if constexpr (is_serialized<Map>)
            {
                for (size_t row = 0; row < num_rows; ++row, ++deferred_row)
                {
                    const size_t position = positions[buckets[deferred_row]]++;
                    keys[position] = getSerializedKey(row, key_columns, *keys_arena);
                    key_hashes[position] = state.map.hash(keys[position]);
                }
            }
            else
            {
                chunk_keys.resize(num_rows);
                packKeys(key_columns, num_rows, chunk_keys.data());
                for (size_t row = 0; row < num_rows; ++row, ++deferred_row)
                    keys[positions[buckets[deferred_row]]++] = chunk_keys[row];
            }
        }
    }
    auto get_hash = [&](size_t i)
    {
        if constexpr (is_serialized<Map>)
            return key_hashes[i];
        else
            return state.map.hash(keys[i]);
    };

    /// The partitions grouped before, by the buckets, to put them into the table of their bucket.
    std::array<size_t, num_buckets + 1> known_offsets{};
    PaddedPODArray<Key> known_keys;
    PaddedPODArray<UInt32> known_groups;
    if (last)
    {
        for (const auto & cell : state.map)
            ++known_offsets[getBucket(state.map.hash(cell.getKey())) + 1];
        for (size_t bucket = 0; bucket < num_buckets; ++bucket)
            known_offsets[bucket + 1] += known_offsets[bucket];
        known_keys.resize(state.map.size());
        known_groups.resize(state.map.size());
        auto positions = known_offsets;
        for (const auto & cell : state.map)
        {
            const size_t position = positions[getBucket(state.map.hash(cell.getKey()))]++;
            known_keys[position] = cell.getKey();
            known_groups[position] = cell.getMapped();
        }
    }

    /// The groups of the deferred rows, in the order of the buckets.
    PaddedPODArray<UInt32> groups(keys.size());
    /// Whether the row is the first of a new partition. The rows of a bucket are in their order, so it is also
    /// the first of the partition in the order of the rows.
    PaddedPODArray<UInt8> is_first(last ? keys.size() : 0);

    /// The groups of the last deferred rows are created in the order of the rows, and not of the buckets, so that
    /// the states, which are read and written for each row, are in the order of the rows too when most partitions
    /// have few rows. Until then, a new partition takes a number from `first_new_group` on.
    const size_t first_new_group = places.size();
    size_t num_new_groups = 0;
    /// Whether a new partition has one row. Not a number of rows, which could wrap around to one.
    PaddedPODArray<UInt8> new_group_has_one_row;

    for (size_t bucket = 0; bucket < num_buckets; ++bucket)
    {
        if (last && isCancelled())
            return;
        if (!last)
        {
            /// The partitions of the deferred rows must be remembered, so they are put into the table of all of them.
            for (size_t i = offsets[bucket]; i < offsets[bucket + 1]; ++i)
            {
                const size_t hash = get_hash(i);
                if constexpr (is_serialized<Map>)
                    groups[i] = emplaceGroup(state.map, ArenaKeyHolder{keys[i], arena}, hash);
                else
                    groups[i] = emplaceGroup(state.map, keys[i], hash);
            }
            continue;
        }

        auto & bucket_map = state.bucket_map;
        for (size_t i = known_offsets[bucket]; i < known_offsets[bucket + 1]; ++i)
        {
            typename Map::LookupResult it = nullptr;
            bool inserted = false;
            bucket_map.emplace(known_keys[i], it, inserted);
            it->getMapped() = known_groups[i];
        }

        for (size_t i = offsets[bucket]; i < offsets[bucket + 1]; ++i)
        {
            typename Map::LookupResult it = nullptr;
            bool inserted = false;
            bucket_map.emplace(keys[i], it, inserted, get_hash(i));
            if (inserted)
            {
                checkNumberOfGroups(first_new_group + num_new_groups + 1);
                it->getMapped() = static_cast<UInt32>(first_new_group + num_new_groups++);
                new_group_has_one_row.push_back(true);
            }
            else if (it->getMapped() >= first_new_group)
            {
                new_group_has_one_row[it->getMapped() - first_new_group] = false;
            }
            groups[i] = it->getMapped();
            is_first[i] = inserted;
        }
        bucket_map.clear();
    }

    /// Freed before the groups are created, which can take their memory.
    PaddedPODArray<Key>().swap(keys);
    PaddedPODArray<size_t>().swap(key_hashes);
    PaddedPODArray<Key>().swap(known_keys);
    PaddedPODArray<UInt32>().swap(known_groups);
    key_holders.clear();
    keys_arena.reset();

    /// A new partition of one row does not take a group: its row is aggregated when it is output, in a state that
    /// is reused, which saves the memory of a group for each row when most partitions have one row.
    size_t num_single_row_groups = 0;
    for (auto has_one_row : new_group_has_one_row)
        num_single_row_groups += has_one_row;
    total_single_row_groups += num_single_row_groups;

    /// The groups are created at once, and the partitions take them in the order of the rows.
    PaddedPODArray<UInt32> created_groups(num_new_groups);
    size_t next_created_group = last ? createGroups(num_new_groups - num_single_row_groups) : 0;

    /// Back to the order of the rows: the rows of a bucket are in their order.
    auto positions = offsets;
    size_t deferred_row = 0;
    for (; num_grouped_chunks < chunks.size(); ++num_grouped_chunks)
    {
        if (last && isCancelled())
            return;
        auto & chunk = chunks[num_grouped_chunks];
        const size_t num_rows = chunk.getNumRows();
        auto chunk_groups = ColumnUInt32::create(num_rows);
        auto & chunk_group_data = chunk_groups->getData();
        for (size_t row = 0; row < num_rows; ++row, ++deferred_row)
        {
            const size_t position = positions[buckets[deferred_row]]++;
            UInt32 group = groups[position];
            if (last && group >= first_new_group)
            {
                if (new_group_has_one_row[group - first_new_group])
                {
                    group = single_row_group;
                }
                else
                {
                    /// With few rows in each partition, most rows are the first of theirs, and only write the group.
                    auto & created = created_groups[group - first_new_group];
                    if (is_first[position])
                        created = static_cast<UInt32>(next_created_group++);
                    group = created;
                }
            }
            chunk_group_data[row] = group;
        }
        aggregateChunk(chunk, std::move(chunk_groups));
    }
    PaddedPODArray<UInt8>().swap(deferred_buckets);
}

template <typename Map, typename KeyHolder>
UInt32 PartitionAggregateTransform::emplaceGroup(Map & map, KeyHolder && key_holder, size_t hash)
{
    typename Map::LookupResult it = nullptr;
    bool inserted = false;
    map.emplace(key_holder, it, inserted, hash);
    if (inserted)
    {
        try
        {
            it->getMapped() = createGroup();
        }
        catch (...)
        {
            /// A copy, as erasing moves the cells.
            const auto key = it->getKey();
            map.erase(key);
            throw;
        }
    }
    return it->getMapped();
}

void PartitionAggregateTransform::checkNumberOfGroups(size_t num_groups)
{
    if (num_groups > max_groups)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED,
            "Too many partitions for a window computed with hash partitioning, the maximum is {}. "
            "Disable `query_plan_window_functions_hash_partitioning`", max_groups);
}

void PartitionAggregateTransform::createStates(AggregateDataPtr place)
{
    size_t created = 0;
    try
    {
        for (; created < functions.size(); ++created)
            functions[created].aggregate_function->create(place + state_offsets[created]);
    }
    catch (...)
    {
        for (size_t i = 0; i < created; ++i)
            functions[i].aggregate_function->destroy(place + state_offsets[i]);
        throw;
    }
}

void PartitionAggregateTransform::destroyStates(AggregateDataPtr place) const noexcept
{
    for (size_t i = 0; i < functions.size(); ++i)
        if (!functions[i].aggregate_function->hasTrivialDestructor())
            functions[i].aggregate_function->destroy(place + state_offsets[i]);
}

UInt32 PartitionAggregateTransform::createGroup()
{
    return static_cast<UInt32>(createGroups(1));
}

size_t PartitionAggregateTransform::createGroups(size_t num_groups)
{
    const size_t first_group = places.size();
    checkNumberOfGroups(first_group + num_groups);
    if (num_groups == 0)
        return first_group;

    /// Reserved before the states are created, so that they are always destroyed.
    places.reserve(first_group + num_groups);
    auto * states = arena.alignedAlloc(num_groups * state_stride, state_alignment);
    for (size_t group = 0; group < num_groups; ++group)
    {
        auto * place = states + group * state_stride;
        createStates(place);
        places.push_back(place);
    }
    return first_group;
}

template <typename Key>
void PartitionAggregateTransform::packKeys(const ColumnRawPtrs & key_columns, size_t num_rows, Key * keys) const
{
    /// Column by column, like `packFixedBatch`, which takes only keys of 1, 2, 4, 8 and 16 bytes: a key assembled
    /// from narrower stores and read at once right after, like in `packFixed`, stalls the store forwarding.
    std::fill(keys, keys + num_rows, Key{});
    size_t offset = 0;
    for (size_t i = 0; i < key_columns.size(); ++i)
    {
        const auto * data = static_cast<const ColumnFixedSizeHelper *>(key_columns[i])->getRawDataBegin<1>();
        auto copy = [&]<size_t size>()
        {
            for (size_t row = 0; row < num_rows; ++row)
                memcpy(reinterpret_cast<char *>(&keys[row]) + offset, data + row * size, size);
        };
        switch (key_sizes[i])
        {
            case 1: copy.template operator()<1>(); break;
            case 2: copy.template operator()<2>(); break;
            case 4: copy.template operator()<4>(); break;
            case 8: copy.template operator()<8>(); break;
            case 16: copy.template operator()<16>(); break;
            default:
                for (size_t row = 0; row < num_rows; ++row)
                    memcpy(reinterpret_cast<char *>(&keys[row]) + offset, data + row * key_sizes[i], key_sizes[i]);
        }
        offset += key_sizes[i];
    }
}

std::string_view PartitionAggregateTransform::getSerializedKey(size_t row, const ColumnRawPtrs & key_columns, Arena & pool) const
{
    if (single_string_key)
        return key_columns[0]->getDataAt(row);
    return serializeKeysToPoolContiguous(row, key_columns.size(), key_columns, pool, nullptr);
}

template <typename Map>
void PartitionAggregateTransform::addGroups(Map & map, size_t num_rows, const ColumnRawPtrs & key_columns, UInt32 * group_data)
{
    if constexpr (is_serialized<Map>)
    {
        for (size_t row = 0; row < num_rows; ++row)
        {
            auto key = getSerializedKey(row, key_columns, arena);
            if (single_string_key)
            {
                group_data[row] = emplaceGroup(map, ArenaKeyHolder{key, arena}, map.hash(key));
                continue;
            }
            const size_t num_groups = map.size();
            group_data[row] = emplaceGroup(map, key, map.hash(key));
            if (map.size() == num_groups)
                arena.rollback(key.size());
        }
    }
    else
    {
        using Key = typename Map::key_type;
        PaddedPODArray<Key> keys(num_rows);
        PaddedPODArray<size_t> hashes(num_rows);
        packKeys(key_columns, num_rows, keys.data());
        for (size_t row = 0; row < num_rows; ++row)
            hashes[row] = map.hash(keys[row]);

        /// Like `Aggregator`: the cell of a later row is prefetched once the table does not fit in the cache.
        const bool prefetch = map.getBufferSizeInBytes() > getL2CacheSize();
        PrefetchingHelper prefetching;
        size_t look_ahead = PrefetchingHelper::getInitialLookAheadValue();
        for (size_t row = 0; row < num_rows; ++row)
        {
            if (prefetch)
            {
                if (row == PrefetchingHelper::iterationsToMeasure())
                    look_ahead = prefetching.calcPrefetchLookAhead();
                if (row + look_ahead < num_rows)
                    map.prefetchByHash(hashes[row + look_ahead]);
            }
            group_data[row] = emplaceGroup(map, keys[row], hashes[row]);
        }
    }
}

void PartitionAggregateTransform::spill()
{
    if (!spilled_header)
    {
        Block header;
        const auto & input_header = getInputPort().getHeader();
        for (size_t i = 0; i < input_header.columns(); ++i)
            if (!is_const_column[i])
                header.insert(input_header.getByPosition(i).cloneEmpty());
        header.insert({ColumnUInt32::create(), std::make_shared<DataTypeUInt32>(), "__partition_aggregate_group"});
        spilled_header = std::make_shared<const Block>(std::move(header));
    }

    /// Throws if there is less free disk space than this.
    auto & stream = spilled.emplace_back(spilled_header, spill_settings.tmp_data, chunks_bytes + spill_settings.min_free_disk_space);
    for (auto & chunk : chunks)
    {
        auto columns = chunk.detachColumns();
        Columns written;
        for (size_t i = 0; i < columns.size(); ++i)
            if (!is_const_column[i])
                written.push_back(columns[i]->convertToFullIfWrapped());
        stream->write(spilled_header->cloneWithColumns(written));
    }
    stream.finishWriting();

    chunks.clear();
    chunks_bytes = 0;
    num_grouped_chunks = 0;
}

void PartitionAggregateTransform::insertResults(size_t function, AggregateDataPtr * result_places, size_t num_places, IColumn & to)
{
    const auto & aggregate_function = *functions[function].aggregate_function;
    const size_t offset = state_offsets[function];
    to.reserve(num_places);
    /// Like `WindowTransform`: the result of a `-State` function is a state to merge into.
    if (aggregate_function.isState())
    {
        for (size_t i = 0; i < num_places; ++i)
            aggregate_function.insertMergeResultInto(result_places[i] + offset, to, &arena);
    }
    /// It destroys the states, which is nothing for these.
    else if (aggregate_function.hasTrivialDestructor())
    {
        aggregate_function.insertResultIntoBatch(0, num_places, result_places, offset, to, &arena);
    }
    else
    {
        for (size_t i = 0; i < num_places; ++i)
            aggregate_function.insertResultInto(result_places[i] + offset, to, &arena);
    }
}

PartitionAggregateTransform::SingleRowStates::~SingleRowStates()
{
    for (size_t row = 0; row < num_created; ++row)
        transform.destroyStates(place(row));
}

void PartitionAggregateTransform::SingleRowStates::aggregate(const Columns & columns, const PaddedPODArray<UInt32> & group_data, size_t num_rows)
{
    auto & buffer = transform.single_row_states_buffer;
    buffer.resize(num_rows * transform.state_stride + transform.state_alignment);
    states = reinterpret_cast<char *>(::Memory::alignUp(reinterpret_cast<uintptr_t>(buffer.data()), transform.state_alignment));
    for (; num_created < num_rows; ++num_created)
        transform.createStates(place(num_created));

    IColumn::Filter is_single(group_data.size());
    for (size_t row = 0; row < group_data.size(); ++row)
        is_single[row] = group_data[row] == single_row_group;

    auto & places_of_rows = transform.row_places;
    places_of_rows.resize(num_rows);
    for (size_t row = 0; row < num_rows; ++row)
        places_of_rows[row] = place(row);

    Columns argument_holders;
    auto arguments = transform.getArguments(columns, is_single, num_rows, {}, argument_holders);
    for (size_t i = 0; i < transform.functions.size(); ++i)
        transform.functions[i].aggregate_function->addBatch(
            0, num_rows, places_of_rows.data(), transform.state_offsets[i], arguments[i].data(), &arena);
}

Chunk PartitionAggregateTransform::generate()
{
    if (!results_ready)
    {
        groupChunks(/*last=*/ true);
        /// `groupChunks` stops early if the query is cancelled, and then there is nothing to output.
        if (isCancelled())
            return {};

        /// With two rows or less in each group, the results are taken for each row from the states, instead of
        /// being taken for each group and then copied to the rows, which takes more memory. Not with more rows in a
        /// group, whose result would be taken for each of its rows. The rows of partitions of one row have no group.
        results_for_each_row = !has_group_of_more_than_two_rows;
        if (!results_for_each_row)
        {
            for (size_t i = 0; i < functions.size(); ++i)
            {
                if (isCancelled())
                    return {};
                auto column = functions[i].aggregate_function->getResultType()->createColumn();
                insertResults(i, places.data(), places.size(), *column);
                results.push_back(std::move(column));
            }
        }
        results_ready = true;
    }

    Columns columns;
    size_t num_rows = 0;
    if (next_chunk < chunks.size())
    {
        num_rows = chunks[next_chunk].getNumRows();
        columns = chunks[next_chunk].detachColumns();
        ++next_chunk;
    }
    else
    {
        Block block;
        while (block.empty())
        {
            if (!spilled_reader)
            {
                if (next_spilled == spilled.size())
                    return {};
                spilled_reader.emplace(spilled[next_spilled++].getReadStream());
            }
            block = (*spilled_reader)->read();
            if (block.empty())
                spilled_reader.reset();
        }

        num_rows = block.rows();
        auto read_columns = block.getColumns();
        const auto & input_header = getInputPort().getHeader();
        size_t read_position = 0;
        for (size_t i = 0; i < is_const_column.size(); ++i)
        {
            if (is_const_column[i])
                columns.push_back(input_header.getByPosition(i).column->cloneResized(num_rows));
            else
                columns.push_back(std::move(read_columns[read_position++]));
        }
    }

    ColumnPtr groups = std::move(columns.back());
    columns.pop_back();
    const auto & group_data = assert_cast<const ColumnUInt32 &>(*groups).getData();

    const size_t num_single_rows = total_single_row_groups ? std::count(group_data.begin(), group_data.end(), single_row_group) : 0;
    SingleRowStates single_row_states(*this);
    if (num_single_rows)
        single_row_states.aggregate(columns, group_data, num_single_rows);

    if (results_for_each_row)
    {
        row_places.resize(num_rows);
        size_t single_row = 0;
        for (size_t row = 0; row < num_rows; ++row)
            row_places[row] = group_data[row] == single_row_group ? single_row_states.place(single_row++) : places[group_data[row]];
        for (size_t i = 0; i < functions.size(); ++i)
        {
            auto column = functions[i].aggregate_function->getResultType()->createColumn();
            insertResults(i, row_places.data(), num_rows, *column);
            columns.push_back(std::move(column));
        }
        return Chunk(std::move(columns), num_rows);
    }

    /// The rows of partitions of one row take their results from their states, and the other rows from the results
    /// of their groups. The results of `-State` functions are then copied for each row, as in `WindowTransform`.
    if (num_single_rows)
    {
        row_places.resize(num_single_rows);
        for (size_t row = 0; row < num_single_rows; ++row)
            row_places[row] = single_row_states.place(row);
    }
    for (size_t i = 0; i < functions.size(); ++i)
    {
        const auto & result = results[i];
        const auto * result_array = typeid_cast<const ColumnArray *>(result.get());
        if (!num_single_rows && !result_array)
        {
            columns.push_back(result->index(*groups, 0));
            continue;
        }

        MutableColumnPtr single_row_results;
        if (num_single_rows)
        {
            single_row_results = result->cloneEmpty();
            insertResults(i, row_places.data(), num_single_rows, *single_row_results);
        }

        auto column = result->cloneEmpty();
        column->reserve(num_rows);
        /// `index` of an array takes each element on its own, this copies each array at once.
        if (result_array)
        {
            const auto & offsets = result_array->getOffsets();
            size_t num_elements = single_row_results ? assert_cast<const ColumnArray &>(*single_row_results).getData().size() : 0;
            for (size_t row = 0; row < num_rows; ++row)
                if (group_data[row] != single_row_group)
                    num_elements += offsets[group_data[row]] - offsets[static_cast<ssize_t>(group_data[row]) - 1];
            assert_cast<ColumnArray &>(*column).getData().reserve(num_elements);
        }
        size_t single_row = 0;
        for (size_t row = 0; row < num_rows; ++row)
        {
            if (group_data[row] == single_row_group)
                column->insertFrom(*single_row_results, single_row++);
            else
                column->insertFrom(*result, group_data[row]);
        }
        columns.push_back(std::move(column));
    }
    return Chunk(std::move(columns), num_rows);
}

}
