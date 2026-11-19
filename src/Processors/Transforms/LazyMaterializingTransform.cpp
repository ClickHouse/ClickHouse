#include <iostream>
#include <Processors/Transforms/LazyMaterializingTransform.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>
#include <Interpreters/Squashing.h>
#include <Interpreters/sortBlock.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/SortDescription.h>
#include <Columns/ColumnsNumber.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Block LazyMaterializingTransform::transformHeader(const Block & main_header, const Block & lazy_header)
{
    auto pos = main_header.getPositionByName("__global_row_index");
    ColumnsWithTypeAndName columns = main_header.getColumnsWithTypeAndName();
    columns.erase(columns.begin() + pos);
    const auto & lazy_columns = lazy_header.getColumnsWithTypeAndName();
    columns.insert(columns.end(), lazy_columns.begin(), lazy_columns.end());
    return Block(std::move(columns));
}

LazyMaterializingTransform::LazyMaterializingTransform(SharedHeader main_header, SharedHeader lazy_header, LazyMaterializingRowsPtr lazy_materializing_rows_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_)
    : IProcessor(
        InputPorts({main_header, lazy_header}),
        OutputPorts({OutputPort(std::make_shared<Block>(transformHeader(*main_header, *lazy_header)))}))
    , lazy_materializing_rows(std::move(lazy_materializing_rows_))
    , updater(std::move(updater_))
{
}

LazyMaterializingTransform::Status LazyMaterializingTransform::prepare()
{
    auto & output = outputs.front();
    auto & main_input = inputs.front();
    auto & lazy_input = inputs.back();

    if (output.isFinished())
    {
        main_input.close();
        lazy_input.close();
        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    if (!main_input.isFinished())
    {
        main_input.setNeeded();
        if (!main_input.hasData())
            return Status::NeedData;

        auto chunk = main_input.pull();
        if (chunk.hasRows())
            chunks.emplace_back(std::move(chunk));

        if (!main_input.isFinished())
            return Status::NeedData;
    }

    if (!result_chunk)
    {
        if (chunks.empty())
        {
            lazy_input.close();
            output.finish();
            return Status::Finished;
        }

        return Status::Ready;
    }

    if (!lazy_input.isFinished())
    {
        lazy_input.setNeeded();
        if (!lazy_input.hasData())
            return Status::NeedData;

        auto chunk = lazy_input.pull();
        if (chunk.hasRows())
            chunks.emplace_back(std::move(chunk));

        if (!lazy_input.isFinished())
            return Status::NeedData;
    }

    if (!chunks.empty())
        return Status::Ready;

    output.push(std::move(*result_chunk));
    output.finish();
    return Status::Finished;
}

void LazyMaterializingTransform::work()
{
    if (!result_chunk)
        prepareMainChunk();
    else
        prepareLazyChunk();
}

LazyMaterializingRows::LazyMaterializingRows(RangesInDataParts ranges_in_data_parts_)
    : ranges_in_data_parts(std::move(ranges_in_data_parts_))
{
}

void LazyMaterializingRows::filterRangesAndFillRows(const PaddedPODArray<UInt64> & sorted_indexes)
{
    rows_in_parts.clear();

    /// Here we have a list of part/ranges and the list of sorted indexes.
    /// We need to remove unused granules from ranges, possibly splitting them to smaller ones.
    /// Ranges are sorted as well, so we need traverse them properly.
    size_t next_index = 0;
    for (auto & part : ranges_in_data_parts)
    {
        PaddedPODArray<UInt64> rows_in_part;
        size_t offset = part.part_starting_offset_in_query;
        MarkRanges ranges;

        while (!part.ranges.empty())
        {
            if (!(next_index < sorted_indexes.size()))
                break;

            size_t row_end = part.data_part->index_granularity->getMarkStartingRow(part.ranges.front().end);
            if (sorted_indexes[next_index] >= offset + row_end)
            {
                part.ranges.pop_front();
                continue;
            }

            size_t row_begin = part.data_part->index_granularity->getMarkStartingRow(part.ranges.front().begin);
            if (sorted_indexes[next_index] < offset + row_begin)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "LazyMaterializingTransform: Row {} is not in any range", sorted_indexes[next_index]);

            auto & range = part.ranges.front();

            auto subrange_begin = range.begin;
            [[maybe_unused]] auto subrange_row_begin = row_begin;

            while (range.begin < range.end)
            {
                ++range.begin;
                row_begin = part.data_part->index_granularity->getMarkStartingRow(range.begin);
                if (sorted_indexes[next_index] < offset + row_begin)
                    break;
                subrange_begin = range.begin;
                subrange_row_begin = row_begin;
            }

            auto subrange_end = range.begin;
            [[maybe_unused]] auto subrange_row_end = row_begin;

            rows_in_part.push_back(sorted_indexes[next_index] - offset);
            ++next_index;

            while (next_index < sorted_indexes.size())
            {
                while (next_index < sorted_indexes.size() && sorted_indexes[next_index] < offset + row_begin)
                {
                    rows_in_part.push_back(sorted_indexes[next_index] - offset);
                    ++next_index;
                }

                if (!(next_index < sorted_indexes.size()))
                    break;

                if (!(range.begin < range.end))
                    break;

                ++range.begin;
                row_begin = part.data_part->index_granularity->getMarkStartingRow(range.begin);

                if (!(sorted_indexes[next_index] < offset + row_begin))
                    break;

                subrange_end = range.begin;
                subrange_row_end = row_begin;
            }

            ranges.push_back({subrange_begin, subrange_end});

            // std::cerr << subrange_begin << " " << subrange_end << " || " << subrange_row_begin << " " << subrange_row_end <<  " || " << rows_in_part.size() << "\n";

            if (!(range.begin < range.end))
                part.ranges.pop_front();
        }

        // std::cerr << "LMT part index " << part.part_index_in_query << " ranges " << ranges.size() << " rows " << rows_in_part.size() << "\n";
        // std::cerr << ranges.describe() << "\n";

        part.ranges = std::move(ranges);
        if (!rows_in_part.empty())
            rows_in_parts[part.part_index_in_query] = std::move(rows_in_part);
    }

    std::erase_if(ranges_in_data_parts, [](const auto & part) { return part.ranges.empty(); });

    size_t total_ranges = 0;
    size_t total_marks = 0;

    for (auto & part : ranges_in_data_parts)
    {
        total_ranges += part.ranges.size();
        total_marks += part.getMarksCount();
    }

    LOG_TRACE(getLogger("LazyMaterializingTransform"), "Lazily reading {} rows from {} parts, {} ranges, {} marks",
        sorted_indexes.size(), ranges_in_data_parts.size(), total_ranges, total_marks);
}

void LazyMaterializingTransform::prepareMainChunk()
{
    UInt64 squash_ms;
    UInt64 sort_ms;
    UInt64 permute_ms;
    UInt64 prepare_offsets_ms;
    UInt64 filter_intervals_ms;

    Stopwatch main_chunk_watch;

    {
        Stopwatch squashing_watch;

        /// call private method squash without handling ChunkInfos
        result_chunk = Squashing::squash(std::move(chunks));
        chunks.clear();
        squash_ms = squashing_watch.elapsedMilliseconds();
    }

    auto rows = result_chunk->getNumRows();
    auto pos = getInputs().front().getHeader().getPositionByName("__global_row_index");
    auto columns = result_chunk->detachColumns();
    auto index_col = columns[pos];
    columns.erase(columns.begin() + pos);
    result_chunk = Chunk(std::move(columns), rows);

    /// Here we create a block with one column with empty name, and sort description with empty name.
    /// It just works.
    Block block({{index_col, std::make_shared<DataTypeUInt64>(), {}}});
    SortDescription descr;
    descr.emplace_back(std::string{});

    {
        Stopwatch permutation_watch;
        stableGetPermutation(block, descr, permutation);
        sort_ms = permutation_watch.elapsedMilliseconds();

        auto sorted_indexes_ptr = IColumn::mutate(index_col->permute(permutation, rows));
        sorted_indexes = std::move(assert_cast<ColumnUInt64 &>(*sorted_indexes_ptr).getData());
        permute_ms = permutation_watch.elapsedMilliseconds() - sort_ms;
    }

    {
        Stopwatch prepare_offsets_watch;

        offsets.clear();
        offsets.reserve(rows);
        for (size_t offset = 0; offset < rows;)
        {
            size_t idx = sorted_indexes[offset];
            size_t count = 1;
            while (offset + count < rows && sorted_indexes[offset + count] == idx)
                ++count;

            offset += count;
            offsets.push_back(offset);
        }

        /// Deduplicate indexes.
        if (offsets.size() != rows)
        {
            size_t prev_offset = 0;
            for (size_t i = 0; i < offsets.size(); ++i)
            {
                sorted_indexes[prev_offset] = sorted_indexes[i];
                prev_offset = offsets[i];
            }
            sorted_indexes.resize(prev_offset);
        }
        prepare_offsets_ms = prepare_offsets_watch.elapsedMilliseconds();
    }

    {
        Stopwatch filter_intervals_watch;
        lazy_materializing_rows->filterRangesAndFillRows(sorted_indexes);

        filter_intervals_ms = filter_intervals_watch.elapsedMilliseconds();
    }

    auto total_ms = main_chunk_watch.elapsedMilliseconds();

    /// Do not spam too much in logs
    if (total_ms >= 100)
        LOG_TRACE(getLogger("LazyMaterializingTransform"), "Preparing chunk {} ms, squashing {} ms, sorting {} ms, permute {} ms, prepare offsets {} ms, filter intervals {} ms",
            total_ms, squash_ms, sort_ms, permute_ms, prepare_offsets_ms, filter_intervals_ms);
}

void LazyMaterializingTransform::prepareLazyChunk()
{
    UInt64 squash_ms;
    UInt64 reverse_permutation_ms = 0;
    UInt64 permute_ms;

    Stopwatch total_watch;

    // std::cerr << "LazyMaterializingTransform::prepareLazyChunk " << chunks.size() << " chunks\n";
    // for (auto & chunk : chunks)
    //     std::cerr << "Chunk with " << chunk.getNumRows() << " rows\n";

    Chunk chunk;
    {
        Stopwatch squash_watch;

        /// call private method squash without handling ChunkInfos
        chunk = Squashing::squash(std::move(chunks));
        chunks.clear();

        squash_ms = squash_watch.elapsedMilliseconds();
    }

    if (chunk.getNumRows() != offsets.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "LazyMaterializingTransform: Number of rows in lazy chunk {} does not match number of offsets {}",
            chunk.getNumRows(), offsets.size());

    IColumn::Permutation inverted_permutation;

    size_t rows = permutation.size();
    bool is_identity_permutation = isIdentityPermutation(permutation, rows);
    bool should_replicate = offsets.size() != rows;

    if (!is_identity_permutation)
    {
        Stopwatch make_reverse_permutation_watch;
        inverted_permutation.resize(rows);
        for (size_t i = 0; i < rows; ++i)
            inverted_permutation[permutation[i]] = i;

        reverse_permutation_ms = make_reverse_permutation_watch.elapsedMilliseconds();
    }

    auto lazy_columns = chunk.detachColumns();

    {
        Stopwatch permute_watch;
        for (auto & col : lazy_columns)
        {
            if (should_replicate)
                col = col->replicate(offsets);

            if (!is_identity_permutation)
                col = col->permute(inverted_permutation, rows);
        }
        permute_ms = permute_watch.elapsedMilliseconds();
    }

    auto columns = result_chunk->detachColumns();
    columns.insert(columns.end(), lazy_columns.begin(), lazy_columns.end());
    result_chunk = Chunk(std::move(columns), rows);

    if (updater)
        updater->recordOutputChunk(*result_chunk, getOutputs().front().getHeader());

    auto total_ms = total_watch.elapsedMilliseconds();
    if (total_ms >= 100)
        LOG_TRACE(getLogger("LazyMaterializingTransform"),
        "Preparing lazy chunk {} ms, squashing {} ms, reversing permutations {} ms, permuting {} ms", total_ms, squash_ms, reverse_permutation_ms, permute_ms);
}

}
