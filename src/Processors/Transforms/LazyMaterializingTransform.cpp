#include <Processors/Transforms/LazyMaterializingTransform.h>
#include <Interpreters/Squashing.h>
#include <Interpreters/sortBlock.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/SortDescription.h>
#include <Columns/ColumnsNumber.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

Block LazyMaterializingTransform::transformHeader(const Block & main_header, const Block & lazy_header)
{
    auto pos = main_header.getPositionByName("__global_offset");
    ColumnsWithTypeAndName columns = main_header.getColumnsWithTypeAndName();
    columns.erase(columns.begin() + pos);
    const auto & lazy_columns = lazy_header.getColumnsWithTypeAndName();
    columns.insert(columns.end(), lazy_columns.begin(), lazy_columns.end());
    return Block(std::move(columns));
}

LazyMaterializingTransform::LazyMaterializingTransform(SharedHeader main_header, SharedHeader lazy_header, LazyMaterializingRowsPtr lazy_materializing_rows_)
    : IProcessor(
        InputPorts({main_header, lazy_header}),
        OutputPorts({OutputPort(std::make_shared<Block>(transformHeader(*main_header, *lazy_header)))}))
    , lazy_materializing_rows(std::move(lazy_materializing_rows_))
{
}

LazyMaterializingTransform::Status LazyMaterializingTransform::prepare()
{
    auto & output = outputs.front();
    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    auto & main_input = inputs.front();
    auto & lazy_input = inputs.back();

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

void LazyMaterializingTransform::prepareMainChunk()
{
    result_chunk = Squashing::squash(std::move(chunks));
    auto rows = result_chunk->getNumRows();
    auto pos = getInputs().front().getHeader().getPositionByName("__global_row_index");
    auto columns = result_chunk->detachColumns();
    auto index_col = columns[pos];
    columns.erase(columns.begin() + pos);
    result_chunk = Chunk(std::move(columns), rows);

    Block block({{index_col, std::make_shared<DataTypeUInt64>(), {}}});
    SortDescription descr;
    descr.emplace_back(std::string{});

    stableGetPermutation(block, descr, permutation);

    {
        auto sorted_indexes_ptr = IColumn::mutate(index_col->permute(permutation, rows));
        sorted_indexes = std::move(assert_cast<ColumnUInt64 &>(*sorted_indexes_ptr).getData());
    }

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

    auto & rows_in_parts = lazy_materializing_rows->rows_in_parts;
    auto & ranges_in_data_parts = lazy_materializing_rows->ranges_in_data_parts;

    rows_in_parts.clear();
    rows_in_parts.reserve(ranges_in_data_parts.size());

    size_t next_index = 0;
    for (auto & part : ranges_in_data_parts)
    {
        auto & rows_in_part = rows_in_parts.emplace_back();
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

            while (range.begin < range.end)
            {
                ++range.begin;
                row_begin = part.data_part->index_granularity->getMarkStartingRow(range.begin);
                if (sorted_indexes[next_index] < offset + row_begin)
                    break;
                subrange_begin = range.begin;
            }

            auto subrange_end = range.begin;

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

                if (!(range.begin < row_end))
                    break;

                ++range.begin;
                row_begin = part.data_part->index_granularity->getMarkStartingRow(range.begin);

                if (!(sorted_indexes[next_index] < offset + row_begin))
                    break;

                subrange_end = range.begin;
            }

            ranges.push_back({subrange_begin, subrange_end});

            if (!(range.begin < range.end))
                part.ranges.pop_front();
        }

        part.ranges = std::move(ranges);
    }
}

void LazyMaterializingTransform::prepareLazyChunk()
{
    auto chunk = Squashing::squash(std::move(chunks));
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
        inverted_permutation.resize(rows);
        for (size_t i = 0; i < rows; ++i)
            inverted_permutation[permutation[i]] = i;
    }

    auto lazy_columns = chunk.detachColumns();
    for (auto & col : lazy_columns)
    {
        if (!is_identity_permutation)
            col = col->permute(inverted_permutation, rows);

        if (should_replicate)
            col = col->replicate(offsets);
    }

    auto columns = result_chunk->detachColumns();
    columns.insert(columns.end(), lazy_columns.begin(), lazy_columns.end());
    result_chunk = Chunk(std::move(columns), rows);
}

}
