#include <Interpreters/CrossJoin.h>

#include <algorithm>
#include <vector>

#include <base/arithmeticOverflow.h>

#include <Columns/ColumnReplicated.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Core/Joins.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

size_t CrossJoin::StoredBlock::allocatedBytes() const
{
    if (columns_info.columns.empty())
        return 0;

    size_t column_rows = columns_info.columns.front()->size();
    if (column_rows == 0)
        return 0;

    size_t res = 0;
    for (const auto & column : columns_info.columns)
        res += column->allocatedBytes();

    return res * rows / column_rows;
}

CrossJoin::CrossJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader right_sample_block_)
    : table_join(std::move(table_join_))
    , right_sample_block(*right_sample_block_)
    , tmp_data(table_join->getTempDataOnDisk())
    , max_joined_block_rows(table_join->maxJoinedBlockRows())
    , max_joined_block_bytes(table_join->maxJoinedBlockBytes())
    , log(getLogger("CrossJoin"))
{
    if (!isCrossOrComma(table_join->kind()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CrossJoin expects CROSS or comma join, got {}", table_join->kind());

    for (auto & column : right_sample_block)
    {
        if (!column.column)
            column.column = column.type->createColumn();
    }

    sample_block_with_columns_to_add = materializeBlock(right_sample_block);
    JoinCommon::createMissedColumns(sample_block_with_columns_to_add);
    saved_block_sample = sample_block_with_columns_to_add.cloneEmpty();

    LOG_TEST(log, "Right header: {}", right_sample_block.dumpStructure());
}

bool CrossJoin::addBlockToJoin(const Block & source_block, bool check_limits)
{
    return addBlockToJoin(source_block, source_block.rows(), check_limits);
}

bool CrossJoin::addBlockToJoin(const Block & source_block, size_t num_rows, bool check_limits)
{
    auto materialized = materializeColumnsFromRightBlock(source_block);

    size_t rows = materialized.rows();
    if (rows == 0 && num_rows != 0 && !materialized.columns())
        rows = num_rows;

    if (!memory_usage_before_adding_blocks)
        memory_usage_before_adding_blocks = JoinCommon::getCurrentQueryMemoryUsage();

    total_rows_to_join += rows;

    Block block_to_save = JoinCommon::filterColumnsPresentInSampleBlock(materialized, saved_block_sample);
    if (shrink_blocks)
        block_to_save = block_to_save.shrinkToFit();

    size_t max_bytes_in_join = table_join->sizeLimits().max_bytes;
    size_t max_rows_in_join = table_join->sizeLimits().max_rows;

    /// Empty blocks can still represent rows when no right-side columns are needed by the query.
    /// Keep them in memory as row-count metadata because Native format cannot persist that row count without columns.
    bool can_spill_block = block_to_save.columns() != 0;
    if (can_spill_block && tmp_data
        && (tmp_stream || (max_bytes_in_join && getTotalByteCount() + block_to_save.allocatedBytes() >= max_bytes_in_join)
            || (max_rows_in_join && getTotalRowCount() + block_to_save.rows() >= max_rows_in_join)))
    {
        if (!tmp_stream)
            tmp_stream.emplace(std::make_shared<const Block>(sample_block_with_columns_to_add), tmp_data);

        tmp_stream.value()->write(block_to_save);
        return true;
    }

    assertBlocksHaveEqualStructureAllowReplicated(saved_block_sample, block_to_save, "joined block");

    size_t min_bytes_to_compress = table_join->crossJoinMinBytesToCompress();
    size_t min_rows_to_compress = table_join->crossJoinMinRowsToCompress();

    if ((min_bytes_to_compress && getTotalByteCount() >= min_bytes_to_compress)
        || (min_rows_to_compress && getTotalRowCount() >= min_rows_to_compress))
    {
        block_to_save = block_to_save.compress();
        have_compressed = true;
    }

    doDebugAsserts();
    right_blocks.emplace_back(ColumnsInfo(block_to_save.getColumns()), rows);
    auto & stored_block = right_blocks.back();
    allocated_size += stored_block.allocatedBytes();
    in_memory_rows += rows;
    doDebugAsserts();

    size_t total_rows = getTotalRowCount();
    size_t total_bytes = getTotalByteCount();
    shrinkStoredBlocksToFit(total_bytes);

    if (!check_limits)
        return true;

    return table_join->sizeLimits().check(total_rows, total_bytes, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void CrossJoin::doDebugAsserts() const
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    size_t debug_allocated_size = 0;
    for (const auto & stored_block : right_blocks)
        debug_allocated_size += stored_block.allocatedBytes();

    if (allocated_size != debug_allocated_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "allocated_size != debug_allocated_size ({} != {})",
            allocated_size,
            debug_allocated_size);
#endif
}

size_t CrossJoin::getTotalByteCount() const
{
    doDebugAsserts();
    return allocated_size;
}

void CrossJoin::shrinkStoredBlocksToFit(size_t & total_bytes_in_join, bool force_optimize)
{
    Int64 current_memory_usage = JoinCommon::getCurrentQueryMemoryUsage();
    Int64 query_memory_usage_delta = current_memory_usage - memory_usage_before_adding_blocks;
    Int64 max_total_bytes_for_query = memory_usage_before_adding_blocks ? table_join->getMaxMemoryUsage() : 0;

    auto max_total_bytes_in_join = table_join->sizeLimits().max_bytes;

    if (!force_optimize)
    {
        if (shrink_blocks)
            return;

        shrink_blocks = (max_total_bytes_in_join && total_bytes_in_join > max_total_bytes_in_join / 2)
            || (max_total_bytes_for_query && query_memory_usage_delta > max_total_bytes_for_query / 2);
        if (!shrink_blocks)
            return;
    }

    LOG_DEBUG(
        log,
        "Shrinking stored blocks, memory consumption is {} {} calculated by join, {} {} by memory tracker",
        ReadableSize(total_bytes_in_join),
        max_total_bytes_in_join ? fmt::format("/ {}", ReadableSize(max_total_bytes_in_join)) : "",
        ReadableSize(query_memory_usage_delta),
        max_total_bytes_for_query ? fmt::format("/ {}", ReadableSize(max_total_bytes_for_query)) : "");

    for (auto & stored_block : right_blocks)
    {
        doDebugAsserts();

        size_t old_size = stored_block.allocatedBytes();

        try
        {
            for (auto & column : stored_block.columns_info.columns)
                column = column->cloneResized(column->size());

            stored_block.columns_info.rebuildReplicatedColumns();
        }
        catch (...)
        {
            stored_block.columns_info.rebuildReplicatedColumns();
            size_t partial_new_size = stored_block.allocatedBytes();
            if (old_size >= partial_new_size)
                allocated_size -= old_size - partial_new_size;
            else
                allocated_size += partial_new_size - old_size;
            throw;
        }

        size_t new_size = stored_block.allocatedBytes();

        if (old_size >= new_size)
        {
            if (allocated_size < old_size - new_size)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Blocks allocated size value is broken: blocks_allocated_size = {}, old_size = {}, new_size = {}",
                    allocated_size,
                    old_size,
                    new_size);

            allocated_size -= old_size - new_size;
        }
        else
            allocated_size += new_size - old_size;

        doDebugAsserts();
    }

    auto new_total_bytes_in_join = getTotalByteCount();
    Int64 new_current_memory_usage = JoinCommon::getCurrentQueryMemoryUsage();

    LOG_DEBUG(
        log,
        "Shrunk stored blocks {} freed ({} by memory tracker), new memory consumption is {} ({} by memory tracker)",
        ReadableSize(total_bytes_in_join - new_total_bytes_in_join),
        ReadableSize(current_memory_usage - new_current_memory_usage),
        ReadableSize(new_total_bytes_in_join),
        ReadableSize(new_current_memory_usage));

    total_bytes_in_join = new_total_bytes_in_join;
}

Block CrossJoin::materializeColumnsFromRightBlock(Block block) const
{
    return JoinCommon::materializeColumnsFromRightBlock(std::move(block), saved_block_sample);
}

class CrossJoinResult final : public IJoinResult
{
public:
    CrossJoinResult(const CrossJoin & join_, Block block_)
        : join(join_)
        , block(std::move(block_))
    {
    }

    JoinResultBlock next() override;

private:
    const CrossJoin & join;
    Block block;
    size_t left_row = 0;
    std::optional<CrossJoin::StoredBlocks::const_iterator> right_block_it;
    std::optional<TemporaryBlockStreamReaderHolder> reader;
};

IJoinResult::JoinResultBlock CrossJoinResult::next()
{
    size_t num_existing_columns = block.columns();
    size_t num_columns_to_add = join.sample_block_with_columns_to_add.columns();

    ColumnRawPtrs src_left_columns;
    MutableColumns dst_columns;

    {
        src_left_columns.reserve(num_existing_columns);
        dst_columns.reserve(num_existing_columns + num_columns_to_add);

        for (const ColumnWithTypeAndName & left_column : block)
        {
            src_left_columns.push_back(left_column.column.get());
            dst_columns.emplace_back(src_left_columns.back()->cloneEmpty());
        }

        for (const ColumnWithTypeAndName & right_column : join.sample_block_with_columns_to_add)
            dst_columns.emplace_back(right_column.column->cloneEmpty());

        size_t to_reserve = 0;
        if (common::mulOverflow(block.rows(), join.total_rows_to_join, to_reserve))
            to_reserve = join.max_joined_block_rows;

        to_reserve = std::min(join.max_joined_block_rows, to_reserve);

        for (auto & dst : dst_columns)
            dst->reserve(to_reserve);
    }

    size_t rows_total = block.rows();
    size_t rows_added = 0;
    size_t bytes_added = 0;

    auto enough_data = [&]()
    {
        return (join.max_joined_block_rows && rows_added > join.max_joined_block_rows)
            || (join.max_joined_block_bytes && bytes_added > join.max_joined_block_bytes);
    };

    for (; left_row < rows_total; ++left_row)
    {
        if (enough_data())
            break;

        auto process_right_block = [&](const ColumnsInfo & columns_info, size_t rows_right)
        {
            rows_added += rows_right;

            for (size_t col_num = 0; col_num < num_existing_columns; ++col_num)
                dst_columns[col_num]->insertManyFrom(*src_left_columns[col_num], left_row, rows_right);

            for (size_t col_num = 0; col_num < num_columns_to_add; ++col_num)
            {
                if (const auto * replicated_column_right = columns_info.replicated_columns[col_num])
                {
                    for (size_t row = 0; row != rows_right; ++row)
                        dst_columns[num_existing_columns + col_num]->insertFrom(
                            *replicated_column_right->getNestedColumn(),
                            replicated_column_right->getIndexes().getIndexAt(row));
                }
                else
                {
                    const IColumn & column_right = *columns_info.columns[col_num];
                    dst_columns[num_existing_columns + col_num]->insertRangeFrom(column_right, 0, rows_right);
                }
            }

            if (join.max_joined_block_bytes)
            {
                bytes_added = 0;
                for (const auto & dst : dst_columns)
                    bytes_added += dst->byteSize();
            }
        };

        if (!right_block_it.has_value())
            right_block_it = join.right_blocks.begin();

        for (; *right_block_it != join.right_blocks.end(); ++*right_block_it)
        {
            if (enough_data())
                break;

            const auto & stored_block = **right_block_it;
            if (!join.have_compressed)
                process_right_block(stored_block.columns_info, stored_block.rows);
            else
            {
                Columns new_columns;
                new_columns.reserve(stored_block.columns_info.columns.size());
                for (const auto & column : stored_block.columns_info.columns)
                    new_columns.emplace_back(column->decompress());

                process_right_block(ColumnsInfo(std::move(new_columns)), stored_block.rows);
            }
        }

        if (*right_block_it != join.right_blocks.end())
            break;

        if (join.tmp_stream)
        {
            if (!reader)
                reader = join.tmp_stream->getReadStream();

            while (reader)
            {
                if (enough_data())
                    break;

                auto block_right = reader.value()->read();
                if (block_right.empty())
                {
                    reader.reset();
                    break;
                }

                process_right_block(ColumnsInfo(block_right.getColumns()), block_right.rows());
            }
        }

        if (reader)
            break;

        right_block_it = std::nullopt;
    }

    auto res = block.cloneEmpty();
    for (const ColumnWithTypeAndName & src_column : join.sample_block_with_columns_to_add)
        res.insert(src_column);

    bool is_last = left_row >= rows_total;
    res = res.cloneWithColumns(std::move(dst_columns));
    return {res, nullptr, is_last};
}

JoinResultPtr CrossJoin::joinBlock(Block block)
{
    return std::make_unique<CrossJoinResult>(*this, std::move(block));
}

}
