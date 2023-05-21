#include "CrossJoin.h"

#include <Core/Block.h>

#include <Common/logger_useful.h>
#include <Common/ErrorCodes.h>
#include <Common/CurrentMetrics.h>

#include <Interpreters/RowRefs.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TemporaryDataOnDisk.h>

namespace CurrentMetrics
{
    extern const Metric TemporaryFilesForJoin;
}

namespace DB {

namespace ErrorCodes
{
extern const int SET_SIZE_LIMIT_EXCEEDED;
}

CrossJoin::CrossJoin(ContextPtr context_, std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_)
    : table_join(table_join_)
    , right_sample_block(right_sample_block_)
    , context(context_)
    , tempData(new TemporaryDataOnDisk(context_->getTempDataOnDisk(), CurrentMetrics::TemporaryFilesForJoin))
    , blockStream(tempData->createStream(right_sample_block))
    , log(&Poco::Logger::get("CrossJoin"))
{
}

enum {
    ABSTRACT_LIMIT = 228,
};

bool CrossJoin::addJoinedBlock(const Block & block, bool check_limits) 
{
    if (unlikely(block.rows() > std::numeric_limits<RowRef::SizeT>::max()))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Too many rows in right table block for HashJoin: {}", block.rows());

    Block materialized = materializeBlock(block);

    ++right_blocks_count;
    right_blocks.push_back(std::move(materialized));
    right_rows += block.rows();
    right_bytes += block.bytes();

    if (right_bytes > context->getSettings().cross_join_in_memory_limit) {
        LOG_DEBUG(log, "Moving blocks to disk...");
        moveBlocksToDisk();
    }

    LOG_DEBUG(log, "addJoinedBlock");
    if (!check_limits)
    {
        return true;
    }
    return table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

struct NotProcessedCrossJoin : public ExtraBlock
{
    size_t left_position;
    size_t right_block;
};

void CrossJoin::joinBlock(Block &block, std::shared_ptr<ExtraBlock> &not_processed)
{
    LOG_DEBUG(log, "joinBlock");
    size_t max_joined_block_rows = table_join->maxJoinedBlockRows();
    size_t start_left_row = 0;
    size_t start_right_block = 0;
    if (!blockStream.isWriteFinished() && right_blocks_count)
    {
        blockStream.finishWriting();
    }
    if (not_processed)
    {
        auto & continuation = static_cast<NotProcessedCrossJoin &>(*not_processed);
        start_left_row = continuation.left_position;
        start_right_block = continuation.right_block;
        not_processed.reset();
    }

    size_t num_existing_columns = block.columns();
    size_t num_columns_to_add = right_sample_block.columns();

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

        for (const ColumnWithTypeAndName & right_column : right_sample_block)
            dst_columns.emplace_back(right_column.column->cloneEmpty());

        for (auto & dst : dst_columns)
            dst->reserve(max_joined_block_rows);
    }

    size_t rows_left = block.rows();
    size_t rows_added = 0;

    for (size_t left_row = start_left_row; left_row < rows_left; ++left_row)
    {
        size_t block_number = 0;
        auto process_right_block = [&](const Block & block_right) {
            ++block_number;
            if (block_number < start_right_block)
                return;

            size_t rows_right = block_right.rows();
            rows_added += rows_right;

            for (size_t col_num = 0; col_num < num_existing_columns; ++col_num)
                dst_columns[col_num]->insertManyFrom(*src_left_columns[col_num], left_row, rows_right);

            for (size_t col_num = 0; col_num < num_columns_to_add; ++col_num)
            {
                const IColumn & column_right = *block_right.getByPosition(col_num).column;
                dst_columns[num_existing_columns + col_num]->insertRangeFrom(column_right, 0, rows_right);
            }
        };

        Block block_right;
        do
        {
            block_right = blockStream.read(false);
            if (block_right)
            {
                process_right_block(block_right);
            }
        } while (block_right);

        blockStream.resetReading();

        for (const Block & block_right_in_memory : right_blocks)
        {
            process_right_block(block_right_in_memory);
        }

        start_right_block = 0;

        if (rows_added > max_joined_block_rows)
        {
            not_processed = std::make_shared<NotProcessedCrossJoin>(
                NotProcessedCrossJoin{{block.cloneEmpty()}, left_row, block_number + 1});
            not_processed->block.swap(block);
            break;
        }
    }

    for (const ColumnWithTypeAndName & src_column : right_sample_block)
        block.insert(src_column);

    block = block.cloneWithColumns(std::move(dst_columns));
}

const TableJoin & CrossJoin::getTableJoin() const
{
    return *table_join;
}

void CrossJoin::checkTypesOfKeys(const Block & /*block*/) const
{
}

size_t CrossJoin::getTotalByteCount() const
{
    return right_bytes + right_bytes_on_disk;
}

size_t CrossJoin::getTotalRowCount() const
{
    return right_rows + right_rows_on_disk;
}

bool CrossJoin::alwaysReturnsEmptySet() const {
    return right_rows_on_disk == 0 && right_rows == 0;
}

IBlocksStreamPtr CrossJoin::getNonJoinedBlocks(const Block & /*left_sample_block*/,
    const Block & /*result_sample_block*/, UInt64 /*max_block_size*/) const
{
    return nullptr;
}

void CrossJoin::moveBlocksToDisk() {
    right_bytes_on_disk += right_bytes;
    right_rows_on_disk += right_rows;
    right_bytes = 0;
    right_rows = 0;
    for (const auto & block : right_blocks) {
        blockStream.write(block);
    }
    right_blocks.clear();
}

}
