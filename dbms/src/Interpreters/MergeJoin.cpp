#include <limits>

#include <Core/NamesAndTypes.h>
#include <Core/SortCursor.h>
#include <Columns/ColumnNullable.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/AnalyzedJoin.h>
#include <Interpreters/sortBlock.h>
#include <Interpreters/join_common.h>
#include <DataStreams/materializeBlock.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/MergingSortedBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/TemporaryFileStream.h>
#include <DataStreams/ConcatBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int NOT_IMPLEMENTED;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int NOT_ENOUGH_SPACE;
    extern const int LOGICAL_ERROR;
}

namespace
{

template <bool has_nulls>
int nullableCompareAt(const IColumn & left_column, const IColumn & right_column, size_t lhs_pos, size_t rhs_pos)
{
    static constexpr int null_direction_hint = 1;

    if constexpr (has_nulls)
    {
        auto * left_nullable = checkAndGetColumn<ColumnNullable>(left_column);
        auto * right_nullable = checkAndGetColumn<ColumnNullable>(right_column);

        if (left_nullable && right_nullable)
        {
            int res = left_column.compareAt(lhs_pos, rhs_pos, right_column, null_direction_hint);
            if (res)
                return res;

            /// NULL != NULL case
            if (left_column.isNullAt(lhs_pos))
                return null_direction_hint;
        }

        if (left_nullable && !right_nullable)
        {
            if (left_column.isNullAt(lhs_pos))
                return null_direction_hint;
            return left_nullable->getNestedColumn().compareAt(lhs_pos, rhs_pos, right_column, null_direction_hint);
        }

        if (!left_nullable && right_nullable)
        {
            if (right_column.isNullAt(rhs_pos))
                return -null_direction_hint;
            return left_column.compareAt(lhs_pos, rhs_pos, right_nullable->getNestedColumn(), null_direction_hint);
        }
    }

    /// !left_nullable && !right_nullable
    return left_column.compareAt(lhs_pos, rhs_pos, right_column, null_direction_hint);
}

Block extractMinMax(const Block & block, const Block & keys)
{
    if (block.rows() == 0)
        throw Exception("Unexpected empty block", ErrorCodes::LOGICAL_ERROR);

    Block min_max = keys.cloneEmpty();
    MutableColumns columns = min_max.mutateColumns();

    for (size_t i = 0; i < columns.size(); ++i)
    {
        auto & src_column = block.getByName(keys.getByPosition(i).name);

        columns[i]->insertFrom(*src_column.column, 0);
        columns[i]->insertFrom(*src_column.column, block.rows() - 1);
    }

    min_max.setColumns(std::move(columns));
    return min_max;
}

}

struct MergeJoinEqualRange
{
    size_t left_start = 0;
    size_t right_start = 0;
    size_t left_length = 0;
    size_t right_length = 0;

    bool empty() const { return !left_length && !right_length; }
};

using Range = MergeJoinEqualRange;


class MergeJoinCursor
{
public:
    MergeJoinCursor(const Block & block, const SortDescription & desc_)
        : impl(SortCursorImpl(block, desc_))
    {}

    size_t position() const { return impl.pos; }
    size_t end() const { return impl.rows; }
    bool atEnd() const { return impl.pos >= impl.rows; }
    void nextN(size_t num) { impl.pos += num; }

    void setCompareNullability(const MergeJoinCursor & rhs)
    {
        has_nullable_columns = false;

        for (size_t i = 0; i < impl.sort_columns_size; ++i)
        {
            bool is_left_nullable = isColumnNullable(*impl.sort_columns[i]);
            bool is_right_nullable = isColumnNullable(*rhs.impl.sort_columns[i]);

            if (is_left_nullable || is_right_nullable)
            {
                has_nullable_columns = true;
                break;
            }
        }
    }

    Range getNextEqualRange(MergeJoinCursor & rhs)
    {
        if (has_nullable_columns)
            return getNextEqualRangeImpl<true>(rhs);
        return getNextEqualRangeImpl<false>(rhs);
    }

    int intersect(const Block & min_max, const Names & key_names)
    {
        if (end() == 0 || min_max.rows() != 2)
            throw Exception("Unexpected block size", ErrorCodes::LOGICAL_ERROR);

        size_t last_position = end() - 1;
        int first_vs_max = 0;
        int last_vs_min = 0;

        for (size_t i = 0; i < impl.sort_columns.size(); ++i)
        {
            auto & left_column = *impl.sort_columns[i];
            auto & right_column = *min_max.getByName(key_names[i]).column; /// cannot get by position cause of possible duplicates

            if (!first_vs_max)
                first_vs_max = nullableCompareAt<true>(left_column, right_column, position(), 1);

            if (!last_vs_min)
                last_vs_min = nullableCompareAt<true>(left_column, right_column, last_position, 0);
        }

        if (first_vs_max > 0)
            return 1;
        if (last_vs_min < 0)
            return -1;
        return 0;
    }

private:
    SortCursorImpl impl;
    bool has_nullable_columns = false;

    template <bool has_nulls>
    Range getNextEqualRangeImpl(MergeJoinCursor & rhs)
    {
        while (!atEnd() && !rhs.atEnd())
        {
            int cmp = compareAt<has_nulls>(rhs, impl.pos, rhs.impl.pos);
            if (cmp < 0)
                impl.next();
            if (cmp > 0)
                rhs.impl.next();
            if (!cmp)
            {
                Range range{impl.pos, rhs.impl.pos, 0, 0};
                range.left_length = getEqualLength();
                range.right_length = rhs.getEqualLength();
                return range;
            }
        }

        return Range{impl.pos, rhs.impl.pos, 0, 0};
    }

    template <bool has_nulls>
    int compareAt(const MergeJoinCursor & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
        int res = 0;
        for (size_t i = 0; i < impl.sort_columns_size; ++i)
        {
            auto * left_column = impl.sort_columns[i];
            auto * right_column = rhs.impl.sort_columns[i];

            res = nullableCompareAt<has_nulls>(*left_column, *right_column, lhs_pos, rhs_pos);
            if (res)
                break;
        }
        return res;
    }

    size_t getEqualLength()
    {
        if (atEnd())
            return 0;

        size_t pos = impl.pos;
        while (sameNext(pos))
            ++pos;
        return pos - impl.pos + 1;
    }

    bool sameNext(size_t lhs_pos) const
    {
        if (lhs_pos + 1 >= impl.rows)
            return false;

        for (size_t i = 0; i < impl.sort_columns_size; ++i)
            if (impl.sort_columns[i]->compareAt(lhs_pos, lhs_pos + 1, *(impl.sort_columns[i]), 1) != 0)
                return false;
        return true;
    }
};

namespace
{

MutableColumns makeMutableColumns(const Block & block, size_t rows_to_reserve = 0)
{
    MutableColumns columns;
    columns.reserve(block.columns());

    for (const auto & src_column : block)
    {
        columns.push_back(src_column.column->cloneEmpty());
        columns.back()->reserve(rows_to_reserve);
    }
    return columns;
}

void makeSortAndMerge(const Names & keys, SortDescription & sort, SortDescription & merge)
{
    NameSet unique_keys;
    for (auto & key_name : keys)
    {
        merge.emplace_back(SortColumnDescription(key_name, 1, 1));

        if (!unique_keys.count(key_name))
        {
            unique_keys.insert(key_name);
            sort.emplace_back(SortColumnDescription(key_name, 1, 1));
        }
    }
}

void copyLeftRange(const Block & block, MutableColumns & columns, size_t start, size_t rows_to_add)
{
    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto & src_column = block.getByPosition(i).column;
        columns[i]->insertRangeFrom(*src_column, start, rows_to_add);
    }
}

void copyRightRange(const Block & right_block, const Block & right_columns_to_add, MutableColumns & columns,
                    size_t row_position, size_t rows_to_add)
{
    for (size_t i = 0; i < right_columns_to_add.columns(); ++i)
    {
        const auto & src_column = right_block.getByName(right_columns_to_add.getByPosition(i).name).column;
        auto & dst_column = columns[i];
        auto * dst_nullable = typeid_cast<ColumnNullable *>(dst_column.get());

        if (dst_nullable && !isColumnNullable(*src_column))
            dst_nullable->insertManyFromNotNullable(*src_column, row_position, rows_to_add);
        else
            dst_column->insertManyFrom(*src_column, row_position, rows_to_add);
    }
}

void joinEqualsAnyLeft(const Block & right_block, const Block & right_columns_to_add, MutableColumns & right_columns, const Range & range)
{
    copyRightRange(right_block, right_columns_to_add, right_columns, range.right_start, range.left_length);
}

void joinEquals(const Block & left_block, const Block & right_block, const Block & right_columns_to_add,
                MutableColumns & left_columns, MutableColumns & right_columns, const Range & range, bool is_all)
{
    size_t left_rows_to_add = range.left_length;
    size_t right_rows_to_add = is_all ? range.right_length : 1;

    size_t row_position = range.right_start;
    for (size_t right_row = 0; right_row < right_rows_to_add; ++right_row, ++row_position)
    {
        copyLeftRange(left_block, left_columns, range.left_start, left_rows_to_add);
        copyRightRange(right_block, right_columns_to_add, right_columns, row_position, left_rows_to_add);
    }
}

void appendNulls(MutableColumns & right_columns, size_t rows_to_add)
{
    for (auto & column : right_columns)
        column->insertManyDefaults(rows_to_add);
}

void joinInequalsLeft(const Block & left_block, MutableColumns & left_columns, MutableColumns & right_columns,
                      size_t start, size_t end, bool copy_left)
{
    if (end <= start)
        return;

    size_t rows_to_add = end - start;
    if (copy_left)
        copyLeftRange(left_block, left_columns, start, rows_to_add);
    appendNulls(right_columns, rows_to_add);
}

Blocks blocksListToBlocks(const BlocksList & in_blocks)
{
    Blocks out_blocks;
    out_blocks.reserve(in_blocks.size());
    for (const auto & block : in_blocks)
        out_blocks.push_back(block);
    return out_blocks;
}

std::unique_ptr<TemporaryFile> flushBlockToFile(const String & tmp_path, const Block & header, Block && block)
{
    auto tmp_file = createTemporaryFile(tmp_path);

    OneBlockInputStream stream(block);
    std::atomic<bool> is_cancelled{false};
    TemporaryFileStream::write(tmp_file->path(), header, stream, &is_cancelled);
    if (is_cancelled)
        throw Exception("Cannot flush MergeJoin data on disk. No space at " + tmp_path, ErrorCodes::NOT_ENOUGH_SPACE);

    return tmp_file;
}

void flushStreamToFiles(const String & tmp_path, const Block & header, IBlockInputStream & stream,
                        std::vector<std::unique_ptr<TemporaryFile>> & files,
                        std::function<void(const Block &)> callback = [](const Block &){})
{
    while (Block block = stream.read())
    {
        if (!block.rows())
            continue;

        callback(block);
        auto tmp_file = flushBlockToFile(tmp_path, header, std::move(block));
        files.emplace_back(std::move(tmp_file));
    }
}

BlockInputStreams makeSortedInputStreams(std::vector<MiniLSM::SortedFiles> & sorted_files, const Block & header)
{
    BlockInputStreams inputs;

    for (const auto & track : sorted_files)
    {
        BlockInputStreams sequence;
        for (const auto & file : track)
            sequence.emplace_back(std::make_shared<TemporaryFileLazyInputStream>(file->path(), header));
        inputs.emplace_back(std::make_shared<ConcatBlockInputStream>(sequence));
    }

    return inputs;
}

}


void MiniLSM::insert(const BlocksList & blocks)
{
    if (blocks.empty())
        return;

    SortedFiles sorted_blocks;
    if (blocks.size() > 1)
    {
        BlockInputStreams inputs;
        inputs.reserve(blocks.size());
        for (auto & block : blocks)
            inputs.push_back(std::make_shared<OneBlockInputStream>(block));

        MergingSortedBlockInputStream sorted_input(inputs, sort_description, rows_in_block);
        flushStreamToFiles(path, sample_block, sorted_input, sorted_blocks);
    }
    else
    {
        OneBlockInputStream sorted_input(blocks.front());
        flushStreamToFiles(path, sample_block, sorted_input, sorted_blocks);
    }

    sorted_files.emplace_back(std::move(sorted_blocks));
    if (sorted_files.size() >= max_size)
        merge();
}

/// TODO: better merge strategy
void MiniLSM::merge(std::function<void(const Block &)> callback)
{
    BlockInputStreams inputs = makeSortedInputStreams(sorted_files, sample_block);
    MergingSortedBlockInputStream sorted_stream(inputs, sort_description, rows_in_block);

    SortedFiles out;
    flushStreamToFiles(path, sample_block, sorted_stream, out, callback);

    sorted_files.clear();
    sorted_files.emplace_back(std::move(out));
}


MergeJoin::MergeJoin(std::shared_ptr<AnalyzedJoin> table_join_, const Block & right_sample_block_)
    : table_join(table_join_)
    , size_limits(table_join->sizeLimits())
    , right_sample_block(right_sample_block_)
    , nullable_right_side(table_join->forceNullableRight())
    , is_all(table_join->strictness() == ASTTableJoin::Strictness::All)
    , is_inner(isInner(table_join->kind()))
    , is_left(isLeft(table_join->kind()))
    , skip_not_intersected(table_join->enablePartialMergeJoinOptimizations())
    , max_rows_in_right_block(table_join->maxRowsInRightBlock())
{
    if (!isLeft(table_join->kind()) && !isInner(table_join->kind()))
        throw Exception("Partial merge supported for LEFT and INNER JOINs only", ErrorCodes::NOT_IMPLEMENTED);

    if (!max_rows_in_right_block)
        throw Exception("partial_merge_join_rows_in_right_blocks cannot be zero", ErrorCodes::PARAMETER_OUT_OF_BOUND);

    if (!size_limits.hasLimits())
    {
        size_limits.max_bytes = table_join->defaultMaxBytes();
        if (!size_limits.max_bytes)
            throw Exception("No limit for MergeJoin (max_rows_in_join, max_bytes_in_join or default_max_bytes_in_join have to be set)",
                            ErrorCodes::PARAMETER_OUT_OF_BOUND);
    }

    JoinCommon::extractKeysForJoin(table_join->keyNamesRight(), right_sample_block, right_table_keys, right_columns_to_add);

    const NameSet required_right_keys = table_join->requiredRightKeys();
    for (const auto & column : right_table_keys)
        if (required_right_keys.count(column.name))
            right_columns_to_add.insert(ColumnWithTypeAndName{nullptr, column.type, column.name});

    JoinCommon::removeLowCardinalityInplace(right_columns_to_add);
    JoinCommon::createMissedColumns(right_columns_to_add);

    if (nullable_right_side)
        JoinCommon::convertColumnsToNullable(right_columns_to_add);

    makeSortAndMerge(table_join->keyNamesLeft(), left_sort_description, left_merge_description);
    makeSortAndMerge(table_join->keyNamesRight(), right_sort_description, right_merge_description);

    lsm = std::make_unique<MiniLSM>(table_join->getTemporaryPath(), right_sample_block, right_sort_description, max_rows_in_right_block);
}

void MergeJoin::setTotals(const Block & totals_block)
{
    totals = totals_block;
    mergeRightBlocks();
}

void MergeJoin::joinTotals(Block & block) const
{
    JoinCommon::joinTotals(totals, right_columns_to_add, table_join->keyNamesRight(), block);
}

void MergeJoin::mergeRightBlocks()
{
    if (is_in_memory)
        mergeInMemoryRightBlocks();
    else
        mergeFlushedRightBlocks();
}

void MergeJoin::mergeInMemoryRightBlocks()
{
    std::unique_lock lock(rwlock);

    if (right_blocks.empty())
        return;

    Blocks blocks_to_merge = blocksListToBlocks(right_blocks);
    clearRightBlocksList();

    /// TODO: there should be no splitted keys by blocks for RIGHT|FULL JOIN
    MergeSortingBlocksBlockInputStream sorted_input(blocks_to_merge, right_sort_description, max_rows_in_right_block);

    while (Block block = sorted_input.read())
    {
        if (!block.rows())
            continue;

        if (skip_not_intersected)
            min_max_right_blocks.emplace_back(extractMinMax(block, right_table_keys));
        countBlockSize(block);
        loaded_right_blocks.emplace_back(std::make_shared<Block>(std::move(block)));
    }
}

void MergeJoin::mergeFlushedRightBlocks()
{
    std::unique_lock lock(rwlock);

    lsm->insert(right_blocks);
    clearRightBlocksList();

    auto callback = [&](const Block & block)
    {
        if (skip_not_intersected)
            min_max_right_blocks.emplace_back(extractMinMax(block, right_table_keys));
        countBlockSize(block);
    };

    lsm->merge(callback);
    flushed_right_blocks.swap(lsm->sorted_files.front());

    /// Get memory limit or aproximate it from row limit and bytes per row factor
    UInt64 memory_limit = size_limits.max_bytes;
    UInt64 rows_limit = size_limits.max_rows;
    if (!memory_limit && rows_limit)
        memory_limit = right_blocks_bytes * rows_limit / right_blocks_row_count;

    cached_right_blocks = std::make_unique<Cache>(memory_limit);
}

void MergeJoin::flushRightBlocks()
{
    /// it's under unique_lock(rwlock)

    is_in_memory = false;
    lsm->insert(right_blocks);
    clearRightBlocksList();
}

bool MergeJoin::saveRightBlock(Block && block)
{
    std::unique_lock lock(rwlock);

    countBlockSize(block);
    right_blocks.emplace_back(std::move(block));

    bool has_memory = size_limits.softCheck(right_blocks_row_count, right_blocks_bytes);
    if (!has_memory)
        flushRightBlocks();
    return true;
}

bool MergeJoin::addJoinedBlock(const Block & src_block)
{
    Block block = materializeBlock(src_block);
    JoinCommon::removeLowCardinalityInplace(block);

    sortBlock(block, right_sort_description);
    return saveRightBlock(std::move(block));
}

void MergeJoin::joinBlock(Block & block)
{
    JoinCommon::checkTypesOfKeys(block, table_join->keyNamesLeft(), right_table_keys, table_join->keyNamesRight());
    materializeBlockInplace(block);
    JoinCommon::removeLowCardinalityInplace(block);

    sortBlock(block, left_sort_description);
    if (is_in_memory)
        joinSortedBlock<true>(block);
    else
        joinSortedBlock<false>(block);
}

template <bool in_memory>
void MergeJoin::joinSortedBlock(Block & block)
{
    std::shared_lock lock(rwlock);

    size_t rows_to_reserve = is_left ? block.rows() : 0;
    MutableColumns left_columns = makeMutableColumns(block, (is_all ? rows_to_reserve : 0));
    MutableColumns right_columns = makeMutableColumns(right_columns_to_add, rows_to_reserve);
    MergeJoinCursor left_cursor(block, left_merge_description);
    size_t left_key_tail = 0;
    size_t right_blocks_count = rightBlocksCount<in_memory>();

    if (is_left)
    {
        for (size_t i = 0; i < right_blocks_count; ++i)
        {
            if (left_cursor.atEnd())
                break;

            if (skip_not_intersected)
            {
                int intersection = left_cursor.intersect(min_max_right_blocks[i], table_join->keyNamesRight());
                if (intersection < 0)
                    break; /// (left) ... (right)
                if (intersection > 0)
                    continue; /// (right) ... (left)
            }

            std::shared_ptr<Block> right_block = loadRightBlock<in_memory>(i);

            leftJoin(left_cursor, block, *right_block, left_columns, right_columns, left_key_tail);
        }

        left_cursor.nextN(left_key_tail);
        joinInequalsLeft(block, left_columns, right_columns, left_cursor.position(), left_cursor.end(), is_all);
        //left_cursor.nextN(left_cursor.end() - left_cursor.position());

        changeLeftColumns(block, std::move(left_columns));
        addRightColumns(block, std::move(right_columns));
    }
    else if (is_inner)
    {
        for (size_t i = 0; i < right_blocks_count; ++i)
        {
            if (left_cursor.atEnd())
                break;

            if (skip_not_intersected)
            {
                int intersection = left_cursor.intersect(min_max_right_blocks[i], table_join->keyNamesRight());
                if (intersection < 0)
                    break; /// (left) ... (right)
                if (intersection > 0)
                    continue; /// (right) ... (left)
            }

            std::shared_ptr<Block> right_block = loadRightBlock<in_memory>(i);

            innerJoin(left_cursor, block, *right_block, left_columns, right_columns, left_key_tail);
        }

        left_cursor.nextN(left_key_tail);
        changeLeftColumns(block, std::move(left_columns));
        addRightColumns(block, std::move(right_columns));
    }
}

void MergeJoin::leftJoin(MergeJoinCursor & left_cursor, const Block & left_block, const Block & right_block,
                         MutableColumns & left_columns, MutableColumns & right_columns, size_t & left_key_tail)
{
    MergeJoinCursor right_cursor(right_block, right_merge_description);
    left_cursor.setCompareNullability(right_cursor);

    while (!left_cursor.atEnd() && !right_cursor.atEnd())
    {
        /// Not zero left_key_tail means there were equality for the last left key in previous leftJoin() call.
        /// Do not join it twice: join only if it's equal with a first right key of current leftJoin() call and skip otherwise.
        size_t left_unequal_position = left_cursor.position() + left_key_tail;
        left_key_tail = 0;

        Range range = left_cursor.getNextEqualRange(right_cursor);

        joinInequalsLeft(left_block, left_columns, right_columns, left_unequal_position, range.left_start, is_all);

        if (range.empty())
            break;

        if (is_all)
            joinEquals(left_block, right_block, right_columns_to_add, left_columns, right_columns, range, is_all);
        else
            joinEqualsAnyLeft(right_block, right_columns_to_add, right_columns, range);

        right_cursor.nextN(range.right_length);

        /// Do not run over last left keys for ALL JOIN (cause of possible duplicates in next right block)
        if (is_all && right_cursor.atEnd())
        {
            left_key_tail = range.left_length;
            break;
        }
        left_cursor.nextN(range.left_length);
    }
}

void MergeJoin::innerJoin(MergeJoinCursor & left_cursor, const Block & left_block, const Block & right_block,
                          MutableColumns & left_columns, MutableColumns & right_columns, size_t & left_key_tail)
{
    MergeJoinCursor right_cursor(right_block, right_merge_description);
    left_cursor.setCompareNullability(right_cursor);

    while (!left_cursor.atEnd() && !right_cursor.atEnd())
    {
        Range range = left_cursor.getNextEqualRange(right_cursor);
        if (range.empty())
            break;

        joinEquals(left_block, right_block, right_columns_to_add, left_columns, right_columns, range, is_all);
        right_cursor.nextN(range.right_length);

        /// Do not run over last left keys for ALL JOIN (cause of possible duplicates in next right block)
        if (is_all && right_cursor.atEnd())
        {
            left_key_tail = range.left_length;
            break;
        }
        left_cursor.nextN(range.left_length);
    }
}

void MergeJoin::changeLeftColumns(Block & block, MutableColumns && columns)
{
    if (is_left && !is_all)
        return;
    block.setColumns(std::move(columns));
}

void MergeJoin::addRightColumns(Block & block, MutableColumns && right_columns)
{
    for (size_t i = 0; i < right_columns_to_add.columns(); ++i)
    {
        const auto & column = right_columns_to_add.getByPosition(i);
        block.insert(ColumnWithTypeAndName{std::move(right_columns[i]), column.type, column.name});
    }
}

template <bool in_memory>
size_t MergeJoin::rightBlocksCount()
{
    if constexpr (!in_memory)
        return flushed_right_blocks.size();
    else
        return loaded_right_blocks.size();
}

template <bool in_memory>
std::shared_ptr<Block> MergeJoin::loadRightBlock(size_t pos)
{
    if constexpr (!in_memory)
    {
        auto load_func = [&]() -> std::shared_ptr<Block>
        {
            TemporaryFileStream input(flushed_right_blocks[pos]->path(), right_sample_block);
            return std::make_shared<Block>(input.block_in->read());
        };

        return cached_right_blocks->getOrSet(pos, load_func).first;
    }
    else
        return loaded_right_blocks[pos];
}

}
