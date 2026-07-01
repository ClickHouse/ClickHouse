#include <Interpreters/ConstantJoin.h>

#include <algorithm>
#include <limits>
#include <vector>

#include <base/arithmeticOverflow.h>

#include <Columns/ColumnReplicated.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Core/Joins.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

namespace
{

Block cutSingleRow(const Block & block, size_t row)
{
    Block result;
    for (const auto & column : block)
        result.insert({column.column->cut(row, 1), column.type, column.name});
    return result;
}

bool useLastRightRow(JoinKind kind, JoinStrictness strictness, bool any_take_last_row)
{
    return any_take_last_row && strictness == JoinStrictness::Any && !isRight(kind);
}

}

size_t ConstantJoin::StoredBlock::allocatedBytes() const
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

ConstantJoin::ConstantJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader right_sample_block_, bool any_take_last_row_)
    : table_join(std::move(table_join_))
    , right_sample_block(*right_sample_block_)
    , tmp_data(table_join->getTempDataOnDisk())
    , any_take_last_row(any_take_last_row_)
    , max_joined_block_rows(table_join->maxJoinedBlockRows())
    , max_joined_block_bytes(table_join->maxJoinedBlockBytes())
    , log(getLogger("ConstantJoin"))
{
    bool is_cross_or_comma = isCrossOrComma(table_join->kind());
    bool is_join_with_constant = table_join->isJoinWithConstant();
    bool is_no_key_join = table_join->getClauses().empty();
    if (!is_cross_or_comma && !is_join_with_constant && !is_no_key_join)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ConstantJoin expects CROSS, comma or JOIN ON constant, got {}", table_join->kind());

    bool is_no_clause_join = is_no_key_join && !table_join->hasOn() && !table_join->hasUsing();

    if (is_cross_or_comma || is_no_clause_join)
        predicate_kind = PredicateKind::True;
    else if (is_join_with_constant && table_join->oneDisjunct())
        predicate_kind = PredicateKind::CompareConstantKeys;
    else
        predicate_kind = PredicateKind::False;

    for (auto & column : right_sample_block)
    {
        if (!column.column)
            column.column = column.type->createColumn();
    }

    if (predicate_kind == PredicateKind::CompareConstantKeys)
    {
        const auto & clause = table_join->getOnlyClause();
        if (clause.key_names_left.size() != 1 || clause.key_names_right.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ConstantJoin expects one constant key on each join side");

        left_constant_key_name = clause.key_names_left.front();
        right_constant_key_name = clause.key_names_right.front();

        Block right_constant_key;
        JoinCommon::splitAdditionalColumns(clause.key_names_right, right_sample_block, right_constant_key, sample_block_with_columns_to_add);
    }
    else
        sample_block_with_columns_to_add = materializeBlock(right_sample_block);

    JoinCommon::createMissedColumns(sample_block_with_columns_to_add);
    saved_block_sample = sample_block_with_columns_to_add.cloneEmpty();

    LOG_TEST(log, "Right header: {}", right_sample_block.dumpStructure());
}

bool ConstantJoin::addBlockToJoin(const Block & source_block, bool check_limits)
{
    return addBlockToJoin(source_block, source_block.rows(), check_limits);
}

bool ConstantJoin::addBlockToJoin(const Block & source_block, size_t num_rows, bool check_limits)
{
    if (num_rows && right_constant_key_name && !right_constant_key_value)
        right_constant_key_value = source_block.getByName(*right_constant_key_name).column->operator[](0);

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

    if (rows)
    {
        /// `SEMI` and old `ANY` (`RightAny`) use the first right row. New `ANY` may use the last one,
        /// depending on the setting.
        bool take_last_right_row = useLastRightRow(table_join->kind(), table_join->strictness(), any_take_last_row);
        if (take_last_right_row || !selected_right_columns_info)
            selected_right_columns_info.emplace(cutSingleRow(block_to_save, take_last_right_row ? rows - 1 : 0).getColumns());
    }

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

void ConstantJoin::doDebugAsserts() const
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

size_t ConstantJoin::getTotalByteCount() const
{
    doDebugAsserts();
    return allocated_size;
}

void ConstantJoin::shrinkStoredBlocksToFit(size_t & total_bytes_in_join, bool force_optimize)
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

Block ConstantJoin::materializeColumnsFromRightBlock(Block block) const
{
    return JoinCommon::materializeColumnsFromRightBlock(std::move(block), saved_block_sample);
}

bool ConstantJoin::constantPredicateMatches(const Block & left_block)
{
    switch (predicate_kind)
    {
        case PredicateKind::True:
            return true;
        case PredicateKind::False:
            return false;
        case PredicateKind::CompareConstantKeys:
            break;
    }

    if (!left_constant_key_name || !right_constant_key_value || left_block.rows() == 0)
        return false;

    Int32 cached_match = constant_predicate_match.load(std::memory_order_acquire);
    if (cached_match != -1)
        return cached_match == 1;

    /// The left key is a dummy constant column produced by the join ActionsDAG, so after the first non-empty
    /// left block its value is fixed for the whole join.
    bool matches = left_block.getByName(*left_constant_key_name).column->operator[](0) == *right_constant_key_value;
    constant_predicate_match.store(matches ? 1 : 0, std::memory_order_release);
    return matches;
}

namespace
{

enum class MatchingRowsOutput
{
    None,
    All,
    AllLeftRowsWithFirstRightRow,
    AllLeftRowsWithLastRightRow,
    FirstLeftRowWithAllRightRows,
    FirstLeftRowWithFirstRightRow,
    FirstLeftRowWithLastRightRow,
};

struct ConstantJoinOutputFlags
{
    MatchingRowsOutput output_matching_rows = MatchingRowsOutput::None;
    bool output_non_matching_left_rows = false;
    bool output_non_matching_right_rows = false;
};

ConstantJoinOutputFlags makeConstantJoinOutputFlags(
    JoinKind kind,
    JoinStrictness strictness,
    bool has_match,
    bool has_matched_right_rows,
    bool any_take_last_row)
{
    const bool is_right_semi = kind == JoinKind::Right && strictness == JoinStrictness::Semi;
    const bool is_right_anti = kind == JoinKind::Right && strictness == JoinStrictness::Anti;

    ConstantJoinOutputFlags flags;
    if (isCrossOrComma(kind))
    {
        if (has_match)
            flags.output_matching_rows = MatchingRowsOutput::All;
        return flags;
    }

    if (has_match)
    {
        if (strictness == JoinStrictness::Any)
        {
            /// Intentionally mirrors `HashJoin`: `join_any_take_last_row` affects modes that select one right row,
            /// but SQL `RIGHT ANY` joins the first matched left row with all right rows.
            if (isRight(kind))
                flags.output_matching_rows = MatchingRowsOutput::FirstLeftRowWithAllRightRows;
            else if (isInner(kind))
                flags.output_matching_rows = any_take_last_row
                    ? MatchingRowsOutput::FirstLeftRowWithLastRightRow
                    : MatchingRowsOutput::FirstLeftRowWithFirstRightRow;
            else
                flags.output_matching_rows = any_take_last_row
                    ? MatchingRowsOutput::AllLeftRowsWithLastRightRow
                    : MatchingRowsOutput::AllLeftRowsWithFirstRightRow;
        }
        else if (strictness == JoinStrictness::RightAny)
        {
            /// Intentionally mirrors `HashJoin`: old `ANY` assumes distinct right keys and emits one right row for
            /// each matching left row. `RIGHT` and `FULL` unmatched right rows are handled by
            /// `output_non_matching_right_rows`.
            flags.output_matching_rows = MatchingRowsOutput::AllLeftRowsWithFirstRightRow;
        }
        else if (strictness == JoinStrictness::All)
            flags.output_matching_rows = MatchingRowsOutput::All;
        else if (strictness == JoinStrictness::Semi)
            flags.output_matching_rows = is_right_semi ? MatchingRowsOutput::FirstLeftRowWithAllRightRows : MatchingRowsOutput::AllLeftRowsWithFirstRightRow;
    }

    flags.output_non_matching_left_rows =
        (strictness == JoinStrictness::Anti && !has_match && !is_right_anti)
        || (strictness != JoinStrictness::Semi && strictness != JoinStrictness::Anti && !has_match && isLeftOrFull(kind));
    flags.output_non_matching_right_rows = isRightOrFull(kind)
        && !is_right_semi
        && !has_matched_right_rows;

    return flags;
}

}

bool ConstantJoin::alwaysReturnsEmptySet() const
{
    const auto kind = table_join->kind();
    const auto strictness = table_join->strictness();

    auto can_output = [&](bool has_match, bool has_matched_right_rows)
    {
        const auto output_flags = makeConstantJoinOutputFlags(kind, strictness, has_match, has_matched_right_rows, any_take_last_row);
        return output_flags.output_matching_rows != MatchingRowsOutput::None
            || output_flags.output_non_matching_left_rows
            || (total_rows_to_join != 0 && output_flags.output_non_matching_right_rows);
    };

    auto predicate_always_returns_empty_set = [&](bool predicate_matches)
    {
        if (!predicate_matches || total_rows_to_join == 0)
            return !can_output(/* has_match */ false, /* has_matched_right_rows */ false);

        /// Consider all left-side cardinalities. For example, `RIGHT SEMI JOIN ON 1` can emit
        /// right rows only after at least one left row was seen, while `RIGHT ANTI JOIN ON 1`
        /// can emit right rows only when no left rows were seen.
        return !can_output(/* has_match */ true, /* has_matched_right_rows */ true)
            && !can_output(/* has_match */ false, /* has_matched_right_rows */ false)
            && !can_output(/* has_match */ false, /* has_matched_right_rows */ true);
    };

    switch (predicate_kind)
    {
        case PredicateKind::True:
            return predicate_always_returns_empty_set(true);
        case PredicateKind::False:
            return predicate_always_returns_empty_set(false);
        case PredicateKind::CompareConstantKeys:
        {
            const auto cached_match = constant_predicate_match.load(std::memory_order_acquire);
            if (cached_match == -1)
                return false;

            return predicate_always_returns_empty_set(cached_match == 1);
        }
    }

    UNREACHABLE();
}

class ConstantJoinResult final : public IJoinResult
{
public:
    ConstantJoinResult(const ConstantJoin & join_, Block block_, bool has_match_)
        : join(join_)
        , block(std::move(block_))
        , output_flags(makeConstantJoinOutputFlags(
            join.table_join->kind(),
            join.table_join->strictness(),
            has_match_,
            join.right_rows_matched.load(),
            join.any_take_last_row))
    {
        left_positions.reserve(block.columns());
        for (size_t i = 0; i != block.columns(); ++i)
        {
            if (join.left_constant_key_name && block.getByPosition(i).name == *join.left_constant_key_name)
                continue;
            left_positions.push_back(i);
        }
    }

    JoinResultBlock next() override;

private:
    const ConstantJoin & join;
    Block block;
    ConstantJoinOutputFlags output_flags;
    std::vector<size_t> left_positions;
    size_t left_row = 0;
    std::optional<ConstantJoin::StoredBlocks::const_iterator> right_block_it;
    std::optional<TemporaryBlockStreamReaderHolder> reader;
};

IJoinResult::JoinResultBlock ConstantJoinResult::next()
{
    size_t num_existing_columns = left_positions.size();
    size_t num_columns_to_add = join.sample_block_with_columns_to_add.columns();

    ColumnRawPtrs src_left_columns;
    MutableColumns dst_columns;
    Block result_sample;

    {
        src_left_columns.reserve(num_existing_columns);
        dst_columns.reserve(num_existing_columns + num_columns_to_add);

        for (size_t left_position : left_positions)
        {
            const auto & left_column = block.getByPosition(left_position);
            result_sample.insert(left_column);
            src_left_columns.push_back(left_column.column.get());
            dst_columns.emplace_back(src_left_columns.back()->cloneEmpty());
        }

        for (const ColumnWithTypeAndName & right_column : join.sample_block_with_columns_to_add)
        {
            result_sample.insert(right_column);
            dst_columns.emplace_back(right_column.column->cloneEmpty());
        }

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

    auto update_bytes = [&]()
    {
        if (!join.max_joined_block_bytes)
            return;

        bytes_added = 0;
        for (const auto & dst : dst_columns)
            bytes_added += dst->byteSize();
    };

    auto insert_left_rows = [&](size_t rows)
    {
        for (size_t col_num = 0; col_num < num_existing_columns; ++col_num)
            dst_columns[col_num]->insertManyFrom(*src_left_columns[col_num], left_row, rows);
    };

    auto insert_right_defaults = [&](size_t rows)
    {
        for (size_t col_num = 0; col_num < num_columns_to_add; ++col_num)
        {
            const auto & right_column = join.sample_block_with_columns_to_add.getByPosition(col_num);
            JoinCommon::addDefaultValues(*dst_columns[num_existing_columns + col_num], right_column.type, rows);
        }
    };

    auto process_right_block = [&](const ColumnsInfo & columns_info, size_t rows_right)
    {
        rows_added += rows_right;
        insert_left_rows(rows_right);

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

        update_bytes();
    };

    auto process_default_right = [&]()
    {
        ++rows_added;
        insert_left_rows(1);
        insert_right_defaults(1);
        update_bytes();
    };

    auto process_selected_right_row = [&](const std::optional<ColumnsInfo> & columns_info)
    {
        if (!columns_info)
            return;
        process_right_block(*columns_info, 1);
    };

    if (output_flags.output_matching_rows == MatchingRowsOutput::None && !output_flags.output_non_matching_left_rows)
        left_row = rows_total;

    for (; left_row < rows_total; ++left_row)
    {
        if (enough_data())
            break;

        if (output_flags.output_non_matching_left_rows)
        {
            process_default_right();
            continue;
        }

        if (output_flags.output_matching_rows == MatchingRowsOutput::AllLeftRowsWithFirstRightRow
            || output_flags.output_matching_rows == MatchingRowsOutput::FirstLeftRowWithFirstRightRow
            || output_flags.output_matching_rows == MatchingRowsOutput::AllLeftRowsWithLastRightRow
            || output_flags.output_matching_rows == MatchingRowsOutput::FirstLeftRowWithLastRightRow)
        {
            process_selected_right_row(join.selected_right_columns_info);
            continue;
        }

        chassert(
            output_flags.output_matching_rows == MatchingRowsOutput::All
            || output_flags.output_matching_rows == MatchingRowsOutput::FirstLeftRowWithAllRightRows);
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

    bool is_last = left_row >= rows_total;
    auto res = result_sample.cloneWithColumns(std::move(dst_columns));
    return {res, nullptr, is_last};
}

class ConstantJoinNotJoinedRightFiller final : public NotJoinedBlocks::RightColumnsFiller
{
public:
    ConstantJoinNotJoinedRightFiller(const ConstantJoin & join_, UInt64 max_block_size_)
        : join(join_)
        , max_block_size(max_block_size_ ? max_block_size_ : std::numeric_limits<UInt64>::max())
    {
    }

    Block getEmptyBlock() override { return join.sample_block_with_columns_to_add.cloneEmpty(); }

    size_t fillColumns(MutableColumns & columns_right) override
    {
        size_t rows_added = 0;

        auto insert_rows = [&](const ColumnsInfo & columns_info, size_t start, size_t rows)
        {
            for (size_t col_num = 0; col_num < columns_right.size(); ++col_num)
            {
                if (const auto * replicated_column_right = columns_info.replicated_columns[col_num])
                {
                    for (size_t row = start; row != start + rows; ++row)
                        columns_right[col_num]->insertFrom(
                            *replicated_column_right->getNestedColumn(),
                            replicated_column_right->getIndexes().getIndexAt(row));
                }
                else
                    columns_right[col_num]->insertRangeFrom(*columns_info.columns[col_num], start, rows);
            }
            rows_added += rows;
        };

        if (!right_block_it)
            right_block_it = join.right_blocks.begin();

        for (; *right_block_it != join.right_blocks.end() && rows_added < max_block_size; ++*right_block_it)
        {
            const auto & stored_block = **right_block_it;
            size_t rows_available = stored_block.rows - right_block_offset;
            size_t rows_to_take = std::min<size_t>(rows_available, max_block_size - rows_added);
            if (rows_to_take == 0)
            {
                right_block_offset = 0;
                continue;
            }

            if (!join.have_compressed)
                insert_rows(stored_block.columns_info, right_block_offset, rows_to_take);
            else
            {
                Columns new_columns;
                new_columns.reserve(stored_block.columns_info.columns.size());
                for (const auto & column : stored_block.columns_info.columns)
                    new_columns.emplace_back(column->decompress());

                insert_rows(ColumnsInfo(std::move(new_columns)), right_block_offset, rows_to_take);
            }

            right_block_offset += rows_to_take;
            if (right_block_offset != stored_block.rows)
                return rows_added;

            right_block_offset = 0;
        }

        if (*right_block_it != join.right_blocks.end())
            return rows_added;

        if (!join.tmp_stream)
            return rows_added;

        if (!reader)
            reader = join.tmp_stream->getReadStream();

        while (reader && rows_added < max_block_size)
        {
            if (!current_spilled_columns_info)
            {
                auto block_right = reader.value()->read();
                if (block_right.empty())
                {
                    reader.reset();
                    break;
                }

                current_spilled_rows = block_right.rows();
                current_spilled_columns_info.emplace(block_right.getColumns());
            }

            size_t rows_available = current_spilled_rows - right_block_offset;
            size_t rows_to_take = std::min<size_t>(rows_available, max_block_size - rows_added);
            if (rows_to_take == 0)
            {
                current_spilled_columns_info.reset();
                current_spilled_rows = 0;
                right_block_offset = 0;
                continue;
            }

            insert_rows(*current_spilled_columns_info, right_block_offset, rows_to_take);

            right_block_offset += rows_to_take;
            if (right_block_offset != current_spilled_rows)
                return rows_added;

            current_spilled_columns_info.reset();
            current_spilled_rows = 0;
            right_block_offset = 0;
        }

        return rows_added;
    }

private:
    const ConstantJoin & join;
    UInt64 max_block_size;
    std::optional<ConstantJoin::StoredBlocks::const_iterator> right_block_it;
    size_t right_block_offset = 0;
    std::optional<TemporaryBlockStreamReaderHolder> reader;
    std::optional<ColumnsInfo> current_spilled_columns_info;
    size_t current_spilled_rows = 0;
};

JoinResultPtr ConstantJoin::joinBlock(Block block)
{
    bool has_match = constantPredicateMatches(block) && total_rows_to_join != 0;
    if (has_match && block.rows())
    {
        const auto kind = table_join->kind();
        const auto strictness = table_join->strictness();
        if ((kind == JoinKind::Right && (strictness == JoinStrictness::Any || strictness == JoinStrictness::Semi))
            || (kind == JoinKind::Inner && strictness == JoinStrictness::Any))
        {
            bool expected = false;
            if (right_rows_matched.compare_exchange_strong(expected, true))
                block = cutSingleRow(block, 0);
            else
                block = block.cloneEmpty();
        }
        else
            right_rows_matched = true;
    }

    return std::make_unique<ConstantJoinResult>(*this, std::move(block), has_match);
}

IBlocksStreamPtr ConstantJoin::getNonJoinedBlocks(const Block &, const Block & result_sample_block, UInt64 max_block_size) const
{
    auto kind = table_join->kind();
    if (!isRightOrFull(kind) || total_rows_to_join == 0)
        return {};

    auto output_flags = makeConstantJoinOutputFlags(
        kind,
        table_join->strictness(),
        /* has_match */ false,
        right_rows_matched.load(),
        any_take_last_row);

    if (!output_flags.output_non_matching_right_rows)
        return {};

    auto filler = std::make_unique<ConstantJoinNotJoinedRightFiller>(*this, max_block_size);
    size_t left_columns_count = result_sample_block.columns() - sample_block_with_columns_to_add.columns();
    return std::make_shared<NotJoinedBlocks>(std::move(filler), result_sample_block, left_columns_count, *table_join);
}

}
