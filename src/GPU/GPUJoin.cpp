#include <GPU/GPUJoin.h>

#if USE_GPU

#include <GPU/GPUTypeMapping.h>

#include <Columns/ColumnsNumber.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <mutex>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

namespace
{
bool everyColumnIsFixedWidthNumeric(const Block & block)
{
    for (const auto & column : block)
    {
        if (!GPU::elementTypeOf(*column.type))
            return false;
    }

    return true;
}

Block rightTableKeysOf(const TableJoin & table_join, const Block & right_sample_block, Block & payload)
{
    Block keys;
    JoinCommon::splitAdditionalColumns(table_join.getOnlyClause().key_names_right, right_sample_block, keys, payload);
    return keys;
}

}

bool GPUHashJoin::isSupported(const TableJoin & table_join, const Block & left_sample_block, const Block & right_sample_block)
{
    if (table_join.kind() != JoinKind::Inner || table_join.strictness() != JoinStrictness::All)
        return false;

    if (!table_join.oneDisjunct() || table_join.getMixedJoinExpression())
        return false;

    const auto & clause = table_join.getOnlyClause();
    if (clause.key_names_left.size() != 1 || clause.key_names_right.size() != 1)
        return false;
    if (clause.on_filter_condition_left || clause.on_filter_condition_right)
        return false;

    if (table_join.isSpecialStorage())
        return false;

    if (!left_sample_block.has(clause.key_names_left[0]) || !right_sample_block.has(clause.key_names_right[0]))
        return false;

    const auto & left_key_type = left_sample_block.getByName(clause.key_names_left[0]).type;
    const auto & right_key_type = right_sample_block.getByName(clause.key_names_right[0]).type;

    if (!left_key_type->equals(*right_key_type))
        return false;

    const auto key_element_type = GPU::elementTypeOf(*left_key_type);
    if (!key_element_type || !GPU::isInteger(*key_element_type))
        return false;

    return everyColumnIsFixedWidthNumeric(left_sample_block) && everyColumnIsFixedWidthNumeric(right_sample_block);
}

GPUHashJoin::GPUHashJoin(
    std::shared_ptr<TableJoin> table_join_, SharedHeader left_sample_block_, SharedHeader right_sample_block_)
    : table_join(std::move(table_join_))
    , left_sample_block(*left_sample_block_)
    , right_sample_block(*right_sample_block_)
    , key_name_left(table_join->getOnlyClause().key_names_left[0])
    , key_name_right(table_join->getOnlyClause().key_names_right[0])
    , log(getLogger("GPUHashJoin"))
{
    if (!isSupported(*table_join, left_sample_block, right_sample_block))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot execute this {} {} JOIN on a GPU",
            toString(table_join->strictness()),
            toString(table_join->kind()));

    const Block right_table_keys = rightTableKeysOf(*table_join, right_sample_block, build_payload_header);
    required_right_keys = table_join->getRequiredRightKeys(right_table_keys, required_right_keys_sources);

    JoinCommon::createMissedColumns(build_payload_header);

    DataTypes payload_types;
    payload_types.reserve(build_payload_header.columns());
    for (const auto & column : build_payload_header)
        payload_types.push_back(column.type);

    hash_table.emplace(*right_sample_block.getByName(key_name_right).type, payload_types);

    LOG_TRACE(
        log,
        "Joining on {} with {} column(s) of the right table on a GPU",
        right_sample_block.getByName(key_name_right).type->getName(),
        build_payload_header.columns());
}

void GPUHashJoin::checkTypesOfKeys(const Block & block) const
{
    JoinCommon::checkTypesOfKeys(
        block, table_join->getOnlyClause().key_names_left, right_sample_block, table_join->getOnlyClause().key_names_right);
}

bool GPUHashJoin::addBlockToJoin(const Block & block, bool check_limits)
{
    const size_t num_rows = block.rows();

    std::lock_guard lock(device_mutex);

    if (num_rows == 0)
        return true;

    const ColumnPtr key_column = block.getByName(key_name_right).column->convertToFullIfWrapped();

    Columns payload_columns;
    payload_columns.reserve(build_payload_header.columns());
    for (const auto & column : build_payload_header)
        payload_columns.push_back(block.getByName(column.name).column->convertToFullIfWrapped());

    hash_table->addBuildBlock(*key_column, payload_columns);

    build_rows.store(hash_table->buildRows(), std::memory_order_relaxed);
    build_bytes.store(hash_table->buildBytes(), std::memory_order_relaxed);

    if (!check_limits)
        return true;

    return table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void GPUHashJoin::finishBuildUnlocked()
{
    hash_table->finishBuild();
}

void GPUHashJoin::onBuildPhaseFinish()
{
    std::lock_guard lock(device_mutex);
    finishBuildUnlocked();
}

Block GPUHashJoin::assembleOutputBlock(const Block & probe_block, const ColumnPtr & probe_indices, Columns gathered_payloads) const
{
    Block probe_part;
    for (const auto & column : probe_block)
    {
        ColumnWithTypeAndName output = column;
        output.column = column.column->index(*probe_indices, 0);
        probe_part.insert(std::move(output));
    }

    Block result = probe_part;

    for (size_t i = 0; i < build_payload_header.columns(); ++i)
    {
        const auto & sample = build_payload_header.getByPosition(i);
        result.insert({gathered_payloads[i], sample.type, table_join->renamedRightColumnName(sample.name)});
    }

    for (size_t i = 0; i < required_right_keys.columns(); ++i)
    {
        const auto & right_key = required_right_keys.getByPosition(i);
        const ColumnWithTypeAndName & left_key = probe_part.getByName(required_right_keys_sources[i]);

        if (!right_key.type->equals(*left_key.type))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The GPU hash join joined {} of {} to {} of {}",
                left_key.name,
                left_key.type->getName(),
                right_key.name,
                right_key.type->getName());

        result.insert(
            {left_key.column->convertToFullColumnIfConst(), right_key.type, table_join->renamedRightColumnName(right_key.name)});
    }

    return result;
}

JoinResultPtr GPUHashJoin::joinBlock(Block block)
{
    const size_t probe_rows = block.rows();
    probe_rows_total.fetch_add(probe_rows, std::memory_order_relaxed);

    GPU::HashTable::Matches matches = hash_table->noMatches();

    if (probe_rows != 0)
    {
        std::lock_guard lock(device_mutex);

        finishBuildUnlocked();

        const ColumnPtr probe_key = block.getByName(key_name_left).column->convertToFullIfWrapped();
        matches = hash_table->probe(*probe_key);
    }

    for (const UInt32 index : matches.probe_row_indices->getData())
    {
        if (index >= probe_rows)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The device returned row index {} for a block of {} rows of the left table",
                index,
                probe_rows);
    }

    Columns gathered_payloads;
    gathered_payloads.reserve(matches.build_payload_columns.size());
    for (auto & column : matches.build_payload_columns)
        gathered_payloads.push_back(std::move(column));

    return IJoinResult::createFromBlock(
        assembleOutputBlock(block, std::move(matches.probe_row_indices), std::move(gathered_payloads)));
}

StepAnalysisReport GPUHashJoin::getAnalysisReport() const
{
    StepAnalysisReport report;
    report.push_back({MetricGroupKey::Left, joinSideMetrics(probe_rows_total.load(std::memory_order_relaxed), std::nullopt)});
    report.push_back({MetricGroupKey::Right, joinSideMetrics(getTotalRowCount(), std::nullopt)});
    return report;
}

}

#endif
