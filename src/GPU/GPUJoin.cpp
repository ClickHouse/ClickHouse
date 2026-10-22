#include <GPU/GPUJoin.h>

#if USE_GPU

#include <GPU/CudfHashJoin.cuh>
#include <GPU/GPUResources.cuh>
#include <GPU/GPUTypeMapping.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <limits>
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

Block rightTableKeysOf(const TableJoin & table_join, const Block & right_sample_block, Block & payload)
{
    Block keys;
    JoinCommon::splitAdditionalColumns(table_join.getOnlyClause().key_names_right, right_sample_block, keys, payload);
    return keys;
}

std::optional<GPU::GPUElementType> integerKeyTypeOf(const IDataType & type)
{
    const auto element_type = GPU::elementTypeOf(type);
    if (!element_type || !GPU::isInteger(*element_type))
        return {};
    return element_type;
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

    /// The keys are compared as the bytes of integers of one type: a `Nullable` key has no such bytes for its nulls, and
    /// keys of two types would compare different bytes.
    const auto & left_key_type = left_sample_block.getByName(clause.key_names_left[0]).type;
    const auto & right_key_type = right_sample_block.getByName(clause.key_names_right[0]).type;
    return left_key_type->equals(*right_key_type) && integerKeyTypeOf(*left_key_type).has_value();
}

GPUHashJoin::GPUHashJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader left_sample_block_, SharedHeader right_sample_block_)
    : table_join(std::move(table_join_))
    , left_sample_block(*left_sample_block_)
    , right_sample_block(*right_sample_block_)
    , key_name_left(table_join->getOnlyClause().key_names_left[0])
    , key_name_right(table_join->getOnlyClause().key_names_right[0])
    , key_type(integerKeyTypeOf(*right_sample_block.getByName(key_name_right).type).value_or(GPU::GPUElementType::UInt8))
    , build_keys(GPU::GPUResources::get().compute)
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

    LOG_TRACE(
        log,
        "Joining on {} with {} column(s) of the right table, keys on a GPU",
        right_sample_block.getByName(key_name_right).type->getName(),
        build_payload_header.columns());
}

GPUHashJoin::~GPUHashJoin() = default;

void GPUHashJoin::checkTypesOfKeys(const Block & block) const
{
    JoinCommon::checkTypesOfKeys(
        block, table_join->getOnlyClause().key_names_left, right_sample_block, table_join->getOnlyClause().key_names_right);
}

bool GPUHashJoin::addBlockToJoin(const Block & block, size_t num_rows, JoinBuildContext context)
{
    if (num_rows == 0)
        return true;

    Block kept;
    kept.insert(block.getByName(key_name_right));
    for (const auto & column : build_payload_header)
        if (column.name != key_name_right)
            kept.insert(block.getByName(column.name));
    for (auto & column : kept)
        column.column = column.column->convertToFullIfWrapped()->convertToFullColumnIfSparse();

    {
        std::lock_guard lock(device_mutex);
        if (built)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A block of the right table after the build of a GPU hash join ended");
        build_rows.fetch_add(kept.rows(), std::memory_order_relaxed);
        build_bytes.fetch_add(kept.allocatedBytes(), std::memory_order_relaxed);
        build_blocks.push_back(std::move(kept));
    }

    if (!context.joinChecksLimits())
        return true;

    return table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void GPUHashJoin::finishBuildUnlocked()
{
    if (built)
        return;
    built = true;

    build_block = build_blocks.empty() ? Block{} : concatenateBlocks(build_blocks);
    build_blocks.clear();

    const size_t rows = build_block.rows();
    if (rows > static_cast<size_t>(std::numeric_limits<Int32>::max()))
        throw Exception(
            ErrorCodes::SET_SIZE_LIMIT_EXCEEDED, "The right table of a GPU hash join has {} rows, more than cuDF indexes", rows);

    if (rows != 0)
        build_keys.append(GPU::rawValuesOf(*build_block.getByName(key_name_right).column, rows, GPU::sizeOf(key_type)));
    hash_join = std::make_unique<GPU::CudfHashJoin>(
        key_type, GPU::DeviceFixedColumn{.element_type = key_type, .data = build_keys.data(), .rows = rows});
}

void GPUHashJoin::onBuildPhaseFinish()
{
    std::lock_guard lock(device_mutex);
    finishBuildUnlocked();
}

Block GPUHashJoin::assembleOutputBlock(const Block & probe_block, const ColumnPtr & probe_indices, const ColumnPtr & build_indices) const
{
    Block result;
    for (const auto & column : probe_block)
    {
        ColumnWithTypeAndName output = column;
        output.column = column.column->convertToFullIfWrapped()->convertToFullColumnIfSparse()->index(*probe_indices, 0);
        result.insert(std::move(output));
    }

    for (const auto & sample : build_payload_header)
    {
        ColumnPtr gathered = build_indices->empty() || build_block.empty() ? sample.column->cloneEmpty()
                                                                    : build_block.getByName(sample.name).column->index(*build_indices, 0);
        result.insert({std::move(gathered), sample.type, table_join->renamedRightColumnName(sample.name)});
    }

    for (size_t i = 0; i < required_right_keys.columns(); ++i)
    {
        const auto & right_key = required_right_keys.getByPosition(i);
        const ColumnWithTypeAndName & left_key = result.getByName(required_right_keys_sources[i]);

        if (!right_key.type->equals(*left_key.type))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The GPU hash join joined {} of {} to {} of {}",
                left_key.name,
                left_key.type->getName(),
                right_key.name,
                right_key.type->getName());

        result.insert({left_key.column, right_key.type, table_join->renamedRightColumnName(right_key.name)});
    }

    return result;
}

JoinResultPtr GPUHashJoin::joinBlock(Block block)
{
    const size_t probe_rows = block.rows();
    probe_rows_total.fetch_add(probe_rows, std::memory_order_relaxed);

    auto probe_indices = ColumnUInt32::create();
    auto build_indices = ColumnUInt32::create();

    if (probe_rows != 0)
    {
        const ColumnPtr probe_key = block.getByName(key_name_left).column->convertToFullIfWrapped()->convertToFullColumnIfSparse();

        {
            std::lock_guard lock(device_mutex);
            finishBuildUnlocked();
        }

        /// Each thread probes on a stream of its own, so that the blocks of the threads are probed at once.
        thread_local GPU::StreamPtr thread_stream = GPU::createStream();
        const cuda::stream_ref stream{thread_stream.get()};

        GPU::DeviceBuffer probe_keys(stream);
        probe_keys.append(GPU::rawValuesOf(*probe_key, probe_rows, GPU::sizeOf(key_type)));
        const GPU::CudfJoinMatches matches
            = hash_join->probe(GPU::DeviceFixedColumn{.element_type = key_type, .data = probe_keys.data(), .rows = probe_rows}, stream);

        probe_indices->getData().resize(matches.size());
        build_indices->getData().resize(matches.size());
        matches.copyOut(probe_indices->getData().data(), build_indices->getData().data(), stream);
    }

    const size_t build_total = build_block.rows();
    for (size_t i = 0; i < probe_indices->size(); ++i)
    {
        if (probe_indices->getData()[i] >= probe_rows || build_indices->getData()[i] >= build_total)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The device returned the pair of rows {} and {} for a left block of {} rows and a right table of {}",
                probe_indices->getData()[i],
                build_indices->getData()[i],
                probe_rows,
                build_total);
    }

    return IJoinResult::createFromBlock(assembleOutputBlock(block, std::move(probe_indices), std::move(build_indices)));
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
