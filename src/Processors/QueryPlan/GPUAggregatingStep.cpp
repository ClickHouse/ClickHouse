#include <Processors/QueryPlan/GPUAggregatingStep.h>

#if USE_GPU

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Block.h>
#include <GPU/GPUAccumulator.h>
#include <GPU/GPUTypeMapping.h>
#include <IO/Operators.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>

#include <algorithm>
#include <memory>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

std::optional<std::vector<GPU::GPUAggregationKind>> gpuAggregationsOf(const Aggregator::Params & params)
{
    std::vector<GPU::GPUAggregationKind> aggregations;
    aggregations.reserve(params.aggregates.size());

    for (const auto & aggregate : params.aggregates)
    {
        if (!aggregate.parameters.empty())
            return {};

        const auto aggregation = GPU::aggregationOf(aggregate.function->getName());
        if (!aggregation)
            return {};

        /// A `count` of no argument, and every other aggregate of one.
        if (aggregate.argument_names.size() != (*aggregation == GPU::GPUAggregationKind::Count ? 0 : 1))
            return {};

        aggregations.push_back(*aggregation);
    }

    return aggregations;
}

namespace
{

/// The aggregation on the device is a pipeline of processors, each a stage of the device side, see `GPUAccumulator.h`:
///
///     GPUUpload ×N -> resize N -> L -> GPUGrouping ×L -> resize L -> 1 -> GPUSplitLanes -> resize 1 -> P -> GPUMergePart ×P
///
/// A stream of the pipeline before is uploaded by one `GPUUpload`, the lanes take the batches of any stream, as the
/// resize hands them over, and each lane splits its groups by the keys into P parts, which are merged each on its own.
/// Without keys the batches are reduced by `GPUReduce`. The chunks between them have no columns: they carry a chunk
/// info, a batch on the device or the groups of a lane.

/// The lanes that group the batches, at most: as Sirius runs 4 to 8 tasks on a device at once, enough that one waiting
/// for the device leaves it to the others, and few, as each keeps groups of its own.
constexpr size_t max_lanes = 8;

SharedHeader emptyHeader()
{
    return std::make_shared<const Block>();
}

/// What the processors of one aggregation share.
struct Plan
{
    /// Of the input: the keys, and, without keys, the argument of each aggregate, or, with keys, each column some
    /// aggregates are of, once. The uploaders send the keys, then those.
    std::vector<size_t> key_positions;
    std::vector<size_t> argument_positions;
    DataTypes key_types;
    DataTypes argument_types;

    /// With keys.
    std::vector<GPU::GPUGroupByAggregate> aggregates;
    /// Without keys.
    std::vector<GPU::GPUAggregationKind> aggregations;
    DataTypes result_types;
    bool empty_result_for_empty_set = false;

    size_t num_aggregates = 0;
    size_t batch_bytes = 0;

    /// The parts the lanes split their groups into by the keys, each merged on its own.
    size_t num_parts = 1;

    DataTypes uploadTypes() const
    {
        DataTypes types = key_types;
        types.insert(types.end(), argument_types.begin(), argument_types.end());
        return types;
    }
};

using PlanPtr = std::shared_ptr<const Plan>;

std::shared_ptr<Plan> planOf(const Block & input_header, const Aggregator::Params & params, size_t batch_bytes)
{
    const auto aggregations = gpuAggregationsOf(params);
    if (!aggregations)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "A GPU aggregation of aggregate functions the device does not have reached the pipeline");

    auto plan = std::make_shared<Plan>();
    plan->num_aggregates = params.aggregates.size();
    plan->batch_bytes = batch_bytes;
    plan->empty_result_for_empty_set = params.empty_result_for_aggregation_by_empty_set;

    if (params.keys.empty())
    {
        for (const auto & aggregate : params.aggregates)
        {
            if (aggregate.argument_names.size() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation without keys of an aggregate of other than one argument");
            const String & argument_name = aggregate.argument_names.front();
            plan->argument_positions.push_back(input_header.getPositionByName(argument_name));
            plan->argument_types.push_back(input_header.getByName(argument_name).type);
            plan->result_types.push_back(aggregate.function->getResultType());
        }
        plan->aggregations = *aggregations;
        return plan;
    }

    for (const auto & key : params.keys)
    {
        plan->key_positions.push_back(input_header.getPositionByName(key));
        plan->key_types.push_back(input_header.getByName(key).type);
    }

    for (size_t i = 0; i < params.aggregates.size(); ++i)
    {
        const auto & aggregate = params.aggregates[i];
        const DataTypePtr result_type = aggregate.function->getResultType();

        if (aggregate.argument_names.empty())
        {
            plan->aggregates.push_back({
                .aggregation = (*aggregations)[i],
                .column = GPU::GPUGroupByAggregate::no_column,
                .result_type = GPU::groupByResultTypeOrThrow(nullptr, *result_type, (*aggregations)[i]),
            });
            continue;
        }

        const size_t position = input_header.getPositionByName(aggregate.argument_names.front());
        const DataTypePtr & argument_type = input_header.getByPosition(position).type;
        auto column = std::find(plan->argument_positions.begin(), plan->argument_positions.end(), position);
        if (column == plan->argument_positions.end())
        {
            plan->argument_positions.push_back(position);
            plan->argument_types.push_back(argument_type);
            column = std::prev(plan->argument_positions.end());
        }

        plan->aggregates.push_back({
            .aggregation = (*aggregations)[i],
            .column = static_cast<uint32_t>(column - plan->argument_positions.begin()),
            .result_type = GPU::groupByResultTypeOrThrow(argument_type.get(), *result_type, (*aggregations)[i]),
        });
    }

    return plan;
}

std::shared_ptr<GPU::GPUGroupingLane> makeLane(const Plan & plan, size_t num_parts)
{
    return std::make_shared<GPU::GPUGroupingLane>(plan.key_types, plan.argument_types, plan.aggregates, std::nullopt, num_parts);
}

struct DeviceBatchInfo : public ChunkInfoCloneable<DeviceBatchInfo>
{
    explicit DeviceBatchInfo(std::shared_ptr<GPU::GPUDeviceBatch> batch_) : batch(std::move(batch_)) { }
    std::shared_ptr<GPU::GPUDeviceBatch> batch;
};

struct LaneGroupsInfo : public ChunkInfoCloneable<LaneGroupsInfo>
{
    explicit LaneGroupsInfo(std::shared_ptr<GPU::GPUGroupingLane> lane_) : lane(std::move(lane_)) { }
    std::shared_ptr<GPU::GPUGroupingLane> lane;
};

using Lanes = std::vector<std::shared_ptr<GPU::GPUGroupingLane>>;

struct LanePartInfo : public ChunkInfoCloneable<LanePartInfo>
{
    LanePartInfo(std::shared_ptr<const Lanes> lanes_, size_t part_) : lanes(std::move(lanes_)), part(part_) { }
    std::shared_ptr<const Lanes> lanes;
    size_t part;
};

template <typename Info>
std::shared_ptr<Info> infoOrThrow(Chunk & chunk, std::string_view processor)
{
    auto info = chunk.getChunkInfos().extract<Info>();
    if (!info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A chunk without the chunk info that {} takes", processor);
    return info;
}

Chunk chunkOf(GPU::GPUDeviceBatch batch)
{
    const size_t rows = batch.rows();
    Chunk chunk(Columns{}, rows);
    chunk.getChunkInfos().add(std::make_shared<DeviceBatchInfo>(std::make_shared<GPU::GPUDeviceBatch>(std::move(batch))));
    return chunk;
}

/// The groups of a lane after `finalize`, of `num_groups`, as a chunk of the aggregation's header: the keys, then the
/// result of each aggregate.
Chunk chunkOfGroups(const Plan & plan, const Block & header, GPU::GPUGroupingLane & lane, size_t num_groups)
{
    if (num_groups == 0)
        return {};

    MutableColumns key_columns;
    key_columns.reserve(plan.key_types.size());
    for (size_t i = 0; i < plan.key_types.size(); ++i)
        key_columns.push_back(header.getByPosition(i).type->createColumn());

    MutableColumns aggregate_columns;
    aggregate_columns.reserve(plan.num_aggregates);
    for (size_t i = 0; i < plan.num_aggregates; ++i)
        aggregate_columns.push_back(header.getByPosition(plan.key_types.size() + i).type->createColumn());

    lane.copyGroupsTo(key_columns, aggregate_columns);

    Columns columns;
    columns.reserve(key_columns.size() + aggregate_columns.size());
    for (auto & column : key_columns)
        columns.push_back(std::move(column));
    for (auto & column : aggregate_columns)
        columns.push_back(std::move(column));
    return Chunk(std::move(columns), num_groups);
}

/// Stages the columns of the blocks of one stream in pinned memory, and sends them to the device as a batch per chunk.
class GPUUploadTransform : public IAccumulatingTransform
{
public:
    GPUUploadTransform(const SharedHeader & input_header, PlanPtr plan_)
        : IAccumulatingTransform(input_header, emptyHeader()), plan(std::move(plan_)), uploader(plan->uploadTypes(), plan->batch_bytes)
    {
    }

    String getName() const override { return "GPUUpload"; }

protected:
    void consume(Chunk chunk) override
    {
        if (chunk.getNumRows() == 0)
            return;

        const Columns & input = chunk.getColumns();
        Columns columns;
        columns.reserve(plan->key_positions.size() + plan->argument_positions.size());
        for (const size_t position : plan->key_positions)
            columns.push_back(input[position]->convertToFullIfWrapped());
        for (const size_t position : plan->argument_positions)
            columns.push_back(input[position]->convertToFullIfWrapped());

        if (uploader.wouldOverflow(columns))
            if (auto batch = uploader.send())
                setReadyChunk(chunkOf(std::move(*batch)));
        uploader.add(columns);
    }

    Chunk generate() override
    {
        if (sent_last)
            return {};
        sent_last = true;
        auto batch = uploader.send();
        return batch ? chunkOf(std::move(*batch)) : Chunk{};
    }

private:
    const PlanPtr plan;
    GPU::GPUBatchUploader uploader;
    bool sent_last = false;
};

/// A lane: groups the batches it takes on a CUDA stream of its own, and then emits its groups, for the merge.
class GPUGroupingTransform : public IAccumulatingTransform
{
public:
    explicit GPUGroupingTransform(PlanPtr plan_)
        : IAccumulatingTransform(emptyHeader(), emptyHeader()), plan(std::move(plan_)), lane(makeLane(*plan, plan->num_parts))
    {
    }

    String getName() const override { return "GPUGrouping"; }

protected:
    void consume(Chunk chunk) override { lane->add(std::move(*infoOrThrow<DeviceBatchInfo>(chunk, getName())->batch)); }

    Chunk generate() override
    {
        if (generated)
            return {};
        generated = true;

        Chunk chunk(Columns{}, 1);
        chunk.getChunkInfos().add(std::make_shared<LaneGroupsInfo>(lane));
        return chunk;
    }

private:
    const PlanPtr plan;
    const std::shared_ptr<GPU::GPUGroupingLane> lane;
    bool generated = false;
};

/// Takes the lanes, each of its groups split into parts by a hash of the keys, and emits a chunk per part, for the merges
/// of the parts, which run each on its own.
class GPUSplitLanesTransform : public IAccumulatingTransform
{
public:
    explicit GPUSplitLanesTransform(PlanPtr plan_) : IAccumulatingTransform(emptyHeader(), emptyHeader()), plan(std::move(plan_)) { }

    String getName() const override { return "GPUSplitLanes"; }

protected:
    void consume(Chunk chunk) override { lanes.push_back(infoOrThrow<LaneGroupsInfo>(chunk, getName())->lane); }

    Chunk generate() override
    {
        if (!all_lanes)
            all_lanes = std::make_shared<const Lanes>(std::move(lanes));
        if (next_part == plan->num_parts)
            return {};

        Chunk chunk(Columns{}, 1);
        chunk.getChunkInfos().add(std::make_shared<LanePartInfo>(all_lanes, next_part++));
        return chunk;
    }

private:
    const PlanPtr plan;
    Lanes lanes;
    std::shared_ptr<const Lanes> all_lanes;
    size_t next_part = 0;
};

/// Merges one part of the groups of every lane, on a CUDA stream of its own, and sends them to the host. The keys of
/// different parts differ, so the parts are the groups of the aggregation.
class GPUMergePartTransform : public IAccumulatingTransform
{
public:
    GPUMergePartTransform(PlanPtr plan_, SharedHeader output_header)
        : IAccumulatingTransform(emptyHeader(), std::move(output_header)), plan(std::move(plan_))
    {
    }

    String getName() const override { return "GPUMergePart"; }

protected:
    void consume(Chunk chunk) override
    {
        const auto info = infoOrThrow<LanePartInfo>(chunk, getName());
        const auto merged = makeLane(*plan, 1);
        for (const auto & lane : *info->lanes)
            merged->absorbPart(*lane, info->part);
        if (Chunk groups = chunkOfGroups(*plan, getOutputPort().getHeader(), *merged, merged->finalize()))
            setReadyChunk(std::move(groups));
    }

    Chunk generate() override { return {}; }

private:
    const PlanPtr plan;
};

/// Without keys: reduces the batches of every stream, and emits the result of each aggregate.
class GPUReduceTransform : public IAccumulatingTransform
{
public:
    GPUReduceTransform(PlanPtr plan_, SharedHeader output_header)
        : IAccumulatingTransform(emptyHeader(), std::move(output_header))
        , plan(std::move(plan_))
        , accumulator(plan->argument_types, plan->result_types, plan->aggregations)
    {
    }

    String getName() const override { return "GPUReduce"; }

protected:
    void consume(Chunk chunk) override
    {
        auto info = infoOrThrow<DeviceBatchInfo>(chunk, getName());
        total_rows += info->batch->rows();
        accumulator.add(std::move(*info->batch));
    }

    Chunk generate() override
    {
        if (generated)
            return {};
        generated = true;

        if (total_rows == 0 && plan->empty_result_for_empty_set)
            return {};

        const std::vector<Field> results = accumulator.finalize();
        const Block & header = getOutputPort().getHeader();
        Columns columns;
        columns.reserve(results.size());
        for (size_t i = 0; i < results.size(); ++i)
        {
            auto column = header.getByPosition(i).type->createColumn();
            column->insert(results[i]);
            columns.push_back(std::move(column));
        }
        return Chunk(std::move(columns), 1);
    }

private:
    const PlanPtr plan;
    GPU::GPUAccumulator accumulator;
    size_t total_rows = 0;
    bool generated = false;
};

}

GPUAggregatingStep::GPUAggregatingStep(const SharedHeader & input_header_, Aggregator::Params params_, size_t batch_bytes_)
    : ITransformingStep(
        input_header_,
        std::make_shared<const Block>(params_.getHeader(*input_header_, /*final=*/true)),
        getTraits())
    , params(std::move(params_))
    , batch_bytes(batch_bytes_)
{
}

bool GPUAggregatingStep::canRunOnDevice(const Block & input_header, const Aggregator::Params & params)
{
    if (params.only_merge || params.overflow_row)
        return false;

    if (params.max_rows_to_group_by != 0)
        return false;

    if (params.aggregates.empty())
        return false;

    const auto aggregations = gpuAggregationsOf(params);
    if (!aggregations)
        return false;

    DataTypes argument_types;
    DataTypes result_types;
    argument_types.reserve(params.aggregates.size());
    result_types.reserve(params.aggregates.size());

    bool any_argument = false;
    for (const auto & aggregate : params.aggregates)
    {
        result_types.push_back(aggregate.function->getResultType());
        if (aggregate.argument_names.empty())
        {
            argument_types.push_back(nullptr);
            continue;
        }

        const auto * argument = input_header.findByName(aggregate.argument_names.front());
        if (!argument)
            return false;

        argument_types.push_back(argument->type);
        any_argument = true;
    }

    /// The device groups rows by their keys along with at least one column of values.
    if (!any_argument)
        return false;

    if (params.keys.empty())
    {
        for (size_t i = 0; i < argument_types.size(); ++i)
        {
            if (!argument_types[i] || !GPU::canReduceOnDevice(*argument_types[i], *result_types[i], (*aggregations)[i]))
                return false;
        }

        return true;
    }

    DataTypes key_types;
    key_types.reserve(params.keys.size());

    for (const auto & key : params.keys)
    {
        const auto * key_column = input_header.findByName(key);
        if (!key_column)
            return false;

        key_types.push_back(key_column->type);
    }

    return GPU::canGroupByReduceOnDevice(key_types, argument_types, result_types, *aggregations);
}

void GPUAggregatingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.dropTotalsAndExtremes();

    const auto plan = planOf(*pipeline.getSharedHeader(), params, batch_bytes);
    const size_t num_lanes = std::clamp<size_t>(pipeline.getNumStreams(), 1, max_lanes);
    plan->num_parts = num_lanes;

    pipeline.addSimpleTransform([&](const SharedHeader & header) { return std::make_shared<GPUUploadTransform>(header, plan); });

    if (params.keys.empty())
    {
        pipeline.resize(1);
        pipeline.addSimpleTransform([&](const SharedHeader &) { return std::make_shared<GPUReduceTransform>(plan, getOutputHeader()); });
        return;
    }

    pipeline.resize(num_lanes);
    pipeline.addSimpleTransform([&](const SharedHeader &) { return std::make_shared<GPUGroupingTransform>(plan); });
    pipeline.resize(1);
    pipeline.addSimpleTransform([&](const SharedHeader &) { return std::make_shared<GPUSplitLanesTransform>(plan); });
    pipeline.resize(plan->num_parts);
    pipeline.addSimpleTransform([&](const SharedHeader &) { return std::make_shared<GPUMergePartTransform>(plan, getOutputHeader()); });
}

void GPUAggregatingStep::describeActions(FormatSettings & settings) const
{
    params.explain(settings);
    settings.out << settings.detail_prefix << "Batch: " << batch_bytes << " bytes\n";
}

void GPUAggregatingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
    map.add("Batch Bytes", batch_bytes);
}

void GPUAggregatingStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(params.getHeader(*input_headers.front(), /*final=*/true));
}

}

#endif
