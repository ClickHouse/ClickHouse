#include <Processors/Executors/Runtime/benchmarks/Shapes.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/Impl/NullFormat.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/Sources/DelayedSource.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/VectorWithMemoryTracking.h>

#include <algorithm>
#include <numeric>

namespace DB::Runtime::Bench
{

namespace
{

SharedHeader numbersHeader()
{
    return std::make_shared<const Block>(Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number")});
}

Chunk numbersChunk(size_t rows_per_chunk, UInt64 first, UInt64 stride)
{
    auto column = ColumnUInt64::create(rows_per_chunk);
    auto & values = column->getData();
    for (size_t row = 0; row < rows_per_chunk; ++row)
        values[row] = first + row * stride;
    return Chunk(Columns{std::move(column)}, rows_per_chunk);
}

Chunks numbers(size_t chunks, size_t rows_per_chunk, UInt64 first, UInt64 stride = 1)
{
    Chunks result;
    for (size_t index = 0; index < chunks; ++index)
        result.push_back(numbersChunk(rows_per_chunk, first + index * rows_per_chunk * stride, stride));
    return result;
}

Pipe numbersPipe(size_t streams, Load load)
{
    const size_t chunks_per_stream = std::max<size_t>(1, load.chunks / streams);
    const UInt64 stride = load.interleaved ? streams : 1;

    Pipes pipes;
    for (size_t stream = 0; stream < streams; ++stream)
    {
        const UInt64 first = load.interleaved ? stream : stream * chunks_per_stream * load.rows_per_chunk;
        pipes.emplace_back(std::make_shared<SourceFromChunks>(numbersHeader(), numbers(chunks_per_stream, load.rows_per_chunk, first, stride)));
    }
    return Pipe::unitePipes(std::move(pipes));
}

QueryPlan readNumbers(size_t streams, Load load, size_t width = 0)
{
    auto pipe = numbersPipe(streams, load);
    if (width > 0 && width != pipe.numOutputPorts())
        pipe.resize(width);

    QueryPlan plan;
    plan.addStep(std::make_unique<ReadFromPreparedSource>(std::move(pipe)));
    return plan;
}

void addFunction(QueryPlan & plan, ContextPtr context, const VectorWithMemoryTracking<String> & functions, const String & input_name, const String & output_name)
{
    ActionsDAG dag(plan.getCurrentHeader()->getNamesAndTypesList());
    const ActionsDAG::Node * node = &dag.findInOutputs(input_name);
    for (size_t index = 0; index < functions.size(); ++index)
    {
        const String name = index + 1 == functions.size() ? output_name : fmt::format("{}_{}", output_name, index);
        node = &dag.addFunction(FunctionFactory::instance().get(functions[index], context), {node}, name);
    }
    dag.getOutputs() = {node};
    plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(dag)));
}

void addLight(QueryPlan & plan, ContextPtr context, const String & input_name, const String & output_name)
{
    addFunction(plan, context, {"intHash64"}, input_name, output_name);
}

void addHeavy(QueryPlan & plan, ContextPtr context, const String & input_name, const String & output_name)
{
    addFunction(plan, context, {"murmurHash3_64"}, input_name, output_name);
}

void addModulo(QueryPlan & plan, ContextPtr context, const String & input_name, const String & output_name, UInt64 divisor)
{
    ActionsDAG dag(plan.getCurrentHeader()->getNamesAndTypesList());
    const auto type = std::make_shared<DataTypeUInt64>();
    const auto & constant = dag.addColumn(type->createColumnConst(1, divisor), type, "divisor");
    const auto & key = dag.addFunction(FunctionFactory::instance().get("modulo", context), {&dag.findInOutputs(input_name), &constant}, output_name);
    dag.getOutputs() = {&key};
    plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(dag)));
}

SortDescription sortedBy(const String & column_name)
{
    SortDescription description;
    description.emplace_back(column_name);
    return description;
}

void addMerge(QueryPlan & plan, const String & column_name, size_t max_block_size)
{
    plan.addStep(std::make_unique<SortingStep>(plan.getCurrentHeader(), sortedBy(column_name), SortingStep::Settings(max_block_size)));
}

void addLimit(QueryPlan & plan, UInt64 limit)
{
    plan.addStep(std::make_unique<LimitStep>(plan.getCurrentHeader(), limit, 0));
}

Aggregator::Params aggregationParams(const String & key_name, size_t max_block_size, size_t max_threads)
{
    return Aggregator::Params(Names{key_name}, AggregateDescriptions{}, false, max_threads, max_block_size, 0.5f, false, true);
}

void addAggregation(QueryPlan & plan, const String & key_name, size_t max_block_size, size_t max_threads)
{
    plan.addStep(std::make_unique<AggregatingStep>(
        plan.getCurrentHeader(), aggregationParams(key_name, max_block_size, max_threads), GroupingSetsParamsList{}, true, max_block_size, 0,
        max_threads, max_threads, false, false, SortDescription{}, SortDescription{}, false, false, false));
}

void addAggregationInOrder(QueryPlan & plan, const String & key_name, size_t max_block_size, size_t max_threads)
{
    const size_t in_order_max_block_bytes = 50'000'000;
    plan.addStep(std::make_unique<AggregatingStep>(
        plan.getCurrentHeader(), aggregationParams(key_name, max_block_size, max_threads), GroupingSetsParamsList{}, true, max_block_size,
        in_order_max_block_bytes, max_threads, max_threads, false, false, sortedBy(key_name), sortedBy(key_name), false, false, false));
}

QueryPipelineBuilderPtr buildPipeline(ContextPtr context, QueryPlan & plan)
{
    QueryPlanOptimizationSettings opt_settings(context->getSettingsRef(), 0, "", ExpressionActionsSettings(context), nullptr, false);
    return plan.buildQueryPipeline(opt_settings, BuildQueryPipelineSettings(context), false);
}

std::shared_ptr<Processors> finish(ContextPtr context, QueryPlan plan)
{
    auto builder = buildPipeline(context, plan);
    builder->resize(1);
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    pipeline.complete(std::make_shared<NullOutputFormat>(pipeline.getSharedHeader()));
    return std::make_shared<Processors>(pipeline.getProcessors());
}

}

std::shared_ptr<Processors> chain(ContextPtr context, Load load)
{
    auto plan = readNumbers(1, load);
    addLight(plan, context, "number", "hash");
    addHeavy(plan, context, "hash", "digest");
    addLimit(plan, load.chunks * load.rows_per_chunk);
    return finish(context, std::move(plan));
}

std::shared_ptr<Processors> fanOut(ContextPtr context, size_t streams, Load load)
{
    auto plan = readNumbers(1, load, streams);
    addHeavy(plan, context, "number", "digest");
    return finish(context, std::move(plan));
}

std::shared_ptr<Processors> fanIn(ContextPtr context, size_t streams, Load load)
{
    auto plan = readNumbers(streams, load);
    addHeavy(plan, context, "number", "digest");
    return finish(context, std::move(plan));
}

std::shared_ptr<Processors> hub(ContextPtr context, size_t streams, Load load)
{
    auto plan = readNumbers(streams, load);
    addAggregationInOrder(plan, "number", load.rows_per_chunk, streams);
    addHeavy(plan, context, "number", "digest");
    return finish(context, std::move(plan));
}

std::shared_ptr<Processors> fanInMerge(ContextPtr context, size_t streams, Load load)
{
    load.interleaved = true;
    auto plan = readNumbers(streams, load);
    addMerge(plan, "number", load.rows_per_chunk);
    return finish(context, std::move(plan));
}

std::shared_ptr<Processors> manySources(ContextPtr context, size_t count, size_t threads, Load load)
{
    auto plan = readNumbers(count, load, threads);
    addLight(plan, context, "number", "hash");
    return finish(context, std::move(plan));
}

std::shared_ptr<Processors> delayed(ContextPtr context, Load load)
{
    auto chunks = std::make_shared<Chunks>(numbers(load.chunks, load.rows_per_chunk, 0));
    auto creator = [context, chunks]
    {
        QueryPlan inner;
        inner.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<SourceFromChunks>(numbersHeader(), std::move(*chunks)))));
        addLight(inner, context, "number", "hash");
        auto builder = buildPipeline(context, inner);
        return std::move(*builder);
    };

    QueryPlan plan;
    plan.addStep(std::make_unique<ReadFromPreparedSource>(createDelayedPipe(
        std::make_shared<const Block>(Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "hash")}), creator, false, false)));
    return finish(context, std::move(plan));
}

std::shared_ptr<Processors> fanInAggregate(ContextPtr context, size_t streams, Load load)
{
    auto plan = readNumbers(streams, load);
    addMerge(plan, "number", load.rows_per_chunk);
    addModulo(plan, context, "number", "key", 100000);
    addAggregation(plan, "key", load.rows_per_chunk, streams);
    return finish(context, std::move(plan));
}

std::shared_ptr<Processors> aggregate(ContextPtr context, size_t streams, Load load)
{
    auto plan = readNumbers(streams, load);
    addModulo(plan, context, "number", "key", 100000);
    addAggregation(plan, "key", load.rows_per_chunk, streams);
    return finish(context, std::move(plan));
}

std::shared_ptr<Processors> manyPartsInOrder(ContextPtr context, size_t parts, size_t streams, Load load)
{
    const size_t parts_per_stream = std::max<size_t>(1, parts / streams);
    const Load part_load{.chunks = parts_per_stream, .rows_per_chunk = load.rows_per_chunk};

    std::vector<QueryPlanPtr> groups;
    SharedHeaders headers;
    for (size_t stream = 0; stream < streams; ++stream)
    {
        auto group = std::make_unique<QueryPlan>(readNumbers(parts_per_stream, part_load));
        addMerge(*group, "number", load.rows_per_chunk);
        headers.push_back(group->getCurrentHeader());
        groups.push_back(std::move(group));
    }

    QueryPlan plan;
    plan.unitePlans(std::make_unique<UnionStep>(std::move(headers)), std::move(groups));
    addAggregationInOrder(plan, "number", load.rows_per_chunk, streams);
    addLight(plan, context, "number", "hash");
    return finish(context, std::move(plan));
}

}
