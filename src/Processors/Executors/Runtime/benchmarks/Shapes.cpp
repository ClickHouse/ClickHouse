#include <Processors/Executors/Runtime/benchmarks/Shapes.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Core/SortCursor.h>
#include <Processors/Formats/Impl/NullFormat.h>
#include <Processors/LimitTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/Sources/DelayedSource.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <algorithm>
#include <numeric>
#include <optional>

namespace DB::Runtime::Bench
{

namespace
{

const IQueryPlanStep * stepTag(const SharedHeader & header)
{
    static std::vector<std::shared_ptr<IQueryPlanStep>> tags;
    tags.push_back(std::make_shared<ExpressionStep>(header, ActionsDAG(header->getNamesAndTypesList())));
    return tags.back().get();
}

template <class Transform>
std::shared_ptr<Transform> tagged(std::shared_ptr<Transform> transform, const IQueryPlanStep * step)
{
    transform->setQueryPlanStep(step);
    return transform;
}

SharedHeader headerOf(const String & column_name)
{
    return std::make_shared<const Block>(Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), column_name)});
}

SharedHeader numbersHeader()
{
    return headerOf("number");
}

Chunk numbersChunk(size_t rows_per_chunk, UInt64 first)
{
    auto column = ColumnUInt64::create(rows_per_chunk);
    auto & values = column->getData();
    std::iota(values.begin(), values.end(), first);
    return Chunk(Columns{std::move(column)}, rows_per_chunk);
}

Chunks numbers(size_t chunks, size_t rows_per_chunk, UInt64 first)
{
    Chunks result;
    for (size_t index = 0; index < chunks; ++index)
        result.push_back(numbersChunk(rows_per_chunk, first + index * rows_per_chunk));
    return result;
}

QueryPipelineBuilder sources(size_t streams, Load load)
{
    const size_t chunks_per_stream = std::max<size_t>(1, load.chunks / streams);

    const auto * step = stepTag(numbersHeader());
    Pipes pipes;
    for (size_t stream = 0; stream < streams; ++stream)
        pipes.emplace_back(tagged(std::make_shared<SourceFromChunks>(numbersHeader(), numbers(chunks_per_stream, load.rows_per_chunk, stream * chunks_per_stream * load.rows_per_chunk)), step));

    QueryPipelineBuilder builder;
    builder.init(Pipe::unitePipes(std::move(pipes)));
    return builder;
}

void addHash(QueryPipelineBuilder & builder, ContextPtr context, const String & input_name, const String & output_name)
{
    ActionsDAG dag(NamesAndTypesList{{input_name, std::make_shared<DataTypeUInt64>()}});
    const auto & hash = dag.addFunction(FunctionFactory::instance().get("intHash64", context), {dag.getInputs().front()}, output_name);
    dag.getOutputs() = {&hash};
    auto expression = std::make_shared<ExpressionActions>(std::move(dag), ExpressionActionsSettings{});

    const auto * step = stepTag(builder.getSharedHeader());
    builder.addSimpleTransform([&](const SharedHeader & header) { return tagged(std::make_shared<ExpressionTransform>(header, expression), step); });
}

void addModulo(QueryPipelineBuilder & builder, ContextPtr context, const String & input_name, const String & output_name, UInt64 divisor)
{
    ActionsDAG dag(NamesAndTypesList{{input_name, std::make_shared<DataTypeUInt64>()}});
    const auto type = std::make_shared<DataTypeUInt64>();
    const auto & constant = dag.addColumn(type->createColumnConst(1, divisor), type, "divisor");
    const auto & key = dag.addFunction(FunctionFactory::instance().get("modulo", context), {dag.getInputs().front(), &constant}, output_name);
    dag.getOutputs() = {&key};
    auto expression = std::make_shared<ExpressionActions>(std::move(dag), ExpressionActionsSettings{});

    const auto * step = stepTag(builder.getSharedHeader());
    builder.addSimpleTransform([&](const SharedHeader & header) { return tagged(std::make_shared<ExpressionTransform>(header, expression), step); });
}

void addMergingSorted(QueryPipelineBuilder & builder, const String & column_name, size_t max_block_size)
{
    SortDescription description;
    description.emplace_back(column_name);
    const auto * step = stepTag(builder.getSharedHeader());
    builder.addTransform(tagged(std::make_shared<MergingSortedTransform>(
        builder.getSharedHeader(), builder.getNumStreams(), description, max_block_size, 0, std::nullopt, SortingQueueStrategy::Batch), step));
}

void addAggregation(QueryPipelineBuilder & builder, const String & key_name, size_t max_block_size)
{
    Aggregator::Params params(Names{key_name}, AggregateDescriptions{}, false, 1, max_block_size, 0.5f, false, true);
    auto transform_params = std::make_shared<AggregatingTransformParams>(builder.getSharedHeader(), params, true);
    const auto * step = stepTag(builder.getSharedHeader());
    builder.addTransform(tagged(std::make_shared<AggregatingTransform>(builder.getSharedHeader(), transform_params, nullptr), step));
}

void addSort(QueryPipelineBuilder & builder, const String & column_name)
{
    SortDescription description;
    description.emplace_back(column_name);
    const auto * step = stepTag(builder.getSharedHeader());
    builder.addSimpleTransform([&](const SharedHeader & header) { return tagged(std::make_shared<PartialSortingTransform>(header, description), step); });
}

void addLimit(QueryPipelineBuilder & builder, UInt64 limit)
{
    const auto * step = stepTag(builder.getSharedHeader());
    builder.addSimpleTransform([&](const SharedHeader & header) { return tagged(std::make_shared<LimitTransform>(header, limit, 0), step); });
}

std::shared_ptr<Processors> finish(QueryPipelineBuilder builder)
{
    builder.resize(1);
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    pipeline.complete(std::make_shared<NullOutputFormat>(pipeline.getSharedHeader()));
    return std::make_shared<Processors>(pipeline.getProcessors());
}

}

std::shared_ptr<Processors> chain(ContextPtr context, Load load)
{
    auto builder = sources(1, load);
    addHash(builder, context, "number", "hash");
    addSort(builder, "hash");
    addLimit(builder, load.chunks * load.rows_per_chunk);
    return finish(std::move(builder));
}

std::shared_ptr<Processors> fanOut(ContextPtr context, size_t streams, Load load)
{
    auto builder = sources(1, load);
    builder.resize(streams);
    addHash(builder, context, "number", "hash");
    return finish(std::move(builder));
}

std::shared_ptr<Processors> fanIn(ContextPtr context, size_t streams, Load load)
{
    auto builder = sources(streams, load);
    addHash(builder, context, "number", "hash");
    return finish(std::move(builder));
}

std::shared_ptr<Processors> hub(ContextPtr context, size_t streams, Load load)
{
    auto builder = sources(streams, load);
    addHash(builder, context, "number", "hash");
    builder.resize(1);
    addHash(builder, context, "hash", "rehash");
    builder.resize(streams);
    addSort(builder, "rehash");
    return finish(std::move(builder));
}

std::shared_ptr<Processors> fanInMerge(ContextPtr, size_t streams, Load load)
{
    auto builder = sources(streams, load);
    addMergingSorted(builder, "number", load.rows_per_chunk);
    return finish(std::move(builder));
}

std::shared_ptr<Processors> fanInAggregate(ContextPtr context, size_t streams, Load load)
{
    auto builder = sources(streams, load);
    addModulo(builder, context, "number", "key", 100000);
    builder.resize(1);
    addAggregation(builder, "key", load.rows_per_chunk);
    return finish(std::move(builder));
}

std::shared_ptr<Processors> manyPartsInOrder(ContextPtr context, size_t parts, size_t streams, Load load)
{
    const size_t parts_per_stream = std::max<size_t>(1, parts / streams);
    const Load part_load{.chunks = parts_per_stream, .rows_per_chunk = load.rows_per_chunk};

    VectorWithMemoryTracking<std::unique_ptr<QueryPipelineBuilder>> groups;
    for (size_t stream = 0; stream < streams; ++stream)
    {
        auto group = std::make_unique<QueryPipelineBuilder>(sources(parts_per_stream, part_load));
        addMergingSorted(*group, "number", load.rows_per_chunk);
        groups.push_back(std::move(group));
    }

    auto builder = QueryPipelineBuilder::unitePipelines(std::move(groups));
    addHash(builder, context, "number", "hash");
    addMergingSorted(builder, "hash", load.rows_per_chunk);
    builder.resize(streams);
    addHash(builder, context, "hash", "rehash");
    return finish(std::move(builder));
}

std::shared_ptr<Processors> manySources(ContextPtr context, size_t count, size_t threads, Load load)
{
    auto builder = sources(count, load);
    builder.resize(threads);
    addHash(builder, context, "number", "hash");
    return finish(std::move(builder));
}

std::shared_ptr<Processors> delayed(ContextPtr context, Load load)
{
    auto chunks = std::make_shared<Chunks>(numbers(load.chunks, load.rows_per_chunk, 0));
    auto creator = [context, chunks]
    {
        QueryPipelineBuilder inner;
        inner.init(Pipe(std::make_shared<SourceFromChunks>(numbersHeader(), std::move(*chunks))));
        addHash(inner, context, "number", "hash");
        return inner;
    };

    QueryPipelineBuilder builder;
    builder.init(createDelayedPipe(headerOf("hash"), creator, false, false));
    return finish(std::move(builder));
}

}
