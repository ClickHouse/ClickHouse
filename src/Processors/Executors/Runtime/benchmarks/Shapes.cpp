#include <Processors/Executors/Runtime/benchmarks/Shapes.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Formats/Impl/NullFormat.h>
#include <Processors/LimitTransform.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

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

Chunks numbers(size_t chunks, size_t rows_per_chunk, UInt64 first)
{
    Chunks result;
    for (size_t index = 0; index < chunks; ++index)
    {
        auto column = ColumnUInt64::create(rows_per_chunk);
        auto & values = column->getData();
        std::iota(values.begin(), values.end(), first + index * rows_per_chunk);
        result.emplace_back(Columns{std::move(column)}, rows_per_chunk);
    }
    return result;
}

QueryPipelineBuilder sources(size_t streams, Load load)
{
    const size_t chunks_per_stream = std::max<size_t>(1, load.chunks / streams);

    Pipes pipes;
    for (size_t stream = 0; stream < streams; ++stream)
        pipes.emplace_back(std::make_shared<SourceFromChunks>(numbersHeader(), numbers(chunks_per_stream, load.rows_per_chunk, stream * chunks_per_stream * load.rows_per_chunk)));

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

    builder.addSimpleTransform([&](const SharedHeader & header) { return std::make_shared<ExpressionTransform>(header, expression); });
}

void addSort(QueryPipelineBuilder & builder, const String & column_name)
{
    SortDescription description;
    description.emplace_back(column_name);
    builder.addSimpleTransform([&](const SharedHeader & header) { return std::make_shared<PartialSortingTransform>(header, description); });
}

void addLimit(QueryPipelineBuilder & builder, UInt64 limit)
{
    builder.addSimpleTransform([&](const SharedHeader & header) { return std::make_shared<LimitTransform>(header, limit, 0); });
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

}
