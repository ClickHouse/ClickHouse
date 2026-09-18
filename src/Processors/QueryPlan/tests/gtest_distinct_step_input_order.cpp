#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "k")}));
}

QueryPipelineBuilder makePipeline(const SharedHeader & header, size_t streams)
{
    Pipes pipes;
    for (size_t i = 0; i < streams; ++i)
        pipes.emplace_back(std::make_shared<NullSource>(header));

    QueryPipelineBuilder pipeline;
    pipeline.init(Pipe::unitePipes(std::move(pipes)));
    pipeline.setMaxThreads(4);
    return pipeline;
}

void expectPipeline(DistinctStep & step, size_t input_streams, size_t output_streams, size_t distincts, bool scatters)
{
    auto pipeline = makePipeline(step.getInputHeaders().front(), input_streams);
    step.transformPipeline(pipeline, BuildQueryPipelineSettings(getContext().context));

    size_t distinct_count = 0;
    bool has_scatter = false;
    for (const auto & processor : pipeline.getProcessors())
    {
        distinct_count += processor->getName() == "DistinctTransform";
        has_scatter |= processor->getName() == "ScatterByPartitionTransform";
    }
    EXPECT_EQ(pipeline.getNumStreams(), output_streams);
    EXPECT_EQ(distinct_count, distincts);
    EXPECT_EQ(has_scatter, scatters);
}

QueryPlanStepPtr roundTrip(DistinctStep & step)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry serialized_sets;
    IQueryPlanStep::Serialization serialization{out, serialized_sets};
    serialization.version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
    step.serialize(serialization);

    QueryPlanSerializationSettings settings;
    step.serializeSettings(settings, serialization.version);
    ReadBufferFromString in(out.str());
    DeserializedSetsRegistry deserialized_sets;
    ContextPtr context = getContext().context;
    IQueryPlanStep::Deserialization deserialization{
        in, deserialized_sets, {}, context, step.getInputHeaders(), step.getOutputHeader(), settings, 0, serialization.version, false};
    auto restored = DistinctStep::deserialize(deserialization, step.isPreliminary());
    EXPECT_TRUE(in.eof());
    return restored;
}

}

TEST(DistinctStepInputOrder, OnlyExplicitOrderRequirementsPreventParallelization)
{
    for (bool preserve_order : {false, true})
    {
        for (bool disjoint : {false, true})
        {
            for (UInt64 initial_hint : {0, 10})
            {
                for (UInt64 updated_hint : {0, 5})
                {
                    SCOPED_TRACE(
                        ::testing::Message() << "preserve_order=" << preserve_order << ", disjoint=" << disjoint
                                             << ", initial_hint=" << initial_hint << ", updated_hint=" << updated_hint);
                    DistinctStep step(makeHeader(), DistinctStep::Settings{}, initial_hint, Names{"k"}, false);
                    step.enableParallelDistinct();
                    if (disjoint)
                        step.skipStreamMerging();
                    if (preserve_order)
                        step.preserveInputOrder();
                    step.updateLimitHint(updated_hint);

                    const size_t streams = preserve_order ? 1 : 4;
                    const size_t distincts = preserve_order ? 1 : 4;
                    const bool scatters = !disjoint && !preserve_order;
                    expectPipeline(step, preserve_order ? 1 : 4, streams, distincts, scatters);
                    auto clone = step.clone();
                    expectPipeline(static_cast<DistinctStep &>(*clone), preserve_order ? 1 : 4, streams, distincts, scatters);
                }
            }
        }
    }
}

TEST(DistinctStepInputOrder, DeserializedStepsDeriveOrderFromTheirInput)
{
    for (bool preliminary : {false, true})
    {
        for (bool sorted : {false, true})
        {
            for (bool distinct_in_order : {false, true})
            {
                SCOPED_TRACE(
                    ::testing::Message() << "preliminary=" << preliminary << ", sorted=" << sorted
                                         << ", distinct_in_order=" << distinct_in_order);
                const auto header = makeHeader();
                DistinctStep step(header, DistinctStep::Settings{}, 0, Names{"k"}, preliminary);
                auto restored = roundTrip(step);
                auto * distinct = static_cast<DistinctStep *>(restored.get());
                EXPECT_FALSE(distinct->preservesInputOrder());

                QueryPlan plan;
                plan.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<NullSource>(header))));
                ContextPtr context = getContext().context;
                if (sorted)
                {
                    SortDescription description;
                    description.push_back(SortColumnDescription("k"));
                    plan.addStep(
                        std::make_unique<SortingStep>(header, std::move(description), 0, SortingStep::Settings(context->getSettingsRef())));
                }
                plan.addStep(std::move(restored));

                QueryPlanOptimizationSettings settings(context);
                settings.distinct_in_order = distinct_in_order;
                QueryPlanOptimizations::applyOrder(settings, *plan.getRootNode());
                EXPECT_EQ(distinct->preservesInputOrder(), sorted && !preliminary);

                if (!distinct_in_order)
                {
                    distinct->enableParallelDistinct();
                    distinct->skipStreamMerging();
                    const size_t streams = sorted && !preliminary ? 1 : 4;
                    expectPipeline(*distinct, streams, streams, streams, false);
                }
            }
        }
    }
}

#ifdef DEBUG_OR_SANITIZER_BUILD
TEST(DistinctStepInputOrderDeathTest, GlobalOrderRequiresSingleInputStream)
#else
TEST(DistinctStepInputOrder, GlobalOrderRequiresSingleInputStream)
#endif
{
    DistinctStep step(makeHeader(), DistinctStep::Settings{}, 0, Names{"k"}, false);
    step.preserveInputOrder();
    auto pipeline = makePipeline(step.getInputHeaders().front(), 4);

#ifdef DEBUG_OR_SANITIZER_BUILD
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_DEATH(
        step.transformPipeline(pipeline, BuildQueryPipelineSettings(getContext().context)),
        "Order-preserving DISTINCT requires a single input stream");
#else
    try
    {
        step.transformPipeline(pipeline, BuildQueryPipelineSettings(getContext().context));
        FAIL() << "Expected an exception for globally ordered input with multiple streams";
    }
    catch (Exception & e)
    {
        e.markAsLogged();
        EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
    }
#endif
}
