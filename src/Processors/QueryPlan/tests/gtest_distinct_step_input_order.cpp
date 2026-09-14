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
#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "k")}));
}

void expectPipeline(DistinctStep & step, size_t streams, size_t distincts, bool scatters)
{
    const auto & header = step.getInputHeaders().front();
    Pipes pipes;
    for (size_t i = 0; i < 4; ++i)
        pipes.emplace_back(std::make_shared<NullSource>(header));

    QueryPipelineBuilder pipeline;
    pipeline.init(Pipe::unitePipes(std::move(pipes)));
    pipeline.setMaxThreads(4);
    step.transformPipeline(pipeline, BuildQueryPipelineSettings(getContext().context));

    size_t distinct_count = 0;
    bool has_scatter = false;
    for (const auto & processor : pipeline.getProcessors())
    {
        distinct_count += processor->getName() == "DistinctTransform";
        has_scatter |= processor->getName() == "ScatterByPartitionTransform";
    }
    EXPECT_EQ(pipeline.getNumStreams(), streams);
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
                    DistinctStep step(makeHeader(), SizeLimits{}, initial_hint, Names{"k"}, false);
                    step.enableParallelDistinct();
                    if (disjoint)
                        step.skipStreamMerging();
                    if (preserve_order)
                        step.preserveInputOrder();
                    step.updateLimitHint(updated_hint);

                    const size_t streams = disjoint && !preserve_order ? 4 : 1;
                    const size_t distincts = preserve_order ? 1 : 4;
                    const bool scatters = !disjoint && !preserve_order;
                    expectPipeline(step, streams, distincts, scatters);
                    auto clone = step.clone();
                    expectPipeline(static_cast<DistinctStep &>(*clone), streams, distincts, scatters);
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
                DistinctStep step(header, SizeLimits{}, 0, Names{"k"}, preliminary);
                auto restored = roundTrip(step);
                auto * distinct = static_cast<DistinctStep *>(restored.get());
                EXPECT_FALSE(distinct->mustPreserveInputOrder());

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
                EXPECT_EQ(distinct->mustPreserveInputOrder(), sorted && !preliminary);

                if (!distinct_in_order)
                {
                    distinct->enableParallelDistinct();
                    distinct->skipStreamMerging();
                    expectPipeline(*distinct, sorted && !preliminary ? 1 : 4, sorted && !preliminary ? 1 : 4, false);
                }
            }
        }
    }
}
