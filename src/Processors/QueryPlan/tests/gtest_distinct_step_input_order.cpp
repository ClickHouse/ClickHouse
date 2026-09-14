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
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
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

TEST(DistinctStepInputOrder, FinalOrderRequirementsOverrideParallelization)
{
    enum class Requirement
    {
        Explicit,
        InitialLimitHint,
        UpdatedLimitHint,
    };
    for (bool disjoint : {false, true})
    {
        for (auto requirement : {Requirement::Explicit, Requirement::InitialLimitHint, Requirement::UpdatedLimitHint})
        {
            SCOPED_TRACE(::testing::Message() << "disjoint=" << disjoint << ", requirement=" << static_cast<int>(requirement));
            DistinctStep step(makeHeader(), SizeLimits{}, requirement == Requirement::InitialLimitHint ? 10 : 0, Names{"k"}, false);
            step.enableParallelDistinct();
            if (disjoint)
                step.skipStreamMerging();
            if (requirement == Requirement::Explicit)
                step.preserveInputOrder();
            if (requirement == Requirement::UpdatedLimitHint)
                step.updateLimitHint(10);

            expectPipeline(step, 1, 1, false);
            auto clone = step.clone();
            expectPipeline(static_cast<DistinctStep &>(*clone), 1, 1, false);
        }
    }
}

TEST(DistinctStepInputOrder, DeserializedFinalStepsPreserveOrder)
{
    for (bool preliminary : {false, true})
    {
        for (bool disjoint : {false, true})
        {
            SCOPED_TRACE(::testing::Message() << "preliminary=" << preliminary << ", disjoint=" << disjoint);
            DistinctStep step(makeHeader(), SizeLimits{}, 0, Names{"k"}, preliminary);
            step.enableParallelDistinct();
            if (disjoint)
                step.skipStreamMerging();
            expectPipeline(step, preliminary || disjoint ? 4 : 1, 4, !preliminary && !disjoint);

            auto restored = roundTrip(step);
            auto & distinct = static_cast<DistinctStep &>(*restored);
            distinct.enableParallelDistinct();
            if (disjoint)
                distinct.skipStreamMerging();
            expectPipeline(distinct, preliminary ? 4 : 1, preliminary ? 4 : 1, false);

            auto clone = distinct.clone();
            expectPipeline(static_cast<DistinctStep &>(*clone), preliminary ? 4 : 1, preliminary ? 4 : 1, false);
        }
    }
}
