#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/CurrentThread.h>
#include <Common/typeid_cast.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace DB
{
void registerAggregatingStep(QueryPlanStepRegistry & registry);
}

namespace
{

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    Block header({ColumnWithTypeAndName(type->createColumn(), type, "k")});
    return std::make_shared<const Block>(std::move(header));
}

std::unique_ptr<AggregatingStep> makeStep(const SharedHeader & header)
{
    /// The short constructor is the merge-only one; the pre-aggregation resize this test is about is
    /// chosen only for an ordinary (not merge-only) aggregation, hence `cloneWithKeys`.
    Aggregator::Params merge_params(
        Names{"k"},              // keys
        AggregateDescriptions{}, // aggregates
        false,                   // overflow_row
        1,                       // max_threads
        65536,                   // max_block_size
        0.5f,                    // min_hit_rate_to_use_consecutive_keys_optimization
        false,                   // serialize_string_with_zero_byte
        true);                   // enable_packed_string_keys
    auto agg_params = merge_params.cloneWithKeys(Names{"k"}, /*only_merge=*/false);

    return std::make_unique<AggregatingStep>(
        header,
        agg_params,
        GroupingSetsParamsList{},
        /*final=*/false,
        /*max_block_size=*/65536,
        /*aggregation_in_order_max_block_bytes=*/0,
        /*merge_threads=*/1,
        /*temporary_data_merge_threads=*/1,
        /*storage_has_evenly_distributed_read=*/false,
        /*group_by_use_nulls=*/false,
        /*sort_description_for_merging=*/SortDescription{},
        /*group_by_sort_description=*/SortDescription{},
        /*should_produce_results_in_order_of_bucket_number=*/false,
        /*memory_bound_merging_of_aggregation_results_enabled=*/false,
        /*explicit_sorting_required_for_aggregation_in_order=*/false);
}

String serializeStep(const IQueryPlanStep & step, UInt64 step_version)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
    ctx.step_version = step_version;
    step.serialize(ctx);
    return out.str();
}

std::unique_ptr<AggregatingStep> deserializeStep(const String & bytes, const SharedHeader & header, UInt64 step_version)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    SharedHeaders input_headers{header};
    QueryPlanSerializationSettings settings;
    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, getContext().context, input_headers, header, settings, 0,
        DBMS_QUERY_PLAN_SERIALIZATION_VERSION, step_version, false};
    auto step = AggregatingStep::deserialize(ctx);
    /// The registry create function is typed as `QueryPlanStepPtr`; the pipeline check below needs
    /// the concrete step.
    if (!typeid_cast<AggregatingStep *>(step.get()))
        return nullptr;
    return std::unique_ptr<AggregatingStep>(static_cast<AggregatingStep *>(step.release()));
}

/// Two-stream pipeline over header-only sources, so the pre-aggregation resize is inserted.
bool buildsGradualResize(AggregatingStep & step, const SharedHeader & header, ContextMutablePtr context)
{
    QueryPipelineBuilder builder;
    Pipes pipes;
    pipes.emplace_back(std::make_shared<SourceFromSingleChunk>(header));
    pipes.emplace_back(std::make_shared<SourceFromSingleChunk>(header));
    builder.init(Pipe::unitePipes(std::move(pipes)));

    BuildQueryPipelineSettings settings(context);
    step.transformPipeline(builder, settings);

    for (const auto & processor : builder.getProcessors())
        if (processor->getName() == "GradualResize")
            return true;
    return false;
}

}

/// The second flags byte is part of serialization version 1 of `Aggregating`, so the registry must
/// hand version 1 to a writer at or above the plan version that introduced it, and version 0 below
/// it. Without the registry entry the writer would keep announcing version 0 while emitting the new
/// byte, and a reader of the same plan version would not be able to tell the two payloads apart.
TEST(AggregatingStepExtraFlags, RegistryMapsPlanVersionToStepVersion)
{
    /// A private registry, so the test does not depend on whether the process-wide one has already
    /// been filled (it refuses a second registration of the same step).
    QueryPlanStepRegistry registry;
    registerAggregatingStep(registry);

    EXPECT_EQ(
        registry.versionToWrite("Aggregating", DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SEMANTICALLY_CONSTANT_GROUP_BY_KEYS - 1),
        0u);
    EXPECT_EQ(
        registry.versionToWrite("Aggregating", DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SEMANTICALLY_CONSTANT_GROUP_BY_KEYS),
        1u);
    EXPECT_EQ(registry.versionToWrite("Aggregating", DBMS_QUERY_PLAN_SERIALIZATION_VERSION), 1u);

    EXPECT_NO_THROW(registry.checkVersionReadable("Aggregating", 0));
    EXPECT_NO_THROW(registry.checkVersionReadable("Aggregating", 1));
    EXPECT_ANY_THROW(registry.checkVersionReadable("Aggregating", 2));
}

/// Version 1 carries both bits, and only version 1 writes the byte at all: the two payloads differ by
/// exactly that byte, and a step version 0 stream leaves the receiver on the strict resize.
TEST(AggregatingStepExtraFlags, StepVersionOneRoundTripsTheBits)
{
    MainThreadStatus::getInstance();
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    const auto & context_holder = getContext();
    auto context = Context::createCopy(context_holder.context);
    context->setSetting("min_rows_per_stream_for_gradual_resize", 1000);

    auto header = makeHeader();

    auto step = makeStep(header);
    step->enableGradualResize();

    const String v1 = serializeStep(*step, /*step_version=*/1);
    const String v0 = serializeStep(*step, /*step_version=*/0);
    EXPECT_EQ(v1.size(), v0.size() + 1);

    /// Version 1: the gradual-resize mark survives the round trip.
    {
        auto restored = deserializeStep(v1, header, /*step_version=*/1);
        ASSERT_NE(restored, nullptr);
        EXPECT_TRUE(buildsGradualResize(*restored, header, context));
    }

    /// Version 0: the receiver never learns the mark and keeps the strict resize.
    {
        auto restored = deserializeStep(v0, header, /*step_version=*/0);
        ASSERT_NE(restored, nullptr);
        EXPECT_FALSE(buildsGradualResize(*restored, header, context));
    }

    /// Both bits together: a semantically constant key set keeps the strict resize even with the
    /// gradual mark set, and that decision also has to reach the receiver.
    {
        auto constant_step = makeStep(header);
        constant_step->enableGradualResize();
        constant_step->markGroupByKeysSemanticallyConstant();
        auto restored = deserializeStep(serializeStep(*constant_step, /*step_version=*/1), header, /*step_version=*/1);
        ASSERT_NE(restored, nullptr);
        EXPECT_FALSE(buildsGradualResize(*restored, header, context));
    }
}
