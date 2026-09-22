#include <gtest/gtest.h>

#include <cmath>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/Optimizations/RuntimeFilterPlanning.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/RuntimeFilterBloomSizing.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/BuildRuntimeFilterTransform.h>
#include <Common/Exception.h>

namespace DB
{

namespace QueryPlanSerializationSetting
{
extern const QueryPlanSerializationSettingsUInt64 join_runtime_filter_exact_values_limit;
extern const QueryPlanSerializationSettingsUInt64 join_runtime_bloom_filter_bytes;
extern const QueryPlanSerializationSettingsUInt64 join_runtime_bloom_filter_hash_functions;
}

namespace QueryPlanOptimizations
{
namespace
{

RuntimeFilterPlanningPolicy makePolicy(bool use_minmax = false)
{
    return RuntimeFilterPlanningPolicy{/*exact_values_limit=*/10,
                                       RuntimeBloomFilterParameters{/*bytes=*/128, /*hash_functions=*/3},
                                       /*max_estimated_set_bits_ratio=*/0.5,
                                       use_minmax};
}

RuntimeFilterPlan chooseUInt64(
    RuntimeFilterKeyEstimate estimate,
    RuntimeFilterPlanningPolicy policy = makePolicy(),
    RuntimeFilterPolarity polarity = RuntimeFilterPolarity::Contains)
{
    return chooseRuntimeFilterPlan(std::make_shared<DataTypeUInt64>(), polarity, estimate, policy);
}

RuntimeFilterBuildOptions
makeBuildOptions(RuntimeFilterPolarity polarity, RuntimeFilterMinMaxMode minmax_mode, bool track_key_range = false)
{
    return RuntimeFilterBuildOptions{
        .exact_values_limit = 10,
        .bloom = RuntimeBloomFilterParameters{128, 3},
        .max_ratio_of_set_bits = 0.5,
        .polarity = polarity,
        .minmax_mode = minmax_mode,
        .track_key_range = track_key_range,
        .distinct_keys_hint = 100,
        .distinct_keys_hint_matches_filter_key = true};
}

SharedHeader makeHeader(const DataTypePtr & type)
{
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "k")}));
}

void constructBuildTransform(const DataTypePtr & type, const RuntimeFilterBuildOptions & options)
{
    const auto header = makeHeader(type);
    BuildRuntimeFilterTransform transform(
        header,
        "k",
        type,
        "filter_name",
        "filter_key",
        /*filters_to_merge=*/0,
        options,
        RuntimeFilterConfig{/*pass_ratio_threshold_for_disabling=*/1.0, /*blocks_to_skip_before_reenabling=*/30},
        /*query_context=*/nullptr);
}

String serializeBuildStep(const IQueryPlanStep & step)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization context{out, registry};
    context.version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
    step.serialize(context);
    return out.str();
}

QueryPlanStepPtr deserializeBuildStep(const String & bytes, const SharedHeader & header, const QueryPlanSerializationSettings & settings)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    SharedHeaders input_headers{header};
    IQueryPlanStep::Deserialization context{
        in,
        registry,
        {},
        /*query_context=*/nullptr,
        input_headers,
        header,
        settings,
        0,
        DBMS_QUERY_PLAN_SERIALIZATION_VERSION,
        false};
    return BuildRuntimeFilterStep::deserialize(context);
}

}

TEST(RuntimeFilterPlanning, KeyEstimateRequiresPositiveNdvAndUsesRowBound)
{
    const auto missing = makeRuntimeFilterKeyEstimate(/*estimated_rows=*/1000, /*key_ndv=*/std::nullopt, false);
    EXPECT_EQ(missing.estimated_distinct_keys, 1000);
    EXPECT_FALSE(missing.has_key_ndv_statistics);

    const auto zero = makeRuntimeFilterKeyEstimate(/*estimated_rows=*/1000, /*key_ndv=*/0, false);
    EXPECT_EQ(zero.estimated_distinct_keys, 1000);
    EXPECT_FALSE(zero.has_key_ndv_statistics);

    const auto bounded = makeRuntimeFilterKeyEstimate(/*estimated_rows=*/50, /*key_ndv=*/100, true);
    EXPECT_EQ(bounded.estimated_distinct_keys, 50);
    EXPECT_TRUE(bounded.has_key_ndv_statistics);
    EXPECT_TRUE(bounded.build_subtree_contains_filter_step);
}

TEST(RuntimeFilterPlanning, MissingKeyNdvCannotSuppressMembership)
{
    EXPECT_EQ(
        chooseUInt64({/*estimated_distinct_keys=*/std::nullopt, /*has_key_ndv_statistics=*/false, false}).kind,
        RuntimeFilterPlanKind::Membership);
    EXPECT_EQ(
        chooseUInt64({/*estimated_distinct_keys=*/1000, /*has_key_ndv_statistics=*/false, false}).kind, RuntimeFilterPlanKind::Membership);
    EXPECT_EQ(
        chooseUInt64({/*estimated_distinct_keys=*/0, /*has_key_ndv_statistics=*/false, false}).kind, RuntimeFilterPlanKind::Membership);
}

TEST(RuntimeFilterPlanning, ExactValueLimitIsInclusive)
{
    auto policy = makePolicy();
    policy.max_estimated_set_bits_ratio = 0.0;
    EXPECT_EQ(chooseUInt64({10, true, false}, policy).kind, RuntimeFilterPlanKind::Membership);
    EXPECT_EQ(chooseUInt64({11, true, false}, policy).kind, RuntimeFilterPlanKind::Skip);
}

TEST(RuntimeFilterPlanning, SaturationComparisonIsStrict)
{
    auto policy = makePolicy();
    const Float64 occupancy = estimateRuntimeBloomFilterSetBitsRatio(1000, policy.bloom);
    policy.max_estimated_set_bits_ratio = occupancy;
    EXPECT_EQ(chooseUInt64({1000, true, false}, policy).kind, RuntimeFilterPlanKind::Membership);

    policy.max_estimated_set_bits_ratio = std::nextafter(occupancy, 0.0);
    EXPECT_EQ(chooseUInt64({1000, true, false}, policy).kind, RuntimeFilterPlanKind::Skip);
}

TEST(RuntimeFilterPlanning, ReliabilityAndDisabledThresholdPreserveMembership)
{
    EXPECT_EQ(chooseUInt64({1000, true, true}).kind, RuntimeFilterPlanKind::Membership);

    auto policy = makePolicy();
    policy.max_estimated_set_bits_ratio = 1.0;
    EXPECT_EQ(chooseUInt64({1000, true, false}, policy).kind, RuntimeFilterPlanKind::Membership);
}

TEST(RuntimeFilterPlanning, UnsupportedAdaptiveTypeFallsBackToMembership)
{
    auto policy = makePolicy(true);
    const auto plan = chooseRuntimeFilterPlan(
        makeNullable(std::make_shared<DataTypeUInt64>()),
        RuntimeFilterPolarity::Contains,
        RuntimeFilterKeyEstimate{1000, true, false},
        policy);
    EXPECT_EQ(plan.kind, RuntimeFilterPlanKind::Membership);
}

TEST(RuntimeFilterPlanning, MinMaxEligibilityUsesCurrentTypeHelper)
{
    auto policy = makePolicy(true);
    policy.max_estimated_set_bits_ratio = 1.0;
    EXPECT_EQ(
        chooseRuntimeFilterPlan(std::make_shared<DataTypeFloat64>(), RuntimeFilterPolarity::Contains, {5, true, false}, policy).kind,
        RuntimeFilterPlanKind::MembershipWithMinMax);
    EXPECT_EQ(
        chooseRuntimeFilterPlan(std::make_shared<DataTypeString>(), RuntimeFilterPolarity::Contains, {5, true, false}, policy).kind,
        RuntimeFilterPlanKind::Membership);
    EXPECT_EQ(
        chooseRuntimeFilterPlan(makeNullable(std::make_shared<DataTypeUInt64>()), RuntimeFilterPolarity::Contains, {5, true, false}, policy)
            .kind,
        RuntimeFilterPlanKind::Membership);
}

TEST(RuntimeFilterPlanning, MinMaxAndPolarityRemainIndependentOfMembershipSuppression)
{
    auto minmax_policy = makePolicy(true);
    EXPECT_EQ(chooseUInt64({1000, true, false}, minmax_policy).kind, RuntimeFilterPlanKind::MinMaxOnly);
    EXPECT_EQ(chooseUInt64({5, true, false}, minmax_policy).kind, RuntimeFilterPlanKind::MembershipWithMinMax);
    EXPECT_EQ(chooseUInt64({1000, true, false}, minmax_policy, RuntimeFilterPolarity::NotContains).kind, RuntimeFilterPlanKind::Membership);

    EXPECT_EQ(chooseUInt64({1000, true, false}, makePolicy(false)).kind, RuntimeFilterPlanKind::Skip);
}

TEST(RuntimeFilterBuildOptions, StepSerializationAndConstructionPreserveLegacyBoundaries)
{
    const auto type = std::make_shared<DataTypeUInt64>();
    const auto header = makeHeader(type);
    auto options = makeBuildOptions(RuntimeFilterPolarity::Contains, RuntimeFilterMinMaxMode::Only);
    options.bloom = RuntimeBloomFilterParameters{0, 0};

    BuildRuntimeFilterStep step(
        header,
        "k",
        type,
        "visible_filter_name",
        "secret_random_key_a",
        options,
        /*pass_ratio_threshold_for_disabling=*/0.7,
        /*blocks_to_skip_before_reenabling=*/30);

    QueryPlanSerializationSettings settings;
    step.serializeSettings(settings, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    EXPECT_EQ(settings[QueryPlanSerializationSetting::join_runtime_filter_exact_values_limit], options.exact_values_limit);
    EXPECT_EQ(settings[QueryPlanSerializationSetting::join_runtime_bloom_filter_bytes], 512 * 1024);
    EXPECT_EQ(settings[QueryPlanSerializationSetting::join_runtime_bloom_filter_hash_functions], 3);

    const String bytes = serializeBuildStep(step);
    EXPECT_EQ(bytes.find("secret_random_key_a"), String::npos);

    BuildRuntimeFilterStep different_random_key(
        header,
        "k",
        type,
        "visible_filter_name",
        "secret_random_key_b",
        options,
        /*pass_ratio_threshold_for_disabling=*/0.7,
        /*blocks_to_skip_before_reenabling=*/30);
    EXPECT_EQ(serializeBuildStep(different_random_key), bytes);
    EXPECT_EQ(serializeBuildStep(*step.clone()), bytes);

    const auto restored = deserializeBuildStep(bytes, header, settings);
    EXPECT_EQ(serializeBuildStep(*restored), bytes);

    auto invalid_options = options;
    invalid_options.bloom.bytes = 16 * 1024 * 1024 + 1;
    EXPECT_THROW(
        BuildRuntimeFilterStep(
            header,
            "k",
            type,
            "visible_filter_name",
            "secret_random_key",
            invalid_options,
            /*pass_ratio_threshold_for_disabling=*/0.7,
            /*blocks_to_skip_before_reenabling=*/30),
        Exception);
}

TEST(RuntimeFilterBuildOptions, PublicConstructionSupportsCurrentModeMatrix)
{
    const auto numeric_type = std::make_shared<DataTypeUInt64>();
    EXPECT_NO_THROW(
        constructBuildTransform(numeric_type, makeBuildOptions(RuntimeFilterPolarity::Contains, RuntimeFilterMinMaxMode::Disabled)));
    EXPECT_NO_THROW(
        constructBuildTransform(numeric_type, makeBuildOptions(RuntimeFilterPolarity::Contains, RuntimeFilterMinMaxMode::Combined)));
    EXPECT_NO_THROW(constructBuildTransform(
        numeric_type, makeBuildOptions(RuntimeFilterPolarity::Contains, RuntimeFilterMinMaxMode::Only, /*track_key_range=*/true)));

    const auto unsupported_adaptive_type = makeNullable(numeric_type);
    EXPECT_NO_THROW(constructBuildTransform(
        unsupported_adaptive_type, makeBuildOptions(RuntimeFilterPolarity::Contains, RuntimeFilterMinMaxMode::Disabled)));
    EXPECT_NO_THROW(
        constructBuildTransform(numeric_type, makeBuildOptions(RuntimeFilterPolarity::NotContains, RuntimeFilterMinMaxMode::Disabled)));
}

TEST(RuntimeFilterBuildOptions, PublicConstructionRejectsCurrentInvalidModes)
{
    const auto numeric_type = std::make_shared<DataTypeUInt64>();
    EXPECT_THROW(
        constructBuildTransform(numeric_type, makeBuildOptions(RuntimeFilterPolarity::NotContains, RuntimeFilterMinMaxMode::Combined)),
        Exception);
    EXPECT_THROW(
        constructBuildTransform(numeric_type, makeBuildOptions(RuntimeFilterPolarity::NotContains, RuntimeFilterMinMaxMode::Only)),
        Exception);
    EXPECT_THROW(
        constructBuildTransform(
            std::make_shared<DataTypeString>(), makeBuildOptions(RuntimeFilterPolarity::Contains, RuntimeFilterMinMaxMode::Only)),
        Exception);
    EXPECT_THROW(
        constructBuildTransform(
            makeNullable(numeric_type), makeBuildOptions(RuntimeFilterPolarity::Contains, RuntimeFilterMinMaxMode::Combined)),
        Exception);
}

}
}
