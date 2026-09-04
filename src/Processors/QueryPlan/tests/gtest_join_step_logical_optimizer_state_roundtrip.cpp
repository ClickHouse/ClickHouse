#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/RelationEstimateInfo.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace
{

constexpr UInt64 current_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
constexpr UInt64 pre_state_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_JOIN_OPTIMIZER_STATE - 1;

SharedHeader makeHeader(const String & column_name)
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, column_name)}));
}

/// The two-header constructor adds one DAG input per column and leaves the outputs empty, while
/// JoinStepLogical takes its output header from the DAG's result columns. A CROSS JOIN with no ON
/// expression passes both sides through, so every input is also an output.
JoinExpressionActions makeExpressionActions(const Block & left_header, const Block & right_header)
{
    JoinExpressionActions expression_actions(left_header, right_header);
    auto actions_dag = expression_actions.getActionsDAG();
    for (const auto * input : actions_dag->getInputs())
        actions_dag->getOutputs().push_back(input);
    return expression_actions;
}

/// A CROSS JOIN of two single-column relations. The join shape is irrelevant to what is under test:
/// the optimizer state travels beside the expression actions, not inside them.
std::unique_ptr<JoinStepLogical> makeStep()
{
    auto left_header = makeHeader("l");
    auto right_header = makeHeader("r");
    QueryPlanSerializationSettings settings;

    return std::make_unique<JoinStepLogical>(
        left_header,
        right_header,
        JoinOperator{},
        makeExpressionActions(*left_header, *right_header),
        ActionsDAG::NodeRawConstPtrs{},
        JoinSettings(settings),
        SortingStep::Settings(settings));
}

/// Every field the version-12 block carries, each at a value distinct from its default and from the
/// value used on the other side, so a field read into the wrong place is observable.
std::unique_ptr<JoinStepLogical> makePopulatedStep(RelationEstimateInfo left_relation, RelationEstimateInfo right_relation)
{
    auto step = makeStep();

    step->setOptimized(
        /*estimated_rows_=*/4242,
        {{"l", ColumnStats{.num_distinct_values = 7, .avg_bytes = 1.5}},
         {"r", ColumnStats{.num_distinct_values = 11, .avg_bytes = 2.25}}},
        /*imprecise_estimate_=*/true);

    step->setRightHashTableCacheKey(0x1122334455667788ULL);
    step->setJoinOutputCacheKey(0x99AABBCCDDEEFF00ULL);
    step->setInputRelations(std::move(left_relation), std::move(right_relation));
    step->setTableStatsHint("lt:1000,rt:2000");
    return step;
}

String serializeStep(const IQueryPlanStep & step, UInt64 version)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = version;
    step.serialize(ctx);
    return out.str();
}

/// The QueryPlanSerializationSettings object is left at its DECLARE defaults, which is what
/// QueryPlan::deserialize hands each step.
std::unique_ptr<JoinStepLogical> deserializeStep(const String & bytes, UInt64 version)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    QueryPlanSerializationSettings settings;
    SharedHeaders input_headers{makeHeader("l"), makeHeader("r")};
    SharedHeader output_header = makeHeader("l");
    ContextPtr context = getContext().context;

    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, context, input_headers, output_header, settings, 0, version, false};

    auto step = JoinStepLogical::deserialize(ctx);
    return std::unique_ptr<JoinStepLogical>(static_cast<JoinStepLogical *>(step.release()));
}

}

/// Regression test for the join optimizer state being dropped when a query plan is serialized.
///
/// Nine values travel in the version-12 block, and a SQL test can only observe `optimized`: as soon as
/// that one flag round-trips the receiver stops re-ordering the join, so a silently dropped estimate,
/// cache key, relation or hint stays invisible at the query level.
///
/// Two oracles, because neither alone covers the block. Byte identity of re-serialization pins that
/// both sides agree on the layout, which is what catches a field written but not read, a field read
/// into the wrong member, and a reordered pair. The value assertions catch the other direction, a
/// write and its read removed together: the streams then still match while the value is silently the
/// receiver's default.
TEST(JoinStepLogicalOptimizerStateRoundTrip, RoundTripPreservesEveryFieldAtCurrentVersion)
{
    /// `RelationEstimateInfo::source`, `imprecise_estimate` and `composite` have no getter of their
    /// own. All three reach `getReadableRelationName`, but that renders one relation per side and
    /// `composite` suppresses the other two, so it takes two steps to make each of them visible.
    /// First pair: the left side's `source` as the `cache` tag, the right side's `composite` as the
    /// bare name in parentheses. Second pair: the left side's `imprecise_estimate`, which is the only
    /// reason an untracked source is tagged at all, against a right side that carries neither.
    const std::vector<std::pair<RelationEstimateInfo, RelationEstimateInfo>> relation_pairs = {
        {RelationEstimateInfo{
             .name = "lt",
             .estimated_rows = 1000,
             .source = RowEstimateSource::HashTableCache,
             .imprecise_estimate = false,
             .composite = false},
         RelationEstimateInfo{
             .name = "rt",
             .estimated_rows = 2000,
             .source = RowEstimateSource::PrimaryIndex,
             .imprecise_estimate = false,
             .composite = true}},
        {RelationEstimateInfo{
             .name = "lx",
             .estimated_rows = 1000,
             .source = RowEstimateSource::NoSource,
             .imprecise_estimate = true,
             .composite = false},
         RelationEstimateInfo{
             .name = "rx",
             .estimated_rows = 2000,
             .source = RowEstimateSource::NoSource,
             .imprecise_estimate = false,
             .composite = false}},
    };

    const std::vector<String> expected_names = {"lt[cache~1000] ", "lx[no_stats~1000] "};
    const std::vector<String> expected_right = {" (rt)", " rx[2000]"};

    for (size_t i = 0; i < relation_pairs.size(); ++i)
    {
        auto step = makePopulatedStep(relation_pairs[i].first, relation_pairs[i].second);

        String first = serializeStep(*step, current_version);
        auto restored = deserializeStep(first, current_version);

        EXPECT_EQ(first, serializeStep(*restored, current_version)) << "pair=" << i;

        /// Readable failures for the fields that do have getters.
        EXPECT_TRUE(restored->isOptimized()) << "pair=" << i;
        EXPECT_EQ(restored->getResultRowsEstimation(), std::optional<UInt64>(4242)) << "pair=" << i;
        EXPECT_TRUE(restored->hasImpreciseEstimate()) << "pair=" << i;
        EXPECT_EQ(restored->getRightHashTableCacheKey(), 0x1122334455667788ULL) << "pair=" << i;
        EXPECT_EQ(restored->getJoinOutputCacheKey(), 0x99AABBCCDDEEFF00ULL) << "pair=" << i;
        EXPECT_EQ(restored->getInputRowsEstimation(JoinTableSide::Left), std::optional<UInt64>(1000)) << "pair=" << i;
        EXPECT_EQ(restored->getInputRowsEstimation(JoinTableSide::Right), std::optional<UInt64>(2000)) << "pair=" << i;
        EXPECT_EQ(restored->getTableStatsHint(), "lt:1000,rt:2000") << "pair=" << i;

        const auto & column_stats = restored->getResultColumnStats();
        ASSERT_EQ(column_stats.size(), 2u) << "pair=" << i;
        ASSERT_TRUE(column_stats.contains("l")) << "pair=" << i;
        ASSERT_TRUE(column_stats.contains("r")) << "pair=" << i;
        EXPECT_EQ(column_stats.at("l").num_distinct_values, 7u) << "pair=" << i;
        EXPECT_EQ(column_stats.at("l").avg_bytes, 1.5) << "pair=" << i;
        EXPECT_EQ(column_stats.at("r").num_distinct_values, 11u) << "pair=" << i;
        EXPECT_EQ(column_stats.at("r").avg_bytes, 2.25) << "pair=" << i;

        /// The two `EXPECT_TRUE`s are what stop the comparison below from holding vacuously: the
        /// renderer returns an empty string when either name is empty, which is also what a dropped
        /// relation would produce on both sides.
        const String rendered = step->getReadableRelationName();
        EXPECT_TRUE(rendered.starts_with(expected_names[i])) << "pair=" << i << " rendered=" << rendered;
        EXPECT_TRUE(rendered.ends_with(expected_right[i])) << "pair=" << i << " rendered=" << rendered;
        EXPECT_EQ(restored->getReadableRelationName(), rendered) << "pair=" << i;
    }
}

/// A peer below version 12 receives none of it, and reading such a stream leaves every field at the
/// default the receiver would have used before the block existed.
TEST(JoinStepLogicalOptimizerStateRoundTrip, PreVersionCarriesNothing)
{
    auto populated = makePopulatedStep(
        RelationEstimateInfo{.name = "lt", .estimated_rows = 1000, .source = RowEstimateSource::HashTableCache},
        RelationEstimateInfo{.name = "rt", .estimated_rows = 2000, .source = RowEstimateSource::PrimaryIndex});
    auto defaulted = makeStep();

    String populated_bytes = serializeStep(*populated, pre_state_version);
    EXPECT_EQ(populated_bytes, serializeStep(*defaulted, pre_state_version));

    auto restored = deserializeStep(populated_bytes, pre_state_version);

    EXPECT_FALSE(restored->isOptimized());
    EXPECT_EQ(restored->getResultRowsEstimation(), std::nullopt);
    EXPECT_FALSE(restored->hasImpreciseEstimate());
    EXPECT_TRUE(restored->getResultColumnStats().empty());
    EXPECT_EQ(restored->getRightHashTableCacheKey(), 0u);
    EXPECT_EQ(restored->getJoinOutputCacheKey(), 0u);
    EXPECT_EQ(restored->getInputRowsEstimation(JoinTableSide::Left), std::nullopt);
    EXPECT_EQ(restored->getInputRowsEstimation(JoinTableSide::Right), std::nullopt);
    EXPECT_TRUE(restored->getTableStatsHint().empty());
}

/// `setOptimized` is the only production writer of all three `optimizer_flags` bits, so the reachable
/// patterns are 0, 1, 3, 5 and 7. The cases above pin 7 and 0, where the three bits are equal to each
/// other, and there a reader that assigns a bit to the wrong member stays invisible to both oracles: the
/// observables all move together, and re-serialization writes the swapped members back into the swapped
/// bits. The patterns below make the bits independently observable. The first is the state five of the
/// six `setOptimized` call sites produce.
TEST(JoinStepLogicalOptimizerStateRoundTrip, OptimizerFlagBitsRoundTripIndependently)
{
    struct Pattern
    {
        void (*setOptimized)(JoinStepLogical &);
        std::optional<UInt64> expected_rows_estimation;
        bool expected_imprecise_estimate;
    };

    const std::vector<Pattern> patterns = {
        /// optimizer_flags = 1
        {[](JoinStepLogical & step) { step.setOptimized(); }, std::nullopt, false},
        /// optimizer_flags = 3
        {[](JoinStepLogical & step) { step.setOptimized(std::nullopt, {}, /*imprecise_estimate_=*/true); }, std::nullopt, true},
        /// optimizer_flags = 5
        {[](JoinStepLogical & step) { step.setOptimized(/*estimated_rows_=*/4242); }, 4242, false},
    };

    for (size_t i = 0; i < patterns.size(); ++i)
    {
        auto step = makeStep();
        patterns[i].setOptimized(*step);

        String first = serializeStep(*step, current_version);
        auto restored = deserializeStep(first, current_version);

        EXPECT_EQ(first, serializeStep(*restored, current_version)) << "pattern=" << i;

        EXPECT_TRUE(restored->isOptimized()) << "pattern=" << i;
        EXPECT_EQ(restored->getResultRowsEstimation(), patterns[i].expected_rows_estimation) << "pattern=" << i;
        EXPECT_EQ(restored->hasImpreciseEstimate(), patterns[i].expected_imprecise_estimate) << "pattern=" << i;
    }
}

/// The presence-flag and empty-container paths: an unoptimized step is the common case on the wire.
TEST(JoinStepLogicalOptimizerStateRoundTrip, EmptyOptionalsAndEmptyMapRoundTrip)
{
    auto step = makeStep();

    String first = serializeStep(*step, current_version);
    auto restored = deserializeStep(first, current_version);

    EXPECT_EQ(first, serializeStep(*restored, current_version));

    EXPECT_FALSE(restored->isOptimized());
    EXPECT_EQ(restored->getResultRowsEstimation(), std::nullopt);
    EXPECT_TRUE(restored->getResultColumnStats().empty());
    EXPECT_EQ(restored->getInputRowsEstimation(JoinTableSide::Left), std::nullopt);
    EXPECT_TRUE(restored->getTableStatsHint().empty());
}
