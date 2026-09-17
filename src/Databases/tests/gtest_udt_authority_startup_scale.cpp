#include <Databases/UDT/AuthorityRecovery.h>
#include <Databases/UDT/AuthorityVerificationSchedule.h>
#include <Databases/UDT/AuthorityVerificationScheduler.h>

#include <DataTypes/UDT/TemplateChecker.h>

#include <IO/WriteHelpers.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

UUID scaleUUID(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

String scaleDigestHex(const Digest & value)
{
    static constexpr char digits[] = "0123456789abcdef";
    String result;
    result.reserve(value.size() * 2);
    for (const UInt8 byte : value)
    {
        result.push_back(digits[byte >> 4]);
        result.push_back(digits[byte & 0x0f]);
    }
    return result;
}

void expectSerializedSchedulerOverrideFieldsEqual(
    const AuthorityVerificationSchedulerLimits & expected, const AuthorityVerificationSchedulerLimits & actual)
{
    EXPECT_EQ(actual.policy.bucket_count, expected.policy.bucket_count);
    EXPECT_EQ(actual.policy.bucket_seed, expected.policy.bucket_seed);
    EXPECT_EQ(actual.policy.recent_catalog_epoch_window, expected.policy.recent_catalog_epoch_window);
    EXPECT_EQ(actual.policy.high_dependency_threshold, expected.policy.high_dependency_threshold);
    EXPECT_EQ(actual.policy.maximum_recent_targets_per_batch, expected.policy.maximum_recent_targets_per_batch);
    EXPECT_EQ(actual.policy.maximum_high_dependency_targets_per_batch, expected.policy.maximum_high_dependency_targets_per_batch);
    EXPECT_EQ(actual.policy.random_targets_per_batch, expected.policy.random_targets_per_batch);

    EXPECT_EQ(actual.schedule.maximum_snapshot_targets, expected.schedule.maximum_snapshot_targets);
    EXPECT_EQ(actual.schedule.maximum_targets_per_batch, expected.schedule.maximum_targets_per_batch);
    EXPECT_EQ(actual.schedule.maximum_buckets, expected.schedule.maximum_buckets);
    EXPECT_EQ(actual.schedule.maximum_reverse_dependency_count, expected.schedule.maximum_reverse_dependency_count);
    EXPECT_EQ(actual.schedule.maximum_canonical_bytes_per_batch, expected.schedule.maximum_canonical_bytes_per_batch);
    EXPECT_EQ(actual.schedule.maximum_verification_work_units_per_batch, expected.schedule.maximum_verification_work_units_per_batch);
    EXPECT_EQ(actual.schedule.maximum_transient_bytes_per_batch, expected.schedule.maximum_transient_bytes_per_batch);
    EXPECT_EQ(actual.schedule.maximum_io_bytes_per_batch, expected.schedule.maximum_io_bytes_per_batch);
    EXPECT_EQ(actual.schedule.maximum_planner_work_units, expected.schedule.maximum_planner_work_units);
    EXPECT_EQ(actual.schedule.maximum_planner_scratch_bytes, expected.schedule.maximum_planner_scratch_bytes);
    EXPECT_EQ(actual.schedule.maximum_retained_canonical_bytes, expected.schedule.maximum_retained_canonical_bytes);

    EXPECT_EQ(actual.successful_batch_interval, expected.successful_batch_interval);
    EXPECT_EQ(actual.retry_interval, expected.retry_interval);
    EXPECT_EQ(actual.empty_root_probe_interval, expected.empty_root_probe_interval);
    EXPECT_EQ(actual.failure_backoff_interval, expected.failure_backoff_interval);
    EXPECT_EQ(actual.snapshot_build_interval, expected.snapshot_build_interval);
    EXPECT_EQ(actual.load_throttle_retry_interval, expected.load_throttle_retry_interval);
    EXPECT_EQ(actual.maximum_run_wall_time, expected.maximum_run_wall_time);
    EXPECT_EQ(actual.maximum_run_cpu_time, expected.maximum_run_cpu_time);
    EXPECT_EQ(actual.maximum_snapshot_targets_per_pass, expected.maximum_snapshot_targets_per_pass);
    EXPECT_EQ(actual.maximum_foreground_queries_for_admission, expected.maximum_foreground_queries_for_admission);
    EXPECT_EQ(actual.maximum_competing_background_tasks_for_admission, expected.maximum_competing_background_tasks_for_admission);
    EXPECT_EQ(actual.os_thread_nice_value, expected.os_thread_nice_value);

    EXPECT_EQ(actual.automatic_repair.maximum_local_wal_transactions, expected.automatic_repair.maximum_local_wal_transactions);
    EXPECT_EQ(actual.automatic_repair.maximum_local_wal_artifacts_examined, expected.automatic_repair.maximum_local_wal_artifacts_examined);
    EXPECT_EQ(actual.automatic_repair.maximum_local_wal_bytes_examined, expected.automatic_repair.maximum_local_wal_bytes_examined);
}

AuthorityRecoveryLimits scaleRecoveryLimits(UInt64 definitions, UInt64 workers, UInt64 batch_records)
{
    AuthorityRecoveryLimits limits;
    const UInt64 admitted_definitions = std::max<UInt64>(definitions, 1);
    limits.root.maximum_definition_records = admitted_definitions;
    limits.root.type_catalog.maximum_definitions = admitted_definitions;
    limits.lowering.maximum_definitions = admitted_definitions;
    limits.lowering.maximum_catalog_string_bytes = 256ULL << 20;
    limits.checker.maximum_definitions = admitted_definitions;
    limits.checker.maximum_catalog_input_bytes = 256ULL << 20;
    limits.checker.maximum_catalog_nodes = 40'960'000;
    limits.checker.maximum_catalog_edges = 2'560'000;
    limits.checker.maximum_catalog_checker_work = 3'287'070'000;
    limits.checker.maximum_canonical_catalog_bytes = 256ULL << 20;
    limits.checker.maximum_scratch_bytes = 1ULL << 30;
    limits.maximum_record_images = admitted_definitions;
    limits.parallel_workers = workers;
    limits.maximum_parallel_batch_records = batch_records;
    return limits;
}

class DefinitionRecoveryDataset final
{
public:
    explicit DefinitionRecoveryDataset(UInt64 definition_count_)
        : database_uuid(scaleUUID(0x8400, definition_count_ + 1))
        , definition_count(definition_count_)
    {
        std::vector<DefinitionInput> inputs;
        inputs.reserve(definition_count);
        for (UInt64 index = 0; index < definition_count; ++index)
        {
            const String local_name = "Value_" + std::to_string(index);
            DefinitionInput input;
            input.identity = {
                .database_uuid = database_uuid,
                .type_uuid = scaleUUID(0x8401, index + 1),
                .revision = 1,
            };
            input.normalized_name = "authority_startup." + local_name;
            input.normalized_local_name = local_name;
            TemplateNode node;
            node.kind = TemplateNodeKind::BuiltIn;
            node.atom = "UInt64";
            input.nodes.push_back(std::move(node));
            inputs.push_back(std::move(input));
        }

        auto checker_limits = scaleRecoveryLimits(definition_count, 1, 1).checker;
        definitions = TemplateChecker::checkAll(std::move(inputs), checker_limits);
        records.reserve(definition_count);
        encoded_records.reserve(definition_count);
        leaves.reserve(definition_count);
        graph_nodes.reserve(definition_count);
        for (const auto & definition : definitions)
        {
            const auto & identity = definition->getIdentity();
            records.push_back(makeRecord(
                *definition,
                {
                    .canonical_definition_sql = "ATTACH TYPE " + definition->getNormalizedName() + " UUID '" + toString(identity.type_uuid)
                        + "' REVISION 1 AS UInt64 DEFINITION HASH '" + scaleDigestHex(definition->getDefinitionHash()) + "'",
                    .canonical_physical_template_sql = "UInt64",
                    .owner_uuid = scaleUUID(0x8402, 1),
                    .owner_display_name = "owner",
                    .comment = {},
                    .creation_time_us_utc = 1,
                }));
            encoded_records.push_back(encodeRecord(records.back()));
            leaves.push_back({
                .key = {
                    .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
                    .object_uuid = identity.type_uuid,
                },
                .object_revision = identity.revision,
                .canonical_record_hash = computeRecordHash(records.back()),
            });
            graph_nodes.push_back({
                .kind = SchemaObjectKind::TypeDefinition,
                .database_uuid = database_uuid,
                .object_uuid = identity.type_uuid,
            });
        }
        std::sort(leaves.begin(), leaves.end(), authorityInventoryLeafLess);
        const auto inventory_summary = buildAuthorityInventorySummary(leaves);
        inventory_snapshot_bytes = encodeAuthorityInventorySnapshot(makeAuthorityInventorySnapshot(database_uuid, leaves));
        auto graph = SchemaObjectDependencyGraph::build(database_uuid, graph_nodes, {});
        graph_snapshot_bytes = graph->encodeSnapshot();
        state = makeAuthorityState(
            database_uuid,
            1,
            definition_authority_capability_mask,
            inventory_summary.leaf_count,
            inventory_summary.merkle_radix_root,
            graph->computeRoot());
    }

    AuthorityRoot::Ptr recover(UInt64 workers = 8, UInt64 batch_records = 64) const
    {
        std::vector<AuthorityRecordImage> images;
        images.reserve(definition_count);
        for (UInt64 index = 0; index < definition_count; ++index)
        {
            images.push_back({
                .key = {
                    .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
                    .object_uuid = records[index].identity.type_uuid,
                },
                .canonical_bytes = encoded_records[index],
            });
        }
        std::reverse(images.begin(), images.end());
        return recoverAuthorityRoot(
            state,
            recoveredTypeIndexGeneration(state),
            inventory_snapshot_bytes,
            graph_snapshot_bytes,
            images,
            scaleRecoveryLimits(definition_count, workers, batch_records));
    }

    std::optional<AuthorityInventoryKey>
    recoverCorrupted(std::span<const UInt64> corrupted_indices, UInt64 workers, UInt64 batch_records) const
    {
        auto corrupted_records = encoded_records;
        for (const UInt64 index : corrupted_indices)
            corrupted_records.at(index) = "corrupt";
        std::vector<AuthorityRecordImage> images;
        images.reserve(definition_count);
        for (UInt64 index = 0; index < definition_count; ++index)
        {
            images.push_back({
                .key = {
                    .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
                    .object_uuid = records[index].identity.type_uuid,
                },
                .canonical_bytes = corrupted_records[index],
            });
        }
        try
        {
            static_cast<void>(recoverAuthorityRoot(
                state,
                recoveredTypeIndexGeneration(state),
                inventory_snapshot_bytes,
                graph_snapshot_bytes,
                images,
                scaleRecoveryLimits(definition_count, workers, batch_records)));
        }
        catch (const AuthorityRecoveryError & error)
        {
            EXPECT_EQ(error.code, AuthorityRecoveryError::Code::RecordMismatch);
            return error.record_key;
        }
        ADD_FAILURE() << "Expected recovery failure";
        return std::nullopt;
    }

    std::vector<AuthorityVerificationTarget> verificationTargets(const AuthorityRoot & root) const
    {
        std::vector<AuthorityVerificationTarget> targets;
        const auto leaves_view = root.pinAuthorityInventory()->getLeaves();
        targets.reserve(leaves_view.size());
        for (const auto & leaf : leaves_view)
        {
            targets.push_back({
                .leaf = leaf,
                .last_changed_catalog_epoch = 1,
                .cost = {
                    .canonical_bytes = 1,
                    .work_units = 1,
                    .transient_bytes = 1,
                    .io_bytes = 1,
                },
            });
        }
        return targets;
    }

    UUID database_uuid;
    UInt64 definition_count;
    std::vector<Definition::Ptr> definitions;
    std::vector<Record> records;
    std::vector<String> encoded_records;
    std::vector<AuthorityInventoryLeaf> leaves;
    std::vector<SchemaObjectID> graph_nodes;
    String inventory_snapshot_bytes;
    String graph_snapshot_bytes;
    AuthorityState state;
};

class UDTAuthorityStartupScale : public testing::TestWithParam<UInt64>
{
};

TEST_P(UDTAuthorityStartupScale, RecoversOneCanonicalRootWithBoundedParallelBatches)
{
    const UInt64 definition_count = GetParam();
    DefinitionRecoveryDataset dataset(definition_count);
    auto root = dataset.recover();
    ASSERT_NE(root, nullptr);
    EXPECT_EQ(root->getInventorySummary().leaf_count, definition_count);
    EXPECT_EQ(root->getDefinitionRecordCount(), definition_count);
    EXPECT_EQ(root->getDatabaseCatalogEpoch(), 1);
    EXPECT_EQ(root->getTypeIndexGeneration(), 1);
    EXPECT_EQ(root->getAuthorityState(), dataset.state);

    AuthorityVerificationSchedulePolicy policy;
    policy.bucket_count = 64;
    AuthorityVerificationScheduleLimits schedule_limits;
    schedule_limits.maximum_targets_per_batch = 64;
    const auto cursor = makeAuthorityVerificationScheduleCursor(dataset.database_uuid, policy, schedule_limits);
    EXPECT_EQ(cursor.database_uuid, dataset.database_uuid);
    EXPECT_EQ(cursor.current_bucket, 0);
    EXPECT_EQ(cursor.completed_rotations, 0);
    EXPECT_EQ(cursor.planned_batches, 0);
    const auto targets = dataset.verificationTargets(*root);
    const auto plan = planPeriodicAuthorityVerification(*root, targets, cursor, policy, schedule_limits);
    EXPECT_EQ(
        plan->getStatus(),
        definition_count == 0 ? AuthorityVerificationScheduleStatus::EmptySnapshot : AuthorityVerificationScheduleStatus::Scheduled);
    EXPECT_EQ(plan->getRoot().authority_anchor, root->getAuthorityState().anchor_hash);
    EXPECT_EQ(plan->getRetryCursor(), cursor);
}

INSTANTIATE_TEST_SUITE_P(ZeroOneTenThousandAndHundredThousand, UDTAuthorityStartupScale, testing::Values(0, 1, 10'000, 100'000));

TEST(UDTAuthorityStartupScaleDeterminism, LowestCanonicalFailureIsIndependentOfWorkerAndBatchCounts)
{
    DefinitionRecoveryDataset dataset(10'000);
    const std::array<UInt64, 2> corruptions{17, 9'001};
    const auto serial = dataset.recoverCorrupted(corruptions, 1, 1);
    const auto parallel = dataset.recoverCorrupted(corruptions, 8, 64);
    ASSERT_TRUE(serial.has_value());
    ASSERT_TRUE(parallel.has_value());
    EXPECT_EQ(*serial, *parallel);
    EXPECT_EQ(serial->object_uuid, dataset.records[corruptions.front()].identity.type_uuid);
}

TEST(UDTAuthorityStartupScaleDeterminism, RecoveredRootProducesStableCursorAndCooperativePlan)
{
    DefinitionRecoveryDataset dataset(10'000);
    auto serial_root = dataset.recover(1, 1);
    auto parallel_root = dataset.recover(8, 64);
    ASSERT_EQ(serial_root->getAuthorityState(), parallel_root->getAuthorityState());
    ASSERT_EQ(serial_root->getTypeIndexContentDigest(), parallel_root->getTypeIndexContentDigest());

    auto serial_targets = dataset.verificationTargets(*serial_root);
    auto parallel_targets = dataset.verificationTargets(*parallel_root);
    ASSERT_EQ(serial_targets, parallel_targets);
    AuthorityVerificationSchedulePolicy policy;
    policy.bucket_count = 64;
    policy.maximum_recent_targets_per_batch = 8;
    policy.maximum_high_dependency_targets_per_batch = 8;
    policy.random_targets_per_batch = 8;
    AuthorityVerificationScheduleLimits limits;
    limits.maximum_targets_per_batch = 64;
    const auto cursor = makeAuthorityVerificationScheduleCursor(dataset.database_uuid, policy, limits);

    auto serial_plan = planPeriodicAuthorityVerification(*serial_root, serial_targets, cursor, policy, limits);
    std::unique_ptr<AuthorityVerificationPlanningContinuation> continuation;
    AuthorityVerificationPlanningResult result;
    AuthorityVerificationPassBudget budget;
    budget.maximum_work_items = 1;
    for (UInt64 invocation = 0; invocation < 1'000'000 && result.status != AuthorityVerificationPlanningStatus::Complete; ++invocation)
        result
            = resumePeriodicAuthorityVerificationPlanning(*parallel_root, parallel_targets, cursor, policy, limits, continuation, budget);
    ASSERT_EQ(result.status, AuthorityVerificationPlanningStatus::Complete);
    ASSERT_NE(result.plan, nullptr);
    EXPECT_EQ(result.plan->getRoot(), serial_plan->getRoot());
    EXPECT_EQ(result.plan->getRetryCursor(), serial_plan->getRetryCursor());
    EXPECT_TRUE(std::ranges::equal(result.plan->getTargets(), serial_plan->getTargets()));
    EXPECT_EQ(result.plan->getStatistics(), serial_plan->getStatistics());
}

TEST(UDTAuthorityVerificationSchedule, RotationProgressSurvivesPrioritySelectionAndTightBatchBudgets)
{
    DefinitionRecoveryDataset dataset(16);
    auto root = dataset.recover(1, 1);
    auto targets = dataset.verificationTargets(*root);
    for (auto & target : targets)
    {
        target.last_changed_catalog_epoch = 0;
        target.reverse_dependency_count = 0;
        target.cost = {.canonical_bytes = 1, .work_units = 1, .transient_bytes = 1, .io_bytes = 1};
    }
    AuthorityVerificationSchedulePolicy policy;
    policy.bucket_count = 64;
    policy.recent_catalog_epoch_window = 1;
    policy.high_dependency_threshold = 10;
    policy.maximum_recent_targets_per_batch = 2;
    policy.maximum_high_dependency_targets_per_batch = 2;
    policy.random_targets_per_batch = 1;
    AuthorityVerificationScheduleLimits limits;
    limits.maximum_targets_per_batch = 8;
    auto cursor = makeAuthorityVerificationScheduleCursor(dataset.database_uuid, policy, limits);

    const auto baseline = planPeriodicAuthorityVerification(*root, targets, cursor, policy, limits);
    const auto is_rotation_leaf = [&](const AuthorityInventoryLeaf & leaf)
    {
        return std::ranges::any_of(
            baseline->getTargets(),
            [&](const auto & selected)
            {
                return selected.leaf == leaf
                    && (selected.reasons & authorityVerificationSelectionReasonMask(AuthorityVerificationSelectionReason::Rotation)) != 0;
            });
    };
    const auto priority = std::ranges::find_if(targets, [&](const auto & target) { return !is_rotation_leaf(target.leaf); });
    ASSERT_NE(priority, targets.end());
    priority->last_changed_catalog_epoch = root->getDatabaseCatalogEpoch();
    priority->reverse_dependency_count = policy.high_dependency_threshold;

    const auto prioritized = planPeriodicAuthorityVerification(*root, targets, cursor, policy, limits);
    const auto selected_priority
        = std::ranges::find(prioritized->getTargets(), priority->leaf, &ScheduledAuthorityVerificationTarget::leaf);
    ASSERT_NE(selected_priority, prioritized->getTargets().end());
    EXPECT_NE(
        selected_priority->reasons & authorityVerificationSelectionReasonMask(AuthorityVerificationSelectionReason::RecentlyChanged), 0);
    EXPECT_NE(
        selected_priority->reasons & authorityVerificationSelectionReasonMask(AuthorityVerificationSelectionReason::HighDependency), 0);
    EXPECT_GE(prioritized->getStatistics().rotation_targets, 1);

    limits.maximum_canonical_bytes_per_batch = 1;
    limits.maximum_verification_work_units_per_batch = 1;
    limits.maximum_transient_bytes_per_batch = 1;
    limits.maximum_io_bytes_per_batch = 1;
    const auto budget_limited = planPeriodicAuthorityVerification(*root, targets, cursor, policy, limits);
    ASSERT_EQ(budget_limited->getTargets().size(), 1);
    EXPECT_NE(
        budget_limited->getTargets().front().reasons
            & authorityVerificationSelectionReasonMask(AuthorityVerificationSelectionReason::Rotation),
        0);
    EXPECT_EQ(budget_limited->getStatistics().verification_canonical_bytes, 1);
    EXPECT_EQ(budget_limited->getStatistics().verification_work_units, 1);
}

TEST(UDTAuthorityVerificationSchedule, RootedOversizedTargetRunsAloneAndFailsAboveItsRootDomain)
{
    DefinitionRecoveryDataset dataset(8);
    auto root = dataset.recover(1, 1);
    auto targets = dataset.verificationTargets(*root);
    AuthorityVerificationSchedulePolicy policy;
    policy.bucket_count = 64;
    policy.maximum_recent_targets_per_batch = 1;
    policy.maximum_high_dependency_targets_per_batch = 1;
    policy.random_targets_per_batch = 1;
    AuthorityVerificationScheduleLimits limits;
    limits.maximum_targets_per_batch = 8;
    limits.maximum_canonical_bytes_per_batch = 2;
    limits.maximum_verification_work_units_per_batch = 2;
    limits.maximum_transient_bytes_per_batch = 2;
    limits.maximum_io_bytes_per_batch = 2;
    limits.maximum_rooted_target_canonical_bytes = 3;
    limits.maximum_rooted_target_verification_work_units = 3;
    limits.maximum_rooted_target_transient_bytes = 3;
    limits.maximum_rooted_target_io_bytes = 3;
    const auto cursor = makeAuthorityVerificationScheduleCursor(dataset.database_uuid, policy, limits);
    const auto baseline = planPeriodicAuthorityVerification(*root, targets, cursor, policy, limits);
    ASSERT_FALSE(baseline->getTargets().empty());
    const auto rotation_leaf = std::ranges::find_if(
        baseline->getTargets(),
        [](const auto & target)
        { return (target.reasons & authorityVerificationSelectionReasonMask(AuthorityVerificationSelectionReason::Rotation)) != 0; });
    ASSERT_NE(rotation_leaf, baseline->getTargets().end());
    const auto oversized = std::ranges::find(targets, rotation_leaf->leaf, &AuthorityVerificationTarget::leaf);
    ASSERT_NE(oversized, targets.end());
    oversized->cost = {.canonical_bytes = 3, .work_units = 3, .transient_bytes = 3, .io_bytes = 3};

    const auto plan = planPeriodicAuthorityVerification(*root, targets, cursor, policy, limits);
    ASSERT_EQ(plan->getTargets().size(), 1);
    EXPECT_EQ(plan->getTargets().front().leaf, oversized->leaf);
    EXPECT_EQ(plan->getStatistics().verification_canonical_bytes, 3);
    EXPECT_EQ(plan->getStatistics().verification_work_units, 3);
    EXPECT_EQ(plan->getStatistics().verification_transient_bytes, 3);
    EXPECT_EQ(plan->getStatistics().verification_io_bytes, 3);

    limits.maximum_rooted_target_canonical_bytes = 2;
    limits.maximum_rooted_target_verification_work_units = 2;
    limits.maximum_rooted_target_transient_bytes = 2;
    limits.maximum_rooted_target_io_bytes = 2;
    try
    {
        static_cast<void>(planPeriodicAuthorityVerification(*root, targets, cursor, policy, limits));
        FAIL() << "Expected an unschedulable rooted target";
    }
    catch (const AuthorityVerificationScheduleError & error)
    {
        EXPECT_EQ(error.code, AuthorityVerificationScheduleError::Code::UnschedulableRotationTarget);
    }
}

TEST(UDTAuthorityVerificationSchedule, CompleteRepairPlanningPartitionsEveryTargetExactlyOnce)
{
    DefinitionRecoveryDataset dataset(10);
    auto root = dataset.recover(1, 1);
    const auto targets = dataset.verificationTargets(*root);
    AuthorityVerificationScheduleLimits limits;
    limits.maximum_targets_per_batch = 3;
    const auto plans = planCompleteAuthorityRepairReverification(*root, targets, limits);
    ASSERT_EQ(plans.size(), 4);

    std::vector<AuthorityInventoryLeaf> planned_leaves;
    for (const auto & plan : plans)
    {
        ASSERT_FALSE(plan->getTargets().empty());
        EXPECT_LE(plan->getTargets().size(), 3);
        for (const auto & target : plan->getTargets())
        {
            EXPECT_NE(target.reasons & authorityVerificationSelectionReasonMask(AuthorityVerificationSelectionReason::Rotation), 0);
            planned_leaves.push_back(target.leaf);
        }
    }
    ASSERT_EQ(planned_leaves.size(), targets.size());
    for (size_t index = 0; index < targets.size(); ++index)
        EXPECT_EQ(planned_leaves[index], targets[index].leaf);
}

TEST(UDTAuthorityVerificationSchedulerOverride, V2RoundTripPreservesEverySerializedFieldAndRejectsDamage)
{
    const UUID database_uuid = scaleUUID(0x8450, 1);
    AuthorityVerificationSchedulerLimits limits;
    limits.policy.bucket_count = 31;
    limits.policy.bucket_seed = 0x123456789abcdef0ULL;
    limits.policy.recent_catalog_epoch_window = 37;
    limits.policy.high_dependency_threshold = 41;
    limits.policy.maximum_recent_targets_per_batch = 43;
    limits.policy.maximum_high_dependency_targets_per_batch = 47;
    limits.policy.random_targets_per_batch = 53;
    limits.schedule.maximum_snapshot_targets = 4'093;
    limits.schedule.maximum_targets_per_batch = 127;
    limits.schedule.maximum_buckets = 509;
    limits.schedule.maximum_reverse_dependency_count = 4'001;
    limits.schedule.maximum_canonical_bytes_per_batch = (128ULL << 20) + 11;
    limits.schedule.maximum_verification_work_units_per_batch = 4'000'013;
    limits.schedule.maximum_transient_bytes_per_batch = (32ULL << 20) + 17;
    limits.schedule.maximum_io_bytes_per_batch = (96ULL << 20) + 19;
    limits.schedule.maximum_planner_work_units = 60'000'023;
    limits.schedule.maximum_planner_scratch_bytes = (15ULL << 20) + 29;
    limits.schedule.maximum_retained_canonical_bytes = (14ULL << 20) + 31;
    limits.successful_batch_interval = std::chrono::milliseconds(10'001);
    limits.retry_interval = std::chrono::milliseconds(10'003);
    limits.empty_root_probe_interval = std::chrono::milliseconds(10'007);
    limits.failure_backoff_interval = std::chrono::milliseconds(10'009);
    limits.snapshot_build_interval = std::chrono::milliseconds(1'011);
    limits.load_throttle_retry_interval = std::chrono::milliseconds(1'013);
    limits.maximum_run_wall_time = std::chrono::milliseconds(1'019);
    limits.maximum_run_cpu_time = std::chrono::milliseconds(1'021);
    limits.maximum_snapshot_targets_per_pass = 257;
    limits.maximum_foreground_queries_for_admission = 263;
    limits.maximum_competing_background_tasks_for_admission = 269;
    limits.os_thread_nice_value = 17;
    limits.automatic_repair.maximum_local_wal_transactions = 271;
    limits.automatic_repair.maximum_local_wal_artifacts_examined = 277;
    limits.automatic_repair.maximum_local_wal_bytes_examined = 283;

    const String encoded = encodeAuthorityVerificationSchedulerOverrideV2(database_uuid, limits);
    const auto decoded = decodeAuthorityVerificationSchedulerOverrideV2(encoded, database_uuid);
    expectSerializedSchedulerOverrideFieldsEqual(limits, decoded);
    EXPECT_EQ(encodeAuthorityVerificationSchedulerOverrideV2(database_uuid, decoded), encoded);

    String damaged = encoded;
    damaged.back() ^= 1;
    EXPECT_THROW(
        static_cast<void>(decodeAuthorityVerificationSchedulerOverrideV2(damaged, database_uuid)), AuthorityVerificationScheduleError);
    EXPECT_THROW(
        static_cast<void>(decodeAuthorityVerificationSchedulerOverrideV2(encoded.substr(0, encoded.size() - 1), database_uuid)),
        AuthorityVerificationScheduleError);
    EXPECT_THROW(
        static_cast<void>(decodeAuthorityVerificationSchedulerOverrideV2(encoded, scaleUUID(0x8450, 2))),
        AuthorityVerificationScheduleError);
}

TEST(UDTAuthorityVerificationSchedulerOverride, MergeCoversEveryPersistedOverrideField)
{
    AuthorityVerificationSchedulerLimits global;
    global.policy.bucket_count = 64;
    global.policy.bucket_seed = 1'001;
    global.policy.recent_catalog_epoch_window = 51;
    global.policy.high_dependency_threshold = 151;
    global.policy.maximum_recent_targets_per_batch = 21;
    global.policy.maximum_high_dependency_targets_per_batch = 23;
    global.policy.random_targets_per_batch = 7;
    global.schedule.maximum_snapshot_targets = 10'000;
    global.schedule.maximum_targets_per_batch = 512;
    global.schedule.maximum_buckets = 2'048;
    global.schedule.maximum_reverse_dependency_count = 100'000;
    global.schedule.maximum_canonical_bytes_per_batch = 192ULL << 20;
    global.schedule.maximum_verification_work_units_per_batch = 7'000'000;
    global.schedule.maximum_transient_bytes_per_batch = 48ULL << 20;
    global.schedule.maximum_io_bytes_per_batch = 192ULL << 20;
    global.schedule.maximum_rooted_target_canonical_bytes = 224ULL << 20;
    global.schedule.maximum_rooted_target_verification_work_units = 8'000'000;
    global.schedule.maximum_rooted_target_transient_bytes = 56ULL << 20;
    global.schedule.maximum_rooted_target_io_bytes = 224ULL << 20;
    global.schedule.maximum_planner_work_units = 50'000'000;
    global.schedule.maximum_planner_scratch_bytes = 12ULL << 20;
    global.schedule.maximum_retained_canonical_bytes = 12ULL << 20;
    global.successful_batch_interval = std::chrono::milliseconds(1'000);
    global.retry_interval = std::chrono::milliseconds(2'200);
    global.empty_root_probe_interval = std::chrono::milliseconds(3'000);
    global.failure_backoff_interval = std::chrono::milliseconds(4'400);
    global.snapshot_build_interval = std::chrono::milliseconds(500);
    global.load_throttle_retry_interval = std::chrono::milliseconds(660);
    global.maximum_run_wall_time = std::chrono::milliseconds(700);
    global.maximum_run_cpu_time = std::chrono::milliseconds(880);
    global.maximum_snapshot_targets_per_pass = 300;
    global.maximum_foreground_queries_for_admission = 9;
    global.maximum_competing_background_tasks_for_admission = 17;
    global.os_thread_nice_value = 8;
    global.executor.maximum_terminal_targets = 503;
    global.automatic_repair.maximum_local_wal_transactions = 101;
    global.automatic_repair.maximum_local_wal_artifacts_examined = 222;
    global.automatic_repair.maximum_local_wal_bytes_examined = 30'303;
    global.load_probe = []
    {
        return AuthorityVerificationSchedulerLoadSnapshot{
            .foreground_queries = 777,
            .competing_background_tasks = 888,
        };
    };

    AuthorityVerificationSchedulerLimits persisted;
    persisted.policy.bucket_count = 32;
    persisted.policy.bucket_seed = 2'002;
    persisted.policy.recent_catalog_epoch_window = 61;
    persisted.policy.high_dependency_threshold = 141;
    persisted.policy.maximum_recent_targets_per_batch = 19;
    persisted.policy.maximum_high_dependency_targets_per_batch = 29;
    persisted.policy.random_targets_per_batch = 5;
    persisted.schedule.maximum_snapshot_targets = 9'000;
    persisted.schedule.maximum_targets_per_batch = 600;
    persisted.schedule.maximum_buckets = 1'024;
    persisted.schedule.maximum_reverse_dependency_count = 110'000;
    persisted.schedule.maximum_canonical_bytes_per_batch = 160ULL << 20;
    persisted.schedule.maximum_verification_work_units_per_batch = 7'500'000;
    persisted.schedule.maximum_transient_bytes_per_batch = 40ULL << 20;
    persisted.schedule.maximum_io_bytes_per_batch = 200ULL << 20;
    persisted.schedule.maximum_rooted_target_canonical_bytes = 240ULL << 20;
    persisted.schedule.maximum_rooted_target_verification_work_units = 8'200'000;
    persisted.schedule.maximum_rooted_target_transient_bytes = 60ULL << 20;
    persisted.schedule.maximum_rooted_target_io_bytes = 240ULL << 20;
    persisted.schedule.maximum_planner_work_units = 55'000'000;
    persisted.schedule.maximum_planner_scratch_bytes = 14ULL << 20;
    persisted.schedule.maximum_retained_canonical_bytes = 10ULL << 20;
    persisted.successful_batch_interval = std::chrono::milliseconds(1'100);
    persisted.retry_interval = std::chrono::milliseconds(2'100);
    persisted.empty_root_probe_interval = std::chrono::milliseconds(3'300);
    persisted.failure_backoff_interval = std::chrono::milliseconds(4'000);
    persisted.snapshot_build_interval = std::chrono::milliseconds(550);
    persisted.load_throttle_retry_interval = std::chrono::milliseconds(600);
    persisted.maximum_run_wall_time = std::chrono::milliseconds(770);
    persisted.maximum_run_cpu_time = std::chrono::milliseconds(800);
    persisted.maximum_snapshot_targets_per_pass = 333;
    persisted.maximum_foreground_queries_for_admission = 7;
    persisted.maximum_competing_background_tasks_for_admission = 19;
    persisted.os_thread_nice_value = 12;
    persisted.executor.maximum_terminal_targets = 509;
    persisted.automatic_repair.maximum_local_wal_transactions = 111;
    persisted.automatic_repair.maximum_local_wal_artifacts_examined = 202;
    persisted.automatic_repair.maximum_local_wal_bytes_examined = 33'333;
    persisted.load_probe = []
    {
        return AuthorityVerificationSchedulerLoadSnapshot{
            .foreground_queries = 999,
            .competing_background_tasks = 1'111,
        };
    };

    const auto merged = mergeAuthorityVerificationSchedulerLimits(global, persisted);
    auto expected = global;
    expected.policy.bucket_count = persisted.policy.bucket_count;
    expected.policy.bucket_seed = persisted.policy.bucket_seed;
    expected.policy.maximum_recent_targets_per_batch = persisted.policy.maximum_recent_targets_per_batch;
    expected.policy.random_targets_per_batch = persisted.policy.random_targets_per_batch;
    expected.schedule.maximum_snapshot_targets = persisted.schedule.maximum_snapshot_targets;
    expected.schedule.maximum_buckets = persisted.schedule.maximum_buckets;
    expected.schedule.maximum_canonical_bytes_per_batch = persisted.schedule.maximum_canonical_bytes_per_batch;
    expected.schedule.maximum_transient_bytes_per_batch = persisted.schedule.maximum_transient_bytes_per_batch;
    expected.schedule.maximum_retained_canonical_bytes = persisted.schedule.maximum_retained_canonical_bytes;
    expected.successful_batch_interval = persisted.successful_batch_interval;
    expected.empty_root_probe_interval = persisted.empty_root_probe_interval;
    expected.snapshot_build_interval = persisted.snapshot_build_interval;
    expected.maximum_run_cpu_time = persisted.maximum_run_cpu_time;
    expected.maximum_foreground_queries_for_admission = persisted.maximum_foreground_queries_for_admission;
    expected.os_thread_nice_value = persisted.os_thread_nice_value;
    expected.automatic_repair.maximum_local_wal_artifacts_examined = persisted.automatic_repair.maximum_local_wal_artifacts_examined;

    expectSerializedSchedulerOverrideFieldsEqual(expected, merged);
    EXPECT_EQ(merged.schedule.maximum_rooted_target_canonical_bytes, global.schedule.maximum_rooted_target_canonical_bytes);
    EXPECT_EQ(merged.schedule.maximum_rooted_target_verification_work_units, global.schedule.maximum_rooted_target_verification_work_units);
    EXPECT_EQ(merged.schedule.maximum_rooted_target_transient_bytes, global.schedule.maximum_rooted_target_transient_bytes);
    EXPECT_EQ(merged.schedule.maximum_rooted_target_io_bytes, global.schedule.maximum_rooted_target_io_bytes);
    EXPECT_EQ(merged.executor.maximum_terminal_targets, global.executor.maximum_terminal_targets);
    ASSERT_TRUE(static_cast<bool>(merged.load_probe));
    const auto load = merged.load_probe();
    EXPECT_EQ(load.foreground_queries, 777);
    EXPECT_EQ(load.competing_background_tasks, 888);
}

}
}
