#include <gtest/gtest.h>

#include <base/unit.h>
#include <Core/Defines.h>
#include <Core/Joins.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/SortingStep.h>

using namespace DB;

/// `allow_experimental_codecs` tells a remote shard that the initiator accepted an experimental
/// `temporary_files_codec`, so the shard resolves the same codec when it spills.
///
/// `QueryPlanSerializationSettings` is a strict named schema: `readBinary` throws on a name it does not know,
/// so a peer that predates the setting rejects any plan carrying it. It must therefore go on the wire only
/// when the spill behavior of the step depends on it - the step can reach temporary files at all, the opt-in
/// is set, and the codec it enables is experimental. For every other plan the reader's default (`false`)
/// encodes the identical behavior, so emitting the name would break a rolling upgrade for nothing.
namespace
{

/// `ALP` is experimental and, unlike `PCO`, always compiled in.
const String experimental_codec = "ALP";
const String plain_codec = "LZ4";

/// The name as it appears in the binary settings stream written by `writeChangedBinary`.
bool wireCarriesSetting(const QueryPlanSerializationSettings & settings)
{
    WriteBufferFromOwnString out;
    settings.writeChangedBinary(out);
    return out.str().find("allow_experimental_codecs") != std::string::npos;
}

bool sortingStepCarriesSetting(const String & codec, bool allow_experimental_codecs, size_t max_bytes_before_external_sort)
{
    SortingStep::Settings sort_settings(/*max_block_size_=*/65536);
    sort_settings.temporary_files_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    sort_settings.temporary_files_codec = codec;
    sort_settings.allow_experimental_codecs = allow_experimental_codecs;
    sort_settings.max_bytes_in_block_before_external_sort = max_bytes_before_external_sort;

    QueryPlanSerializationSettings settings;
    sort_settings.updatePlanSettings(settings);
    return wireCarriesSetting(settings);
}

JoinSettings makeJoinSettings(const String & codec, bool allow_experimental_codecs, std::vector<JoinAlgorithm> algorithms)
{
    JoinSettings join_settings(QueryPlanSerializationSettings{});
    join_settings.temporary_files_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    join_settings.temporary_files_codec = codec;
    join_settings.allow_experimental_codecs = allow_experimental_codecs;
    join_settings.join_algorithms = std::move(algorithms);
    join_settings.max_bytes_before_external_join = 0;
    join_settings.max_bytes_ratio_before_external_join = 0.;
    join_settings.max_rows_in_join = 0;
    join_settings.max_bytes_in_join = 0;
    return join_settings;
}

bool joinCarriesSetting(const JoinSettings & join_settings)
{
    QueryPlanSerializationSettings settings;
    join_settings.updatePlanSettings(settings);
    return wireCarriesSetting(settings);
}

}

TEST(ExperimentalSpillCodecPlanSetting, EmittedOnlyWhenSpillingCanReachTheCodec)
{
    /// External aggregation: `Aggregator` reaches `writeToTemporaryFile` only with a non-zero
    /// `max_bytes_before_external_group_by`, which is what `AggregatingStep::serializeSettings` passes here.
    EXPECT_TRUE(spillCodecNeedsExperimentalCodecsOptIn(/*spill_is_reachable=*/true, true, experimental_codec));
    EXPECT_FALSE(spillCodecNeedsExperimentalCodecsOptIn(/*spill_is_reachable=*/false, true, experimental_codec));

    /// Nothing to communicate for a codec the receiver accepts without the opt-in, or when the initiator
    /// itself did not opt in.
    EXPECT_FALSE(spillCodecNeedsExperimentalCodecsOptIn(true, true, plain_codec));
    EXPECT_FALSE(spillCodecNeedsExperimentalCodecsOptIn(true, true, /*compression_codec=*/""));
    EXPECT_FALSE(spillCodecNeedsExperimentalCodecsOptIn(true, false, experimental_codec));
}

TEST(ExperimentalSpillCodecPlanSetting, SortingStepEmitsItOnlyForAnExternalSort)
{
    EXPECT_TRUE(sortingStepCarriesSetting(experimental_codec, true, /*max_bytes_before_external_sort=*/1_MiB));

    /// `MergeSortingTransform::consume` never touches the temporary data with the threshold at `0`.
    EXPECT_FALSE(sortingStepCarriesSetting(experimental_codec, true, /*max_bytes_before_external_sort=*/0));

    EXPECT_FALSE(sortingStepCarriesSetting(plain_codec, true, 1_MiB));
    EXPECT_FALSE(sortingStepCarriesSetting(experimental_codec, false, 1_MiB));
}

TEST(ExperimentalSpillCodecPlanSetting, JoinEmitsItOnlyForASpillingJoin)
{
    /// An in-memory hash join with no external-join threshold and no in-memory size limits never reaches
    /// temporary-file join code.
    auto in_memory_hash = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::HASH});
    EXPECT_FALSE(in_memory_hash.canSpillToTemporaryFiles());
    EXPECT_FALSE(joinCarriesSetting(in_memory_hash));

    auto in_memory_parallel_hash = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::PARALLEL_HASH});
    EXPECT_FALSE(joinCarriesSetting(in_memory_parallel_hash));

    /// The automatic conversion to a spilling hash join is gated on an external-join threshold; either the
    /// absolute setting or the ratio enables it.
    auto spilling_hash = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::HASH});
    spilling_hash.max_bytes_before_external_join = 1_MiB;
    EXPECT_TRUE(spilling_hash.canSpillToTemporaryFiles());
    EXPECT_TRUE(joinCarriesSetting(spilling_hash));

    auto ratio_hash = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::HASH});
    ratio_hash.max_bytes_ratio_before_external_join = 0.5;
    EXPECT_TRUE(joinCarriesSetting(ratio_hash));

    /// `grace_hash` always spills; `partial_merge` and `auto` can end up in `MergeJoin`, which writes the
    /// right table to disk.
    for (auto algorithm : {JoinAlgorithm::GRACE_HASH, JoinAlgorithm::PARTIAL_MERGE,
                           JoinAlgorithm::PREFER_PARTIAL_MERGE, JoinAlgorithm::AUTO})
        EXPECT_TRUE(joinCarriesSetting(makeJoinSettings(experimental_codec, true, {algorithm})));

    /// `ConstantJoin` (`CROSS` and comma joins) spills once the in-memory size limits would be exceeded,
    /// whatever the algorithm is.
    auto hash_with_size_limit = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::HASH});
    hash_with_size_limit.max_bytes_in_join = 1_MiB;
    EXPECT_TRUE(joinCarriesSetting(hash_with_size_limit));

    auto hash_with_row_limit = makeJoinSettings(experimental_codec, true, {JoinAlgorithm::HASH});
    hash_with_row_limit.max_rows_in_join = 1000;
    EXPECT_TRUE(joinCarriesSetting(hash_with_row_limit));

    /// A spilling join still needs nothing on the wire for a non-experimental codec, or without the opt-in.
    auto plain = makeJoinSettings(plain_codec, true, {JoinAlgorithm::GRACE_HASH});
    EXPECT_FALSE(joinCarriesSetting(plain));

    auto no_opt_in = makeJoinSettings(experimental_codec, false, {JoinAlgorithm::GRACE_HASH});
    EXPECT_FALSE(joinCarriesSetting(no_opt_in));
}
