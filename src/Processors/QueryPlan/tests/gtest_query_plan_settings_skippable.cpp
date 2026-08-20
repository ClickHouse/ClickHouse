#include <gtest/gtest.h>

#include <Core/BaseSettings.h>
#include <Core/SettingsEnums.h>
#include <Core/ProtocolDefines.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>

namespace DB
{
namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsUInt64 max_block_size;
    extern const QueryPlanSerializationSettingsNonZeroUInt64 grace_hash_join_initial_buckets;
    extern const QueryPlanSerializationSettingsBool aggregation_in_order_memory_bound_merging;
    extern const QueryPlanSerializationSettingsFloat remerge_sort_lowered_memory_bytes_ratio;
    extern const QueryPlanSerializationSettingsDouble max_bytes_ratio_before_external_sort;
    extern const QueryPlanSerializationSettingsString temporary_files_codec;
    extern const QueryPlanSerializationSettingsJoinAlgorithm join_algorithm;
    extern const QueryPlanSerializationSettingsOverflowMode distinct_overflow_mode;
    extern const QueryPlanSerializationSettingsOverflowModeGroupBy group_by_overflow_mode;
    extern const QueryPlanSerializationSettingsTotalsMode totals_mode;
}
}

using namespace DB;

/// From `DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SKIPPABLE_SETTINGS` on, the plan settings of a step are
/// written so that a peer can read past a name it does not know. That is what keeps a setting added to the list from
/// making every plan - including the ones the setting cannot affect - unreadable by a peer of the previous release,
/// and it is why such a setting no longer needs a version gate of its own. A name the writer declared `IMPORTANT`
/// is still rejected: ignoring it would change what the query returns.
namespace
{

constexpr UInt64 skippable_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SKIPPABLE_SETTINGS;
constexpr UInt64 strict_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SKIPPABLE_SETTINGS - 1;

/// A settings stream as a peer one release ahead would write it: the settings this version knows, plus one it does
/// not. `BaseSettingsHelpers` is what `BaseSettings::write` itself writes with, so this is the same encoding.
String streamWithUnknownSetting(std::string_view unknown_name, bool important)
{
    WriteBufferFromOwnString out;

    BaseSettingsHelpers::writeString("max_block_size", out);
    BaseSettingsHelpers::writeFlags({}, out);
    BaseSettingsHelpers::writeString("4096", out);

    BaseSettingsHelpers::writeString(unknown_name, out);
    BaseSettingsHelpers::writeFlags(important ? BaseSettingsHelpers::Flags::IMPORTANT : BaseSettingsHelpers::Flags{}, out);
    BaseSettingsHelpers::writeString("1", out);

    /// The empty name that ends the settings of a step.
    BaseSettingsHelpers::writeString(std::string_view{}, out);
    return out.str();
}

}

TEST(QueryPlanSettingsSkippable, ValuesSurviveTheRoundTrip)
{
    /// A value now travels as the string the setting prints itself as, so every type the list declares has to come
    /// back from its own text - one setting of each is exercised here, the floating-point ones with a value that
    /// says whether the text was rounded.
    QueryPlanSerializationSettings written;
    written[QueryPlanSerializationSetting::max_block_size] = 4096;
    written[QueryPlanSerializationSetting::grace_hash_join_initial_buckets] = 8;
    written[QueryPlanSerializationSetting::aggregation_in_order_memory_bound_merging] = true;
    written[QueryPlanSerializationSetting::remerge_sort_lowered_memory_bytes_ratio] = 0.3f;
    written[QueryPlanSerializationSetting::max_bytes_ratio_before_external_sort] = 0.1;
    written[QueryPlanSerializationSetting::temporary_files_codec] = "NONE";
    written[QueryPlanSerializationSetting::join_algorithm] = "direct,parallel_hash,hash";
    written[QueryPlanSerializationSetting::distinct_overflow_mode] = OverflowMode::BREAK;
    written[QueryPlanSerializationSetting::group_by_overflow_mode] = OverflowMode::ANY;
    written[QueryPlanSerializationSetting::totals_mode] = TotalsMode::AFTER_HAVING_INCLUSIVE;

    WriteBufferFromOwnString out;
    written.writeChangedBinary(out, skippable_version);

    ReadBufferFromString in(out.str());
    QueryPlanSerializationSettings read;
    read.readBinary(in, skippable_version);

    EXPECT_EQ(read[QueryPlanSerializationSetting::max_block_size], 4096u);
    EXPECT_EQ(read[QueryPlanSerializationSetting::grace_hash_join_initial_buckets], 8u);
    EXPECT_EQ(read[QueryPlanSerializationSetting::aggregation_in_order_memory_bound_merging], true);
    EXPECT_EQ(read[QueryPlanSerializationSetting::remerge_sort_lowered_memory_bytes_ratio], 0.3f);
    EXPECT_EQ(read[QueryPlanSerializationSetting::max_bytes_ratio_before_external_sort], 0.1);
    EXPECT_EQ(read[QueryPlanSerializationSetting::temporary_files_codec].value, "NONE");
    EXPECT_EQ(read[QueryPlanSerializationSetting::join_algorithm].toString(), "direct,parallel_hash,hash");
    EXPECT_EQ(read[QueryPlanSerializationSetting::distinct_overflow_mode], OverflowMode::BREAK);
    EXPECT_EQ(read[QueryPlanSerializationSetting::group_by_overflow_mode], OverflowMode::ANY);
    EXPECT_EQ(read[QueryPlanSerializationSetting::totals_mode], TotalsMode::AFTER_HAVING_INCLUSIVE);
}

TEST(QueryPlanSettingsSkippable, TheOlderFormStillRoundTrips)
{
    QueryPlanSerializationSettings written;
    written[QueryPlanSerializationSetting::max_block_size] = 8192;

    WriteBufferFromOwnString out;
    written.writeChangedBinary(out, strict_version);

    ReadBufferFromString in(out.str());
    QueryPlanSerializationSettings read;
    read.readBinary(in, strict_version);

    EXPECT_EQ(read[QueryPlanSerializationSetting::max_block_size], 8192u);
}

TEST(QueryPlanSettingsSkippable, AnUnknownSettingIsSkipped)
{
    auto stream = streamWithUnknownSetting("a_setting_from_a_later_release", /*important=*/ false);

    ReadBufferFromString in(stream);
    QueryPlanSerializationSettings read;
    ASSERT_NO_THROW(read.readBinary(in, skippable_version));

    /// The settings around the unknown one are read, and it leaves this version at its own default.
    EXPECT_EQ(read[QueryPlanSerializationSetting::max_block_size], 4096u);
}

TEST(QueryPlanSettingsSkippable, AnUnknownImportantSettingIsRejected)
{
    auto stream = streamWithUnknownSetting("a_setting_that_changes_the_result", /*important=*/ true);

    ReadBufferFromString in(stream);
    QueryPlanSerializationSettings read;
    EXPECT_THROW(read.readBinary(in, skippable_version), Exception);
}
