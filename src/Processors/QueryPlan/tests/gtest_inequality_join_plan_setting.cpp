#include <gtest/gtest.h>

#include <Core/Settings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>

using namespace DB;

namespace DB::QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsQueryPlanJoinAlgorithm join_algorithm;
}

/// The `join_algorithm` value `ie_join` was renamed to `inequality` in 26.12. A server released before that rejects
/// `inequality`, and the join step of a serialized query plan carries the whole `join_algorithm` list, whose default
/// contains the inequality join. So the plan must spell it `ie_join`, which every server accepts, even when the user
/// never set `join_algorithm`.
namespace
{

String writeJoinAlgorithms(const std::vector<JoinAlgorithm> & algorithms)
{
    QueryPlanSerializationSettings settings;
    settings[QueryPlanSerializationSetting::join_algorithm] = algorithms;
    WriteBufferFromOwnString out;
    settings.write(out);
    return out.str();
}

std::vector<JoinAlgorithm> readJoinAlgorithms(const String & stream)
{
    QueryPlanSerializationSettings settings;
    ReadBufferFromString in(stream);
    settings.read(in);
    return settings[QueryPlanSerializationSetting::join_algorithm];
}

}

TEST(InequalityJoinPlanSetting, DefaultIsWrittenWithFormerName)
{
    Settings query_settings;
    JoinSettings join_settings(query_settings);
    ASSERT_NE(std::ranges::find(join_settings.join_algorithms, JoinAlgorithm::INEQUALITY), join_settings.join_algorithms.end());

    String stream = writeJoinAlgorithms(join_settings.join_algorithms);
    EXPECT_TRUE(stream.contains("ie_join")) << stream;
    EXPECT_FALSE(stream.contains("inequality")) << stream;

    EXPECT_EQ(readJoinAlgorithms(stream), join_settings.join_algorithms);
}

TEST(InequalityJoinPlanSetting, BothNamesAreRead)
{
    /// A plan written by a server from before this change, or by one that spells the algorithm `inequality`.
    String stream = writeJoinAlgorithms({JoinAlgorithm::INEQUALITY});
    ASSERT_TRUE(stream.contains("ie_join")) << stream;
    EXPECT_EQ(readJoinAlgorithms(stream), std::vector<JoinAlgorithm>{JoinAlgorithm::INEQUALITY});

    String renamed = stream;
    renamed.replace(renamed.find("ie_join"), strlen("ie_join"), "inequality");
    /// The value is a length-prefixed string; patch the length byte that precedes it.
    size_t length_pos = renamed.find("inequality") - 1;
    ASSERT_EQ(static_cast<UInt8>(renamed[length_pos]), strlen("ie_join"));
    renamed[length_pos] = static_cast<char>(strlen("inequality"));
    EXPECT_EQ(readJoinAlgorithms(renamed), std::vector<JoinAlgorithm>{JoinAlgorithm::INEQUALITY});
}
