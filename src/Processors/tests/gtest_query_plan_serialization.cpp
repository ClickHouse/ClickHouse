#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>

#include <Columns/ColumnString.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/ReadFromTableStep.h>

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "value")});
}

QueryPlan makeReadFromTablePlan(bool use_parallel_replicas)
{
    QueryPlan plan;
    plan.addStep(std::make_unique<ReadFromTableStep>(
        makeHeader(),
        "default.t",
        TableExpressionModifiers{},
        use_parallel_replicas,
        nullptr));
    return plan;
}

const ReadFromTableStep & getReadFromTableStep(const QueryPlan & plan)
{
    const auto * step = typeid_cast<const ReadFromTableStep *>(plan.getRootNode()->step.get());
    EXPECT_NE(step, nullptr);
    return *step;
}

}

TEST(QueryPlanSerialization, ReadFromTableVersionZeroKeepsLegacyParallelReplicasPayload)
{
    QueryPlan plan = makeReadFromTablePlan(true);

    WriteBufferFromOwnString out;
    plan.serialize(out, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);

    ReadBufferFromString in(out.str());
    auto plan_and_sets = QueryPlan::deserialize(in, getContext().context);
    ASSERT_TRUE(in.eof());

    const auto & step = getReadFromTableStep(plan_and_sets.plan);
    EXPECT_TRUE(step.useParallelReplicas());
}

TEST(QueryPlanSerialization, ReadFromTableVersionZeroWithoutParallelReplicasHasNoExtraPayload)
{
    QueryPlan plan = makeReadFromTablePlan(false);

    WriteBufferFromOwnString out;
    plan.serialize(out, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);

    ReadBufferFromString in(out.str());
    auto plan_and_sets = QueryPlan::deserialize(in, getContext().context);
    ASSERT_TRUE(in.eof());

    const auto & step = getReadFromTableStep(plan_and_sets.plan);
    EXPECT_FALSE(step.useParallelReplicas());
}

TEST(QueryPlanSerialization, QueryPlanCacheSerializationUsesPrivateVersion)
{
    QueryPlan plan = makeReadFromTablePlan(false);

    WriteBufferFromOwnString out;
    plan.serializeForQueryPlanCache(out);
    const auto serialized = out.str();

    ReadBufferFromString version_in(serialized);
    UInt64 version = 0;
    readVarUInt(version, version_in);
    EXPECT_EQ(version, QUERY_PLAN_CACHE_SERIALIZATION_VERSION);

    ReadBufferFromString plan_in(serialized);
    auto plan_and_sets = QueryPlan::deserializeForQueryPlanCache(plan_in, getContext().context);
    ASSERT_TRUE(plan_in.eof());

    const auto & step = getReadFromTableStep(plan_and_sets.plan);
    EXPECT_FALSE(step.useParallelReplicas());
}
