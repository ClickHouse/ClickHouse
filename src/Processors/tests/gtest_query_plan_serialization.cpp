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
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Processors/QueryPlan/Serialization.h>

#include <mutex>

using namespace DB;

namespace DB
{
void registerReadFromTableStep(QueryPlanStepRegistry & registry);
}

namespace
{

void ensureReadFromTableStepRegistered()
{
    static std::once_flag flag;
    std::call_once(flag, [] { registerReadFromTableStep(QueryPlanStepRegistry::instance()); });
}

SharedHeader makeHeader()
{
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "value")});
}

QueryPlan makeReadFromTablePlan(bool use_parallel_replicas, bool with_node_name_mapping = false)
{
    std::unordered_map<std::string, ColumnWithTypeAndName> node_name_to_input_node_column;
    if (with_node_name_mapping)
        node_name_to_input_node_column.emplace("__table1.value", ColumnWithTypeAndName(nullptr, std::make_shared<DataTypeString>(), "value"));

    QueryPlan plan;
    plan.addStep(std::make_unique<ReadFromTableStep>(
        makeHeader(),
        "default.t",
        TableExpressionModifiers{},
        use_parallel_replicas,
        nullptr,
        nullptr,
        std::move(node_name_to_input_node_column)));
    return plan;
}

String serializePlan(const QueryPlan & plan, size_t version)
{
    WriteBufferFromOwnString out;
    plan.serialize(out, version);
    return out.str();
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
    ensureReadFromTableStepRegistered();

    QueryPlan plan = makeReadFromTablePlan(true);

    WriteBufferFromOwnString out;
    plan.serialize(out, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);

    ReadBufferFromString in(out.str());
    auto plan_and_sets = QueryPlan::deserialize(in, getContext().context, /*max_type_complexity=*/ 0);
    ASSERT_TRUE(in.eof());

    const auto & step = getReadFromTableStep(plan_and_sets.plan);
    EXPECT_TRUE(step.useParallelReplicas());
}

TEST(QueryPlanSerialization, ReadFromTableVersionZeroWithoutParallelReplicasHasNoExtraPayload)
{
    ensureReadFromTableStepRegistered();

    QueryPlan plan = makeReadFromTablePlan(false);

    WriteBufferFromOwnString out;
    plan.serialize(out, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);

    ReadBufferFromString in(out.str());
    auto plan_and_sets = QueryPlan::deserialize(in, getContext().context, /*max_type_complexity=*/ 0);
    ASSERT_TRUE(in.eof());

    const auto & step = getReadFromTableStep(plan_and_sets.plan);
    EXPECT_FALSE(step.useParallelReplicas());
}

TEST(QueryPlanSerialization, QueryPlanCacheSerializationUsesCurrentVersion)
{
    ensureReadFromTableStepRegistered();

    QueryPlan plan = makeReadFromTablePlan(false);

    WriteBufferFromOwnString out;
    plan.serializeForQueryPlanCache(out);
    const auto serialized = out.str();

    ReadBufferFromString version_in(serialized);
    UInt64 version = 0;
    readVarUInt(version, version_in);
    EXPECT_EQ(version, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);

    ReadBufferFromString plan_in(serialized);
    auto plan_and_sets = QueryPlan::deserializeForQueryPlanCache(plan_in, getContext().context);
    ASSERT_TRUE(plan_in.eof());

    const auto & step = getReadFromTableStep(plan_and_sets.plan);
    EXPECT_FALSE(step.useParallelReplicas());
}

TEST(QueryPlanSerialization, ReadFromTableWireFormatKeepsLegacyLayout)
{
    ensureReadFromTableStepRegistered();

    /// The node-name mapping is a query plan cache payload: in a stream that can reach another server it
    /// must not change the bytes, and the legacy parallel-replicas byte must be written at every version,
    /// because readers of older versions consume it whenever the flag is set.
    for (bool use_parallel_replicas : {false, true})
    {
        for (size_t version : {size_t{0}, size_t{DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_STEP_VERSIONS}, size_t{DBMS_QUERY_PLAN_SERIALIZATION_VERSION}})
        {
            const auto with_mapping = serializePlan(makeReadFromTablePlan(use_parallel_replicas, true), version);
            const auto without_mapping = serializePlan(makeReadFromTablePlan(use_parallel_replicas, false), version);
            EXPECT_EQ(with_mapping, without_mapping);

            ReadBufferFromString in(with_mapping);
            auto plan_and_sets = QueryPlan::deserialize(in, getContext().context, /*max_type_complexity=*/ 0);
            ASSERT_TRUE(in.eof());

            const auto & step = getReadFromTableStep(plan_and_sets.plan);
            EXPECT_EQ(step.useParallelReplicas(), use_parallel_replicas);
            EXPECT_TRUE(step.getNodeNameToInputNodeColumn().empty());
        }
    }

    /// The parallel-replicas plan has exactly one more byte than the plain one: the legacy payload.
    EXPECT_EQ(
        serializePlan(makeReadFromTablePlan(true), DBMS_QUERY_PLAN_SERIALIZATION_VERSION).size(),
        serializePlan(makeReadFromTablePlan(false), DBMS_QUERY_PLAN_SERIALIZATION_VERSION).size() + 1);
}

TEST(QueryPlanSerialization, QueryPlanCacheKeepsNodeNameMapping)
{
    ensureReadFromTableStepRegistered();

    QueryPlan plan = makeReadFromTablePlan(true, true);

    WriteBufferFromOwnString out;
    plan.serializeForQueryPlanCache(out);

    ReadBufferFromString in(out.str());
    auto plan_and_sets = QueryPlan::deserializeForQueryPlanCache(in, getContext().context);
    ASSERT_TRUE(in.eof());

    const auto & step = getReadFromTableStep(plan_and_sets.plan);
    EXPECT_TRUE(step.useParallelReplicas());
    ASSERT_EQ(step.getNodeNameToInputNodeColumn().size(), 1);
    EXPECT_EQ(step.getNodeNameToInputNodeColumn().begin()->first, "__table1.value");

    /// A cache-only payload is refused in a stream that did not come from the query plan cache.
    ReadBufferFromString wire_in(out.str());
    EXPECT_THROW(QueryPlan::deserialize(wire_in, getContext().context, /*max_type_complexity=*/ 0), DB::Exception);
}

TEST(QueryPlanSerialization, QueryPlanCacheRejectsFutureVersion)
{
    WriteBufferFromOwnString out;
    writeVarUInt(DBMS_QUERY_PLAN_SERIALIZATION_VERSION + 1, out);
    out.finalize();

    ReadBufferFromString in(out.str());
    EXPECT_THROW(QueryPlan::deserializeForQueryPlanCache(in, getContext().context), DB::Exception);
}
