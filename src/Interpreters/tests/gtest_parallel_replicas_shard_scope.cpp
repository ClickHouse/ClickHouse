#include <Columns/ColumnConst.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

using namespace DB;
using namespace DB::ClusterProxy;

/// These cases cannot be written in SQL: they need a `_shard_num` scalar with no provenance column,
/// which only a server predating that column produces, and a cluster with no name.
namespace
{

ClusterPtr makeCluster(const Settings & settings, const String & name, size_t shards)
{
    HostsByShard hosts;
    for (size_t i = 0; i < shards; ++i)
        hosts.push_back({"127.0.0.1"});

    /// ClusterConnectionParameters holds references, so these must outlive the call.
    const String username = "default";
    const String password;
    const String bind_host;
    ClusterConnectionParameters params{
        username, password, 9000, false, false, false, bind_host, Priority{1}, name, ""};
    return std::make_shared<Cluster>(settings, hosts, params);
}

ContextMutablePtr makeContextWithScalar(const Block & shard_num_scalar)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    context->addScalar("_shard_num", shard_num_scalar);
    return context;
}

Block singleColumnShardNum(UInt32 shard_num)
{
    ColumnPtr column = DataTypeUInt32().createColumnConst(1, shard_num);
    return Block{{column, std::make_shared<DataTypeUInt32>(), "_shard_num"}};
}

}

/// An initiator predating the provenance column ships a single-column block. Rejecting it would strip the
/// shard scope from every cross-version distributed parallel-replicas query, so it must still be honoured.
TEST(ParallelReplicasShardScope, AbsentProvenanceIsTrusted)
{
    auto context = makeContextWithScalar(singleColumnShardNum(2));
    auto cluster = makeCluster(context->getSettingsRef(), "some_cluster", 3);

    const auto scope = getShardScopeForCluster(context, *cluster);
    EXPECT_EQ(scope.kind, ShardScopeKind::Scoped);
    EXPECT_EQ(scope.shard_num, 2u);
}

TEST(ParallelReplicasShardScope, MatchingProvenanceIsScoped)
{
    auto context = makeContextWithScalar(makeShardNumScalar(2, "some_cluster"));
    auto cluster = makeCluster(context->getSettingsRef(), "some_cluster", 3);

    const auto scope = getShardScopeForCluster(context, *cluster);
    EXPECT_EQ(scope.kind, ShardScopeKind::Scoped);
    EXPECT_EQ(scope.shard_num, 2u);
}

TEST(ParallelReplicasShardScope, ForeignProvenanceIsRejected)
{
    auto context = makeContextWithScalar(makeShardNumScalar(2, "producing_cluster"));
    auto cluster = makeCluster(context->getSettingsRef(), "another_cluster", 3);

    const auto scope = getShardScopeForCluster(context, *cluster);
    EXPECT_EQ(scope.kind, ShardScopeKind::Foreign);
    EXPECT_EQ(scope.shard_num, 2u);
}

/// A cluster built from a host list without a name reports an empty name, so an empty provenance value would
/// compare equal to it and authenticate any such cluster against any other.
TEST(ParallelReplicasShardScope, EmptyProvenanceNeverMatches)
{
    auto context = makeContextWithScalar(makeShardNumScalar(2, ""));
    auto cluster = makeCluster(context->getSettingsRef(), "", 3);

    const auto scope = getShardScopeForCluster(context, *cluster);
    EXPECT_EQ(scope.kind, ShardScopeKind::Foreign);
}

/// Provenance lives in the `_shard_num` block rather than beside it so that a server overwriting the shard
/// number cannot leave the previous cluster's provenance attached to it - which would authenticate a shard
/// number against a cluster it was never produced for.
TEST(ParallelReplicasShardScope, OverwritingShardNumDropsProvenance)
{
    auto context = makeContextWithScalar(makeShardNumScalar(2, "producing_cluster"));
    /// A server predating the provenance column overwrites the whole block, as ReadFromRemote does.
    context->addScalar("_shard_num", singleColumnShardNum(1));

    const auto scalars = context->getScalars();
    const Block & block = scalars.at("_shard_num");
    EXPECT_EQ(block.columns(), 1u);
    EXPECT_FALSE(block.has("_cluster_for_parallel_replicas"));
}

/// `shardNum()` and the shard-scope consumers read position 0, so the added column must not shift it.
TEST(ParallelReplicasShardScope, ShardNumStaysAtPositionZero)
{
    const auto block = makeShardNumScalar(7, "some_cluster");
    EXPECT_EQ(block.safeGetByPosition(0).name, "_shard_num");
    EXPECT_EQ(block.safeGetByPosition(0).column->getUInt(0), 7u);
}

/// No scalar at all is the ordinary non-distributed case and must not be confused with a rejected one.
TEST(ParallelReplicasShardScope, NoScalarIsNone)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    auto cluster = makeCluster(context->getSettingsRef(), "some_cluster", 3);

    const auto scope = getShardScopeForCluster(context, *cluster);
    EXPECT_EQ(scope.kind, ShardScopeKind::None);
    EXPECT_EQ(scope.shard_num, 0u);
}
