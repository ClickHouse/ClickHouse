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

/// `replicas_per_shard` replicas on distinct hosts, so that deriving replicas-as-shards is not collapsed by
/// its duplicate-host skip and the derived cluster really is renumbered.
ClusterPtr makeCluster(const Settings & settings, const String & name, size_t shards, size_t replicas_per_shard = 1)
{
    HostsByShard hosts;
    size_t host = 1;
    for (size_t i = 0; i < shards; ++i)
    {
        Strings replicas;
        for (size_t j = 0; j < replicas_per_shard; ++j)
            replicas.push_back("127.0.0." + std::to_string(host++));
        hosts.push_back(std::move(replicas));
    }

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

/// `clusterAllReplicas` turns each replica into a shard of its own, so a shard number of the derived cluster
/// denotes a different shard than the same number does in the original - while the name is copied unchanged.
/// Comparing names would authenticate such a number against the original and index an unrelated shard.
TEST(ParallelReplicasShardScope, RenumberedDerivedClusterIsForeign)
{
    auto original = makeCluster(getContext().context->getSettingsRef(), "some_cluster", 2, 3);
    auto derived = original->getClusterWithReplicasAsShards(getContext().context->getSettingsRef());
    ASSERT_EQ(original->getShardCount(), 2u);
    ASSERT_EQ(derived->getShardCount(), 6u);
    ASSERT_EQ(derived->getName(), original->getName());

    /// The renumbering is unauthenticable rather than differently authenticated, so pin the mechanism.
    EXPECT_TRUE(derived->getShardScopeIdentity().empty());

    /// A shard number produced against the derived numbering says nothing about the original's shards.
    auto derived_context = makeContextWithScalar(makeShardNumScalar(6, derived->getShardScopeIdentity()));
    const auto foreign = getShardScopeForCluster(derived_context, *original);
    EXPECT_EQ(foreign.kind, ShardScopeKind::Foreign);
    EXPECT_EQ(foreign.shard_num, 6u);

    /// The converse must still hold, or the arm could pass by making every scope foreign. Only the original is
    /// asserted: a derived cluster is reachable only as the producer of a scope, never as its target, so
    /// asserting a kind against `*derived` would pin an accident rather than a contract.
    auto original_context = makeContextWithScalar(makeShardNumScalar(2, original->getShardScopeIdentity()));
    EXPECT_EQ(getShardScopeForCluster(original_context, *original).kind, ShardScopeKind::Scoped);
}

/// A derived cluster must carry no identity at all rather than a distinguishable spelling: cluster names and
/// `Replicated` database names share one namespace, so any non-empty value a reader could construct is also a
/// value a user could name a database, making the scope forgeable.
TEST(ParallelReplicasShardScope, DerivedClusterIdentityIsNotForgeable)
{
    const auto & settings = getContext().context->getSettingsRef();
    auto original = makeCluster(settings, "c", 2, 3);
    auto derived = original->getClusterWithReplicasAsShards(settings);
    ASSERT_EQ(derived->getShardCount(), 6u);

    /// A cluster a user can produce by naming a `Replicated` database, which resolves to a cluster of that name.
    auto impostor = makeCluster(settings, "c (replicas as shards)", 3);

    auto context = makeContextWithScalar(makeShardNumScalar(6, derived->getShardScopeIdentity()));
    EXPECT_EQ(getShardScopeForCluster(context, *impostor).kind, ShardScopeKind::Foreign);
}

/// Taking a subset of shards preserves each shard's number, so a shard number keeps its meaning and the
/// identity must carry over: `optimize_skip_unused_shards` reads through such a cluster.
TEST(ParallelReplicasShardScope, ShardSubsetKeepsIdentity)
{
    auto original = makeCluster(getContext().context->getSettingsRef(), "some_cluster", 3);
    auto subset = original->getClusterWithMultipleShards({1});
    ASSERT_EQ(subset->getShardsInfo().at(0).shard_num, 2u);

    auto context = makeContextWithScalar(makeShardNumScalar(2, original->getShardScopeIdentity()));
    EXPECT_EQ(getShardScopeForCluster(context, *subset).kind, ShardScopeKind::Scoped);
    EXPECT_EQ(subset->getShardScopeIdentity(), original->getShardScopeIdentity());
}
