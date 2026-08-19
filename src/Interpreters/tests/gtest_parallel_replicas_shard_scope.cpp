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

/// The wire values of `ShardScopeKind`, so a kind can be compared without naming the enumerators
/// outside a template. `getShardScopeCompat` static_asserts that they still agree with the enum.
enum : UInt8
{
    SCOPE_NONE = 0,
    SCOPE_SCOPED = 1,
    SCOPE_FOREIGN = 2,
};

struct CompatShardScope
{
    UInt8 kind = SCOPE_NONE;
    UInt64 shard_num = 0;
};

/// A test body is not a template, so an `if constexpr` branch inside it is still compiled; every use
/// of a symbol the merge base lacks must therefore sit in a discarded branch, which is what these
/// three accessors are for. The dependence must come from an argument type: with concrete parameters
/// the name is looked up even inside the `requires`. Each fallback reports the shard scope a server
/// without these symbols computes - it reads the shipped number and trusts it, so a scope is never
/// foreign, and there is no numbering identity to compare against.
template <typename StringT>
Block makeShardNumScalarCompat(UInt32 shard_num, const StringT & shard_scope_identity)
{
    if constexpr (requires { makeShardNumScalar(shard_num, shard_scope_identity); })
        return makeShardNumScalar(shard_num, shard_scope_identity);
    else
        return singleColumnShardNum(shard_num);
}

template <typename ClusterT>
String getShardScopeIdentityCompat(const ClusterT & cluster)
{
    if constexpr (requires { cluster.getShardScopeIdentity(); })
        return cluster.getShardScopeIdentity();
    else
        return {};
}

template <typename ClusterT>
CompatShardScope getShardScopeCompat(const ContextMutablePtr & context, const ClusterT & cluster)
{
    if constexpr (requires { getShardScopeForCluster(context, cluster); })
    {
        const auto scope = getShardScopeForCluster(context, cluster);
        using Kind = std::remove_cvref_t<decltype(scope.kind)>;
        static_assert(static_cast<UInt8>(Kind::None) == SCOPE_NONE);
        static_assert(static_cast<UInt8>(Kind::Scoped) == SCOPE_SCOPED);
        static_assert(static_cast<UInt8>(Kind::Foreign) == SCOPE_FOREIGN);
        return {static_cast<UInt8>(scope.kind), scope.shard_num};
    }
    else
    {
        const auto scalars = context->hasQueryContext() ? context->getQueryContext()->getScalars() : Scalars{};
        const auto it = scalars.find("_shard_num");
        if (it == scalars.end())
            return {};
        const UInt64 shard_num = it->second.safeGetByPosition(0).column->getUInt(0);
        return {shard_num ? UInt8{SCOPE_SCOPED} : UInt8{SCOPE_NONE}, shard_num};
    }
}

}

/// An initiator predating the provenance column ships a single-column block. Rejecting it would strip the
/// shard scope from every cross-version distributed parallel-replicas query, so it must still be honoured.
TEST(ParallelReplicasShardScope, AbsentProvenanceIsTrusted)
{
    auto context = makeContextWithScalar(singleColumnShardNum(2));
    auto cluster = makeCluster(context->getSettingsRef(), "some_cluster", 3);

    const auto scope = getShardScopeCompat(context, *cluster);
    EXPECT_EQ(scope.kind, SCOPE_SCOPED);
    EXPECT_EQ(scope.shard_num, 2u);
}

TEST(ParallelReplicasShardScope, MatchingProvenanceIsScoped)
{
    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, String("some_cluster")));
    auto cluster = makeCluster(context->getSettingsRef(), "some_cluster", 3);

    const auto scope = getShardScopeCompat(context, *cluster);
    EXPECT_EQ(scope.kind, SCOPE_SCOPED);
    EXPECT_EQ(scope.shard_num, 2u);
}

TEST(ParallelReplicasShardScope, ForeignProvenanceIsRejected)
{
    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, String("producing_cluster")));
    auto cluster = makeCluster(context->getSettingsRef(), "another_cluster", 3);

    const auto scope = getShardScopeCompat(context, *cluster);
    EXPECT_EQ(scope.kind, SCOPE_FOREIGN);
    EXPECT_EQ(scope.shard_num, 2u);
}

/// A cluster built from a host list without a name reports an empty name, so an empty provenance value would
/// compare equal to it and authenticate any such cluster against any other.
TEST(ParallelReplicasShardScope, EmptyProvenanceNeverMatches)
{
    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, String("")));
    auto cluster = makeCluster(context->getSettingsRef(), "", 3);

    const auto scope = getShardScopeCompat(context, *cluster);
    EXPECT_EQ(scope.kind, SCOPE_FOREIGN);
}

/// Provenance lives in the `_shard_num` block rather than beside it so that a server overwriting the shard
/// number cannot leave the previous cluster's provenance attached to it - which would authenticate a shard
/// number against a cluster it was never produced for.
TEST(ParallelReplicasShardScope, OverwritingShardNumDropsProvenance)
{
    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, String("producing_cluster")));
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
    const auto block = makeShardNumScalarCompat(7, String("some_cluster"));
    EXPECT_EQ(block.safeGetByPosition(0).name, "_shard_num");
    EXPECT_EQ(block.safeGetByPosition(0).column->getUInt(0), 7u);
}

/// No scalar at all is the ordinary non-distributed case and must not be confused with a rejected one.
TEST(ParallelReplicasShardScope, NoScalarIsNone)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    auto cluster = makeCluster(context->getSettingsRef(), "some_cluster", 3);

    const auto scope = getShardScopeCompat(context, *cluster);
    EXPECT_EQ(scope.kind, SCOPE_NONE);
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
    EXPECT_TRUE(getShardScopeIdentityCompat(*derived).empty());

    /// A shard number produced against the derived numbering says nothing about the original's shards.
    auto derived_context = makeContextWithScalar(makeShardNumScalarCompat(6, getShardScopeIdentityCompat(*derived)));
    const auto foreign = getShardScopeCompat(derived_context, *original);
    EXPECT_EQ(foreign.kind, SCOPE_FOREIGN);
    EXPECT_EQ(foreign.shard_num, 6u);

    /// The converse must still hold, or the arm could pass by making every scope foreign. Only the original is
    /// asserted: a derived cluster is reachable only as the producer of a scope, never as its target, so
    /// asserting a kind against `*derived` would pin an accident rather than a contract.
    auto original_context = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*original)));
    EXPECT_EQ(getShardScopeCompat(original_context, *original).kind, SCOPE_SCOPED);
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

    auto context = makeContextWithScalar(makeShardNumScalarCompat(6, getShardScopeIdentityCompat(*derived)));
    EXPECT_EQ(getShardScopeCompat(context, *impostor).kind, SCOPE_FOREIGN);
}

/// `canUseLocalPlanForParallelReplicas` asks only whether a shard number was shipped, which does not depend on
/// the numbering it indexes. It must therefore resolve no cluster: it is reached from projection analysis on a
/// follower, where `cluster_for_parallel_replicas` need not name a cluster this server can resolve, and turning
/// that into an exception would fail a read that has nothing to do with the shard scope.
TEST(ParallelReplicasShardScope, LocalPlanPredicateResolvesNoCluster)
{
    auto with_local_plan = [](const ContextMutablePtr & context)
    {
        context->setSetting("allow_experimental_analyzer", Field{true});
        context->setSetting("parallel_replicas_local_plan", Field{true});
        context->setSetting("parallel_replicas_prefer_local_replica", Field{true});
        /// Deliberately unset, as it is on a server that cannot resolve the initiator's cluster.
        context->setSetting("cluster_for_parallel_replicas", Field{""});
        return context;
    };

    auto no_scalar = with_local_plan(Context::createCopy(getContext().context));
    no_scalar->makeQueryContext();
    EXPECT_TRUE(canUseLocalPlanForParallelReplicas(no_scalar));

    auto shipped = with_local_plan(makeContextWithScalar(makeShardNumScalarCompat(2, String("some_cluster"))));
    EXPECT_FALSE(canUseLocalPlanForParallelReplicas(shipped));

    /// An unresolvable name must be no different from an unset one: `Context::tryGetCluster` returns null
    /// for a `Replicated` database whose Keeper state is momentarily unavailable, and this predicate runs
    /// on a read that has nothing to do with the shard scope.
    shipped->setSetting("cluster_for_parallel_replicas", Field{"no_such_cluster_04727"});
    EXPECT_FALSE(canUseLocalPlanForParallelReplicas(shipped));
}

/// Taking a subset of shards preserves each shard's number, so a shard number keeps its meaning and the
/// identity must carry over: `optimize_skip_unused_shards` reads through such a cluster.
TEST(ParallelReplicasShardScope, ShardSubsetKeepsIdentity)
{
    auto original = makeCluster(getContext().context->getSettingsRef(), "some_cluster", 3);
    auto subset = original->getClusterWithMultipleShards({1});
    ASSERT_EQ(subset->getShardsInfo().at(0).shard_num, 2u);

    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*original)));
    EXPECT_EQ(getShardScopeCompat(context, *subset).kind, SCOPE_SCOPED);
    EXPECT_EQ(getShardScopeIdentityCompat(*subset), getShardScopeIdentityCompat(*original));
}
