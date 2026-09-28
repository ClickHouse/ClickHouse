#include <Columns/ColumnConst.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Common/tests/gtest_global_context.h>

#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>

#include <gtest/gtest.h>

#include <sstream>

using namespace DB;
using namespace DB::ClusterProxy;

/// These cases cannot be written in SQL: they need a `_shard_num` scalar with no provenance column,
/// which only a server predating that column produces, and a cluster with no name.
namespace
{

/// `ClusterConnectionParameters` holds references, so a caller must keep these alive across the call.
struct ConnectionParameterStorage
{
    const String username = "default";
    const String password;
    const String bind_host;

    ClusterConnectionParameters params(const String & name) const
    {
        return ClusterConnectionParameters{username, password, 9000, false, false, false, bind_host, Priority{1}, name, ""};
    }
};

/// `replicas_per_shard` replicas on distinct hosts, so that deriving replicas-as-shards is not collapsed by
/// its duplicate-host skip and the derived cluster really is renumbered.
HostsByShard makeHosts(size_t shards, size_t replicas_per_shard)
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
    return hosts;
}

/// A cluster built from a bare host list, as `remote()` builds one. It carries no shard-scope identity:
/// nothing here says what its shard numbers mean, since the same name can be handed a different host list.
ClusterPtr makeCluster(const Settings & settings, const String & name, size_t shards, size_t replicas_per_shard = 1)
{
    const HostsByShard hosts = makeHosts(shards, replicas_per_shard);
    const ConnectionParameterStorage storage;
    return std::make_shared<Cluster>(settings, hosts, storage.params(name));
}

/// A cluster discovered from Keeper: `ClusterDiscovery::makeCluster` groups the currently visible nodes by
/// their `shard_id` and lets `Cluster` renumber the groups `1..N`, so the shard ids are what a shard number
/// of the result means, and they are what identifies its numbering.
/// `name` is the `remote_servers` entry the cluster is configured under; `discovery_path` is the
/// `<zk_name><zk_root>` pair `ClusterDiscovery::makeCluster` keys the shard-scope identity by, the same
/// for every entry pointing at one path.
/// On a tree without either parameter this falls back to the shorter form, which is what makes the arms
/// below report what an unpatched server computes instead of failing to compile.
template <typename SettingsT>
ClusterPtr makeDiscoveredCluster(
    const SettingsT & settings,
    const String & name,
    const Strings & shard_ids,
    size_t replicas_per_shard = 1,
    const String & discovery_path = {})
{
    const HostsByShard hosts = makeHosts(shard_ids.size(), replicas_per_shard);
    const ConnectionParameterStorage storage;
    const auto params = storage.params(name);

    if constexpr (requires { Cluster(settings, hosts, params, shard_ids, discovery_path); })
        return std::make_shared<Cluster>(settings, hosts, params, shard_ids, discovery_path);
    else if constexpr (requires { Cluster(settings, hosts, params, shard_ids); })
        return std::make_shared<Cluster>(settings, hosts, params, shard_ids);
    else
        return std::make_shared<Cluster>(settings, hosts, params);
}

/// What `DatabaseReplicated::updateCluster` and `ClusterDiscovery::makeCluster` key the shard-scope
/// identity by: the Keeper name together with the path. On a tree without the helper this falls back to
/// the path alone, which is what makes the arm below report what an unpatched server computes.
template <typename NameT>
String makeKeeperScopeKeyCompat(const NameT & zookeeper_name, const String & zookeeper_path)
{
    if constexpr (requires { Cluster::makeKeeperScopeKey(zookeeper_name, zookeeper_path); })
        return Cluster::makeKeeperScopeKey(zookeeper_name, zookeeper_path);
    else
        return zookeeper_path;
}

/// A `Replicated` database's cluster: `DatabaseReplicated::getClusterImpl` walks the visible replicas,
/// starts a new shard each time the shard name parsed out of Keeper changes, and lets `Cluster` renumber
/// the groups `1..N`. A shard disappears from that walk once its last visible replica is filtered out by
/// the local replica group or carries a `DROPPED_MARK`, and every later shard then shifts down.
/// `name` is the spelling the cluster is resolved by (`<db>` or `all_groups.<db>`); `zookeeper_path` is
/// what `DatabaseReplicated::updateCluster` keys the shard-scope identity by, the same for both spellings.
/// On a tree without the key parameter this falls back to the form that keys the identity by the spelling,
/// which is what makes the arms below report what an unpatched server computes instead of failing to compile.
template <typename SettingsT>
ClusterPtr makeReplicatedDatabaseCluster(
    const SettingsT & settings, const String & name, const Strings & shard_names, const String & zookeeper_path = {})
{
    std::vector<std::vector<DatabaseReplicaInfo>> infos;
    for (size_t i = 0; i < shard_names.size(); ++i)
        infos.push_back({DatabaseReplicaInfo{"127.0.0." + std::to_string(i + 1), shard_names[i], "replica1", {}}});

    const ConnectionParameterStorage storage;
    const auto params = storage.params(name);

    if constexpr (requires { Cluster(settings, infos, params, false, zookeeper_path); })
        return std::make_shared<Cluster>(settings, infos, params, false, zookeeper_path);
    else
        return std::make_shared<Cluster>(settings, infos, params);
}

/// A cluster read out of `remote_servers`, as each server reads its own configuration. The initiator and
/// a shard resolve `cluster_for_parallel_replicas` independently, so while a configuration change rolls
/// out the same name can stand for a different shard numbering on the two sides. `shard_names` names
/// every shard (`<name>`) or none.
ClusterPtr makeConfigCluster(
    const Settings & settings, const String & name, const HostsByShard & shards, const Strings & shard_names = {})
{
    std::ostringstream xml;
    xml << "<clickhouse><remote_servers><" << name << ">";
    for (size_t i = 0; i < shards.size(); ++i)
    {
        xml << "<shard>";
        if (!shard_names.empty())
            xml << "<name>" << shard_names.at(i) << "</name>";
        for (const auto & host : shards[i])
            xml << "<replica><host>" << host << "</host><port>9000</port></replica>";
        xml << "</shard>";
    }
    xml << "</" << name << "></remote_servers></clickhouse>";

    std::istringstream stream(xml.str());
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(stream);
    return std::make_shared<Cluster>(*config, settings, "remote_servers", name);
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
    auto cluster = makeDiscoveredCluster(getContext().context->getSettingsRef(), "some_cluster", {"0", "1", "2"});
    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*cluster)));

    const auto scope = getShardScopeCompat(context, *cluster);
    EXPECT_EQ(scope.kind, SCOPE_SCOPED);
    EXPECT_EQ(scope.shard_num, 2u);
}

TEST(ParallelReplicasShardScope, ForeignProvenanceIsRejected)
{
    /// Provenance an initiator running another build might spell; no identity here is a bare name.
    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, String("producing_cluster")));
    auto cluster = makeDiscoveredCluster(context->getSettingsRef(), "another_cluster", {"0", "1", "2"});

    const auto scope = getShardScopeCompat(context, *cluster);
    EXPECT_EQ(scope.kind, SCOPE_FOREIGN);
    EXPECT_EQ(scope.shard_num, 2u);
}

/// A cluster whose numbering cannot be identified carries an empty identity, so an empty provenance value
/// would compare equal to it and authenticate any such cluster against any other.
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
    auto original = makeDiscoveredCluster(getContext().context->getSettingsRef(), "some_cluster", {"0", "1"}, 3);
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
    auto original = makeDiscoveredCluster(settings, "c", {"0", "1"}, 3);
    auto derived = original->getClusterWithReplicasAsShards(settings);
    ASSERT_EQ(derived->getShardCount(), 6u);

    /// A cluster a user can produce by naming a `Replicated` database, which resolves to a cluster of that name.
    auto impostor = makeDiscoveredCluster(settings, "c (replicas as shards)", {"0", "1", "2"});

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

/// A discovered cluster is renumbered from whichever nodes are visible when it is built, so its name
/// describes a different numbering on a producer that can see a shard the consumer cannot. Both sides
/// resolve the same name, so comparing names authenticated the number: `shard_num = 3` against a 2-shard
/// consumer, which `prepareClusterForParallelReplicas` answers with `Shard number is greater than shard
/// count` rather than by declining the scope.
TEST(ParallelReplicasShardScope, DiscoveredClusterWithAnotherMembershipIsForeign)
{
    const auto & settings = getContext().context->getSettingsRef();
    auto producer = makeDiscoveredCluster(settings, "discovered", {"0", "1", "2"});
    auto consumer = makeDiscoveredCluster(settings, "discovered", {"0", "1"});
    ASSERT_EQ(consumer->getShardCount(), 2u);

    auto context = makeContextWithScalar(makeShardNumScalarCompat(3, getShardScopeIdentityCompat(*producer)));
    const auto scope = getShardScopeCompat(context, *consumer);
    EXPECT_EQ(scope.kind, SCOPE_FOREIGN);
    EXPECT_EQ(scope.shard_num, 3u);
}

/// The converse, or the arm above could pass by declining every discovered cluster: such a cluster is a
/// legitimate `cluster_for_parallel_replicas`, and the identity must not depend on the replicas either,
/// since a discovered cluster gains and loses those as servers come and go.
TEST(ParallelReplicasShardScope, DiscoveredClusterWithTheSameShardsIsScoped)
{
    const auto & settings = getContext().context->getSettingsRef();
    auto producer = makeDiscoveredCluster(settings, "discovered", {"0", "1"});
    auto consumer = makeDiscoveredCluster(settings, "discovered", {"0", "1"});

    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*producer)));
    EXPECT_EQ(getShardScopeCompat(context, *producer).kind, SCOPE_SCOPED);
    EXPECT_EQ(getShardScopeCompat(context, *consumer).kind, SCOPE_SCOPED);
}

/// The same for a `Replicated` database, where the shipped number stays in range instead: `shard_num = 2`
/// denotes `shard2` on a producer that sees both shards, and `shard1` on a consumer whose `shard1` has
/// been filtered out - the half of the defect that returned another shard's rows with no error at all.
TEST(ParallelReplicasShardScope, ReplicatedDatabaseWithADroppedShardIsForeign)
{
    const auto & settings = getContext().context->getSettingsRef();
    auto producer = makeReplicatedDatabaseCluster(settings, "replicated_db", {"shard1", "shard2"});
    auto consumer = makeReplicatedDatabaseCluster(settings, "replicated_db", {"shard2"});
    ASSERT_EQ(consumer->getShardCount(), 1u);

    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*producer)));
    EXPECT_EQ(getShardScopeCompat(context, *consumer).kind, SCOPE_FOREIGN);

    /// Replica groups make the two sides see different replicas of a shard in ordinary operation, so only
    /// a shard leaving the cluster may decline; the same shards seen on both sides must stay scoped.
    auto unchanged = makeReplicatedDatabaseCluster(settings, "replicated_db", {"shard1", "shard2"});
    EXPECT_EQ(getShardScopeCompat(context, *unchanged).kind, SCOPE_SCOPED);
}

/// Databases name their shards identically by default, so the identity carries a key for the database as
/// well as the shard names - otherwise one database's shard number would index another's shards. This one
/// holds on the merge base too, where the name is the whole identity; it pins what dropping the key would cost.
TEST(ParallelReplicasShardScope, ReplicatedDatabasesSharingShardNamesAreForeign)
{
    const auto & settings = getContext().context->getSettingsRef();
    auto producer = makeReplicatedDatabaseCluster(settings, "db_a", {"shard1", "shard2"}, "/clickhouse/db_a");
    auto consumer = makeReplicatedDatabaseCluster(settings, "db_b", {"shard1", "shard2"}, "/clickhouse/db_b");

    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*producer)));
    EXPECT_EQ(getShardScopeCompat(context, *consumer).kind, SCOPE_FOREIGN);
}

/// The key is the database's ZooKeeper path, not its name: the name is local to each server, so two
/// unrelated `Replicated` databases can be resolved by the same `cluster_for_parallel_replicas` on the
/// initiator and on a shard. With equal default shard names, the shipped number is in range on the other
/// database and denotes another database's shard - the silent wrong read, keyed on the name alone.
TEST(ParallelReplicasShardScope, ReplicatedDatabasesSharingTheNameAreForeign)
{
    const auto & settings = getContext().context->getSettingsRef();
    auto producer = makeReplicatedDatabaseCluster(settings, "db", {"shard1", "shard2"}, "/clickhouse/first/db");
    auto consumer = makeReplicatedDatabaseCluster(settings, "db", {"shard1", "shard2"}, "/clickhouse/second/db");

    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*producer)));
    EXPECT_EQ(getShardScopeCompat(context, *consumer).kind, SCOPE_FOREIGN);
}

/// The path is unique only inside one Keeper: `DatabaseReplicated::getZooKeeper` resolves the database
/// through its `zookeeper_name`, so two unrelated databases mounted at the same path on two auxiliary
/// Keepers, with equal default shard names, are the same silent wrong read keyed on the path alone. The
/// key must carry the Keeper name too - and only the name, so the same database is still one identity.
TEST(ParallelReplicasShardScope, ReplicatedDatabasesSharingThePathOnAnotherKeeperAreForeign)
{
    const auto & settings = getContext().context->getSettingsRef();
    const Strings shard_names{"shard1", "shard2"};
    const String on_aux1 = makeKeeperScopeKeyCompat(String("aux1"), "/clickhouse/db");
    const String on_aux2 = makeKeeperScopeKeyCompat(String("aux2"), "/clickhouse/db");
    auto producer = makeReplicatedDatabaseCluster(settings, "db", shard_names, on_aux1);
    auto consumer = makeReplicatedDatabaseCluster(settings, "db", shard_names, on_aux2);
    EXPECT_NE(getShardScopeIdentityCompat(*producer), getShardScopeIdentityCompat(*consumer));

    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*producer)));
    EXPECT_EQ(getShardScopeCompat(context, *consumer).kind, SCOPE_FOREIGN);

    auto same_database = makeReplicatedDatabaseCluster(settings, "db", shard_names, on_aux1);
    EXPECT_EQ(getShardScopeCompat(context, *same_database).kind, SCOPE_SCOPED);

    /// The name is length-prefixed, so moving characters between the name and the path is not the same key.
    const String shifted_boundary = makeKeeperScopeKeyCompat(String("aux1/clickhouse"), "/db");
    auto shifted = makeReplicatedDatabaseCluster(settings, "db", shard_names, shifted_boundary);
    EXPECT_EQ(getShardScopeCompat(context, *shifted).kind, SCOPE_FOREIGN);
}

/// With `replica_group_name` configured the same database is resolved both as `<db>` (the local group's
/// replicas) and as `all_groups.<db>` (every replica). Which replicas of a shard are visible differs
/// between the two, but while every shard keeps a visible replica the ordered shards are the same, and
/// so is what each shard number denotes: a read must not be declined for crossing the spellings. Only a
/// shard that the local group has no replica of - which shifts the numbering - makes the spellings foreign.
TEST(ParallelReplicasShardScope, ReplicatedDatabaseAliasWithTheSameShardsIsScoped)
{
    const auto & settings = getContext().context->getSettingsRef();
    auto all_groups = makeReplicatedDatabaseCluster(settings, "all_groups.db", {"shard1", "shard2"}, "/clickhouse/db");
    auto local_group = makeReplicatedDatabaseCluster(settings, "db", {"shard1", "shard2"}, "/clickhouse/db");
    EXPECT_EQ(getShardScopeIdentityCompat(*all_groups), getShardScopeIdentityCompat(*local_group));

    auto from_all_groups = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*all_groups)));
    EXPECT_EQ(getShardScopeCompat(from_all_groups, *local_group).kind, SCOPE_SCOPED);

    auto from_local_group = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*local_group)));
    EXPECT_EQ(getShardScopeCompat(from_local_group, *all_groups).kind, SCOPE_SCOPED);

    auto local_group_without_shard1 = makeReplicatedDatabaseCluster(settings, "db", {"shard2"}, "/clickhouse/db");
    EXPECT_EQ(getShardScopeCompat(from_all_groups, *local_group_without_shard1).kind, SCOPE_FOREIGN);
}

/// Both the key and the shard names are chosen by whoever creates the database, so their assembly must be
/// unambiguous: a key spelled like the punctuation plus one fewer shard must not produce the identity of
/// a shorter key plus one more shard. Like the arm above, this pins the assembly rather than reproducing
/// the defect - it also holds where the name is the whole identity.
TEST(ParallelReplicasShardScope, ShardNameIdentityIsUnambiguous)
{
    const auto & settings = getContext().context->getSettingsRef();
    auto producer = makeReplicatedDatabaseCluster(settings, "db", {"shard1", "shard2"}, "/db");
    auto impostor = makeReplicatedDatabaseCluster(settings, "db", {"shard2"}, "/db 6:shard1");

    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*producer)));
    EXPECT_EQ(getShardScopeCompat(context, *impostor).kind, SCOPE_FOREIGN);
}

/// No identity is a bare cluster name, and no two shapes spell the same identity: a config cluster, a
/// discovered cluster and a `Replicated` database can all share a name, and their shard `1` is not the
/// same shard. A cluster name is an XML element name, so the shape prefix is spelled with a space.
TEST(ParallelReplicasShardScope, ShardKeyIdentityIsNotSpellableAsAClusterName)
{
    const auto & settings = getContext().context->getSettingsRef();

    /// The same name and the same single shard key in every shape, so only the shape prefix differs.
    const String configured = getShardScopeIdentityCompat(*makeConfigCluster(settings, "x", {{"127.0.0.1"}}, {"s"}));
    const String discovered = getShardScopeIdentityCompat(*makeDiscoveredCluster(settings, "x", {"s"}));
    const String replicated = getShardScopeIdentityCompat(*makeReplicatedDatabaseCluster(settings, "x", {"s"}));
    EXPECT_NE(discovered, replicated);
    EXPECT_NE(configured, discovered);
    EXPECT_NE(configured, replicated);

    for (const auto & identity : {configured, discovered, replicated})
    {
        EXPECT_NE(identity, "x");
        EXPECT_NE(identity.find(' '), String::npos);
    }
}

/// `remote_servers` is per-server configuration. While a change to it rolls out, the initiator ships a
/// shard number of the cluster as it reads it, and the shard resolves the same name against its own copy.
/// With the shards reordered there, the number is in range and denotes a different shard: the silent
/// wrong-shard read.
TEST(ParallelReplicasShardScope, ConfigClusterWithReorderedShardsIsForeign)
{
    const auto & settings = getContext().context->getSettingsRef();
    const HostsByShard shard_a = {{"127.0.0.1", "127.0.0.2"}};
    const HostsByShard shard_b = {{"127.0.0.3", "127.0.0.4"}};

    auto initiator = makeConfigCluster(settings, "rolling", {shard_a[0], shard_b[0]});
    auto follower = makeConfigCluster(settings, "rolling", {shard_b[0], shard_a[0]});

    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*initiator)));
    const auto scope = getShardScopeCompat(context, *follower);
    EXPECT_EQ(scope.kind, SCOPE_FOREIGN);
    EXPECT_EQ(scope.shard_num, 2u);
}

/// A shard the follower's copy of the configuration does not have yet: the number is out of range there,
/// which used to be thrown on as `Shard number is greater than shard count`. Now the scope is declined.
TEST(ParallelReplicasShardScope, ConfigClusterWithAnAddedShardIsForeign)
{
    const auto & settings = getContext().context->getSettingsRef();

    auto initiator = makeConfigCluster(settings, "rolling", {{"127.0.0.1"}, {"127.0.0.2"}, {"127.0.0.3"}});
    auto follower = makeConfigCluster(settings, "rolling", {{"127.0.0.1"}, {"127.0.0.2"}});

    auto context = makeContextWithScalar(makeShardNumScalarCompat(3, getShardScopeIdentityCompat(*initiator)));
    EXPECT_EQ(getShardScopeCompat(context, *follower).kind, SCOPE_FOREIGN);
}

/// The control: two servers reading the same configuration identify the same numbering, so parallel
/// replicas over a plain config cluster keep engaging.
TEST(ParallelReplicasShardScope, ConfigClusterWithTheSameShardsIsScoped)
{
    const auto & settings = getContext().context->getSettingsRef();
    const HostsByShard shards = {{"127.0.0.1", "127.0.0.2"}, {"127.0.0.3", "127.0.0.4"}};

    auto initiator = makeConfigCluster(settings, "same", shards);
    auto follower = makeConfigCluster(settings, "same", shards);

    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*initiator)));
    const auto scope = getShardScopeCompat(context, *follower);
    EXPECT_EQ(scope.kind, SCOPE_SCOPED);
    EXPECT_EQ(scope.shard_num, 2u);
}

/// Named shards are identified by their names: a replica added to a shard on one side does not change
/// which shard a number denotes, so the scope holds, while the same names in another order do not.
TEST(ParallelReplicasShardScope, ConfigClusterNamedShardsAreIdentifiedByName)
{
    const auto & settings = getContext().context->getSettingsRef();

    auto initiator = makeConfigCluster(settings, "named", {{"127.0.0.1"}, {"127.0.0.3"}}, {"s1", "s2"});
    auto more_replicas = makeConfigCluster(settings, "named", {{"127.0.0.1", "127.0.0.2"}, {"127.0.0.3", "127.0.0.4"}}, {"s1", "s2"});
    auto reordered = makeConfigCluster(settings, "named", {{"127.0.0.3"}, {"127.0.0.1"}}, {"s2", "s1"});

    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*initiator)));
    EXPECT_EQ(getShardScopeCompat(context, *more_replicas).kind, SCOPE_SCOPED);
    EXPECT_EQ(getShardScopeCompat(context, *reordered).kind, SCOPE_FOREIGN);
}

/// Without shard names, the replicas are all that says which shard a number denotes, so a replica set
/// that differs between the two copies declines the scope rather than guessing that the shards still
/// line up.
TEST(ParallelReplicasShardScope, ConfigClusterUnnamedShardsAreIdentifiedByReplicas)
{
    const auto & settings = getContext().context->getSettingsRef();

    auto initiator = makeConfigCluster(settings, "unnamed", {{"127.0.0.1"}, {"127.0.0.3"}});
    auto more_replicas = makeConfigCluster(settings, "unnamed", {{"127.0.0.1", "127.0.0.2"}, {"127.0.0.3", "127.0.0.4"}});

    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*initiator)));
    EXPECT_EQ(getShardScopeCompat(context, *more_replicas).kind, SCOPE_FOREIGN);
}

/// A shard number denotes the shard, not the order of the `<replica>` elements inside it. Two copies of the
/// configuration that list the same replicas of a shard in another order still agree on the numbering, so
/// the scope holds and parallel replicas keep engaging; reordering the shards themselves stays declined.
TEST(ParallelReplicasShardScope, ConfigClusterUnnamedShardReplicaOrderIsNotAnIdentity)
{
    const auto & settings = getContext().context->getSettingsRef();

    auto initiator = makeConfigCluster(settings, "unnamed", {{"127.0.0.1", "127.0.0.2"}, {"127.0.0.3", "127.0.0.4"}});
    auto replicas_reordered = makeConfigCluster(settings, "unnamed", {{"127.0.0.2", "127.0.0.1"}, {"127.0.0.4", "127.0.0.3"}});
    auto shards_reordered = makeConfigCluster(settings, "unnamed", {{"127.0.0.4", "127.0.0.3"}, {"127.0.0.2", "127.0.0.1"}});

    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*initiator)));
    const auto scope = getShardScopeCompat(context, *replicas_reordered);
    EXPECT_EQ(scope.kind, SCOPE_SCOPED);
    EXPECT_EQ(scope.shard_num, 2u);
    EXPECT_EQ(getShardScopeIdentityCompat(*replicas_reordered), getShardScopeIdentityCompat(*initiator));
    EXPECT_EQ(getShardScopeCompat(context, *shards_reordered).kind, SCOPE_FOREIGN);
}

/// Two names for the same ordered shards are one numbering: a distributed hop that ships `_shard_num`
/// from `alias_a` and a nested `SETTINGS cluster_for_parallel_replicas = 'alias_b'` still reads the shard
/// the number denotes, so the scope must hold rather than turn parallel replicas off. Shard `<name>`s are
/// chosen per cluster and say nothing outside it, so aliases whose shards only share names stay foreign.
TEST(ParallelReplicasShardScope, ConfigClusterAliasWithTheSameShardsIsScoped)
{
    const auto & settings = getContext().context->getSettingsRef();
    const HostsByShard shards = {{"127.0.0.1", "127.0.0.2"}, {"127.0.0.3", "127.0.0.4"}};

    auto alias_a = makeConfigCluster(settings, "alias_a", shards);
    auto alias_b = makeConfigCluster(settings, "alias_b", shards);

    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*alias_a)));
    const auto scope = getShardScopeCompat(context, *alias_b);
    EXPECT_EQ(scope.kind, SCOPE_SCOPED);
    EXPECT_EQ(scope.shard_num, 2u);
    EXPECT_EQ(getShardScopeIdentityCompat(*alias_b), getShardScopeIdentityCompat(*alias_a));

    auto named_a = makeConfigCluster(settings, "named_a", {{"127.0.0.1"}, {"127.0.0.3"}}, {"s1", "s2"});
    auto named_b = makeConfigCluster(settings, "named_b", {{"127.0.0.5"}, {"127.0.0.6"}}, {"s1", "s2"});

    auto named_context = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*named_a)));
    EXPECT_EQ(getShardScopeCompat(named_context, *named_b).kind, SCOPE_FOREIGN);
}

/// Two `remote_servers` entries pointing at one discovery path are one numbering: the nodes register at
/// `<zk_root>/shards/<server uuid>`, with nothing in the path standing for the entry's name, so both
/// entries read the same znodes and shard `N` denotes the same shard through either. Keying the identity
/// by the name would classify a `_shard_num` produced through one alias as `Foreign` through the other
/// and turn parallel replicas off for a read that is perfectly in scope.
/// The shard keys themselves are each node's own `discovery.shard`, a per-cluster number, so the
/// namespace still has to separate two discovery paths that happen to use the same shard ids.
TEST(ParallelReplicasShardScope, DiscoveredClusterAliasWithTheSameShardsIsScoped)
{
    const auto & settings = getContext().context->getSettingsRef();
    auto alias_a = makeDiscoveredCluster(settings, "alias_a", {"0", "1"}, 1, "zookeeper/clickhouse/discovery/some_cluster");
    auto alias_b = makeDiscoveredCluster(settings, "alias_b", {"0", "1"}, 1, "zookeeper/clickhouse/discovery/some_cluster");

    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*alias_a)));
    const auto scope = getShardScopeCompat(context, *alias_b);
    EXPECT_EQ(scope.kind, SCOPE_SCOPED);
    EXPECT_EQ(scope.shard_num, 2u);
    EXPECT_EQ(getShardScopeIdentityCompat(*alias_b), getShardScopeIdentityCompat(*alias_a));

    auto another_path
        = makeDiscoveredCluster(settings, "alias_a", {"0", "1"}, 1, "zookeeper/clickhouse/discovery/another_cluster");
    EXPECT_EQ(getShardScopeCompat(context, *another_path).kind, SCOPE_FOREIGN);
}

/// Taking a subset of shards preserves each shard's number, so a shard number keeps its meaning and the
/// identity must carry over: `optimize_skip_unused_shards` reads through such a cluster.
TEST(ParallelReplicasShardScope, ShardSubsetKeepsIdentity)
{
    auto original = makeDiscoveredCluster(getContext().context->getSettingsRef(), "some_cluster", {"0", "1", "2"});
    auto subset = original->getClusterWithMultipleShards({1});
    ASSERT_EQ(subset->getShardsInfo().at(0).shard_num, 2u);

    auto context = makeContextWithScalar(makeShardNumScalarCompat(2, getShardScopeIdentityCompat(*original)));
    EXPECT_EQ(getShardScopeCompat(context, *subset).kind, SCOPE_SCOPED);
    EXPECT_EQ(getShardScopeIdentityCompat(*subset), getShardScopeIdentityCompat(*original));
}
