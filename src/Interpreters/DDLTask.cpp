#include <Interpreters/DDLTask.h>
#include <base/sort.h>
#include <Common/DNSResolver.h>
#include <Common/isLocalAddress.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <Poco/Net/NetException.h>
#include <Common/logger_useful.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Databases/DatabaseReplicated.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int UNKNOWN_TYPE_OF_QUERY;
    extern const int INCONSISTENT_CLUSTER_DEFINITION;
    extern const int LOGICAL_ERROR;
}

HostID HostID::fromString(const String & host_port_str)
{
    HostID res;
    std::tie(res.host_name, res.port) = Cluster::Address::fromString(host_port_str);
    return res;
}

bool HostID::isLocalAddress(UInt16 clickhouse_port) const
{
    try
    {
        return DB::isLocalAddress(DNSResolver::instance().resolveAddress(host_name, port), clickhouse_port);
    }
    catch (const Poco::Net::NetException &)
    {
        /// Avoid "Host not found" exceptions
        return false;
    }
}

void DDLLogEntry::assertVersion() const
{
    constexpr UInt64 max_version = 2;
    if (version == 0 || max_version < version)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unknown DDLLogEntry format version: {}."
                                                            "Maximum supported version is {}", version, max_version);
}

void DDLLogEntry::setSettingsIfRequired(ContextPtr context)
{
    version = context->getSettingsRef().distributed_ddl_entry_format_version;
    if (version == 2)
        settings.emplace(context->getSettingsRef().changes());
}

String DDLLogEntry::toString() const
{
    WriteBufferFromOwnString wb;

    wb << "version: " << version << "\n";
    wb << "query: " << escape << query << "\n";

    bool write_hosts = version == 1 || !hosts.empty();
    if (write_hosts)
    {
        Strings host_id_strings(hosts.size());
        std::transform(hosts.begin(), hosts.end(), host_id_strings.begin(), HostID::applyToString);
        wb << "hosts: " << host_id_strings << "\n";
    }

    wb << "initiator: " << initiator << "\n";

    bool write_settings = 1 <= version && settings && !settings->empty();
    if (write_settings)
    {
        ASTSetQuery ast;
        ast.is_standalone = false;
        ast.changes = *settings;
        wb << "settings: " << serializeAST(ast) << "\n";
    }

    return wb.str();
}

void DDLLogEntry::parse(const String & data)
{
    ReadBufferFromString rb(data);

    rb >> "version: " >> version >> "\n";
    assertVersion();

    Strings host_id_strings;
    rb >> "query: " >> escape >> query >> "\n";
    if (version == 1)
    {
        rb >> "hosts: " >> host_id_strings >> "\n";

        if (!rb.eof())
            rb >> "initiator: " >> initiator >> "\n";
        else
            initiator.clear();
    }
    else if (version == 2)
    {

        if (!rb.eof() && *rb.position() == 'h')
            rb >> "hosts: " >> host_id_strings >> "\n";
        if (!rb.eof() && *rb.position() == 'i')
            rb >> "initiator: " >> initiator >> "\n";
        if (!rb.eof() && *rb.position() == 's')
        {
            String settings_str;
            rb >> "settings: " >> settings_str >> "\n";
            ParserSetQuery parser{true};
            constexpr UInt64 max_size = 4096;
            constexpr UInt64 max_depth = 16;
            ASTPtr settings_ast = parseQuery(parser, settings_str, max_size, max_depth);
            settings.emplace(std::move(settings_ast->as<ASTSetQuery>()->changes));
        }
    }

    assertEOF(rb);

    if (!host_id_strings.empty())
    {
        hosts.resize(host_id_strings.size());
        std::transform(host_id_strings.begin(), host_id_strings.end(), hosts.begin(), HostID::fromString);
    }
}


void DDLTaskBase::parseQueryFromEntry(ContextPtr context)
{
    const char * begin = entry.query.data();
    const char * end = begin + entry.query.size();
    const auto & settings = context->getSettingsRef();

    ParserQuery parser_query(end, settings.allow_settings_after_format_in_insert);
    String description;
    query = parseQuery(parser_query, begin, end, description, 0, settings.max_parser_depth);
}

ContextMutablePtr DDLTaskBase::makeQueryContext(ContextPtr from_context, const ZooKeeperPtr & /*zookeeper*/)
{
    auto query_context = Context::createCopy(from_context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId(""); // generate random query_id
    query_context->getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    if (entry.settings)
        query_context->applySettingsChanges(*entry.settings);
    return query_context;
}


bool DDLTask::findCurrentHostID(ContextPtr global_context, Poco::Logger * log)
{
    bool host_in_hostlist = false;

    for (const HostID & host : entry.hosts)
    {
        auto maybe_secure_port = global_context->getTCPPortSecure();

        /// The port is considered local if it matches TCP or TCP secure port that the server is listening.
        bool is_local_port = (maybe_secure_port && host.isLocalAddress(*maybe_secure_port))
                             || host.isLocalAddress(global_context->getTCPPort());

        if (!is_local_port)
            continue;

        if (host_in_hostlist)
        {
            /// This check could be slow a little bit
            LOG_WARNING(log, "There are two the same ClickHouse instances in task {}: {} and {}. Will use the first one only.",
                             entry_name, host_id.readableString(), host.readableString());
        }
        else
        {
            host_in_hostlist = true;
            host_id = host;
            host_id_str = host.toString();
        }
    }

    return host_in_hostlist;
}

void DDLTask::setClusterInfo(ContextPtr context, Poco::Logger * log)
{
    auto * query_on_cluster = dynamic_cast<ASTQueryWithOnCluster *>(query.get());
    if (!query_on_cluster)
        throw Exception("Received unknown DDL query", ErrorCodes::UNKNOWN_TYPE_OF_QUERY);

    cluster_name = query_on_cluster->cluster;
    cluster = context->tryGetCluster(cluster_name);

    if (!cluster)
        throw Exception(ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION,
                        "DDL task {} contains current host {} in cluster {}, but there is no such cluster here.",
                        entry_name, host_id.readableString(), cluster_name);

    /// Try to find host from task host list in cluster
    /// At the first, try find exact match (host name and ports should be literally equal)
    /// If the attempt fails, try find it resolving host name of each instance

    if (!tryFindHostInCluster())
    {
        LOG_WARNING(log, "Not found the exact match of host {} from task {} in cluster {} definition. Will try to find it using host name resolving.",
                         host_id.readableString(), entry_name, cluster_name);

        if (!tryFindHostInClusterViaResolving(context))
            throw Exception(ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION, "Not found host {} in definition of cluster {}",
                                                                 host_id.readableString(), cluster_name);

        LOG_INFO(log, "Resolved host {} from task {} as host {} in definition of cluster {}",
                 host_id.readableString(), entry_name, address_in_cluster.readableString(), cluster_name);
    }

    WithoutOnClusterASTRewriteParams params;
    params.default_database = address_in_cluster.default_database;
    params.host_id = address_in_cluster.toString();
    query = query_on_cluster->getRewrittenASTWithoutOnCluster(params);
    query_on_cluster = nullptr;
}

bool DDLTask::tryFindHostInCluster()
{
    const auto & shards = cluster->getShardsAddresses();
    bool found_exact_match = false;
    String default_database;

    for (size_t shard_num = 0; shard_num < shards.size(); ++shard_num)
    {
        for (size_t replica_num = 0; replica_num < shards[shard_num].size(); ++replica_num)
        {
            const Cluster::Address & address = shards[shard_num][replica_num];

            if (address.host_name == host_id.host_name && address.port == host_id.port)
            {
                if (found_exact_match)
                {
                    if (default_database == address.default_database)
                    {
                        throw Exception(ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION,
                                        "There are two exactly the same ClickHouse instances {} in cluster {}",
                                        address.readableString(), cluster_name);
                    }
                    else
                    {
                        /* Circular replication is used.
                         * It is when every physical node contains
                         * replicas of different shards of the same table.
                         * To distinguish one replica from another on the same node,
                         * every shard is placed into separate database.
                         * */
                        is_circular_replicated = true;
                        auto * query_with_table = dynamic_cast<ASTQueryWithTableAndOutput *>(query.get());

                        /// For other DDLs like CREATE USER, there is no database name and should be executed successfully.
                        if (query_with_table)
                        {
                            if (!query_with_table->database)
                                throw Exception(ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION,
                                                "For a distributed DDL on circular replicated cluster its table name must be qualified by database name.");

                            if (default_database == query_with_table->getDatabase())
                                return true;
                        }
                    }
                }
                found_exact_match = true;
                host_shard_num = shard_num;
                host_replica_num = replica_num;
                address_in_cluster = address;
                default_database = address.default_database;
            }
        }
    }

    return found_exact_match;
}

bool DDLTask::tryFindHostInClusterViaResolving(ContextPtr context)
{
    const auto & shards = cluster->getShardsAddresses();
    bool found_via_resolving = false;

    for (size_t shard_num = 0; shard_num < shards.size(); ++shard_num)
    {
        for (size_t replica_num = 0; replica_num < shards[shard_num].size(); ++replica_num)
        {
            const Cluster::Address & address = shards[shard_num][replica_num];

            if (auto resolved = address.getResolvedAddress(); resolved
                && (isLocalAddress(*resolved, context->getTCPPort())
                    || (context->getTCPPortSecure() && isLocalAddress(*resolved, *context->getTCPPortSecure()))))
            {
                if (found_via_resolving)
                {
                    throw Exception(ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION,
                                    "There are two the same ClickHouse instances in cluster {} : {} and {}",
                                    cluster_name, address_in_cluster.readableString(), address.readableString());
                }
                else
                {
                    found_via_resolving = true;
                    host_shard_num = shard_num;
                    host_replica_num = replica_num;
                    address_in_cluster = address;
                }
            }
        }
    }

    return found_via_resolving;
}

String DDLTask::getShardID() const
{
    /// Generate unique name for shard node, it will be used to execute the query by only single host
    /// Shard node name has format 'replica_name1,replica_name2,...,replica_nameN'
    /// Where replica_name is 'replica_config_host_name:replica_port'

    auto shard_addresses = cluster->getShardsAddresses().at(host_shard_num);

    Strings replica_names;
    for (const Cluster::Address & address : shard_addresses)
        replica_names.emplace_back(address.readableString());
    ::sort(replica_names.begin(), replica_names.end());

    String res;
    for (auto it = replica_names.begin(); it != replica_names.end(); ++it)
        res += *it + (std::next(it) != replica_names.end() ? "," : "");

    return res;
}

DatabaseReplicatedTask::DatabaseReplicatedTask(const String & name, const String & path, DatabaseReplicated * database_)
    : DDLTaskBase(name, path)
    , database(database_)
{
    host_id_str = database->getFullReplicaName();
}

String DatabaseReplicatedTask::getShardID() const
{
    return database->shard_name;
}

void DatabaseReplicatedTask::parseQueryFromEntry(ContextPtr context)
{
    DDLTaskBase::parseQueryFromEntry(context);
    if (auto * ddl_query = dynamic_cast<ASTQueryWithTableAndOutput *>(query.get()))
    {
        /// Update database name with actual name of local database
        assert(!ddl_query->database);
        ddl_query->setDatabase(database->getDatabaseName());
    }
}

ContextMutablePtr DatabaseReplicatedTask::makeQueryContext(ContextPtr from_context, const ZooKeeperPtr & zookeeper)
{
    auto query_context = DDLTaskBase::makeQueryContext(from_context, zookeeper);
    query_context->getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    query_context->getClientInfo().is_replicated_database_internal = true;
    query_context->setCurrentDatabase(database->getDatabaseName());

    auto txn = std::make_shared<ZooKeeperMetadataTransaction>(zookeeper, database->zookeeper_path, is_initial_query, entry_path);
    query_context->initZooKeeperMetadataTransaction(txn);

    if (is_initial_query)
    {
        txn->addOp(zkutil::makeRemoveRequest(entry_path + "/try", -1));
        txn->addOp(zkutil::makeCreateRequest(entry_path + "/committed", host_id_str, zkutil::CreateMode::Persistent));
        txn->addOp(zkutil::makeSetRequest(database->zookeeper_path + "/max_log_ptr", toString(getLogEntryNumber(entry_name)), -1));
    }

    txn->addOp(getOpToUpdateLogPointer());

    for (auto & op : ops)
        txn->addOp(std::move(op));
    ops.clear();

    return query_context;
}

Coordination::RequestPtr DatabaseReplicatedTask::getOpToUpdateLogPointer()
{
    return zkutil::makeSetRequest(database->replica_path + "/log_ptr", toString(getLogEntryNumber(entry_name)), -1);
}

String DDLTaskBase::getLogEntryName(UInt32 log_entry_number)
{
    return zkutil::getSequentialNodeName("query-", log_entry_number);
}

UInt32 DDLTaskBase::getLogEntryNumber(const String & log_entry_name)
{
    constexpr const char * name = "query-";
    assert(startsWith(log_entry_name, name));
    UInt32 num = parse<UInt32>(log_entry_name.substr(strlen(name)));
    assert(num < std::numeric_limits<Int32>::max());
    return num;
}

void ZooKeeperMetadataTransaction::commit()
{
    if (state != CREATED)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect state ({}), it's a bug", state);
    state = FAILED;
    current_zookeeper->multi(ops);
    state = COMMITTED;
}

ClusterPtr tryGetReplicatedDatabaseCluster(const String & cluster_name)
{
    if (const auto * replicated_db = dynamic_cast<const DatabaseReplicated *>(DatabaseCatalog::instance().tryGetDatabase(cluster_name).get()))
        return replicated_db->getCluster();
    return {};
}

}
