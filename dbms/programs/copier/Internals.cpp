#include "Internals.h"

namespace DB
{

ConfigurationPtr getConfigurationFromXMLString(const std::string & xml_data)
{
    std::stringstream ss(xml_data);
    Poco::XML::InputSource input_source{ss};
    return {new Poco::Util::XMLConfiguration{&input_source}};
}


String getQuotedTable(const String & database, const String & table)
{
    if (database.empty())
        return backQuoteIfNeed(table);

    return backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
}

String getQuotedTable(const DatabaseAndTableName & db_and_table)
{
    return getQuotedTable(db_and_table.first, db_and_table.second);
}


// Creates AST representing 'ENGINE = Distributed(cluster, db, table, [sharding_key])
std::shared_ptr<ASTStorage> createASTStorageDistributed(
        const String & cluster_name, const String & database, const String & table,
        const ASTPtr & sharding_key_ast)
{
    auto args = std::make_shared<ASTExpressionList>();
    args->children.emplace_back(std::make_shared<ASTLiteral>(cluster_name));
    args->children.emplace_back(std::make_shared<ASTIdentifier>(database));
    args->children.emplace_back(std::make_shared<ASTIdentifier>(table));
    if (sharding_key_ast)
        args->children.emplace_back(sharding_key_ast);

    auto engine = std::make_shared<ASTFunction>();
    engine->name = "Distributed";
    engine->arguments = args;

    auto storage = std::make_shared<ASTStorage>();
    storage->set(storage->engine, engine);

    return storage;
}


BlockInputStreamPtr squashStreamIntoOneBlock(const BlockInputStreamPtr & stream)
{
    return std::make_shared<SquashingBlockInputStream>(
            stream,
            std::numeric_limits<size_t>::max(),
            std::numeric_limits<size_t>::max());
}

Block getBlockWithAllStreamData(const BlockInputStreamPtr & stream)
{
    return squashStreamIntoOneBlock(stream)->read();
}


bool isExtendedDefinitionStorage(const ASTPtr & storage_ast)
{
    const auto & storage = storage_ast->as<ASTStorage &>();
    return storage.partition_by || storage.order_by || storage.sample_by;
}

ASTPtr extractPartitionKey(const ASTPtr & storage_ast)
{
    String storage_str = queryToString(storage_ast);

    const auto & storage = storage_ast->as<ASTStorage &>();
    const auto & engine = storage.engine->as<ASTFunction &>();

    if (!endsWith(engine.name, "MergeTree"))
    {
        throw Exception(
                "Unsupported engine was specified in " + storage_str + ", only *MergeTree engines are supported",
                ErrorCodes::BAD_ARGUMENTS);
    }

    if (isExtendedDefinitionStorage(storage_ast))
    {
        if (storage.partition_by)
            return storage.partition_by->clone();

        static const char * all = "all";
        return std::make_shared<ASTLiteral>(Field(all, strlen(all)));
    }
    else
    {
        bool is_replicated = startsWith(engine.name, "Replicated");
        size_t min_args = is_replicated ? 3 : 1;

        if (!engine.arguments)
            throw Exception("Expected arguments in " + storage_str, ErrorCodes::BAD_ARGUMENTS);

        ASTPtr arguments_ast = engine.arguments->clone();
        ASTs & arguments = arguments_ast->children;

        if (arguments.size() < min_args)
            throw Exception("Expected at least " + toString(min_args) + " arguments in " + storage_str,
                            ErrorCodes::BAD_ARGUMENTS);

        ASTPtr & month_arg = is_replicated ? arguments[2] : arguments[1];
        return makeASTFunction("toYYYYMM", month_arg->clone());
    }
}

ShardPriority getReplicasPriority(const Cluster::Addresses & replicas, const std::string & local_hostname, UInt8 random)
{
    ShardPriority res;

    if (replicas.empty())
        return res;

    res.is_remote = 1;
    for (auto & replica : replicas)
    {
        if (isLocalAddress(DNSResolver::instance().resolveHost(replica.host_name)))
        {
            res.is_remote = 0;
            break;
        }
    }

    res.hostname_difference = std::numeric_limits<size_t>::max();
    for (auto & replica : replicas)
    {
        size_t difference = getHostNameDifference(local_hostname, replica.host_name);
        res.hostname_difference = std::min(difference, res.hostname_difference);
    }

    res.random = random;
    return res;
}

}
