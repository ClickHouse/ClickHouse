#include "StorageCassandra.h"
#include <memory>
#include <mutex>
#include <cassandra.h>
#include <Common/config.h>
#include "Processors/Transforms/CassandraHelpers.h"

#if USE_CASSANDRA

#include "Common/parseAddress.h"
#include "QueryPipeline/Pipe.h"
#include "Storages/IStorage.h"
#include "Storages/StorageFactory.h"
#include "StorageInMemoryMetadata.h"
#include "Storages/registerStorages.h"
#include "base/types.h"
#include "transformQueryForExternalDatabase.h"
#include <Interpreters/evaluateConstantExpression.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int BAD_ARGUMENTS;
}

CassConsistency StorageCassandra::nameConsistency(const std::string & cons_string_) {
    if (cons_string_ == "One")
        return CASS_CONSISTENCY_ONE;
    else if (cons_string_ == "Two")
        return CASS_CONSISTENCY_TWO;
    else if (cons_string_ == "Three")
        return CASS_CONSISTENCY_THREE;
    else if (cons_string_ == "All")
        return CASS_CONSISTENCY_ALL;
    else if (cons_string_ == "EachQuorum")
        return CASS_CONSISTENCY_EACH_QUORUM;
    else if (cons_string_ == "Quorum")
        return CASS_CONSISTENCY_QUORUM;
    else if (cons_string_ == "LocalQuorum")
        return CASS_CONSISTENCY_LOCAL_QUORUM;
    else if (cons_string_ == "LocalOne")
        return CASS_CONSISTENCY_LOCAL_ONE;
    else if (cons_string_ == "Serial")
        return CASS_CONSISTENCY_SERIAL;
    else if (cons_string_ == "LocalSerial")
        return CASS_CONSISTENCY_LOCAL_SERIAL;
    else    /// CASS_CONSISTENCY_ANY is only valid for writes
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Unsupported consistency level: {}", cons_string_);
}


StorageCassandra::StorageCassandra(
        const StorageID & table_id_,
        const std::string & host_,
        const UInt16 & port_,

        const std::string & keyspaces_,
        const std::string & table_name_,
        const std::string & cons_string_,

        const std::string & username_,
        const std::string & password_,

        const std::string & options_,

        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment)
        : IStorage(table_id_)
        , host(host_)
        , port(port_)
        , keyspaces(keyspaces_)
        , table_name(table_name_)
        , username(username_)
        , password(password_)
        , options(options_)
    {
        consistency = nameConsistency(cons_string_);
        StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(columns_);
        storage_metadata.setConstraints(constraints_);
        storage_metadata.setComment(comment);
        setInMemoryMetadata(storage_metadata);

        cassandraCheck(cass_cluster_set_contact_points(cluster, host.c_str()));
        if (port)
            cassandraCheck(cass_cluster_set_port(cluster, port));
        cass_cluster_set_credentials(cluster, username.c_str(), password.c_str());
        
        cassandraCheck(cass_cluster_set_consistency(cluster, consistency));
    }

void registerStorageCassandra(StorageFactory & factory)
{
    factory.registerStorage("Cassandra", [](const StorageFactory::Arguments & args)
    {
        auto configuration = StorageCassandra::getConfiguration(args.engine_args, args.getLocalContext());
        return std::make_shared<StorageCassandra>(
            args.table_id,
            configuration.host,
            configuration.port,
            configuration.database,
            configuration.table,
            configuration.consistency,
            configuration.username,
            configuration.password,
            configuration.options,
            args.columns,
            args.constraints,
            args.comment);
    },
    {
        .source_access_type = AccessType::CASSANDRA,
    });
}


StorageCassandraConfiguration StorageCassandra::getConfiguration(ASTs engine_args, ContextPtr context)
{
    StorageCassandraConfiguration configuration;
    if (auto named_collection = getExternalDataSourceConfiguration(engine_args, context))
    {
        auto [common_configuration, storage_specific_args, _] = named_collection.value();

        configuration.set(common_configuration);

        for (const auto & [arg_name, arg_value] : storage_specific_args)
        {
            if (arg_name == "options")
                configuration.options = arg_value->as<ASTLiteral>()->value.safeGet<String>();
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Unexpected key-value argument."
                        "Got: {}, but expected one of:"
                        "host, port, keyspaces, table_name, consistency, username, password, options ", arg_name);
        }
    }
    else
    {
        if (engine_args.size() < 6 || engine_args.size() > 7)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Storage Cassandra requires from 6 to 7 parameters: "
                            "Cassandra('host:port', 'keyspaces', 'table_name', 'consistency',  'username', 'password', [, 'options']. Got: {}",
                            engine_args.size());

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

        
        auto parsed_host_port = parseAddress(engine_args[0]->as<ASTLiteral &>().value.safeGet<String>(),9042);

        configuration.host = parsed_host_port.first;
        configuration.port = parsed_host_port.second;
        configuration.database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        configuration.table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        configuration.consistency = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
        configuration.username = engine_args[4]->as<ASTLiteral &>().value.safeGet<String>();
        configuration.password = engine_args[5]->as<ASTLiteral &>().value.safeGet<String>();

        if (engine_args.size() == 7)
            configuration.options = engine_args[6]->as<ASTLiteral &>().value.safeGet<String>();
    }
    context->getRemoteHostFilter().checkHostAndPort(configuration.host, toString(configuration.port));

    return configuration;
}

bool StorageCassandra::allowFiltering() {
    return options == "allow_filtering";
}

Pipe StorageCassandra::read(
    const Names & column_names_,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info_,
    ContextPtr context_,
    QueryProcessingStage::Enum /*processed_stage_*/,
    size_t max_block_size_,
    unsigned)
{
    storage_snapshot->check(column_names_);

//отличие от dictionary
    String query = transformQueryForExternalDatabase(
        query_info_, storage_snapshot->metadata->getColumns().getOrdinary(),
        IdentifierQuotingStyle::None, keyspaces, table_name, context_);
    
    if (allowFiltering())
        query += " ALLOW FILTERING";

    Block sample_block;
    for (const String & column_name : column_names_)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({column_data.type, column_data.name});
    }
    return Pipe(std::make_shared<CassandraSource>(StorageCassandra::getSession(), query, sample_block, max_block_size_));
}

CassSessionShared StorageCassandra::getSession() 
{
    auto session = maybe_session.lock();
    if (session)
        return session;
    std::lock_guard lock(connect_mutex);
    session = maybe_session.lock();
    if (session)
        return session;
    session = std::make_shared<CassSessionPtr>();
    CassFuturePtr future = cass_session_connect(*session, cluster);
    cassandraWaitAndCheck(future);
    maybe_session = session;
    return session;
}

// SinkToStoragePtr StrorageCassandra::write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) {
//     return 
// }

}


#endif
