#include "StoragePostgreSQLReplica.h"

#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>

#include <DataTypes/DataTypeString.h>

#include <Core/Settings.h>
#include <Common/parseAddress.h>
#include <Common/assert_cast.h>
#include <Parsers/ASTLiteral.h>
#include <Columns/ColumnNullable.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <Processors/Pipe.h>
#include <IO/WriteHelpers.h>
#include <Common/Macros.h>
#include <Core/Settings.h>
#include <Parsers/ASTCreateQuery.h>
#include "PostgreSQLReplicationSettings.h"
#include <Storages/StorageFactory.h>
#include "PostgreSQLReplicaBlockInputStream.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StoragePostgreSQLReplica::StoragePostgreSQLReplica(
    const StorageID & table_id_,
    const String & remote_table_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const Context & context_,
    const PostgreSQLReplicationHandler & replication_handler_,
    std::unique_ptr<PostgreSQLReplicationSettings> replication_settings_)
    : IStorage(table_id_)
    , remote_table_name(remote_table_name_)
    , global_context(context_)
    , replication_settings(std::move(replication_settings_))
    , replication_handler(std::make_unique<PostgreSQLReplicationHandler>(replication_handler_))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
}


void StoragePostgreSQLReplica::startup()
{
    replication_handler->startup();
}


void StoragePostgreSQLReplica::shutdown()
{
    //replication_handler->dropReplicationSlot();
}


Pipe StoragePostgreSQLReplica::read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & /* query_info */,
        const Context & /* context */,
        QueryProcessingStage::Enum /* processed_stage */,
        size_t /* max_block_size */,
        unsigned /* num_streams */)
{
    auto sample_block = metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID());
    return Pipe();
}


void registerStoragePostgreSQLReplica(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;
        bool has_settings = args.storage_def->settings;
        auto postgresql_replication_settings = std::make_unique<PostgreSQLReplicationSettings>();

        if (has_settings)
            postgresql_replication_settings->loadFromQuery(*args.storage_def);

        if (engine_args.size() != 5)
            throw Exception("Storage PostgreSQLReplica requires 5 parameters: "
                            "PostgreSQL('host:port', 'database', 'table', 'username', 'password'",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.local_context);

        auto parsed_host_port = parseAddress(engine_args[0]->as<ASTLiteral &>().value.safeGet<String>(), 5432);
        const String & remote_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        const String & remote_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

        String connection_str;
        connection_str = fmt::format("dbname={} host={} port={} user={} password={}",
                remote_database,
                parsed_host_port.first, std::to_string(parsed_host_port.second),
                engine_args[3]->as<ASTLiteral &>().value.safeGet<String>(),
                engine_args[4]->as<ASTLiteral &>().value.safeGet<String>());

        auto global_context(args.context.getGlobalContext());
        auto replication_slot_name = global_context.getMacros()->expand(postgresql_replication_settings->postgresql_replication_slot_name.value);
        auto publication_name = global_context.getMacros()->expand(postgresql_replication_settings->postgresql_publication_name.value);

        PostgreSQLReplicationHandler replication_handler(global_context, remote_database, remote_table, connection_str, replication_slot_name, publication_name);

        return StoragePostgreSQLReplica::create(
                args.table_id, remote_table, args.columns, args.constraints, global_context,
                replication_handler, std::move(postgresql_replication_settings));
    };

    factory.registerStorage(
            "PostgreSQLReplica",
            creator_fn,
            StorageFactory::StorageFeatures{ .supports_settings = true, .source_access_type = AccessType::POSTGRES,
    });
}

NamesAndTypesList StoragePostgreSQLReplica::getVirtuals() const
{
    return NamesAndTypesList{
    };
}

}

