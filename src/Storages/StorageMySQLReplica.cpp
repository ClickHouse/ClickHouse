#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Storages/StorageMySQLReplica.h>

#include <Core/Field.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Pipe.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageLogSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

StorageMySQLReplica::StorageMySQLReplica(
    const StorageID & table_id_,
    Context & context_,
    ColumnsDescription columns_description_,
    ConstraintsDescription constraints_,
    /* slave data */
    const String & mysql_hostname_and_port,
    const String & mysql_database_name,
    const String & mysql_table_name_,
    const String & mysql_user_name,
    const String & mysql_user_password,
    MaterializeMySQLSettingsPtr settings_,
    DiskPtr disk_,
    const String & relative_path_)
    : IStorage(table_id_)
    , global_context(context_.getGlobalContext())
    , mysql_table_name(mysql_table_name_)
    , settings(settings_)
    , disk(disk_)
    , table_path(relative_path_)
    , log(&Poco::Logger::get("MySQLReplica (" + table_id_.table_name + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(std::move(columns_description_));
    storage_metadata.setConstraints(std::move(constraints_));
    setInMemoryMetadata(storage_metadata);

    disk->createDirectories(table_path);

    materialize_thread = getMySQLReplicationThread(
        mysql_hostname_and_port,
        mysql_database_name,
        mysql_user_name,
        mysql_user_password,
        context_);
}

void StorageMySQLReplica::startup()
{
    materialize_thread->registerConsumerStorage(
        getStorageID(),
        mysql_table_name,
        getDataPaths().front() + "/.metadata",
        settings);
    materialize_thread->startSynchronization();
}

void StorageMySQLReplica::shutdown()
{
    materialize_thread->stopSynchronization();
}

void registerStorageMySQLReplica(StorageFactory & factory)
{
    factory.registerStorage("MySQLReplica", [](const StorageFactory::Arguments & args)
    {
        //TODO: copy some logic from StorageMySQL
        ASTs & engine_args = args.engine_args;
        if (engine_args.size() != 5)
        {
            throw Exception("StorageMySQLReplica requires exactly 5 parameters"
                "MySQLReplica("
                    "'hostname:port', "
                    "'mysql_db_name', "
                    "'mysql_table_name', "
                    "'mysql_user_name', "
                    "'mysql_user_password'])",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        std::string mysql_hostname_and_port = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();

        std::string mysql_database_name = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        std::string mysql_table_name = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();

        std::string mysql_user_name = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
        std::string mysql_user_password = engine_args[4]->as<ASTLiteral &>().value.safeGet<String>();

        auto materialize_settings = std::make_shared<MaterializeMySQLSettings>();
        if (args.storage_def->settings)
        {
            materialize_settings->loadFromQuery(*args.storage_def);
        }

        String disk_name = getDiskName(*args.storage_def);
        DiskPtr disk = args.context.getDisk(disk_name);

        return StorageMySQLReplica::create(
            args.table_id,
            args.context,
            args.columns,
            args.constraints,
            mysql_hostname_and_port,
            mysql_database_name,
            mysql_table_name,
            mysql_user_name,
            mysql_user_password,
            materialize_settings,
            disk,
            args.relative_data_path);
    }, StorageFactory::StorageFeatures{ .supports_settings = true, });
}

}

#endif
