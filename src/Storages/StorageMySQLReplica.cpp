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

// TODO: delete this
#include <chrono>
#include <thread>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

class MemorySource : public SourceWithProgress
{
public:
    /// We use range [first, last] which includes right border.
    /// Blocks are stored in std::list which may be appended in another thread.
    /// We don't use synchronisation here, because elements in range [first, last] won't be modified.
    MemorySource(
        Names column_names_,
        BlocksList::iterator first_,
        size_t num_blocks_,
        const StorageMySQLReplica & storage,
        const StorageMetadataPtr & metadata_snapshot)
        : SourceWithProgress(metadata_snapshot->getSampleBlockForColumns(column_names_, storage.getVirtuals(), storage.getStorageID()))
        , column_names(std::move(column_names_))
        , current_it(first_)
        , num_blocks(num_blocks_)
    {
    }

    /// If called, will initialize the number of blocks at first read.
    /// It allows to read data which was inserted into memory table AFTER Storage::read was called.
    /// This hack is needed for global subqueries.
    void delayInitialization(BlocksList * data_, std::mutex * mutex_)
    {
        data = data_;
        mutex = mutex_;
    }

    String getName() const override { return "Memory"; }

protected:
    Chunk generate() override
    {
        if (data)
        {
            std::lock_guard guard(*mutex);
            current_it = data->begin();
            num_blocks = data->size();
            is_finished = num_blocks == 0;

            data = nullptr;
            mutex = nullptr;
        }

        if (is_finished)
        {
            return {};
        }
        else
        {
            const Block & src = *current_it;
            Columns columns;
            columns.reserve(column_names.size());

            /// Add only required columns to `res`.
            for (const auto & name : column_names)
                columns.emplace_back(src.getByName(name).column);

            ++current_block_idx;

            if (current_block_idx == num_blocks)
                is_finished = true;
            else
                ++current_it;

            return Chunk(std::move(columns), src.rows());
        }
    }
private:
    Names column_names;
    BlocksList::iterator current_it;
    size_t current_block_idx = 0;
    size_t num_blocks;
    bool is_finished = false;

    BlocksList * data = nullptr;
    std::mutex * mutex = nullptr;
};

Pipe StorageMySQLReplica::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned num_streams)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    std::lock_guard lock(mutex);

    size_t size = data.size();

    if (num_streams > size)
        num_streams = size;

    Pipes pipes;

    BlocksList::iterator it = data.begin();

    size_t offset = 0;
    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        size_t next_offset = (stream + 1) * size / num_streams;
        size_t num_blocks = next_offset - offset;

        assert(num_blocks > 0);

        pipes.emplace_back(std::make_shared<MemorySource>(column_names, it, num_blocks, *this, metadata_snapshot));

        while (offset < next_offset)
        {
            ++it;
            ++offset;
        }
    }

    return Pipe::unitePipes(std::move(pipes));
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
        global_context);
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
