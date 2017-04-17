#pragma once

#include <mutex>
#include <thread>
#include <ext/shared_ptr_helper.hpp>
#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Poco/Event.h>

namespace Poco { class Logger; }

namespace DB
{

class Context;

/** Stores incoming blocks until some thresholds are exceeded, then sends
  * them to the table it looks into in the same order they came to the buffer.
  *
  * Thresolds are checked during insert and in background thread (to control
  * time thresholds).
  * If inserted block exceedes max limits, buffer is flushed and then the incoming
  * block is appended to buffer.
  *
  * Destroying TrivialBuffer or shutting down lead to the buffer flushing.
  * The data in the buffer is not replicated, logged or stored. After hard reset of the
  * server, the data is lost.
  */
class StorageTrivialBuffer : private ext::shared_ptr_helper<StorageTrivialBuffer>, public IStorage
{
friend class ext::shared_ptr_helper<StorageTrivialBuffer>;
friend class TrivialBufferBlockInputStream;
friend class TrivialBufferBlockOutputStream;

public:
    struct Thresholds
    {
        time_t time;    /// Seconds after insertion of first block.
        size_t rows;    /// Number of rows in buffer.
        size_t bytes;    /// Number of bytes (incompressed) in buffer.
    };

    static StoragePtr create(const std::string & name_, NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        Context & context_, size_t num_blocks_to_deduplicate_,
        const Thresholds & min_thresholds_, const Thresholds & max_thresholds_,
        const String & destination_database_, const String & destination_table_);

    std::string getName() const override { return "TrivialBuffer"; }
    std::string getTableName() const override { return name; }

    const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }

    BlockInputStreams read(
        const Names & column_names,
        ASTPtr query,
        const Context & context,
        const Settings & settings,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size = DEFAULT_BLOCK_SIZE,
        unsigned threads = 1) override;

    BlockOutputStreamPtr write(ASTPtr query, const Settings & settings) override;

    bool checkThresholds(const time_t current_time, const size_t additional_rows = 0,
                const size_t additional_bytes = 0) const;
    bool checkThresholdsImpl(const size_t rows, const size_t bytes,
                const time_t time_passed) const;

    /// Writes all the blocks in buffer into the destination table.
    void shutdown() override;
    bool optimize(const String & partition, bool final, const Settings & settings) override;

    void rename(const String & new_path_to_db, const String & new_database_name,
            const String & new_table_name) override { name = new_table_name; }

    bool supportsSampling() const override { return true; }
    bool supportsPrewhere() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsIndexForIn() const override { return true; }
    bool supportsParallelReplicas() const override { return true; }

    /// Does not check or alter the structure of dependent table.
    void alter(const AlterCommands & params, const String & database_name,
            const String & table_name, const Context & context) override;

private:
    String name;
    NamesAndTypesListPtr columns;

    Context & context;

    std::mutex mutex;

    BlocksList data;

    size_t current_rows = 0;
    size_t current_bytes = 0;
    time_t first_write_time = 0;
    const size_t num_blocks_to_deduplicate;
    using HashType = UInt64;
    using DeduplicationBuffer = std::unordered_set<HashType>;
    /// We insert new blocks' hashes into 'current_hashes' and perform lookup
    /// into both sets. If 'current_hashes' is overflowed, it flushes into
    /// into 'previous_hashes', and new set is created for 'current'.
    std::unique_ptr<DeduplicationBuffer> current_hashes, previous_hashes;
    const Thresholds min_thresholds;
    const Thresholds max_thresholds;

    const String destination_database;
    const String destination_table;
    /// If set, forces to clean out buffer, not write to destination table.
    bool no_destination;

    Poco::Logger * log;

    Poco::Event shutdown_event;
    /// Executes flushing by the time thresholds.
    std::thread flush_thread;

    StorageTrivialBuffer(const std::string & name_, NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        Context & context_, size_t num_blocks_to_deduplicate_,
        const Thresholds & min_thresholds_, const Thresholds & max_thresholds_,
        const String & destination_database_, const String & destination_table_);

    void addBlock(const Block & block);
    /// Parameter 'table' is passed because it's sometimes pre-computed. It should
    /// conform the 'destination_table'.
    void writeBlockToDestination(const Block & block, StoragePtr table);


    void flush(bool check_thresholds = true, bool is_called_from_background = false);
    void flushThread();
};

}
