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


/** During insertion, buffers the data in the RAM until certain thresholds are exceeded.
  * When thresholds are exceeded, flushes the data to another table.
  * When reading, it reads both from its buffers and from the subordinate table.
  *
  * The buffer is a set of num_shards blocks.
  * When writing, select the block number by the remainder of the `ThreadNumber` division by `num_shards` (or one of the others),
  *  and add rows to the corresponding block.
  * When using a block, it is blocked by some mutex. If during write the corresponding block is already occupied
  *  - try to block the next block clockwise, and so no more than `num_shards` times (further blocked).
  * Thresholds are checked on insertion, and, periodically, in the background thread (to implement time thresholds).
  * Thresholds act independently for each shard. Each shard can be flushed independently of the others.
  * If a block is inserted into the table, which itself exceeds the max-thresholds, it is written directly to the subordinate table without buffering.
  * Thresholds can be exceeded. For example, if max_rows = 1 000 000, the buffer already had 500 000 rows,
  *  and a part of 800,000 lines is added, then there will be 1 300 000 rows in the buffer, and then such a block will be written to the subordinate table
  *
  * When you destroy a Buffer type table and when you quit, all data is discarded.
  * The data in the buffer is not replicated, not logged to disk, not indexed. With a rough restart of the server, the data is lost.
  */
class StorageBuffer : private ext::shared_ptr_helper<StorageBuffer>, public IStorage
{
friend class ext::shared_ptr_helper<StorageBuffer>;
friend class BufferBlockInputStream;
friend class BufferBlockOutputStream;

public:
    /// Thresholds.
    struct Thresholds
    {
        time_t time;    /// The number of seconds from the insertion of the first row into the block.
        size_t rows;    /// The number of rows in the block.
        size_t bytes;   /// The number of (uncompressed) bytes in the block.
    };

    /** num_shards - the level of internal parallelism (the number of independent buffers)
      * The buffer is reset if all minimum thresholds or at least one of the maximum thresholds are exceeded.
      */
    static StoragePtr create(const std::string & name_, NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        Context & context_,
        size_t num_shards_, const Thresholds & min_thresholds_, const Thresholds & max_thresholds_,
        const String & destination_database_, const String & destination_table_);

    std::string getName() const override { return "Buffer"; }
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

    /// Resets all buffers to the subordinate table.
    void shutdown() override;
    bool optimize(const String & partition, bool final, bool deduplicate, const Settings & settings) override;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override { name = new_table_name; }

    bool supportsSampling() const override { return true; }
    bool supportsPrewhere() const override { return false; }
    bool supportsFinal() const override { return true; }
    bool supportsIndexForIn() const override { return true; }
    bool supportsParallelReplicas() const override { return true; }

    /// The structure of the subordinate table is not checked and does not change.
    void alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context) override;

private:
    String name;
    NamesAndTypesListPtr columns;

    Context & context;

    struct Buffer
    {
        time_t first_write_time = 0;
        Block data;
        std::mutex mutex;
    };

    /// There are `num_shards` of independent buffers.
    const size_t num_shards;
    std::vector<Buffer> buffers;

    const Thresholds min_thresholds;
    const Thresholds max_thresholds;

    const String destination_database;
    const String destination_table;
    bool no_destination;    /// If set, do not write data from the buffer, but simply empty the buffer.

    Poco::Logger * log;

    Poco::Event shutdown_event;
    /// Resets data by timeout.
    std::thread flush_thread;

    StorageBuffer(const std::string & name_, NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        Context & context_,
        size_t num_shards_, const Thresholds & min_thresholds_, const Thresholds & max_thresholds_,
        const String & destination_database_, const String & destination_table_);

    void flushAllBuffers(bool check_thresholds = true);
    /// Reset the buffer. If check_thresholds is set - resets only if thresholds are exceeded.
    void flushBuffer(Buffer & buffer, bool check_thresholds);
    bool checkThresholds(const Buffer & buffer, time_t current_time, size_t additional_rows = 0, size_t additional_bytes = 0) const;
    bool checkThresholdsImpl(size_t rows, size_t bytes, time_t time_passed) const;

    /// `table` argument is passed, as it is sometimes evaluated beforehand. It must match the `destination`.
    void writeBlockToDestination(const Block & block, StoragePtr table);

    void flushThread();
};

}
