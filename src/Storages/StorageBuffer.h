#pragma once

#include <mutex>
#include <atomic>
#include <thread>
#include <ext/shared_ptr_helper.h>
#include <Core/NamesAndTypes.h>
#include <Core/BackgroundSchedulePool.h>
#include <Storages/IStorage.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Poco/Event.h>


namespace Poco { class Logger; }


namespace DB
{


/** During insertion, buffers the data in the RAM until certain thresholds are exceeded.
  * When thresholds are exceeded, flushes the data to another table.
  * When reading, it reads both from its buffers and from the subordinate table.
  *
  * The buffer is a set of num_shards blocks.
  * When writing, select the block number by the remainder of the `ThreadNumber` division by `num_shards` (or one of the others),
  *  and add rows to the corresponding block.
  * When using a block, it is locked by some mutex. If during write the corresponding block is already occupied
  *  - try to lock the next block in a round-robin fashion, and so no more than `num_shards` times (then wait for lock).
  * Thresholds are checked on insertion, and, periodically, in the background thread (to implement time thresholds).
  * Thresholds act independently for each shard. Each shard can be flushed independently of the others.
  * If a block is inserted into the table, which itself exceeds the max-thresholds, it is written directly to the subordinate table without buffering.
  * Thresholds can be exceeded. For example, if max_rows = 1 000 000, the buffer already had 500 000 rows,
  *  and a part of 800 000 rows is added, then there will be 1 300 000 rows in the buffer, and then such a block will be written to the subordinate table.
  *
  * When you destroy a Buffer table, all remaining data is flushed to the subordinate table.
  * The data in the buffer is not replicated, not logged to disk, not indexed. With a rough restart of the server, the data is lost.
  */
class StorageBuffer final : public ext::shared_ptr_helper<StorageBuffer>, public IStorage
{
friend struct ext::shared_ptr_helper<StorageBuffer>;
friend class BufferSource;
friend class BufferBlockOutputStream;

public:
    /// Thresholds.
    struct Thresholds
    {
        time_t time;    /// The number of seconds from the insertion of the first row into the block.
        size_t rows;    /// The number of rows in the block.
        size_t bytes;   /// The number of (uncompressed) bytes in the block.
    };

    std::string getName() const override { return "Buffer"; }

    QueryProcessingStage::Enum getQueryProcessingStage(const Context &, QueryProcessingStage::Enum /*to_stage*/, SelectQueryInfo &) const override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    bool supportsParallelInsert() const override { return true; }

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, const Context & context) override;

    void startup() override;
    /// Flush all buffers into the subordinate table and stop background thread.
    void shutdown() override;
    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Context & context) override;

    bool supportsSampling() const override { return true; }
    bool supportsPrewhere() const override
    {
        if (!destination_id)
            return false;
        auto dest = DatabaseCatalog::instance().tryGetTable(destination_id, global_context);
        if (dest && dest.get() != this)
            return dest->supportsPrewhere();
        return false;
    }
    bool supportsFinal() const override { return true; }
    bool supportsIndexForIn() const override { return true; }

    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, const Context & query_context, const StorageMetadataPtr & metadata_snapshot) const override;

    void checkAlterIsPossible(const AlterCommands & commands, const Settings & /* settings */) const override;

    /// The structure of the subordinate table is not checked and does not change.
    void alter(const AlterCommands & params, const Context & context, TableLockHolder & table_lock_holder) override;

    std::optional<UInt64> totalRows(const Settings & settings) const override;
    std::optional<UInt64> totalBytes(const Settings & settings) const override;

    std::optional<UInt64> lifetimeRows() const override { return writes.rows; }
    std::optional<UInt64> lifetimeBytes() const override { return writes.bytes; }


private:
    const Context & global_context;

    struct Buffer
    {
        time_t first_write_time = 0;
        Block data;
        mutable std::mutex mutex;
    };

    /// There are `num_shards` of independent buffers.
    const size_t num_shards;
    std::vector<Buffer> buffers;

    const Thresholds min_thresholds;
    const Thresholds max_thresholds;

    StorageID destination_id;
    bool allow_materialized;

    /// Lifetime
    struct LifeTimeWrites
    {
        std::atomic<size_t> rows = 0;
        std::atomic<size_t> bytes = 0;
    } writes;

    Poco::Logger * log;

    void flushAllBuffers(bool check_thresholds = true, bool reset_blocks_structure = false);
    /// Reset the buffer. If check_thresholds is set - resets only if thresholds
    /// are exceeded. If reset_block_structure is set - clears inner block
    /// structure inside buffer (useful in OPTIMIZE and ALTER).
    void flushBuffer(Buffer & buffer, bool check_thresholds, bool locked = false, bool reset_block_structure = false);
    bool checkThresholds(const Buffer & buffer, time_t current_time, size_t additional_rows = 0, size_t additional_bytes = 0) const;
    bool checkThresholdsImpl(size_t rows, size_t bytes, time_t time_passed) const;

    /// `table` argument is passed, as it is sometimes evaluated beforehand. It must match the `destination`.
    void writeBlockToDestination(const Block & block, StoragePtr table);

    void flushBack();
    void reschedule();

    BackgroundSchedulePool & bg_pool;
    BackgroundSchedulePoolTaskHolder flush_handle;

protected:
    /** num_shards - the level of internal parallelism (the number of independent buffers)
      * The buffer is flushed if all minimum thresholds or at least one of the maximum thresholds are exceeded.
      */
    StorageBuffer(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const Context & context_,
        size_t num_shards_,
        const Thresholds & min_thresholds_,
        const Thresholds & max_thresholds_,
        const StorageID & destination_id,
        bool allow_materialized_);
};

}
