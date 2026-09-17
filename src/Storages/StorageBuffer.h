#pragma once

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/UUID.h>
#include <Storages/IStorage.h>
#include <Common/ThreadPool_fwd.h>

#include <Poco/Event.h>
#include <Poco/Net/SocketAddress.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>


namespace Poco { class Logger; }


namespace DB
{

class AccessRightsElements;


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
  * There are also separate thresholds for flush, those thresholds are checked only for non-direct flush.
  * This maybe useful if you do not want to add extra latency for INSERT queries,
  * so you can set max_rows=1e6 and flush_rows=500e3, then each 500e3 rows buffer will be flushed in background only.
  *
  * When you destroy a Buffer table, all remaining data is flushed to the subordinate table.
  * The data in the buffer is not replicated, not logged to disk, not indexed. With a rough restart of the server, the data is lost.
  *
  * A flush runs with the identity of the user whose INSERT put the rows into the buffer (see `Writer`), so it has
  * exactly the rights of that user, also on the remote shards of a `Distributed` subordinate table. Rows of different
  * writers are never mixed in one buffer.
  */
class StorageBuffer final : public IStorage, WithContext
{
friend class BufferSource;
friend class BufferSink;

    static VirtualColumnsDescription createVirtuals();

public:
    struct Thresholds
    {
        time_t time = 0;  /// The number of seconds from the insertion of the first row into the block.
        size_t rows = 0;  /// The number of rows in the block.
        size_t bytes = 0; /// The number of (uncompressed) bytes in the block.

        std::string toString() const;
    };

    /** num_shards - the level of internal parallelism (the number of independent buffers)
      * The buffer is flushed if all minimum thresholds or at least one of the maximum thresholds are exceeded.
      */
    StorageBuffer(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        size_t num_shards_,
        const Thresholds & min_thresholds_,
        const Thresholds & max_thresholds_,
        const Thresholds & flush_thresholds_,
        const StorageID & destination_id,
        bool allow_materialized_);

    std::string getName() const override { return "Buffer"; }

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;
    bool isRemote() const override;
    bool readsFromOtherTables() const override { return static_cast<bool>(destination_id); }
    StoragePtr getDestinationTable() const;

    bool supportsParallelInsert() const override { return true; }

    bool supportsSubcolumns() const override { return true; }

    bool supportsColumnsWithDynamicStructure() const override { return true; }

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool /*async_insert*/) override;

    void startup() override;
    /// Flush all buffers into the subordinate table and stop background thread.
    size_t flushBufferedRowsBeforeShutdown() override;

    void flushAndPrepareForShutdown() override;
    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        ContextPtr context) override;

    bool supportsSampling() const override
    {
        /// During reads, Buffer queries both the in-memory buffers and the destination table simultaneously.
        /// Sampling on the buffer part is handled probabilistically (no sampling key required).
        /// Sampling on the destination part requires the destination to have a sampling key.
        /// If there is no destination, only the buffer is read, so sampling is always supported.
        if (auto destination = getDestinationTable())
            return destination->supportsSampling();
        return true;
    }
    bool supportsPrewhere() const override;
    /// read() hands the built PREWHERE to the destination (converting declared-type differences
    /// with a prefix), so a column must exist there and be allowed by the destination's own
    /// contract. Fails closed like supportsPrewhere(): no destination means nothing is supported.
    std::optional<NameSet> supportedPrewhereColumns() const override;
    bool supportedPrewhereColumnsIncludeSubcolumns() const override;
    bool canMoveConditionsToPrewhere() const override;
    /// read() forwards the already-analyzed query straight to the destination table, so the
    /// initiator must not rewrite functions to subcolumns when the destination opts out (e.g.
    /// Distributed). Fails closed like supportsPrewhere(): no destination means no rewrite.
    bool supportsOptimizationToSubcolumns() const override;
    bool supportsOptimizationToTupleElementSubcolumns() const override;
    bool supportsFinal() const override { return true; }

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    /// The structure of the subordinate table is not checked and does not change.
    void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & table_lock_holder, DDLGuardPtr & ddl_guard) override;

    std::optional<UInt64> totalRows(ContextPtr query_context) const override;
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override;

    std::optional<UInt64> lifetimeRows() const override { return lifetime_writes.rows; }
    std::optional<UInt64> lifetimeBytes() const override { return lifetime_writes.bytes; }


private:
    /// The identity under which the rows of a buffer are flushed into the subordinate table.
    ///
    /// It is captured from the context of the `INSERT` that put the rows into the buffer, so the flush
    /// has exactly the rights of the writer: locally, and on the remote shards of a `Distributed`
    /// subordinate table. Those authenticate `initial_user` behind the interserver secret, and run a
    /// query that arrives without one with full access. Rows of different writers are never mixed in
    /// one buffer; an `INSERT` by another writer flushes the buffer first.
    ///
    /// A writer without a user, i.e. a server-initiated insert such as a system log flush or a push
    /// from a materialized view without SQL security, keeps the identity-less flush of the buffer
    /// context.
    struct Writer
    {
        std::optional<UUID> user_id;
        /// Sorted, so that two sessions of the same user with the same roles compare equal.
        std::vector<UUID> current_roles;
        std::vector<UUID> external_roles;
        std::shared_ptr<const AccessRightsElements> authentication_grants;
        time_t authentication_valid_until = 0;
        String current_user;
        String initial_user;
        String authenticated_user;
        std::optional<Poco::Net::SocketAddress> current_address;
        std::optional<Poco::Net::SocketAddress> initial_address;

        static Writer fromContext(const ContextPtr & context);

        /// Whether the rows of the two writers may share a buffer, i.e. would be flushed with the same
        /// rights. The addresses are not part of the identity.
        bool sameIdentity(const Writer & other) const;
    };

    struct Buffer
    {
        time_t first_write_time = 0;
        Block data;
        /// The writer of `data`. Meaningless while the buffer is empty.
        Writer writer;

        /// Schema version, checked to avoid mixing blocks with different sets of columns, from
        /// before and after an ALTER. There are some remaining mild problems if an ALTER happens
        /// in the middle of a long-running INSERT:
        ///  * The data produced by the INSERT after the ALTER is not visible to SELECTs until flushed.
        ///    That's because BufferSource skips buffers with old metadata_version instead of converting
        ///    them to the latest schema, for simplicity.
        ///  * If there are concurrent INSERTs, some of which started before the ALTER and some started
        ///    after, then the buffer's metadata_version will oscillate back and forth between the two
        ///    schemas, flushing the buffer each time. This is probably fine because long-running INSERTs
        ///    usually don't produce lots of small blocks.
        int32_t metadata_version = 0;

        std::unique_lock<std::mutex> lockForReading() const;
        std::unique_lock<std::mutex> lockForWriting() const;
        std::unique_lock<std::mutex> tryLock() const;

    private:
        mutable std::mutex mutex;

        std::unique_lock<std::mutex> lockImpl(bool read) const;
    };

    /// There are `num_shards` of independent buffers.
    const size_t num_shards;
    std::unique_ptr<ThreadPool> flush_pool;
    std::vector<Buffer> buffers;

    const Thresholds min_thresholds;
    const Thresholds max_thresholds;
    const Thresholds flush_thresholds;

    StorageID destination_id;
    bool allow_materialized;

    struct Writes
    {
        std::atomic<size_t> rows = 0;
        std::atomic<size_t> bytes = 0;
    };
    Writes lifetime_writes;
    Writes total_writes;

    LoggerPtr log;

    void flushAllBuffers(bool check_thresholds = true);
    bool flushBuffer(Buffer & buffer, bool check_thresholds, bool locked = false);
    bool checkThresholds(const Buffer & buffer, bool direct, time_t current_time, size_t additional_rows = 0, size_t additional_bytes = 0) const;
    bool checkThresholdsImpl(bool direct, size_t rows, size_t bytes, time_t time_passed) const;

    /// `table` argument is passed, as it is sometimes evaluated beforehand. It must match the `destination`.
    void writeBlockToDestination(const Block & block, StoragePtr table, const Writer & writer);

    /// A query context for flushing into the subordinate table under the identity of `writer`, with
    /// the settings of the buffer context (the `buffer_profile` server setting) in force.
    ContextMutablePtr createFlushContext(const Writer & writer) const;

    void backgroundFlush();
    void reschedule(size_t min_delay);

    BackgroundSchedulePoolPtr bg_pool;
    BackgroundSchedulePoolTaskHolder flush_handle;

    static constexpr size_t BACKGROUND_RESCHEDULE_MIN_DELAY = 1;
};

}
