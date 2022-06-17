#include <Interpreters/Context_fwd.h>
#include <Interpreters/IJoin.h>

#include <Core/Block.h>

#include <Common/MultiVersion.h>

#include <mutex>

namespace DB
{

class TableJoin;
class HashJoin;

class GraceHashJoin final : public IJoin
{
    class FileBucket;
    class DelayedBlocks;
    using Buckets = std::vector<std::shared_ptr<FileBucket>>;
    using BucketsSnapshot = std::shared_ptr<const Buckets>;

public:
    GraceHashJoin(
        ContextPtr context_, std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_, bool any_take_last_row_ = false);

    const TableJoin & getTableJoin() const override { return *table_join; }

    bool addJoinedBlock(const Block & block, bool check_limits) override;
    void checkTypesOfKeys(const Block & block) const override;
    // NB: In GraceHashJoin, joinBlock does not joins 
    void joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed) override;

    void setTotals(const Block & block) override { totals = block; }
    const Block & getTotals() const override { return totals; }

    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;
    bool alwaysReturnsEmptySet() const override;

    std::shared_ptr<NotJoinedBlocks>
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

    /// Open iterator over joined blocks.
    /// Must be called after all @joinBlock calls.
    std::unique_ptr<IDelayedJoinedBlocksStream> getDelayedBlocks(IDelayedJoinedBlocksStream * prev_cursor) override;

private:
    using InMemoryJoin = HashJoin;
    using InMemoryJoinPtr = std::unique_ptr<InMemoryJoin>;

    /// Split block into multiple shards by hash.
    template <bool right>
    Blocks scatterBlock(const Block & block, size_t shards) const;

    /// Create empty join for in-memory processing.
    InMemoryJoinPtr makeInMemoryJoin();
    /// Read right table blocks from @bucket to the @join. Calls @rehash on overflow.
    void fillInMemoryJoin(InMemoryJoinPtr & join, FileBucket * bucket);
    /// Add right table block to the @join. Calls @rehash on overflow.
    void addJoinedBlockImpl(InMemoryJoinPtr & join, size_t bucket_index, const Block & block);
    /// Rebuild @join after rehash: scatter the blocks in join and write parts that belongs to the other shards to disk.
    void rehashInMemoryJoin(InMemoryJoinPtr & join, const BucketsSnapshot & snapshot, size_t bucket);
    /// Check that @join satisifes limits on rows/bytes in @table_join.
    bool fitsInMemory(InMemoryJoin * join) const;

    /// Create new bucket at the end of @destination.
    void addBucket(Buckets & destination, const FileBucket * parent);
    /// Read and join left table block. Called by DelayedBlocks itself (see @DelayedBlocks::next).
    Block joinNextBlockInBucket(DelayedBlocks & iterator);

    /// Increase number of buckets to match desired_size.
    /// Called when HashJoin in-memory table for one bucket exceeds the limits.
    BucketsSnapshot rehash(size_t desired_size);
    /// Perform some bookkeeping after all calls to @joinBlock.
    void startReadingDelayedBlocks();

    Poco::Logger * log;
    ContextPtr context;
    std::shared_ptr<TableJoin> table_join;
    Block right_sample_block;
    bool any_take_last_row;
    size_t initial_num_buckets;
    size_t max_num_buckets;

    InMemoryJoinPtr first_bucket;
    MultiVersion<Buckets> buckets;
    std::mutex rehash_mutex;
    std::atomic<bool> started_reading_delayed_blocks{false};

    Block totals;
};

}
