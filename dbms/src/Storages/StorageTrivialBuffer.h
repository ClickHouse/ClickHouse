#pragma once

#include <mutex>
#include <thread>

#include <Common/SipHash.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockOutputStream.h>
#include <ext/shared_ptr_helper.h>
#include <Poco/Event.h>
#include <Storages/IStorage.h>

#include <Common/ZooKeeper/ZooKeeper.h>

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
class StorageTrivialBuffer : public ext::shared_ptr_helper<StorageTrivialBuffer>, public IStorage
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

    std::string getName() const override { return "TrivialBuffer"; }
    std::string getTableName() const override { return name; }

    const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    bool checkThresholds(const time_t current_time, const size_t additional_rows = 0,
                const size_t additional_bytes = 0) const;
    bool checkThresholdsImpl(const size_t rows, const size_t bytes,
                const time_t time_passed) const;

    /// Start flushing thread.
    void startup() override;
    /// Writes all the blocks in buffer into the destination table. Stop flushing thread.
    void shutdown() override;
    bool optimize(const ASTPtr & query, const String & partition, bool final, bool deduplicate, const Settings & settings) override;

    void rename(const String & new_path_to_db, const String & new_database_name,
            const String & new_table_name) override { name = new_table_name; }

    bool supportsSampling() const override { return true; }
    bool supportsPrewhere() const override { return false; }
    bool supportsFinal() const override { return true; }
    bool supportsIndexForIn() const override { return true; }
    bool supportsParallelReplicas() const override { return true; }

    /// Does not check or alter the structure of dependent table.
    void alter(const AlterCommands & params, const String & database_name,
        const String & table_name, const Context & context) override;

    class ZookeeperDeduplicationController
    {
    public:
        using HashType = String;

        static HashType getHashFrom(SipHash & hash) { return std::to_string(hash.get64()); }

        bool contains(HashType block_hash)
        {
            std::string res;
            return zookeeper->tryGet(path_in_zk_for_deduplication + "/" + block_hash, res);
        }

        void insert(HashType block_hash)
        {
            std::vector<String> current_hashes;
            if (zookeeper->tryGetChildren(path_in_zk_for_deduplication, current_hashes) == ZNONODE)
            {
                throw DB::Exception("No node \'" + path_in_zk_for_deduplication + "\' to control deduplication.");
            }

            // Cleanup zookeeper if needed.
            if (current_hashes.size() >= 2*num_blocks_to_deduplicate)
            {
                using HashWithTimestamp = std::pair<String, time_t>;
                std::vector<HashWithTimestamp> hashes_with_timestamps;
                for (auto & hash : current_hashes)
                {
                    zkutil::Stat stat;
                    String res;
                    String path_in_zk = path_in_zk_for_deduplication + "/" + hash;
                    if (!zookeeper->tryGet(path_in_zk, res, &stat))
                    {
                        throw DB::Exception("Seems like a race conditions between replics was found, path: " + path_in_zk);
                    }
                    hashes_with_timestamps.emplace_back(path_in_zk, stat.ctime);
                }
                // We do not need to sort all the hashes, only 'num_blocks_to_deduplicate' hashes
                // with minimum creation time.
                auto hashes_with_timestamps_end = hashes_with_timestamps.end();
                if (hashes_with_timestamps.size() > num_blocks_to_deduplicate)
                    hashes_with_timestamps_end = hashes_with_timestamps.begin() + num_blocks_to_deduplicate;
                std::partial_sort(hashes_with_timestamps.begin(), hashes_with_timestamps_end, hashes_with_timestamps.end(),
                    [] (const HashWithTimestamp & a, const HashWithTimestamp & b) -> bool
                    {
                        return a.second > b.second;
                    }
                );
                zkutil::Ops nodes_to_remove;
                for (auto it = hashes_with_timestamps.begin(); it != hashes_with_timestamps_end; ++it)
                {
                    nodes_to_remove.emplace_back(std::make_unique<zkutil::Op::Remove>(it->first, -1));
                }
                zookeeper->tryMulti(nodes_to_remove);
            }

            // Finally, inserting new node.
            std::string path_for_insert = path_in_zk_for_deduplication + "/" + block_hash;
            if (zookeeper->tryCreate(path_for_insert, {},
                zkutil::CreateMode::Persistent) != ZOK)
            {
                throw DB::Exception("Cannot create node at path: " + path_for_insert);
            }

        }

        void updateOnDeduplication(HashType block_hash)
        {
            zookeeper->createOrUpdate(path_in_zk_for_deduplication + "/" + block_hash,
                                      {}, zkutil::CreateMode::Persistent);
        }

        ZookeeperDeduplicationController(size_t num_blocks_to_deduplicate_, zkutil::ZooKeeperPtr zookeeper_,
                                     const std::string & path_in_zk_for_deduplication_)
        : num_blocks_to_deduplicate(num_blocks_to_deduplicate_),
        zookeeper(zookeeper_), path_in_zk_for_deduplication(path_in_zk_for_deduplication_)
        { }

    private:
        using DeduplicationBuffer = std::unordered_set<HashType>;

        size_t num_blocks_to_deduplicate;
        zkutil::ZooKeeperPtr zookeeper;
        const std::string path_in_zk_for_deduplication;
    };


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
    const String path_in_zk_for_deduplication;
    zkutil::ZooKeeperPtr zookeeper;
    ZookeeperDeduplicationController deduplication_controller;

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
        const String & path_in_zk_for_deduplication_,
        const Thresholds & min_thresholds_, const Thresholds & max_thresholds_,
        const String & destination_database_, const String & destination_table_);

    template <typename DeduplicationController>
    void addBlock(const Block & block, DeduplicationController & deduplication_controller);
    /// Parameter 'table' is passed because it's sometimes pre-computed. It should
    /// conform the 'destination_table'.
    void writeBlockToDestination(const Block & block, StoragePtr table);


    void flush(bool check_thresholds = true, bool is_called_from_background = false);
    void flushThread();
};

}
