#pragma once
#include <libnuraft/log_store.hxx>
#include <mutex>
#include <Core/Types.h>
#include <Coordination/Changelog.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/Keeper4LWInfo.h>
#include <base/defines.h>

namespace DB
{

/// Wrapper around Changelog class. Implements RAFT log storage.
class KeeperLogStore : public nuraft::log_store
{
public:
    KeeperLogStore(LogFileSettings log_file_settings, FlushSettings flush_settings, KeeperContextPtr keeper_context);

    /// Read log storage from filesystem starting from last_commited_log_index
    void init(uint64_t last_commited_log_index, uint64_t logs_to_keep);

    uint64_t start_index() const override;

    uint64_t next_slot() const override;

    /// return last entry from log
    nuraft::ptr<nuraft::log_entry> last_entry() const override;

    /// Append new entry to log
    uint64_t append(nuraft::ptr<nuraft::log_entry> & entry) override;

    /// Remove all entries starting from index and write entry into index position
    void write_at(uint64_t index, nuraft::ptr<nuraft::log_entry> & entry) override;

    /// Return entries between [start, end)
    nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> log_entries(uint64_t start, uint64_t end) override;

    /// Return entry at index
    nuraft::ptr<nuraft::log_entry> entry_at(uint64_t index) override;

    bool is_conf(uint64_t index) override;

    /// Term if the index
    uint64_t term_at(uint64_t index) override;

    /// Serialize entries in interval [index, index + cnt)
    nuraft::ptr<nuraft::buffer> pack(uint64_t index, int32_t cnt) override;

    /// Apply serialized entries starting from index
    void apply_pack(uint64_t index, nuraft::buffer & pack) override;

    /// Entries from last_log_index can be removed from memory and from disk
    bool compact(uint64_t last_log_index) override;

    /// Call fsync to the stored data
    bool flush() override;

    /// Stop background cleanup thread in change
    void shutdownChangelog();

    /// Flush logstore and call shutdown of background thread in changelog
    bool flushChangelogAndShutdown();

    /// Current log storage size
    uint64_t size() const;

    uint64_t last_durable_index() override;

    /// Flush batch of appended entries
    void end_of_append_batch(uint64_t start_index, uint64_t count) override;

    /// Get entry with latest config in logstore
    nuraft::ptr<nuraft::log_entry> getLatestConfigChange() const;

    void setRaftServer(const nuraft::ptr<nuraft::raft_server> & raft_server);

    void getKeeperLogInfo(KeeperLogInfo & log_info) const;

private:
    mutable std::mutex changelog_lock;
    LoggerPtr log;
    Changelog changelog TSA_GUARDED_BY(changelog_lock);
};

}
