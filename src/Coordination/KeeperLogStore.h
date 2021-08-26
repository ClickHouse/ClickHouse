#pragma once
#include <libnuraft/log_store.hxx> // Y_IGNORE
#include <map>
#include <mutex>
#include <Core/Types.h>
#include <Coordination/Changelog.h>
#include <common/logger_useful.h>

namespace DB
{

class KeeperLogStore : public nuraft::log_store
{
public:
    KeeperLogStore(const std::string & changelogs_path, uint64_t rotate_interval_, bool force_sync_);

    void init(uint64_t last_commited_log_index, uint64_t logs_to_keep);

    uint64_t start_index() const override;

    uint64_t next_slot() const override;

    nuraft::ptr<nuraft::log_entry> last_entry() const override;

    uint64_t append(nuraft::ptr<nuraft::log_entry> & entry) override;

    void write_at(uint64_t index, nuraft::ptr<nuraft::log_entry> & entry) override;

    nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> log_entries(uint64_t start, uint64_t end) override;

    nuraft::ptr<nuraft::log_entry> entry_at(uint64_t index) override;

    uint64_t term_at(uint64_t index) override;

    nuraft::ptr<nuraft::buffer> pack(uint64_t index, int32_t cnt) override;

    void apply_pack(uint64_t index, nuraft::buffer & pack) override;

    bool compact(uint64_t last_log_index) override;

    bool flush() override;

    uint64_t size() const;

    void end_of_append_batch(uint64_t start_index, uint64_t count) override;

private:
    mutable std::mutex changelog_lock;
    Poco::Logger * log;
    Changelog changelog;
};

}
