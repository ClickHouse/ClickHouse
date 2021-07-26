#pragma once
#include <libnuraft/log_store.hxx> // Y_IGNORE
#include <map>
#include <mutex>
#include <Core/Types.h>
#include <Coordination/Changelog.h>
#include <common/logger_useful.h>

namespace DB
{

class NuKeeperLogStore : public nuraft::log_store
{
public:
    NuKeeperLogStore(const std::string & changelogs_path, size_t rotate_interval_, bool force_sync_);

    void init(size_t from_log_idx);

    size_t start_index() const override;

    size_t next_slot() const override;

    nuraft::ptr<nuraft::log_entry> last_entry() const override;

    size_t append(nuraft::ptr<nuraft::log_entry> & entry) override;

    void write_at(size_t index, nuraft::ptr<nuraft::log_entry> & entry) override;

    nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> log_entries(size_t start, size_t end) override;

    nuraft::ptr<nuraft::log_entry> entry_at(size_t index) override;

    size_t term_at(size_t index) override;

    nuraft::ptr<nuraft::buffer> pack(size_t index, int32_t cnt) override;

    void apply_pack(size_t index, nuraft::buffer & pack) override;

    bool compact(size_t last_log_index) override;

    bool flush() override;

    size_t size() const;

private:
    mutable std::mutex changelog_lock;
    Poco::Logger * log;
    Changelog changelog;
    bool force_sync;
};

}
