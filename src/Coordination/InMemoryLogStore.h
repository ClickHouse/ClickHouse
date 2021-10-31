#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <Core/Types.h>
#include <libnuraft/log_store.hxx>

namespace DB
{

class InMemoryLogStore : public nuraft::log_store
{
public:
    InMemoryLogStore();

    uint64_t start_index() const override;

    uint64_t next_slot() const override;

    nuraft::ptr<nuraft::log_entry> last_entry() const override;

    uint64_t append(nuraft::ptr<nuraft::log_entry> & entry) override;

    void write_at(uint64_t index, nuraft::ptr<nuraft::log_entry> & entry) override;

    nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> log_entries(uint64_t start, uint64_t end) override;

    nuraft::ptr<nuraft::log_entry> entry_at(uint64_t index) override;

    uint64_t term_at(uint64_t index) override;

    nuraft::ptr<nuraft::buffer> pack(uint64_t index, Int32 cnt) override;

    void apply_pack(uint64_t index, nuraft::buffer & pack) override;

    bool compact(uint64_t last_log_index) override;

    bool flush() override { return true; }

private:
    std::map<uint64_t, nuraft::ptr<nuraft::log_entry>> logs;
    mutable std::mutex logs_lock;
    std::atomic<uint64_t> start_idx;
};

}
