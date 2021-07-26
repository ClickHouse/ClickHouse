#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <Core/Types.h>
#include <libnuraft/log_store.hxx> // Y_IGNORE

namespace DB
{

class InMemoryLogStore : public nuraft::log_store
{
public:
    InMemoryLogStore();

    size_t start_index() const override;

    size_t next_slot() const override;

    nuraft::ptr<nuraft::log_entry> last_entry() const override;

    size_t append(nuraft::ptr<nuraft::log_entry> & entry) override;

    void write_at(size_t index, nuraft::ptr<nuraft::log_entry> & entry) override;

    nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> log_entries(size_t start, size_t end) override;

    nuraft::ptr<nuraft::log_entry> entry_at(size_t index) override;

    size_t term_at(size_t index) override;

    nuraft::ptr<nuraft::buffer> pack(size_t index, Int32 cnt) override;

    void apply_pack(size_t index, nuraft::buffer & pack) override;

    bool compact(size_t last_log_index) override;

    bool flush() override { return true; }

private:
    std::map<size_t, nuraft::ptr<nuraft::log_entry>> logs;
    mutable std::mutex logs_lock;
    std::atomic<size_t> start_idx;
};

}
