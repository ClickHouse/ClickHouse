#include <Coordination/InMemoryLogStore.h>

namespace DB
{

namespace
{
using namespace nuraft;
ptr<log_entry> makeClone(const ptr<log_entry> & entry)
{
    ptr<log_entry> clone = cs_new<log_entry>(entry->get_term(), buffer::clone(entry->get_buf()), entry->get_val_type());
    return clone;
}
}

InMemoryLogStore::InMemoryLogStore()
    : start_idx(1)
{
    nuraft::ptr<nuraft::buffer> buf = nuraft::buffer::alloc(0);
    logs[0] = nuraft::cs_new<nuraft::log_entry>(0, buf);
}

uint64_t InMemoryLogStore::start_index() const
{
    return start_idx;
}

uint64_t InMemoryLogStore::next_slot() const
{
    std::lock_guard l(logs_lock);
    // Exclude the dummy entry.
    return start_idx + logs.size() - 1;
}

nuraft::ptr<nuraft::log_entry> InMemoryLogStore::last_entry() const
{
    uint64_t next_idx = next_slot();
    std::lock_guard lock(logs_lock);
    auto entry = logs.find(next_idx - 1);
    if (entry == logs.end())
        entry = logs.find(0);

    return makeClone(entry->second);
}

uint64_t InMemoryLogStore::append(nuraft::ptr<nuraft::log_entry> & entry)
{
    ptr<log_entry> clone = makeClone(entry);

    std::lock_guard l(logs_lock);
    uint64_t idx = start_idx + logs.size() - 1;
    logs[idx] = clone;
    return idx;
}

void InMemoryLogStore::write_at(uint64_t index, nuraft::ptr<nuraft::log_entry> & entry)
{
    nuraft::ptr<log_entry> clone = makeClone(entry);

    // Discard all logs equal to or greater than `index.
    std::lock_guard l(logs_lock);
    auto itr = logs.lower_bound(index);
    while (itr != logs.end())
        itr = logs.erase(itr);
    logs[index] = clone;
}

nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> InMemoryLogStore::log_entries(uint64_t start, uint64_t end)
{
    nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>> ret =
        nuraft::cs_new<std::vector<nuraft::ptr<nuraft::log_entry>>>();

    ret->resize(end - start);
    uint64_t cc = 0;
    for (uint64_t i = start; i < end; ++i)
    {
        nuraft::ptr<nuraft::log_entry> src = nullptr;
        {
            std::lock_guard l(logs_lock);
            auto entry = logs.find(i);
            if (entry == logs.end())
            {
                entry = logs.find(0);
                assert(0);
            }
            src = entry->second;
        }
        (*ret)[cc++] = makeClone(src);
    }
    return ret;
}

nuraft::ptr<nuraft::log_entry> InMemoryLogStore::entry_at(uint64_t index)
{
    nuraft::ptr<nuraft::log_entry> src = nullptr;
    {
        std::lock_guard l(logs_lock);
        auto entry = logs.find(index);
        if (entry == logs.end())
            entry = logs.find(0);
        src = entry->second;
    }
    return makeClone(src);
}

uint64_t InMemoryLogStore::term_at(uint64_t index)
{
    uint64_t term = 0;
    {
        std::lock_guard l(logs_lock);
        auto entry = logs.find(index);
        if (entry == logs.end())
            entry = logs.find(0);
        term = entry->second->get_term();
    }
    return term;
}

nuraft::ptr<nuraft::buffer> InMemoryLogStore::pack(uint64_t index, Int32 cnt)
{
    std::vector<nuraft::ptr<nuraft::buffer>> returned_logs;

    uint64_t uint64_total = 0;
    for (uint64_t ii = index; ii < index + cnt; ++ii)
    {
        ptr<log_entry> le = nullptr;
        {
            std::lock_guard l(logs_lock);
            le = logs[ii];
        }
        assert(le.get());
        nuraft::ptr<nuraft::buffer> buf = le->serialize();
        uint64_total += buf->size();
        returned_logs.push_back(buf);
    }

    nuraft::ptr<buffer> buf_out = nuraft::buffer::alloc(sizeof(int32) + cnt * sizeof(int32) + uint64_total);
    buf_out->pos(0);
    buf_out->put(cnt);

    for (auto & entry : returned_logs)
    {
        nuraft::ptr<nuraft::buffer> & bb = entry;
        buf_out->put(static_cast<Int32>(bb->size()));
        buf_out->put(*bb);
    }
    return buf_out;
}

void InMemoryLogStore::apply_pack(uint64_t index, nuraft::buffer & pack)
{
    pack.pos(0);
    Int32 num_logs = pack.get_int();

    for (Int32 i = 0; i < num_logs; ++i)
    {
        uint64_t cur_idx = index + i;
        Int32 buf_size = pack.get_int();

        nuraft::ptr<nuraft::buffer> buf_local = nuraft::buffer::alloc(buf_size);
        pack.get(buf_local);

        nuraft::ptr<nuraft::log_entry> le = nuraft::log_entry::deserialize(*buf_local);
        {
            std::lock_guard l(logs_lock);
            logs[cur_idx] = le;
        }
    }

    {
        std::lock_guard l(logs_lock);
        auto entry = logs.upper_bound(0);
        if (entry != logs.end())
            start_idx = entry->first;
        else
            start_idx = 1;
    }
}

bool InMemoryLogStore::compact(uint64_t last_log_index)
{
    std::lock_guard l(logs_lock);
    for (uint64_t ii = start_idx; ii <= last_log_index; ++ii)
    {
        auto entry = logs.find(ii);
        if (entry != logs.end())
            logs.erase(entry);
    }

    start_idx = last_log_index + 1;
    return true;
}

bool InMemoryLogStore::is_conf(uint64_t index)
{
    auto entry = entry_at(index);
    return entry != nullptr && entry->get_val_type() == nuraft::conf;
}

}
