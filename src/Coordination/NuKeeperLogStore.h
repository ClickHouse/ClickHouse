#pragma once
#include <libnuraft/log_store.hxx> // Y_IGNORE
#include <map>
#include <mutex>
#include <Core/Types.h>
#include <Coordination/Changelog.h>

namespace DB
{

class NuKeeperLogStore : public nuraft::log_store
{
public:
    NuKeeperLogStore(const std::string & changelogs_path, size_t rotate_interval_);


private:
    mutable std::mutex logs_lock;
    std::atomic<size_t> start_idx;
    Changelog in_memory_changelog;
    ChangelogOnDiskHelper on_disk_changelog_helper;
};

}
