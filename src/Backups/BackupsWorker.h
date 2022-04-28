#pragma once

#include <Backups/BackupStatus.h>
#include <Common/ThreadPool.h>
#include <Core/UUID.h>
#include <unordered_map>


namespace DB
{

/// Manager of backups and restores: executes backups and restores' threads in the background.
/// Keeps information about backups and restores started in this session.
class BackupsWorker
{
public:
    static BackupsWorker & instance();

    size_t add(const String & backup_name, BackupStatus status, const String & error = {});
    void update(size_t task_id, BackupStatus status, const String & error = {});

    struct Entry
    {
        String backup_name;
        size_t task_id;
        BackupStatus status;
        String error;
        time_t timestamp;
    };

    Entry getEntry(size_t task_id) const;
    std::vector<Entry> getEntries() const;

    /// Schedules a new task and performs it in the background thread.
    void run(std::function<void()> && task);

    /// Waits until all tasks have been completed.
    void shutdown();

private:
    BackupsWorker();

    mutable std::mutex mutex;
    std::vector<Entry> entries;
    std::unordered_map<String, size_t /* position in entries */> entries_by_name;
    std::unordered_map<size_t /* task_id */, size_t /* position in entries */ > entries_by_task_id;
    size_t current_task_id = 0;
    ThreadPool thread_pool;
};

}
