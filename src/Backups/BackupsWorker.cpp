#include <Backups/BackupsWorker.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

BackupsWorker & BackupsWorker::instance()
{
    static BackupsWorker the_instance;
    return the_instance;
}

BackupsWorker::BackupsWorker() = default;

size_t BackupsWorker::add(const String & backup_name, BackupStatus status, const String & error)
{
    std::lock_guard lock{mutex};

    size_t task_id = ++current_task_id;
    size_t pos;
    auto it = entries_by_name.find(backup_name);
    if (it != entries_by_name.end())
    {
        pos = it->second;
        entries_by_task_id.erase(entries[pos].task_id);
    }
    else
    {
        pos = entries.size();
        entries.emplace_back().backup_name = backup_name;
        entries_by_name.emplace(backup_name, pos);
    }

    entries_by_task_id.emplace(task_id, pos);

    Entry & entry = entries[pos];
    entry.task_id = task_id;
    entry.status = status;
    entry.error = error;
    entry.timestamp = std::time(nullptr);

    return task_id;
}

void BackupsWorker::update(size_t task_id, BackupStatus status, const String & error)
{
    std::lock_guard lock{mutex};
    auto it = entries_by_task_id.find(task_id);
    if ((it == entries_by_task_id.end()) || (it->second >= entries.size()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BackupsWorker: entry_id is out of range");
    Entry & entry = entries[it->second];
    entry.status = status;
    entry.error = error;
    entry.timestamp = std::time(nullptr);
}

BackupsWorker::Entry BackupsWorker::getEntry(size_t task_id) const
{
    std::lock_guard lock{mutex};
    auto it = entries_by_task_id.find(task_id);
    if ((it == entries_by_task_id.end()) || (it->second >= entries.size()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BackupsWorker: entry_id is out of range");
    return entries[it->second];
}

std::vector<BackupsWorker::Entry> BackupsWorker::getEntries() const
{
    std::lock_guard lock{mutex};
    return entries;
}

void BackupsWorker::run(std::function<void()> && task)
{
    thread_pool.scheduleOrThrowOnError(std::move(task));
}

void BackupsWorker::shutdown()
{
    size_t num_active_tasks = thread_pool.active();
    if (!num_active_tasks)
        return;
    LOG_INFO(&Poco::Logger::get("BackupsWorker"), "Waiting for {} backup or restore tasks to be finished", num_active_tasks);
    thread_pool.wait();
    LOG_INFO(&Poco::Logger::get("BackupsWorker"), "All backup and restore tasks have finished");
}

}
