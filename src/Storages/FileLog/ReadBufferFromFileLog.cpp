#include <Interpreters/Context.h>
#include <Storages/FileLog/ReadBufferFromFileLog.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>

#include <common/logger_useful.h>
#include <common/sleep.h>

#include <boost/algorithm/string/join.hpp>
#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMMIT_OFFSET;
}

ReadBufferFromFileLog::ReadBufferFromFileLog(
    const String & path_, Poco::Logger * log_, size_t max_batch_size, size_t poll_timeout_, ContextPtr context_)
    : ReadBuffer(nullptr, 0)
    , path(path_)
    , log(log_)
    , batch_size(max_batch_size)
    , poll_timeout(poll_timeout_)
    , context(context_)
{
}

void ReadBufferFromFileLog::open()
{
    Poco::File file(path);

    if (file.isFile())
    {
        file_status[path].reader = std::ifstream(path);
    }
    else if (file.isDirectory())
    {
        path_is_directory = true;
        Poco::DirectoryIterator dir_iter(file);
        Poco::DirectoryIterator end;
        while (dir_iter != end)
        {
            if (dir_iter->isFile())
                file_status[dir_iter->path()].reader = std::ifstream(dir_iter->path());
            ++dir_iter;
        }
    }

    wait_task = context->getMessageBrokerSchedulePool().createTask("waitTask", [this] { waitFunc(); });
    wait_task->deactivate();

    if (path_is_directory)
    {
        select_task = context->getMessageBrokerSchedulePool().createTask("watchTask", [this] { watchFunc(); });
        select_task->activateAndSchedule();
    }

    cleanUnprocessed();
    allowed = false;
}

void ReadBufferFromFileLog::cleanUnprocessed()
{
    records.clear();
    current = records.begin();
    BufferBase::set(nullptr, 0, 0);
}

void ReadBufferFromFileLog::close()
{
    wait_task->deactivate();

    if (path_is_directory)
        select_task->deactivate();

    for (auto & status : file_status)
        status.second.reader.close();
}

bool ReadBufferFromFileLog::poll()
{

    if (hasMorePolledRecords())
    {
        allowed = true;
        return true;
    }

    buffer_status = BufferStatus::NO_RECORD_RETURNED;

    auto new_records = pollBatch(batch_size);
    if (new_records.empty())
    {
        LOG_TRACE(log, "No records returned");
        return false;
    }
    else
    {
        records = std::move(new_records);
        current = records.begin();

        LOG_TRACE(log, "Polled batch of {} records. ", records.size());

        buffer_status = BufferStatus::NOT_STALLED;
        allowed = true;
        return true;
    }


}

ReadBufferFromFileLog::Records ReadBufferFromFileLog::pollBatch(size_t batch_size_)
{
    Records new_records;
    new_records.reserve(batch_size_);

    readNewRecords(new_records, batch_size);
    if (new_records.size() == batch_size_)
        return new_records;

    wait_task->activateAndSchedule();
    while (!time_out && new_records.size() != batch_size_)
    {
        readNewRecords(new_records, batch_size);
    }

    wait_task->deactivate();
    time_out = false;
    return new_records;
}

void ReadBufferFromFileLog::readNewRecords(ReadBufferFromFileLog::Records & new_records, size_t batch_size_)
{
    std::lock_guard<std::mutex> lock(status_mutex);

    size_t need_records_size = batch_size_ - new_records.size();
    size_t read_records_size = 0;

    for (auto & status : file_status)
    {
        if (status.second.status == FileStatus::NO_CHANGE)
            continue;

        if (status.second.status == FileStatus::REMOVED)
            file_status.erase(status.first);

        while (read_records_size < need_records_size && status.second.reader.good() && !status.second.reader.eof())
        {
            Record record;
            std::getline(status.second.reader, record);
            new_records.emplace_back(record);
            ++read_records_size;
        }

        // Read to the end of the file
        if (status.second.reader.eof())
            status.second.status = FileStatus::NO_CHANGE;

        if (read_records_size == need_records_size)
            break;
    }
}

bool ReadBufferFromFileLog::nextImpl()
{
    if (!allowed || !hasMorePolledRecords())
        return false;

    auto * new_position = const_cast<char *>(current->data());
    BufferBase::set(new_position, current->size(), 0);
    allowed = false;

    ++current;

    return true;
}

void ReadBufferFromFileLog::waitFunc()
{
    sleepForMicroseconds(poll_timeout);
    time_out = true;
}

void ReadBufferFromFileLog::watchFunc()
{
    FileLogDirectoryWatcher dw(path);
    while (true)
    {
        sleepForNanoseconds(poll_timeout);

        auto error = dw.getError();
        if (error)
            LOG_INFO(log, "Error happened during watching directory {}.", dw.getPath());

        auto events = dw.getEvents();
        std::lock_guard<std::mutex> lock(status_mutex);

        for (const auto & event : events)
        {
            switch (event.type)
            {

                case Poco::DirectoryWatcher::DW_ITEM_ADDED:
                    LOG_TRACE(log, "New event {} watched.", event.callback);
                    file_status[event.path].reader = std::ifstream(event.path);
                    break;

                case Poco::DirectoryWatcher::DW_ITEM_MODIFIED:
                    LOG_TRACE(log, "New event {} watched.", event.callback);
                    file_status[event.path].status = FileStatus::UPDATED;
                    break;

                case Poco::DirectoryWatcher::DW_ITEM_REMOVED:
                case Poco::DirectoryWatcher::DW_ITEM_MOVED_TO:
                case Poco::DirectoryWatcher::DW_ITEM_MOVED_FROM:
                    LOG_TRACE(log, "New event {} watched.", event.callback);
                    file_status[event.path].status = FileStatus::REMOVED;
                    break;
            }
        }
    }
}
}
