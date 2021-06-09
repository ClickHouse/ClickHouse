#include <Interpreters/Context.h>
#include <Storages/FileLog/ReadBufferFromFileLog.h>

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

using namespace std::chrono_literals;

ReadBufferFromFileLog::ReadBufferFromFileLog(
    const std::vector<String> & log_files_,
    Poco::Logger * log_,
    size_t max_batch_size,
    size_t poll_timeout_,
    ContextPtr context_)
    : ReadBuffer(nullptr, 0)
    , log(log_)
    , batch_size(max_batch_size)
    , poll_timeout(poll_timeout_)
    , context(context_)
    , log_files(log_files_.begin(), log_files_.end())
{
}

void ReadBufferFromFileLog::open()
{
    for (const auto & file : log_files)
        file_status[file].reader = std::ifstream(file);

    wait_task = context->getMessageBrokerSchedulePool().createTask("waitTask", [this] { waitFunc(); });
    wait_task->deactivate();

    select_task = context->getMessageBrokerSchedulePool().createTask("selectTask", [this] { selectFunc(); });
    select_task->activateAndSchedule();

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
    select_task->deactivate();

    for (auto & status : file_status)
        status.second.reader.close();
}

// it do the poll when needed
bool ReadBufferFromFileLog::poll()
{

    if (hasMorePolledRecords())
    {
        allowed = true;
        return true;
    }

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
    size_t need_records_size = batch_size_ - new_records.size();
    size_t read_records_size = 0;
    for (auto & status : file_status)
    {
        while (read_records_size < need_records_size && status.second.reader.good() && !status.second.reader.eof())
        {
            Record record;
            std::getline(status.second.reader, record);
            new_records.emplace_back(record);
            ++read_records_size;
        }
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

void ReadBufferFromFileLog::selectFunc()
{
}
}

