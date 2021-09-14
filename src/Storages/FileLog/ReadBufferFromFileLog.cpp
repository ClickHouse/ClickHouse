#include <Interpreters/Context.h>
#include <Storages/FileLog/ReadBufferFromFileLog.h>
#include <Common/Stopwatch.h>

#include <common/logger_useful.h>

#include <algorithm>
#include <filesystem>
#include <boost/algorithm/string/join.hpp>

namespace DB
{

ReadBufferFromFileLog::ReadBufferFromFileLog(
    StorageFileLog & storage_,
    size_t max_batch_size,
    size_t poll_timeout_,
    ContextPtr context_,
    size_t stream_number_,
    size_t max_streams_number_)
    : ReadBuffer(nullptr, 0)
    , log(&Poco::Logger::get("ReadBufferFromFileLog " + toString(stream_number)))
    , storage(storage_)
    , batch_size(max_batch_size)
    , poll_timeout(poll_timeout_)
    , context(context_)
    , stream_number(stream_number_)
    , max_streams_number(max_streams_number_)
{
    cleanUnprocessed();
    allowed = false;
}

void ReadBufferFromFileLog::cleanUnprocessed()
{
    records.clear();
    current = records.begin();
    BufferBase::set(nullptr, 0, 0);
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

        buffer_status = BufferStatus::POLLED_OK;
        allowed = true;
        return true;
    }


}

ReadBufferFromFileLog::Records ReadBufferFromFileLog::pollBatch(size_t batch_size_)
{
    Records new_records;
    new_records.reserve(batch_size_);

    readNewRecords(new_records, batch_size);
    if (new_records.size() == batch_size_ || stream_out)
        return new_records;

    Stopwatch watch;
    while (watch.elapsedMilliseconds() < poll_timeout && new_records.size() != batch_size_)
    {
        readNewRecords(new_records, batch_size);
        /// All ifstrem reach end, no need to wait for timeout,
        /// since file status can not be updated during a streamToViews
        if (stream_out)
            break;
    }

    return new_records;
}

void ReadBufferFromFileLog::readNewRecords(ReadBufferFromFileLog::Records & new_records, size_t batch_size_)
{
    size_t need_records_size = batch_size_ - new_records.size();
    size_t read_records_size = 0;

    const auto & file_names = storage.getFileNames();
    auto & file_status = storage.getFileStatus();

    size_t files_per_stream = file_names.size() / max_streams_number;
    size_t start = stream_number * files_per_stream;
    size_t end = stream_number == max_streams_number - 1 ? file_names.size() : (stream_number + 1) * files_per_stream;

    for (size_t i = start; i < end; ++i)
    {
        auto & file = file_status[file_names[i]];
        if (file.status == StorageFileLog::FileStatus::NO_CHANGE)
            continue;

        auto reader = std::ifstream(file_names[i]);

        reader.seekg(0, reader.end);
        auto stream_end = reader.tellg();

        reader.seekg(file.last_read_position);

        while (read_records_size < need_records_size && reader.tellg() < stream_end)
        {
            Record record;
            std::getline(reader, record);
            new_records.emplace_back(record);
            ++read_records_size;
        }

        file.last_read_position = reader.tellg();

        if (reader.tellg() == stream_end)
        {
            file.status = StorageFileLog::FileStatus::NO_CHANGE;
            /// All ifstream reach end
            if (i == end - 1)
            {
                stream_out = true;
            }
        }

        if (read_records_size == need_records_size)
        {
            break;
        }
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

}
