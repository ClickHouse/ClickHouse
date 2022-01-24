#include <Interpreters/Context.h>
#include <Storages/FileLog/ReadBufferFromFileLog.h>
#include <Common/Stopwatch.h>

#include <base/logger_useful.h>

#include <algorithm>
#include <filesystem>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
}

ReadBufferFromFileLog::ReadBufferFromFileLog(
    StorageFileLog & storage_,
    size_t max_batch_size,
    size_t poll_timeout_,
    ContextPtr context_,
    size_t stream_number_,
    size_t max_streams_number_)
    : ReadBuffer(nullptr, 0)
    , log(&Poco::Logger::get("ReadBufferFromFileLog " + toString(stream_number_)))
    , storage(storage_)
    , batch_size(max_batch_size)
    , poll_timeout(poll_timeout_)
    , context(context_)
    , stream_number(stream_number_)
    , max_streams_number(max_streams_number_)
{
    current = records.begin();
    allowed = false;
}

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
        buffer_status = BufferStatus::NO_RECORD_RETURNED;
        LOG_TRACE(log, "No new records to read");
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

    auto & file_infos = storage.getFileInfos();

    size_t files_per_stream = file_infos.file_names.size() / max_streams_number;
    size_t start = stream_number * files_per_stream;
    size_t end = stream_number == max_streams_number - 1 ? file_infos.file_names.size() : (stream_number + 1) * files_per_stream;

    for (size_t i = start; i < end; ++i)
    {
        const auto & file_name = file_infos.file_names[i];

        auto & file_ctx = StorageFileLog::findInMap(file_infos.context_by_name, file_name);
        if (file_ctx.status == StorageFileLog::FileStatus::NO_CHANGE)
            continue;

        auto & file_meta = StorageFileLog::findInMap(file_infos.meta_by_inode, file_ctx.inode);

        if (!file_ctx.reader)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Ifstream for file {} is not initialized", file_meta.file_name);

        auto & reader = file_ctx.reader.value();
        StorageFileLog::assertStreamGood(reader);

        Record record;
        while (read_records_size < need_records_size)
        {
            /// Need to get offset before reading record from stream
            auto offset = reader.tellg();
            if (static_cast<UInt64>(offset) >= file_meta.last_open_end)
                break;
            record.offset = offset;
            StorageFileLog::assertStreamGood(reader);

            record.file_name = file_name;


            std::getline(reader, record.data);
            StorageFileLog::assertStreamGood(reader);

            new_records.emplace_back(record);
            ++read_records_size;
        }

        UInt64 current_position = reader.tellg();
        StorageFileLog::assertStreamGood(reader);

        file_meta.last_writen_position = current_position;

        /// stream reach to end
        if (current_position == file_meta.last_open_end)
        {
            file_ctx.status = StorageFileLog::FileStatus::NO_CHANGE;
        }

        /// All ifstream reach end
        if (i == end - 1 && (file_ctx.status == StorageFileLog::FileStatus::NO_CHANGE))
        {
            stream_out = true;
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

    auto * new_position = const_cast<char *>(current->data.data());
    BufferBase::set(new_position, current->data.size(), 0);
    allowed = false;

    current_file = current->file_name;
    current_offset = current->offset;

    ++current;

    return true;
}

}
