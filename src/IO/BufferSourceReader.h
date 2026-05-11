#pragma once

#include <IO/ISourceReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/logger_useful.h>

#include <functional>

namespace DB
{

/// ISourceReader adapter for any source that produces a ReadBufferFromFileBase.
/// Used for BackupSource, CustomSource, and other non-standard sources.
/// Creates a buffer per read call, then uses seek + read.
class BufferSourceReader : public ISourceReader
{
public:
    using BufferFactory = std::function<std::unique_ptr<ReadBufferFromFileBase>(const StoredObject & object)>;

    explicit BufferSourceReader(BufferFactory factory_, String name_ = "BufferSource")
        : factory(std::move(factory_))
        , source_name(std::move(name_))
    {
    }

    size_t read(
        const StoredObject & object,
        size_t offset, size_t size,
        char * buffer) override
    {
        LOG_TRACE(log, "read: object={}, offset={}, size={}", object.remote_path, offset, size);

        auto buf = factory(object);

        if (buf->supportsReadAt())
        {
            size_t bytes_read = buf->readBigAt(buffer, size, offset, {});
            LOG_TRACE(log, "read: readBigAt got {} bytes", bytes_read);
            return bytes_read;
        }

        buf->seek(offset, SEEK_SET);

        size_t total_read = 0;
        while (total_read < size)
        {
            size_t remaining = size - total_read;
            size_t bytes = buf->read(buffer + total_read, remaining);
            if (bytes == 0)
                break;
            total_read += bytes;
        }

        LOG_TRACE(log, "read: seek+read got {} bytes", total_read);
        return total_read;
    }

    String name() const override { return source_name; }

private:
    BufferFactory factory;
    String source_name;
    LoggerPtr log = getLogger("BufferSourceReader");
};

}
