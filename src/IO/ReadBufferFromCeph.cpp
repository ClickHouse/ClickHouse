#include "ReadBufferFromCeph.h"
#include <memory>
#include <optional>
#include "Common/logger_useful.h"

#if USE_CEPH
#    include <IO/Ceph/RadosIO.h>
#    include <rados/librados.hpp>
#    include <Common/ElapsedTimeProfileEventIncrement.h>
#    include <Common/Exception.h>
#    include <Common/Scheduler/ResourceGuard.h>
#    include <Common/Throttler.h>
#    include <Common/safe_cast.h>

namespace ProfileEvents
{
extern const Event ReadBufferFromCephMicroseconds;
extern const Event ReadBufferFromCephBytes;
extern const Event ReadBufferFromCephRequestsErrors;
extern const Event ReadBufferFromCephInitMicroseconds;
extern const Event RemoteReadThrottlerBytes;
extern const Event RemoteReadThrottlerSleepMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
extern const int CEPH_ERROR;
extern const int NETWORK_ERROR;
extern const int LOGICAL_ERROR;
extern const int CANNOT_SEEK_THROUGH_FILE;
extern const int SEEK_POSITION_OUT_OF_BOUND;
}

struct ReadBufferFromCeph::Impl : public BufferWithOwnMemory<SeekableReadBuffer>
{
    std::shared_ptr<Ceph::RadosIO> io;
    String object_id;
    ReadSettings read_settings;

    off_t file_offset = 0;
    off_t read_until_position = 0;

    std::optional<size_t> file_size;

    LoggerPtr log = getLogger("ReadBufferFromCeph::Impl");

    Impl(
        std::shared_ptr<Ceph::RadosIO> io_,
        const String & object_id_,
        const ReadSettings & read_settings_,
        size_t offset_,
        size_t read_until_position_,
        bool use_external_buffer_)
        : BufferWithOwnMemory<SeekableReadBuffer>(use_external_buffer_ ? 0 : read_settings_.remote_fs_buffer_size)
        , io(std::move(io_))
        , object_id(object_id_)
        , read_settings(read_settings_)
        , file_offset(offset_)
        , read_until_position(read_until_position_)
    {
        io->connect();
    }

    size_t readImpl(char * to, size_t len, off_t begin) const
    {
        ResourceGuard rlock(read_settings.resource_link, len);
        size_t bytes_read = 0;
        try
        {
            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ReadBufferFromCephMicroseconds);
            bytes_read = io->read(object_id, to, len, begin);
            if (read_settings.remote_throttler && bytes_read)
                read_settings.remote_throttler->add(
                    bytes_read, ProfileEvents::RemoteReadThrottlerBytes, ProfileEvents::RemoteReadThrottlerSleepMicroseconds);
        }
        catch (...)
        {
            read_settings.resource_link.accumulate(len); // We assume no resource was used in case of failure
            throw;
        }

        read_settings.resource_link.adjust(len, bytes_read);
        ProfileEvents::increment(ProfileEvents::ReadBufferFromCephBytes, bytes_read);
        return bytes_read;
    }

    size_t getFileSize()
    {
        if (file_size)
            return *file_size;
        size_t psize;
        io->stat(object_id, &psize, nullptr);
        file_size = psize;
        return psize;
    }

    bool nextImpl() override
    {
        size_t num_bytes_to_read;
        if (read_until_position)
        {
            if (read_until_position == file_offset)
            {
                return false;
            }

            if (read_until_position < file_offset)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", file_offset, read_until_position - 1);

            num_bytes_to_read = std::min<size_t>(read_until_position - file_offset, internal_buffer.size());
        }
        else
        {
            num_bytes_to_read = internal_buffer.size();
        }

        LOG_DEBUG(log, "Reading {} bytes from offset {}", num_bytes_to_read, file_offset);
        auto bytes_read = readImpl(internal_buffer.begin(), num_bytes_to_read, file_offset);
        LOG_DEBUG(log, "Read {} bytes from offset {}", bytes_read, file_offset);
        if (bytes_read)
        {
            working_buffer = internal_buffer;
            working_buffer.resize(bytes_read);
            file_offset += bytes_read;
            return true;
        }
        LOG_DEBUG(log, "No more data to read from offset {}", file_offset);
        return false;
    }

    off_t seek(off_t offset_, int whence) override
    {
        if (whence != SEEK_SET)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Only SEEK_SET is supported");

        file_offset = offset_;
        resetWorkingBuffer();
        return file_offset;
    }

    off_t getPosition() override { return file_offset; }

    size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & /*progress_callback*/) const override
    {
        size_t total_read = 0;
        while (n > 0)
        {
            auto bytes_copied = readImpl(to, n, range_begin);
            if (bytes_copied == 0)
                break;
            total_read += bytes_copied;
            range_begin += bytes_copied;
            to += bytes_copied;
            n -= bytes_copied;
        }
        return total_read;
    }
};

ReadBufferFromCeph::ReadBufferFromCeph(
    std::shared_ptr<librados::Rados> rados_,
    const String & pool,
    const String & nspace,
    const String & object_id_,
    const ReadSettings & read_settings_,
    bool use_external_buffer_,
    size_t offset_,
    size_t read_until_position_,
    std::optional<size_t> file_size_)
    : ReadBufferFromFileBase(read_settings_.remote_fs_buffer_size, nullptr, 0, file_size_), use_external_buffer(use_external_buffer_)
{
    auto io = std::make_shared<Ceph::RadosIO>(rados_, pool, nspace);
    impl = std::make_unique<ReadBufferFromCeph::Impl>(
        std::move(io), object_id_, read_settings_, offset_, read_until_position_, use_external_buffer);
}

ReadBufferFromCeph::ReadBufferFromCeph(
    std::shared_ptr<Ceph::RadosIO> io_,
    const String & object_id_,
    const ReadSettings & read_settings_,
    bool use_external_buffer_,
    size_t offset_,
    size_t read_until_position_,
    std::optional<size_t> file_size_)
    : ReadBufferFromFileBase(read_settings_.remote_fs_buffer_size, nullptr, 0, file_size_)
    , impl(std::make_unique<ReadBufferFromCeph::Impl>(
          std::move(io_), object_id_, read_settings_, offset_, read_until_position_, use_external_buffer_))
    , use_external_buffer(use_external_buffer_)
{
}

ReadBufferFromCeph::~ReadBufferFromCeph() = default;

bool ReadBufferFromCeph::nextImpl()
{
    if (use_external_buffer)
    {
        impl->set(internal_buffer.begin(), internal_buffer.size());
        assert(working_buffer.begin() != nullptr);
        assert(!internal_buffer.empty());
    }
    else
    {
        impl->position() = impl->buffer().begin() + offset();
        assert(!impl->hasPendingData());
    }

    auto result = impl->next();

    if (result)
        BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset()); /// use the buffer returned by `impl`

    return result;
}

off_t ReadBufferFromCeph::seek(off_t offset_, int whence)
{
    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed.");

    if (offset_ < 0)
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}", offset_);

    if (!working_buffer.empty() && size_t(offset_) >= impl->getPosition() - working_buffer.size() && offset_ < impl->getPosition())
    {
        pos = working_buffer.end() - (impl->getPosition() - offset_);
        assert(pos >= working_buffer.begin());
        assert(pos <= working_buffer.end());

        return getPosition();
    }

    resetWorkingBuffer();
    impl->seek(offset_, whence);
    return impl->getPosition();
}

size_t ReadBufferFromCeph::getFileSize()
{
    if (file_size)
        return *file_size;
    return impl->getFileSize();
}

off_t ReadBufferFromCeph::getPosition()
{
    return impl->getPosition() - available();
}

size_t ReadBufferFromCeph::getFileOffsetOfBufferEnd() const
{
    return impl->getPosition();
}

String ReadBufferFromCeph::getFileName() const
{
    return impl->object_id;
}

size_t ReadBufferFromCeph::readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) const
{
    return impl->readBigAt(to, n, range_begin, progress_callback);
}

}

#endif
