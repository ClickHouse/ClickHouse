#include <Storages/SFTP/ReadBufferFromSFTP.h>

#include <IO/Progress.h>
#include <Common/Throttler.h>
#include <Common/safe_cast.h>

#include <Common/Scheduler/ResourceGuard.h>
#include <Storages/SFTP/SSHWrapper.h>

#include <mutex>


namespace ProfileEvents {
    extern const Event RemoteReadThrottlerBytes;
    extern const Event RemoteReadThrottlerSleepMicroseconds;
}

namespace DB
{

namespace ErrorCodes {
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}


struct ReadBufferFromSFTP::ReadBufferFromSFTPImpl : public BufferWithOwnMemory<SeekableReadBuffer> {
    std::shared_ptr<SFTPWrapper> client;

    String file_path;
    SFTPWrapper::FileStream fin;

    ReadSettings read_settings;

    off_t file_offset = 0;
    off_t read_until_position = 0;
    off_t file_size;

    explicit ReadBufferFromSFTPImpl(
            const std::shared_ptr<SFTPWrapper> &client_,
            const std::string &file_path_,
            const ReadSettings &read_settings_,
            size_t read_until_position_,
            bool use_external_buffer_)
            : BufferWithOwnMemory<SeekableReadBuffer>(
            use_external_buffer_ ? 0 : read_settings_.remote_fs_buffer_size)
            ,client(client_)
            ,file_path(file_path_)
            ,fin(client_->openFile(file_path, O_RDONLY))
            ,read_settings(read_settings_)
            ,read_until_position(read_until_position_) {
        auto attributes = fin.getAttributes();
        file_size = static_cast<size_t>(attributes.getSize());
    }

    size_t getFileSize() const {
        return file_size;
    }

    bool nextImpl() override {
        size_t num_bytes_to_read;
        if (read_until_position) {
            if (read_until_position == file_offset)
                return false;

            if (read_until_position < file_offset)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Attempt to read beyond right offset ({} > {})", file_offset,
                                read_until_position - 1);

            num_bytes_to_read = std::min<size_t>(read_until_position - file_offset, internal_buffer.size());
        } else {
            num_bytes_to_read = internal_buffer.size();
        }
        if (file_size != 0 && file_offset >= file_size) {
            return false;
        }

        ResourceGuard rlock(read_settings.resource_link, num_bytes_to_read);
        off_t bytes_read;
        try {
            bytes_read = fin.read(internal_buffer.begin(), num_bytes_to_read);
        }
        catch (...) {
            read_settings.resource_link.accumulate(
                    num_bytes_to_read); // We assume no resource was used in case of failure
            throw;
        }
        rlock.unlock();

        read_settings.resource_link.adjust(num_bytes_to_read, bytes_read);

        if (bytes_read) {
            working_buffer = internal_buffer;
            working_buffer.
                    resize(bytes_read);
            file_offset +=
                    bytes_read;
            if (read_settings.remote_throttler)
                read_settings.remote_throttler->
                        add(bytes_read, ProfileEvents::RemoteReadThrottlerBytes,
                            ProfileEvents::RemoteReadThrottlerSleepMicroseconds
                );
            return true;
        }

        return false;
    }

    off_t seek(off_t file_offset_, int whence) override {
        if (whence != SEEK_SET)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Only SEEK_SET is supported");

        fin.seek(file_offset_);
        file_offset = file_offset_;

        resetWorkingBuffer();

        return file_offset;
    }

    off_t getPosition() override {
        return file_offset;
    }
};

ReadBufferFromSFTP::ReadBufferFromSFTP(
    const std::shared_ptr<SFTPWrapper> &client_,
    const String &file_path_,
    const ReadSettings &read_settings_,
    size_t read_until_position_,
    bool use_external_buffer_)
    : ReadBufferFromFileBase(read_settings_.remote_fs_buffer_size, nullptr, 0)
    , impl(std::make_unique<ReadBufferFromSFTPImpl>(
        client_, file_path_, read_settings_, read_until_position_, use_external_buffer_)),
    use_external_buffer(use_external_buffer_)
{}

ReadBufferFromSFTP::~ReadBufferFromSFTP() = default;

size_t ReadBufferFromSFTP::getFileSize() {
    return impl->getFileSize();
}

bool ReadBufferFromSFTP::nextImpl() {
    if (use_external_buffer) {
        impl->set(internal_buffer.begin(), internal_buffer.size());
        assert(working_buffer.begin() != nullptr);
        assert(!internal_buffer.empty());
    } else {
        impl->position() = impl->buffer().begin() + offset();
        assert(!impl->hasPendingData());
    }

    auto result = impl->next();

    if (result)
        BufferBase::set(impl->buffer().begin(), impl->buffer().size(),
                        impl->offset()); /// use the buffer returned by `impl`

    return result;
}


off_t ReadBufferFromSFTP::seek(off_t offset_, int whence) {
    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed.");

    if (offset_ < 0)
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}",
                        offset_);

    if (!working_buffer.empty()
        && size_t(offset_) >= impl->getPosition() - working_buffer.size()
        && offset_ < impl->getPosition()) {
        pos = working_buffer.end() - (impl->getPosition() - offset_);
        assert(pos >= working_buffer.begin());
        assert(pos <= working_buffer.end());

        return getPosition();
    }

    resetWorkingBuffer();
    impl->seek(offset_, whence);
    return impl->getPosition();
}


off_t ReadBufferFromSFTP::getPosition() {
    return impl->getPosition() - available();
}

size_t ReadBufferFromSFTP::getFileOffsetOfBufferEnd() const {
    return impl->getPosition();
}

String ReadBufferFromSFTP::getFileName() const {
    return impl->file_path;
}

}
