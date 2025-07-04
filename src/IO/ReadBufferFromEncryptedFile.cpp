#include <IO/ReadBufferFromEncryptedFile.h>
#include "Common/JemallocNodumpSTLAllocator.h"

#if USE_SSL
#include <Common/logger_useful.h>
#include <base/demangle.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
}

ReadBufferFromEncryptedFile::ReadBufferFromEncryptedFile(
    const String & file_name_,
    size_t buffer_size_,
    std::unique_ptr<ReadBufferFromFileBase> in_,
    const NoDumpString & key_,
    const FileEncryption::Header & header_,
    size_t offset_)
    : ReadBufferFromFileBase(buffer_size_, nullptr, 0)
    , file_name(file_name_)
    , in(std::move(in_))
    , encrypted_buffer(buffer_size_)
    , encryptor(header_.algorithm, key_, header_.init_vector)
    , log(getLogger("ReadBufferFromEncryptedFile"))
{
    offset = offset_;
    need_seek = true;
    LOG_TEST(log, "Decrypting {}: version={}, algorithm={}", file_name, header_.version, toString(header_.algorithm));
}

std::optional<size_t> ReadBufferFromEncryptedFile::tryGetFileSize()
{
    auto file_size = in->tryGetFileSize();
    if (!file_size || (*file_size < FileEncryption::Header::kSize))
        return {};
    return *file_size - FileEncryption::Header::kSize;
}

off_t ReadBufferFromEncryptedFile::seek(off_t off, int whence)
{
    off_t old_pos = getPosition();

    off_t new_pos;
    if (whence == SEEK_SET)
    {
        if (off < 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "SEEK_SET underflow: off = {}", off);
        new_pos = off;
    }
    else if (whence == SEEK_CUR)
    {
        if (off < 0 && -off > old_pos)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "SEEK_CUR shift out of bounds");
        new_pos = old_pos + off;
    }
    else
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "ReadBufferFromFileEncrypted::seek expects SEEK_SET or SEEK_CUR as whence");

    if (read_until_position && new_pos > *read_until_position)
        new_pos = *read_until_position;

    if ((offset - static_cast<off_t>(working_buffer.size()) <= new_pos) && (new_pos <= offset) && !need_seek)
    {
        /// Position is still inside buffer.
        pos = working_buffer.end() - offset + new_pos;
        assert(pos >= working_buffer.begin());
        assert(pos <= working_buffer.end());
    }
    else
    {
        need_seek = true;
        offset = new_pos;

        LOG_TEST(log, "Seek to position {} (old_pos = {}) in {}", new_pos, old_pos, getFileName());

        /// No more reading from the current working buffer until next() is called.
        resetWorkingBuffer();
        assert(!hasPendingData());
    }

    return new_pos;
}

off_t ReadBufferFromEncryptedFile::getPosition()
{
    return offset - available();
}

void ReadBufferFromEncryptedFile::setReadUntilPosition(size_t position)
{
    if (read_until_position == position)
        return;

    if (static_cast<off_t>(position) < getPosition())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Attempt to set read_until_position to {} before already read data (position: {}) while reading {}",
                        position, getPosition(), getFileName());
    }

    read_until_position = position;
    need_set_read_until_position = true;

    if (static_cast<off_t>(position) < offset)
    {
        working_buffer.resize(working_buffer.size() - (offset - position));
        offset = position;
        need_seek = true;
    }
}

void ReadBufferFromEncryptedFile::setReadUntilEnd()
{
    if (!read_until_position)
        return;

    read_until_position.reset();
    need_set_read_until_position = true;
}

bool ReadBufferFromEncryptedFile::nextImpl()
{
    if (need_seek || need_set_read_until_position)
        performSeekAndSetReadUntilPosition();

    if (in->eof())
        return false;

    /// We check the current file position in the inner buffer because it is used in the decryption algorithm.
    /// Using a wrong file position could give a completely wrong byte sequence and produce very weird errors,
    /// so it's better to check it.
    auto in_position = in->getPosition();
    if (in_position != static_cast<off_t>(offset + FileEncryption::Header::kSize))
    {
        const auto & in_ref = *in;
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ReadBufferFromEncryptedFile: Wrong file position {} (expected: {}) in the inner buffer {} while reading {}",
            in_position, offset + FileEncryption::Header::kSize, demangle(typeid(in_ref).name()), getFileName());
    }

    size_t bytes_to_read = std::min(encrypted_buffer.size(), internal_buffer.size());
    if (read_until_position)
    {
        if (offset > *read_until_position)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Attempt to read after `read_until_position` ({}) (position: {}) while reading {}",
                            *read_until_position, offset, getFileName());
        }
        bytes_to_read = std::min(bytes_to_read, static_cast<size_t>(*read_until_position - offset));
    }

    /// Read up to the size of `encrypted_buffer`.
    size_t count = 0;
    while (bytes_to_read && !in->eof())
    {
        count += in->read(encrypted_buffer.data() + count, bytes_to_read);
        bytes_to_read -= count;
    }

    if (!count)
        return false;

    LOG_TEST(log, "Decrypting bytes {}..{} from {}", offset, offset + count - 1, getFileName());

    /// The used cipher algorithms generate the same number of bytes in output as it were in input,
    /// so after deciphering the numbers of bytes will be still `count`.
    working_buffer.resize(count);

    /// The decryptor needs to know what the current offset is (because it's used in the decryption algorithm).
    encryptor.setOffset(offset);

    encryptor.decrypt(encrypted_buffer.data(), count, working_buffer.begin());

    offset += count;
    pos = working_buffer.begin();
    return true;
}

void ReadBufferFromEncryptedFile::performSeekAndSetReadUntilPosition()
{
    std::optional<off_t> in_position;
    std::optional<off_t> new_in_position;
    if (need_seek)
    {
        in_position = in->getPosition();
        new_in_position = offset + FileEncryption::Header::kSize;
    }

    auto do_seek = [&]
    {
        offset = in->seek(*new_in_position, SEEK_SET) - FileEncryption::Header::kSize;
        need_seek = false;
    };

    /// If we seek backward, then first we seek and then we apply new `read_until_position`.
    /// If we seek forward, then first we apply new `read_until_position` and then we seek.
    /// That is so because the current position in the internal buffer must be always less than `read_until_position`

    if (need_seek && *new_in_position < *in_position) /// Seek backward.
        do_seek();

    if (need_set_read_until_position)
    {
        if (read_until_position)
            in->setReadUntilPosition(*read_until_position + FileEncryption::Header::kSize);
        else
            in->setReadUntilEnd();
        need_set_read_until_position = false;
    }

    if (need_seek && *new_in_position > *in_position) /// Seek forward.
        do_seek();
}

void ReadBufferFromEncryptedFile::prefetch(Priority priority)
{
    in->prefetch(priority);
}

}

#endif
