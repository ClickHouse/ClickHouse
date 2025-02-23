#include <IO/ReadBufferFromEncryptedFile.h>

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
    const String & key_,
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
                        "Attempt to set read_until_position to {} before already read data (info: {})",
                        position, getInfoForLog());
    }

    read_until_position = position;
    need_set_read_until_position = true;

    if (static_cast<off_t>(position) < offset)
    {
        size_t delta = offset - position;
        working_buffer.resize(working_buffer.size() - delta);
        offset -= delta;
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
        performSeekAndApplyReadUntilPosition();

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
                        "ReadBufferFromEncryptedFile: Wrong file position {} in the inner buffer (expected: {}, info: {})",
                        in_position, offset + FileEncryption::Header::kSize, getInfoForLog());
    }

    size_t bytes_to_read = std::min(encrypted_buffer.size(), internal_buffer.size());
    if (read_until_position)
    {
        if (offset > *read_until_position)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read after `read_until_position` (info: {})", getInfoForLog());
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

void ReadBufferFromEncryptedFile::performSeekAndApplyReadUntilPosition()
{
    bool seek_forward = false;
    bool seek_backward = false;
    off_t new_in_position = offset + FileEncryption::Header::kSize;

    if (need_seek)
    {
        auto in_position = in->getPosition();
        seek_forward = (new_in_position > in_position);
        seek_backward = (new_in_position < in_position);
    }

    auto do_seek = [&]
    {
        offset = in->seek(new_in_position, SEEK_SET) - FileEncryption::Header::kSize;
        need_seek = false;
    };

    if (seek_backward)
        do_seek();

    if (need_set_read_until_position)
    {
        if (read_until_position)
            in->setReadUntilPosition(*read_until_position + FileEncryption::Header::kSize);
        else
            in->setReadUntilEnd();
        need_set_read_until_position = false;
    }

    if (seek_forward)
        do_seek();
}

String ReadBufferFromEncryptedFile::getInfoForLog()
{
    return fmt::format(
        "ReadBufferFromEncryptedFile(file_name: {}, encryption_algorithm: {}, "
        "position: {}, file_offset_of_buffer_end: {}, read_until_position: {}, impl: {})",
        getFileName(), encryptor.getAlgorithm(),
        getPosition(), offset, read_until_position ? std::to_string(*read_until_position) : "end", in->getInfoForLog());
}

void ReadBufferFromEncryptedFile::prefetch(Priority priority)
{
    in->prefetch(priority);
}

}

#endif
