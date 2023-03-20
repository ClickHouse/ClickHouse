#include <IO/ReadBufferFromEncryptedFile.h>

#if USE_SSL

namespace DB
{
namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

ReadBufferFromEncryptedFile::ReadBufferFromEncryptedFile(
    size_t buffer_size_,
    std::unique_ptr<ReadBufferFromFileBase> in_,
    const String & key_,
    const FileEncryption::Header & header_,
    size_t offset_)
    : ReadBufferFromFileBase(buffer_size_, nullptr, 0)
    , in(std::move(in_))
    , encrypted_buffer(buffer_size_)
    , encryptor(header_.algorithm, key_, header_.init_vector)
{
    offset = offset_;
    encryptor.setOffset(offset_);
    need_seek = true;
}

off_t ReadBufferFromEncryptedFile::seek(off_t off, int whence)
{
    off_t new_pos;
    if (whence == SEEK_SET)
    {
        if (off < 0)
            throw Exception("SEEK_SET underflow: off = " + std::to_string(off), ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        new_pos = off;
    }
    else if (whence == SEEK_CUR)
    {
        if (off < 0 && -off > getPosition())
            throw Exception("SEEK_CUR shift out of bounds", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        new_pos = getPosition() + off;
    }
    else
        throw Exception("ReadBufferFromFileEncrypted::seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

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

    /// The encryptor always needs to know what the current offset is.
    encryptor.setOffset(new_pos);

    return new_pos;
}

off_t ReadBufferFromEncryptedFile::getPosition()
{
    return offset - available();
}

bool ReadBufferFromEncryptedFile::nextImpl()
{
    if (need_seek)
    {
        off_t raw_offset = offset + FileEncryption::Header::kSize;
        if (in->seek(raw_offset, SEEK_SET) != raw_offset)
            return false;
        need_seek = false;
    }

    if (in->eof())
        return false;

    /// Read up to the size of `encrypted_buffer`.
    size_t bytes_read = 0;
    while (bytes_read < encrypted_buffer.size() && !in->eof())
    {
        bytes_read += in->read(encrypted_buffer.data() + bytes_read, encrypted_buffer.size() - bytes_read);
    }

    /// The used cipher algorithms generate the same number of bytes in output as it were in input,
    /// so after deciphering the numbers of bytes will be still `bytes_read`.
    working_buffer.resize(bytes_read);
    encryptor.decrypt(encrypted_buffer.data(), bytes_read, working_buffer.begin());

    pos = working_buffer.begin();
    return true;
}

}

#endif
