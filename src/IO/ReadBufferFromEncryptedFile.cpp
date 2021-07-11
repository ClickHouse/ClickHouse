#include <IO/ReadBufferFromEncryptedFile.h>

#if USE_SSL

namespace DB
{
namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

ReadBufferFromEncryptedFile::ReadBufferFromEncryptedFile(
    size_t buf_size_,
    std::unique_ptr<ReadBufferFromFileBase> in_,
    const String & init_vector_,
    const FileEncryption::EncryptionKey & key_,
    const size_t iv_offset_)
    : ReadBufferFromFileBase(buf_size_, nullptr, 0)
    , in(std::move(in_))
    , buf_size(buf_size_)
    , decryptor(FileEncryption::Decryptor(init_vector_, key_))
    , iv_offset(iv_offset_)
{
}

off_t ReadBufferFromEncryptedFile::seek(off_t off, int whence)
{
    if (whence == SEEK_CUR)
    {
        if (off < 0 && -off > getPosition())
            throw Exception("SEEK_CUR shift out of bounds", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (!working_buffer.empty() && static_cast<size_t>(offset() + off) < working_buffer.size())
        {
            pos += off;
            return getPosition();
        }
        else
            start_pos = off + getPosition();
    }
    else if (whence == SEEK_SET)
    {
        if (off < 0)
            throw Exception("SEEK_SET underflow: off = " + std::to_string(off), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (!working_buffer.empty() && static_cast<size_t>(off) >= start_pos
            && static_cast<size_t>(off) < (start_pos + working_buffer.size()))
        {
            pos = working_buffer.begin() + (off - start_pos);
            return getPosition();
        }
        else
            start_pos = off;
    }
    else
        throw Exception("ReadBufferFromEncryptedFile::seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    initialize();
    return start_pos;
}

bool ReadBufferFromEncryptedFile::nextImpl()
{
    if (in->eof())
        return false;

    if (initialized)
        start_pos += working_buffer.size();
    initialize();
    return true;
}

void ReadBufferFromEncryptedFile::initialize()
{
    size_t in_pos = start_pos + iv_offset;

    String data;
    data.resize(buf_size);
    size_t data_size = 0;

    in->seek(in_pos, SEEK_SET);
    while (data_size < buf_size && !in->eof())
    {
        auto size = in->read(data.data() + data_size, buf_size - data_size);
        data_size += size;
        in_pos += size;
        in->seek(in_pos, SEEK_SET);
    }

    data.resize(data_size);
    working_buffer.resize(data_size);

    decryptor.decrypt(data.data(), working_buffer.begin(), data_size, start_pos);

    pos = working_buffer.begin();
    initialized = true;
}

}

#endif
