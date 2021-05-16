#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Functions/FileEncryption.h>

#include <common/logger_useful.h>

namespace DB
{

using namespace FileEncryption;

class ReadEncryptedBuffer : public ReadBufferFromFileBase
{
public:
    explicit ReadEncryptedBuffer(
        size_t buf_size_,
        std::unique_ptr<ReadBufferFromFileBase> in_,
        const String & init_vector_,
        const EncryptionKey & key_,
	const size_t iv_offset_)
        : ReadBufferFromFileBase(buf_size_, nullptr, 0)
        , in(std::move(in_))
        , buf_size(buf_size_)
        , decryptor(Decryptor(init_vector_, key_))
        , start_pos(iv_offset_)
	, iv_offset(iv_offset_)
    { }

    off_t seek(off_t off, int whence) override
    {
        if (whence == SEEK_CUR)
        {
            if (off < 0 && -off > getPosition())
                throw Exception("SEEK_CUR shift out of bounds", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

            if (!working_buffer.empty() && size_t(offset() + off) < working_buffer.size())
            {
                pos += off;
                return getPosition();
            }
            else
                start_pos = off + getPosition() + iv_offset;
        }
        else if (whence == SEEK_SET)
        {
            if (off < 0)
                throw Exception("SEEK_SET underflow", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

            if (!working_buffer.empty() && size_t(off) >= start_pos - working_buffer.size()
                && size_t(off) < start_pos)
            {
                pos = working_buffer.end() - (start_pos - off);
                return getPosition();
            }
            else
                start_pos = off + iv_offset;
        }
        else
            throw Exception("ReadEncryptedBuffer::seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        initialize();
        return start_pos - iv_offset;
    }

    off_t getPosition() override { return start_pos + offset() - iv_offset; }

    std::string getFileName() const override { return in->getFileName(); }

private:

    bool nextImpl() override
    {
        if (in->eof())
            return false;

        if (initialized)
            start_pos += working_buffer.size();
        initialize();
        return true;
    }

    void initialize()
    {
        size_t in_pos = start_pos;

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

        decryptor.Decrypt(data.data(), working_buffer.begin(), data_size, start_pos - iv_offset);

	pos = working_buffer.begin();
        initialized = true;
    }

    std::unique_ptr<ReadBufferFromFileBase> in;
    size_t buf_size;

    Decryptor decryptor;
    bool initialized = false;
    size_t start_pos = 0;
    size_t iv_offset = 0;
};

}
