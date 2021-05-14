#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <Functions/FileEncryption.h>

#include <common/logger_useful.h>

namespace DB
{

class ReadEncryptedBuffer : public ReadBufferFromFileBase
{
public:
    explicit ReadEncryptedBuffer(
        size_t buf_size_,
        std::unique_ptr<ReadBufferFromFileBase> in_,
        const EVP_CIPHER * evp_cipher_,
        const InitVector & init_vector_,
        const EncryptionKey & key_,
	const size_t iv_offset_)
        : ReadBufferFromFileBase(buf_size_, nullptr, 0)
        , in(std::move(in_))
        , buf_size(buf_size_)
        , iv(init_vector_)
        , decryptor(Decryptor(init_vector_, key_, evp_cipher_))
        , start_pos(iv_offset_)
	, iv_offset(iv_offset_)
    {
        LOG_WARNING(log, "ReadEncryptedBuffer() buf_size = {}; iv = {}\n", buf_size, iv.Data());
    }

    off_t seek(off_t off, int whence) override
    {
        LOG_WARNING(log, "ReadEncryptedBuffer::seek()");
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
            {
                start_pos = off + getPosition() + iv_offset;
            }
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
            {
                start_pos = off + iv_offset;
            }
        }
        else
            throw Exception("ReadEncryptedBuffer::seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        initialize();
        return start_pos - iv_offset;
    }

    off_t getPosition() override { return bytes + offset(); }

    std::string getFileName() const override { return in->getFileName(); }

private:

    bool nextImpl() override
    {
        LOG_WARNING(log, "ReadEncryptedBuffer::nextImpl()\n");
        if (in->eof())
            return false;

        if (initialized)
            start_pos += working_buffer.size();
        initialize();
        return true;
    }

    void initialize()
    {
        LOG_WARNING(log, "ReadEncryptedBuffer::initialize()\n");
        size_t in_pos = start_pos;

        String data;
	data.resize(buf_size);
        size_t data_size = 0;

        LOG_WARNING(log, "in_pos = {}, expected_size = {}\n", in_pos, buf_size);
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

        LOG_WARNING(log, "read {} bytes : {}\n", data_size, data);
        decryptor.Decrypt(data.data(), working_buffer, data_size);

	pos = working_buffer.begin();
        initialized = true;
    }

    std::unique_ptr<ReadBufferFromFileBase> in;
    size_t buf_size;

    InitVector iv;
    Decryptor decryptor;
    bool initialized = false;
    size_t start_pos = 0;
    size_t iv_offset = 0;
    Poco::Logger * log = &Poco::Logger::get("ReadEncryptedBuffer");
};

}
