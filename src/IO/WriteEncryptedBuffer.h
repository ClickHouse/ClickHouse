#pragma once

#include <IO/WriteBufferFromFileBase.h>
#include <Functions/FileEncryption.h>

#include <common/logger_useful.h>

namespace DB
{

class WriteEncryptedBuffer : public WriteBufferFromFileBase
{
public:
    WriteEncryptedBuffer(
        size_t buf_size_,
        std::unique_ptr<WriteBufferFromFileBase> out_,
        const EVP_CIPHER * evp_cipher_,
        const InitVector & init_vector_,
        const EncryptionKey & key_,
        const size_t & file_size)
        : WriteBufferFromFileBase(buf_size_, nullptr, 0)
        , out(std::move(out_))
        , flush_iv(!file_size)
        , iv(init_vector_)
        , encryptor(Encryptor(init_vector_, key_, evp_cipher_, file_size))
    { }

    ~WriteEncryptedBuffer() override
    {
        LOG_WARNING(log, "~WriteEncryptedBuffer()");
        try
        {
            finalize();
	}
	catch (...)
	{
            tryLogCurrentException(__PRETTY_FUNCTION__);
	}
    }

    void finalize() override
    {
        LOG_WARNING(log, "WriteEncryptedBuffer::finalize()");
        if (finalized)
	    return;

        LOG_WARNING(log, "WriteEncryptedBuffer::next()");
        next();
        LOG_WARNING(log, "WriteEncryptedBuffer::out::finalize()");
        out->finalize();

	finalized = true;
    }

    void sync() override
    {
        LOG_WARNING(log, "WriteEncryptedBuffer::sync()");
        out->sync();
    }

    std::string getFileName() const override { return out->getFileName(); }

private:
    void nextImpl() override
    {
        LOG_WARNING(log, "WriteEncryptedBuffer::nextImpl()");
        if (!offset())
            return;
        if (flush_iv)
        {
            LOG_WARNING(log, "flush_iv == true");
            WriteIV(iv, *out);
            flush_iv = false;
        }

        encryptor.Encrypt(working_buffer.begin(), *out, offset());
    }

    bool finalized = false;
    std::unique_ptr<WriteBufferFromFileBase> out;

    bool flush_iv;
    InitVector iv;
    Encryptor encryptor;
    Poco::Logger * log = &Poco::Logger::get("WriteEncryptedBuffer");
};

}
