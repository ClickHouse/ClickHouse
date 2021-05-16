#pragma once

#include <IO/WriteBufferFromFileBase.h>
#include <Functions/FileEncryption.h>

#include <common/logger_useful.h>

namespace DB
{

using namespace FileEncryption;

class WriteEncryptedBuffer : public WriteBufferFromFileBase
{
public:
    WriteEncryptedBuffer(
        size_t buf_size_,
        std::unique_ptr<WriteBufferFromFileBase> out_,
        const String & init_vector_,
        const EncryptionKey & key_,
        const size_t & file_size)
        : WriteBufferFromFileBase(buf_size_, nullptr, 0)
        , out(std::move(out_))
        , flush_iv(!file_size)
        , iv(init_vector_)
        , encryptor(Encryptor(init_vector_, key_, file_size))
    { }

    ~WriteEncryptedBuffer() override
    {
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
        if (finalized)
	    return;

        next();
        out->finalize();

	finalized = true;
    }

    void sync() override
    {
        out->sync();
    }

    std::string getFileName() const override { return out->getFileName(); }

private:
    void nextImpl() override
    {
        if (!offset())
            return;
        if (flush_iv)
        {
            writeText(iv.data(), *out);
            flush_iv = false;
        }

        encryptor.Encrypt(working_buffer.begin(), *out, offset());
    }

    bool finalized = false;
    std::unique_ptr<WriteBufferFromFileBase> out;

    bool flush_iv;
    String iv;
    Encryptor encryptor;
};

}
