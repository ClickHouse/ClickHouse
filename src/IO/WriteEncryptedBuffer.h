#pragma once

#if USE_SSL

#include <IO/WriteBufferFromFileBase.h>
#include <Functions/FileEncryption.h>
#include <Common/MemoryTracker.h>


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
        /// FIXME move final flush into the caller
        MemoryTracker::LockExceptionInThread lock(VariableContext::Global);
        finish();
    }

    void sync() override
    {
        /// If buffer has pending data - write it.
        next();
        out->sync();
    }

    void finalize() override { finish(); }

    std::string getFileName() const override { return out->getFileName(); }

private:
    void nextImpl() override
    {
        if (!offset())
            return;

        if (flush_iv)
        {
            WriteIV(iv, *out);
            flush_iv = false;
        }

        encryptor.Encrypt(working_buffer.begin(), *out, offset());
    }

    void finish()
    {
        if (finished)
            return;

        try
        {
            finishImpl();
            out->finalize();
            finished = true;
        }
        catch (...)
        {
            /// Do not try to flush next time after exception.
            out->position() = out->buffer().begin();
            finished = true;
            throw;
        }
    }

    void finishImpl()
    {
        /// If buffer has pending data - write it.
        next();
        out->finalize();
    }

    bool finished = false;
    std::unique_ptr<WriteBufferFromFileBase> out;

    bool flush_iv;
    String iv;
    Encryptor encryptor;
};

}


#endif
