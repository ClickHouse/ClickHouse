#include <IO/WriteBufferFromEncryptedFile.h>

#if USE_SSL
#include <Common/MemoryTracker.h>

namespace DB
{

WriteBufferFromEncryptedFile::WriteBufferFromEncryptedFile(
    size_t buf_size_,
    std::unique_ptr<WriteBufferFromFileBase> out_,
    const String & init_vector_,
    const FileEncryption::EncryptionKey & key_,
    const size_t & file_size)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , out(std::move(out_))
    , flush_iv(!file_size)
    , iv(init_vector_)
    , encryptor(FileEncryption::Encryptor(init_vector_, key_, file_size))
{
}

WriteBufferFromEncryptedFile::~WriteBufferFromEncryptedFile()
{
    /// FIXME move final flush into the caller
    MemoryTracker::LockExceptionInThread lock(VariableContext::Global);
    finish();
}

void WriteBufferFromEncryptedFile::finish()
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

void WriteBufferFromEncryptedFile::finishImpl()
{
    /// If buffer has pending data - write it.
    next();
    out->finalize();
}

void WriteBufferFromEncryptedFile::sync()
{
    /// If buffer has pending data - write it.
    next();
    out->sync();
}

void WriteBufferFromEncryptedFile::nextImpl()
{
    if (!offset())
        return;

    if (flush_iv)
    {
        FileEncryption::writeIV(iv, *out);
        flush_iv = false;
    }

    encryptor.encrypt(working_buffer.begin(), *out, offset());
}
}

#endif
