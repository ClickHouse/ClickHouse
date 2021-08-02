#include <IO/WriteBufferFromEncryptedFile.h>

#if USE_SSL
#include <Common/MemoryTracker.h>

namespace DB
{

WriteBufferFromEncryptedFile::WriteBufferFromEncryptedFile(
    size_t buffer_size_,
    std::unique_ptr<WriteBufferFromFileBase> out_,
    const String & key_,
    const FileEncryption::Header & header_,
    size_t old_file_size)
    : WriteBufferFromFileBase(buffer_size_, nullptr, 0)
    , out(std::move(out_))
    , header(header_)
    , flush_header(!old_file_size)
    , encryptor(header.algorithm, key_, header.init_vector)
{
    encryptor.setOffset(old_file_size);
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

    /// Note that if there is no data to write an empty file will be written, even without the initialization vector
    /// (see nextImpl(): it writes the initialization vector only if there is some data ready to write).
    /// That's fine because DiskEncrypted allows files without initialization vectors when they're empty.

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

    if (flush_header)
    {
        header.write(*out);
        flush_header = false;
    }

    encryptor.encrypt(working_buffer.begin(), offset(), *out);
}

}

#endif
