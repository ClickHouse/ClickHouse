#include <IO/WriteBufferFromEncryptedFile.h>
#include <Common/logger_useful.h>

#if USE_SSL

namespace DB
{

WriteBufferFromEncryptedFile::WriteBufferFromEncryptedFile(
    size_t buffer_size_,
    std::unique_ptr<WriteBufferFromFileBase> out_,
    const String & key_,
    const FileEncryption::Header & header_,
    size_t old_file_size,
    bool use_adaptive_buffer_size_,
    size_t adaptive_buffer_initial_size)
    : WriteBufferDecorator<WriteBufferFromFileBase>(std::move(out_), use_adaptive_buffer_size_ ? adaptive_buffer_initial_size : buffer_size_, nullptr, 0)
    , header(header_)
    , flush_header(!old_file_size)
    , encryptor(header.algorithm, key_, header.init_vector)
    , use_adaptive_buffer_size(use_adaptive_buffer_size_)
    , adaptive_buffer_max_size(buffer_size_)
{
    encryptor.setOffset(old_file_size);
}

WriteBufferFromEncryptedFile::~WriteBufferFromEncryptedFile()
{
    /// That destructor could be call with finalized=false in case of exceptions.
    if (!finalized)
        LOG_INFO(log, "WriteBufferFromEncryptedFile is not finalized in destructor");
}

void WriteBufferFromEncryptedFile::finalFlushBefore()
{
    /// If buffer has pending data - write it.
    next();

    /// Note that if there is no data to write an empty file will be written, even without the initialization vector
    /// (see nextImpl(): it writes the initialization vector only if there is some data ready to write).
    /// That's fine because DiskEncrypted allows files without initialization vectors when they're empty.
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

    /// Increase buffer size for next data if adaptive buffer size is used and nextImpl was called because of end of buffer.
    if (!available() && use_adaptive_buffer_size && memory.size() < adaptive_buffer_max_size)
    {
        memory.resize(std::min(memory.size() * 2, adaptive_buffer_max_size));
        BufferBase::set(memory.data(), memory.size(), 0);
    }
}

}

#endif
