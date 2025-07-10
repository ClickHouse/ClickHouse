#include <IO/WriteBufferFromEncryptedFile.h>
#include <Common/logger_useful.h>

#if USE_SSL

namespace DB
{

WriteBufferFromEncryptedFile::WriteBufferFromEncryptedFile(
    size_t buffer_size_,
    std::unique_ptr<WriteBufferFromFileBase> out_,
    const NoDumpString & key_,
    const FileEncryption::Header & header_,
    size_t old_file_size)
    : WriteBufferDecorator<WriteBufferFromFileBase>(std::move(out_), buffer_size_, nullptr, 0)
    , header(header_)
    , flush_header(!old_file_size)
    , encryptor(header.algorithm, key_, header.init_vector)
{
    encryptor.setOffset(old_file_size);
}

WriteBufferFromEncryptedFile::~WriteBufferFromEncryptedFile()
{
    /// That destructor could be call with finalized=false in case of exceptions.
    if (!finalized)
        LOG_INFO(log, "WriteBufferFromEncryptedFile is not finalized in destructor");
}

void WriteBufferFromEncryptedFile::finalizeBefore()
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
}

}

#endif
