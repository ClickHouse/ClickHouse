#include <Disks/DiskEncryptedTransaction.h>

#if USE_SSL
#include <IO/FileEncryptionCommon.h>
#include <Common/Exception.h>
#include <boost/algorithm/hex.hpp>
#include <IO/ReadBufferFromEncryptedFile.h>
#include <IO/ReadBufferFromFileDecorator.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromEncryptedFile.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DATA_ENCRYPTION_ERROR;
}


namespace
{

FileEncryption::Header readHeader(ReadBufferFromFileBase & read_buffer)
{
    try
    {
        FileEncryption::Header header;
        header.read(read_buffer);
        return header;
    }
    catch (Exception & e)
    {
        e.addMessage("While reading the header of encrypted file " + quoteString(read_buffer.getFileName()));
        throw;
    }
}

}

String DiskEncryptedSettings::findKeyByFingerprint(UInt128 key_fingerprint, const String & path_for_logs) const
{
    auto it = all_keys.find(key_fingerprint);
    if (it == all_keys.end())
    {
        throw Exception(
            ErrorCodes::DATA_ENCRYPTION_ERROR,
            "Not found an encryption key required to decipher file {}",
            quoteString(path_for_logs));
    }
    return it->second;
}

void DiskEncryptedTransaction::copyFile(const std::string & from_file_path, const std::string & to_file_path, const ReadSettings & read_settings, const WriteSettings & write_settings)
{
    auto wrapped_from_path = wrappedPath(from_file_path);
    auto wrapped_to_path = wrappedPath(to_file_path);
    delegate_transaction->copyFile(wrapped_from_path, wrapped_to_path, read_settings, write_settings);
}

std::unique_ptr<WriteBufferFromFileBase> DiskEncryptedTransaction::writeFile( // NOLINT
    const std::string & path,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings,
    bool autocommit)
{
    auto wrapped_path = wrappedPath(path);
    FileEncryption::Header header;
    String key;
    UInt64 old_file_size = 0;
    if (mode == WriteMode::Append && delegate_disk->existsFile(wrapped_path))
    {
        size_t size = delegate_disk->getFileSize(wrapped_path);
        old_file_size = size > FileEncryption::Header::kSize ? (size - FileEncryption::Header::kSize) : 0;
        if (old_file_size)
        {
            /// Append mode: we continue to use the same header.
            auto read_buffer = delegate_disk->readFile(wrapped_path, getReadSettings().adjustBufferSize(FileEncryption::Header::kSize));
            header = readHeader(*read_buffer);
            key = current_settings.findKeyByFingerprint(header.key_fingerprint, path);
        }
    }
    if (!old_file_size)
    {
        /// Rewrite mode: we generate a new header.
        header.algorithm = current_settings.current_algorithm;
        key = current_settings.current_key;
        header.key_fingerprint = current_settings.current_key_fingerprint;
        header.init_vector = FileEncryption::InitVector::random();
    }
    auto buffer = delegate_transaction->writeFile(wrapped_path, buf_size, mode, settings, autocommit);
    return std::make_unique<WriteBufferFromEncryptedFile>(buf_size, std::move(buffer), key, header, old_file_size);

}

}

#endif
