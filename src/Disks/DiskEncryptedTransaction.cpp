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

std::unique_ptr<WriteBufferFromFileBase>
DiskEncryptedTransaction::writeFileWithAutoCommit(
    const std::string & path,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings)
{
    return writeFileImpl(/*autocommit*/ true, path, buf_size, mode, settings);
}

std::unique_ptr<WriteBufferFromFileBase>
DiskEncryptedTransaction::writeFile(
    const std::string & path,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings)
{
    return writeFileImpl(/*autocommit*/ false, path, buf_size, mode, settings);
}

std::unique_ptr<WriteBufferFromFileBase> DiskEncryptedTransaction::writeFileImpl(
    bool autocommit,
    const std::string & path,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings)
{
    auto wrapped_path = wrappedPath(path);
    FileEncryption::Header header;
    String key;
    UInt64 old_file_size = 0;
    if (mode == WriteMode::Append && delegate_disk->existsFile(wrapped_path))
    {
        size_t size = delegate_disk->getFileSize(wrapped_path);

        if (size > FileEncryption::Header::kSize)
        {
            /// Append mode: we continue to use the same header.
            old_file_size = size - FileEncryption::Header::kSize;
            auto read_buffer = delegate_disk->readFile(wrapped_path, getReadSettings().adjustBufferSize(FileEncryption::Header::kSize));
            header = readHeader(*read_buffer);
            key = current_settings.findKeyByFingerprint(header.key_fingerprint, path);
        }
        else if (size == FileEncryption::Header::kSize)
        {
            /// The file holds nothing but the header, which is what a write interrupted before the payload reached
            /// the disk leaves behind, or a file that was truncated to its header. There is no payload to keep, so
            /// the file is started over: the delegate is opened in rewrite mode and gets a fresh header below.
            ///
            /// Neither of the two other options is acceptable. Appending a fresh header after the existing one
            /// would make the reader decipher the second header and the payload with the initialization vector of
            /// the first one: nothing detects that, and the file could never be read again (the garbage that the wrong
            /// initialization vector produces is reported by the compression layer as an unknown codec). Continuing the
            /// existing header would encrypt the new payload from offset 0 with the initialization vector that the old
            /// payload, if there was one before the truncation, was encrypted with: for the counter mode ciphers this
            /// is keystream reuse, and anyone who has both ciphertexts learns the XOR of both plaintexts.
            ///
            /// Only a valid header is started over: a 64-byte file which is not an encrypted file (wrong signature,
            /// version or algorithm) is not ours to replace, so `readHeader` throws `DATA_ENCRYPTION_ERROR` for it.
            auto read_buffer = delegate_disk->readFile(wrapped_path, getReadSettings().adjustBufferSize(FileEncryption::Header::kSize));
            readHeader(*read_buffer);
            mode = WriteMode::Rewrite;
        }
        else if (size > 0)
        {
            /// The file ends inside its own header, which is what a write interrupted while writing
            /// the header leaves behind. There is no header to continue, and no payload to keep.
            throw Exception(
                ErrorCodes::DATA_ENCRYPTION_ERROR,
                "Cannot append to encrypted file {}: it is {} bytes long, less than the {} bytes of an encryption header, "
                "so it holds no readable data and no header to continue. Remove the file to start a new one",
                path,
                size,
                FileEncryption::Header::kSize);
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

    auto buffer = autocommit
        ? delegate_transaction->writeFileWithAutoCommit(wrapped_path, buf_size, mode, settings)
        : delegate_transaction->writeFile(wrapped_path, buf_size, mode, settings);

    return std::make_unique<WriteBufferFromEncryptedFile>(buf_size, std::move(buffer), key, header, old_file_size, settings.use_adaptive_write_buffer, settings.adaptive_write_buffer_initial_size);
}

}

#endif
