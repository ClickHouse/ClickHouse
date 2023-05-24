#include <Disks/DiskEncryptedTransaction.h>
#include <IO/FileEncryptionCommon.h>
#include <Common/Exception.h>
#include <boost/algorithm/hex.hpp>
#include <IO/ReadBufferFromEncryptedFile.h>
#include <IO/ReadBufferFromFileDecorator.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromEncryptedFile.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DATA_ENCRYPTION_ERROR;
    extern const int NOT_IMPLEMENTED;
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

String getCurrentKey(const String & path, const DiskEncryptedSettings & settings)
{
    auto it = settings.keys.find(settings.current_key_id);
    if (it == settings.keys.end())
        throw Exception(
            ErrorCodes::DATA_ENCRYPTION_ERROR,
            "Not found a key with the current ID {} required to cipher file {}",
            settings.current_key_id,
            quoteString(path));

    return it->second;
}

String getKey(const String & path, const FileEncryption::Header & header, const DiskEncryptedSettings & settings)
{
    auto it = settings.keys.find(header.key_id);
    if (it == settings.keys.end())
        throw Exception(
            ErrorCodes::DATA_ENCRYPTION_ERROR,
            "Not found a key with ID {} required to decipher file {}",
            header.key_id,
            quoteString(path));

    String key = it->second;
    if (FileEncryption::calculateKeyHash(key) != header.key_hash)
        throw Exception(
            ErrorCodes::DATA_ENCRYPTION_ERROR, "Wrong key with ID {}, could not decipher file {}", header.key_id, quoteString(path));

    return key;
}

}

void DiskEncryptedTransaction::copyFile(const std::string & from_file_path, const std::string & to_file_path)
{
    auto wrapped_from_path = wrappedPath(from_file_path);
    auto wrapped_to_path = wrappedPath(to_file_path);
    delegate_transaction->copyFile(wrapped_from_path, wrapped_to_path);
}

std::unique_ptr<WriteBufferFromFileBase> DiskEncryptedTransaction::writeFile(
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
    if (mode == WriteMode::Append && delegate_disk->exists(path))
    {
        old_file_size = delegate_disk->getFileSize(path);
        if (old_file_size)
        {
            /// Append mode: we continue to use the same header.
            auto read_buffer = delegate_disk->readFile(wrapped_path, ReadSettings().adjustBufferSize(FileEncryption::Header::kSize));
            header = readHeader(*read_buffer);
            key = getKey(path, header, current_settings);
        }
    }
    if (!old_file_size)
    {
        /// Rewrite mode: we generate a new header.
        key = getCurrentKey(path, current_settings);
        header.algorithm = current_settings.current_algorithm;
        header.key_id = current_settings.current_key_id;
        header.key_hash = FileEncryption::calculateKeyHash(key);
        header.init_vector = FileEncryption::InitVector::random();
    }
    auto buffer = delegate_transaction->writeFile(wrapped_path, buf_size, mode, settings, autocommit);
    return std::make_unique<WriteBufferFromEncryptedFile>(buf_size, std::move(buffer), key, header, old_file_size);

}
void DiskEncryptedTransaction::writeFileUsingCustomWriteObject(
    const String &,
    WriteMode,
    std::function<size_t(const StoredObject & object, WriteMode mode, const std::optional<ObjectAttributes> & object_attributes)>)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method `writeFileUsingCustomWriteObject()` is not implemented");
}



}
