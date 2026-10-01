#include <Server/IcebergRESTCatalog/IcebergRESTCatalogStorage.h>

#include <Core/Defines.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/WriteMode.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteSettings.h>

#include <fmt/format.h>

namespace DB
{

String readObjectToString(const IObjectStorage & object_storage, const String & key, const ReadSettings & read_settings, size_t max_size)
{
    LimitReadBuffer buffer(
        object_storage.readObject(StoredObject(key), read_settings),
        LimitReadBuffer::Settings{
            .read_no_more = max_size, .expect_eof = true, .excetion_hint = fmt::format("object {} is larger than {} bytes", key, max_size)});
    String content;
    readStringUntilEOF(content, buffer);
    return content;
}

void writeNewObject(IObjectStorage & object_storage, const String & key, const String & content, WriteSettings write_settings)
{
    /// Every metadata file has a fresh uuid in its name, so an existing object means a bug or a foreign writer.
    write_settings.object_storage_write_if_none_match = "*";
    auto buffer = object_storage.writeObject(StoredObject(key), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, write_settings);
    buffer->write(content.data(), content.size());
    buffer->finalize();
}

}
