#include <Server/IcebergRESTCatalog/IcebergRESTCatalogStorage.h>

#include <Core/Defines.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/WriteMode.h>
#include <IO/CompressionMethod.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteSettings.h>

#include <fmt/format.h>

namespace DB
{

CompressionMethod getMetadataFileCompressionMethod(const String & path)
{
    constexpr std::string_view metadata_suffix = ".metadata.json";
    if (!path.ends_with(metadata_suffix))
        return CompressionMethod::None;
    return chooseCompressionMethod(path.substr(0, path.size() - metadata_suffix.size()), "auto");
}

String readObjectToString(
    const IObjectStorage & object_storage,
    const String & key,
    const ReadSettings & read_settings,
    size_t max_size,
    CompressionMethod compression_method)
{
    /// The limit applies to the decompressed content, so a small compressed object cannot expand past it.
    LimitReadBuffer buffer(
        wrapReadBufferWithCompressionMethod(object_storage.readObject(StoredObject(key), read_settings), compression_method),
        LimitReadBuffer::Settings{
            .read_no_more = max_size, .expect_eof = true, .excetion_hint = fmt::format("object {} is larger than {} bytes", key, max_size)});
    String content;
    readStringUntilEOF(content, buffer);
    return content;
}

void writeNewObject(
    IObjectStorage & object_storage,
    const String & key,
    const String & content,
    WriteSettings write_settings,
    CompressionMethod compression_method,
    int compression_level)
{
    /// Every metadata file has a fresh uuid in its name, so an existing object means a bug or a foreign writer.
    write_settings.object_storage_write_if_none_match = "*";
    auto buffer = object_storage.writeObject(StoredObject(key), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, write_settings);
    auto compressed_buffer = wrapWriteBufferWithCompressionMethod(std::move(buffer), compression_method, compression_level);
    compressed_buffer->write(content.data(), content.size());
    compressed_buffer->finalize();
}

}
