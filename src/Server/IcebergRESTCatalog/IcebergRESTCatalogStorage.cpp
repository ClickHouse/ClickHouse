#include <Server/IcebergRESTCatalog/IcebergRESTCatalogStorage.h>

#include <Common/Exception.h>
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

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace IcebergRESTCatalog
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
    CompressionMethod compression_method)
{
    /// The limit applies to the decompressed content, so a small compressed object cannot expand past it.
    LimitReadBuffer buffer(
        wrapReadBufferWithCompressionMethod(object_storage.readObject(StoredObject(key), read_settings), compression_method),
        LimitReadBuffer::Settings{
            .read_no_more = MAX_METADATA_FILE_SIZE,
            .expect_eof = true,
            .excetion_hint = fmt::format("object {} is larger than {} bytes", key, MAX_METADATA_FILE_SIZE)});
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
    /// The same limit as on read, applied to the uncompressed content. A larger object could never be read back.
    if (content.size() > MAX_METADATA_FILE_SIZE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Metadata is {} bytes, larger than the limit of {} bytes", content.size(), MAX_METADATA_FILE_SIZE);

    /// Every metadata file has a fresh uuid in its name, so an existing object means a bug or a foreign writer.
    write_settings.object_storage_write_if_none_match = "*";
    auto buffer = object_storage.writeObject(StoredObject(key), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, write_settings);
    auto compressed_buffer = wrapWriteBufferWithCompressionMethod(std::move(buffer), compression_method, compression_level);
    compressed_buffer->write(content.data(), content.size());
    compressed_buffer->finalize();
}

}

}
