#include <IO/Archives/createArchiveWriter.h>
#include <IO/Archives/ZipArchiveWriter.h>
#include <IO/WriteBuffer.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PACK_ARCHIVE;
    extern const int SUPPORT_IS_DISABLED;
}


std::shared_ptr<IArchiveWriter> createArchiveWriter(const String & path_to_archive)
{
    return createArchiveWriter(path_to_archive, nullptr);
}


std::shared_ptr<IArchiveWriter> createArchiveWriter(
    const String & path_to_archive,
    [[maybe_unused]] std::unique_ptr<WriteBuffer> archive_write_buffer)
{
    if (path_to_archive.ends_with(".zip") || path_to_archive.ends_with(".zipx"))
    {
#if USE_MINIZIP
        return std::make_shared<ZipArchiveWriter>(path_to_archive, std::move(archive_write_buffer));
#else
        throw Exception("minizip library is disabled", ErrorCodes::SUPPORT_IS_DISABLED);
#endif
    }
    else
        throw Exception(ErrorCodes::CANNOT_PACK_ARCHIVE, "Cannot determine the type of archive {}", path_to_archive);
}

}
