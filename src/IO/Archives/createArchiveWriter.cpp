#include <IO/Archives/ArchiveUtils.h>
#include <IO/Archives/TarArchiveWriter.h>
#include <IO/Archives/ZipArchiveWriter.h>
#include <IO/Archives/createArchiveWriter.h>
#include <IO/WriteBuffer.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_PACK_ARCHIVE;
extern const int SUPPORT_IS_DISABLED;
}


std::shared_ptr<IArchiveWriter> createArchiveWriter(
    const String & path_to_archive,
    [[maybe_unused]] std::unique_ptr<WriteBuffer> archive_write_buffer,
    [[maybe_unused]] size_t buf_size,
    [[maybe_unused]] size_t adaptive_buffer_max_size)
{
    if (hasSupportedZipExtension(path_to_archive))
    {
#if USE_MINIZIP
        return std::make_shared<ZipArchiveWriter>(path_to_archive, std::move(archive_write_buffer));
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "minizip library is disabled");
#endif
    }
    else if (hasSupportedTarExtension(path_to_archive))
    {
#if USE_LIBARCHIVE
        return std::make_shared<TarArchiveWriter>(path_to_archive, std::move(archive_write_buffer), buf_size, adaptive_buffer_max_size);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "libarchive library is disabled");
#endif
    }
    else
        throw Exception(ErrorCodes::CANNOT_PACK_ARCHIVE, "Cannot determine the type of archive {}", path_to_archive);
}
}
