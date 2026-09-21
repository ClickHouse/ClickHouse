#include <IO/Archives/LibArchiveReader.h>
#include <IO/Archives/ZipArchiveReader.h>
#include <IO/Archives/createArchiveReader.h>
#include <IO/Archives/ArchiveUtils.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_UNPACK_ARCHIVE;
extern const int SUPPORT_IS_DISABLED;
}

std::shared_ptr<IArchiveReader> createArchiveReader(const String & path_to_archive)
{
    return createArchiveReader(path_to_archive, {}, 0);
}


std::shared_ptr<IArchiveReader> createArchiveReader(
    const String & path_to_archive,
    [[maybe_unused]] const std::function<std::unique_ptr<SeekableReadBuffer>()> & archive_read_function,
    [[maybe_unused]] size_t archive_size)
{
    if (hasSupportedZipExtension(path_to_archive))
    {
#if USE_MINIZIP
        return std::make_shared<ZipArchiveReader>(path_to_archive, archive_read_function, archive_size);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "minizip library is disabled");
#endif
    }
    else if (hasSupportedTarExtension(path_to_archive))
    {
#if USE_LIBARCHIVE
        return std::make_shared<TarArchiveReader>(path_to_archive, archive_read_function);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "libarchive library is disabled");
#endif
    }
    else if (hasSupported7zExtension(path_to_archive))
    {
#if USE_LIBARCHIVE
        if (archive_read_function)
            throw Exception(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "7z archive supports only local files reading");
        else
            return std::make_shared<SevenZipArchiveReader>(path_to_archive);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "libarchive library is disabled");
#endif
    }
    else
    {
        throw Exception(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "Cannot determine the type of archive {}", path_to_archive);
    }
}

}
