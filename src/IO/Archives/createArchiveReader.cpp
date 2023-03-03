#include <IO/Archives/createArchiveReader.h>
#include <IO/Archives/ZipArchiveReader.h>
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
    if (path_to_archive.ends_with(".zip") || path_to_archive.ends_with(".zipx"))
    {
#if USE_MINIZIP
        return ZipArchiveReader::create(path_to_archive, archive_read_function, archive_size);
#else
        throw Exception("minizip library is disabled", ErrorCodes::SUPPORT_IS_DISABLED);
#endif
    }
    else
        throw Exception(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "Cannot determine the type of archive {}", path_to_archive);
}

}
