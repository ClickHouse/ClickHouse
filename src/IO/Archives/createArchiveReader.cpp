#include <IO/Archives/LibArchiveReader.h>
#include <IO/Archives/ZipArchiveReader.h>
#include <IO/Archives/createArchiveReader.h>
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
    using namespace std::literals;
    static constexpr std::array tar_extensions{
        ".tar"sv, ".tar.gz"sv, ".tgz"sv, ".tar.zst"sv, ".tzst"sv, ".tar.xz"sv, ".tar.bz2"sv, ".tar.lzma"sv};

    if (path_to_archive.ends_with(".zip") || path_to_archive.ends_with(".zipx"))
    {
#if USE_MINIZIP
        return std::make_shared<ZipArchiveReader>(path_to_archive, archive_read_function, archive_size);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "minizip library is disabled");
#endif
    }
    else if (std::any_of(
                 tar_extensions.begin(), tar_extensions.end(), [&](const auto extension) { return path_to_archive.ends_with(extension); }))
    {
#if USE_LIBARCHIVE
        return std::make_shared<TarArchiveReader>(path_to_archive, archive_read_function);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "libarchive library is disabled");
#endif
    }
    else if (path_to_archive.ends_with(".7z"))
    {
#if USE_LIBARCHIVE
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
