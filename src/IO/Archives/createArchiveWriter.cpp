#include <IO/Archives/LibArchiveWriter.h>
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


std::shared_ptr<IArchiveWriter> createArchiveWriter(const String & path_to_archive)
{
    return createArchiveWriter(path_to_archive, nullptr);
}


std::shared_ptr<IArchiveWriter>
createArchiveWriter(const String & path_to_archive, [[maybe_unused]] std::unique_ptr<WriteBuffer> archive_write_buffer)
{
    using namespace std::literals;
    static constexpr std::array tar_extensions{
        ".tar"sv, ".tar.gz"sv, ".tgz"sv, ".tar.bz2"sv, ".tar.lzma"sv, ".tar.zst"sv, ".tzst"sv, ".tar.xz"sv};
    if (path_to_archive.ends_with(".zip") || path_to_archive.ends_with(".zipx"))
    {
#if USE_MINIZIP
        return std::make_shared<ZipArchiveWriter>(path_to_archive, std::move(archive_write_buffer));
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "minizip library is disabled");
#endif
    }
    else if (std::any_of(
                 tar_extensions.begin(), tar_extensions.end(), [&](const auto extension) { return path_to_archive.ends_with(extension); }))
    {
#if USE_LIBARCHIVE
        return std::make_shared<TarArchiveWriter>(path_to_archive, std::move(archive_write_buffer));
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "libarchive library is disabled");
#endif
    }
    else
        throw Exception(ErrorCodes::CANNOT_PACK_ARCHIVE, "Cannot determine the type of archive {}", path_to_archive);
}
}
