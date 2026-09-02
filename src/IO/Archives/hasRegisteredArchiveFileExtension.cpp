#include <IO/Archives/hasRegisteredArchiveFileExtension.h>

#include <algorithm>
#include <array>

namespace DB
{

bool hasRegisteredArchiveFileExtension(const String & path)
{
    using namespace std::literals;
    static constexpr std::array archive_extensions{
        ".zip"sv, ".zipx"sv, ".tar"sv, ".tar.gz"sv, ".tgz"sv, ".tar.bz2"sv, ".tar.lzma"sv, ".tar.zst"sv, ".tzst"sv, ".tar.xz"sv};
    return std::any_of(
        archive_extensions.begin(), archive_extensions.end(), [&](const auto extension) { return path.ends_with(extension); });
}
}
