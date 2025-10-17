#include <IO/Archives/ArchiveUtils.h>

#include <string_view>
#include <array>

namespace DB
{

namespace
{

using namespace std::literals;
constexpr std::array tar_extensions{".tar"sv, ".tar.gz"sv, ".tgz"sv, ".tar.zst"sv, ".tzst"sv, ".tar.xz"sv, ".tar.bz2"sv, ".tar.lzma"sv};
constexpr std::array zip_extensions{".zip"sv, ".zipx"sv};
constexpr std::array sevenz_extensiosns{".7z"sv};

bool hasSupportedExtension(std::string_view path, const auto & supported_extensions)
{
    for (auto supported_extension : supported_extensions)
    {
        if (path.ends_with(supported_extension))
            return true;
    }

    return false;
}

}

bool hasSupportedTarExtension(std::string_view path)
{
    return hasSupportedExtension(path, tar_extensions);
}

bool hasSupportedZipExtension(std::string_view path)
{
    return hasSupportedExtension(path, zip_extensions);
}

bool hasSupported7zExtension(std::string_view path)
{
    return hasSupportedExtension(path, sevenz_extensiosns);
}

bool hasSupportedArchiveExtension(std::string_view path)
{
    return hasSupportedTarExtension(path) || hasSupportedZipExtension(path) || hasSupported7zExtension(path);
}

std::pair<std::string, std::optional<std::string>> getURIAndArchivePattern(const std::string & source)
{
    size_t pos = source.find("::");
    if (pos == std::string::npos)
        return {source, std::nullopt};

    std::string_view path_to_archive_view = std::string_view{source}.substr(0, pos);
    bool contains_spaces_around_operator = false;
    while (path_to_archive_view.ends_with(' '))
    {
        contains_spaces_around_operator = true;
        path_to_archive_view.remove_suffix(1);
    }

    std::string_view archive_pattern_view = std::string_view{source}.substr(pos + 2);
    while (archive_pattern_view.starts_with(' '))
    {
        contains_spaces_around_operator = true;
        archive_pattern_view.remove_prefix(1);
    }

    /// possible situations when the first part can be archive is only if one of the following is true:
    /// - it contains supported extension
    /// - it contains spaces after or before :: (URI cannot contain spaces)
    /// - it contains characters that could mean glob expression
    if (archive_pattern_view.empty() || path_to_archive_view.empty()
        || (!contains_spaces_around_operator && !hasSupportedArchiveExtension(path_to_archive_view)
            && path_to_archive_view.find_first_of("*?{") == std::string_view::npos))
        return {source, std::nullopt};

    return std::pair{std::string{path_to_archive_view}, std::string{archive_pattern_view}};
}
}
