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

}
