#include <Common/getExecutablePath.h>
#include <filesystem>


std::string getExecutablePath()
{
    std::error_code ec;
    std::filesystem::path canonical_path = std::filesystem::canonical("/proc/self/exe", ec);

    if (ec)
        return {};
    return canonical_path;
}
