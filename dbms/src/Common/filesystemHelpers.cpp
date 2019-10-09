#include <Common/filesystemHelpers.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/Version.h>

namespace DB
{

bool enoughSpaceInDirectory(const std::string & path [[maybe_unused]], size_t data_size [[maybe_unused]])
{
#if POCO_VERSION >= 0x01090000
    auto free_space = Poco::File(path).freeSpace();
    if (data_size > free_space)
        return false;
#endif
    return true;
}

std::unique_ptr<TemporaryFile> createTemporaryFile(const std::string & path)
{
    Poco::File(path).createDirectories();

    /// NOTE: std::make_shared cannot use protected constructors
    return std::make_unique<TemporaryFile>(path);
}

}
