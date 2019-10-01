#include <Common/filesystemHelpers.h>
#include <common/config_common.h>
#include <Poco/File.h>
#include <Poco/Path.h>

namespace DB
{

bool checkFreeSpace(const std::string & path, size_t data_size)
{
#if !UNBUNDLED
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
