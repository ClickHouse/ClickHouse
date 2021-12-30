#include <Coordination/pathUtils.h>
#include <iostream>

namespace DB
{

static size_t findLastSlash(StringRef path)
{
    if (path.size == 0)
        return std::string::npos;

    for (size_t i = path.size - 1; i > 0; --i)
    {
        if (path.data[i] == '/')
            return i;
    }

    if (path.data[0] == '/')
        return 0;

    return std::string::npos;
}

std::string parentPath(StringRef path)
{
    auto rslash_pos = findLastSlash(path);
    if (rslash_pos > 0)
        return std::string{path.data, rslash_pos};
    return "/";
}

std::string getBaseName(StringRef path)
{
    size_t basename_start = findLastSlash(path);
    return std::string{path.data + basename_start + 1, path.size - basename_start - 1};
}

}
