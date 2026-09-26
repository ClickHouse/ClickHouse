#include <IO/OpenedFileCache.h>

namespace DB
{

OpenedFileCache & OpenedFileCache::instance()
{
    static OpenedFileCache res;
    return res;
}

}
