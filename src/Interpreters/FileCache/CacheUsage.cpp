#include <Interpreters/FileCache/CacheUsage.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>


namespace DB
{

CacheUsage::~CacheUsage()
{
    try
    {
        if (on_drained)
            on_drained();
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("CacheUsage"));
    }
}

}
