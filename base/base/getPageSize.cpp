#include <base/getPageSize.h>
#include <unistd.h>
#include <cstdlib>

Int64 getPageSize()
{
    Int64 page_size = sysconf(_SC_PAGESIZE);
    if (page_size < 0)
        abort();
    return page_size;
}
