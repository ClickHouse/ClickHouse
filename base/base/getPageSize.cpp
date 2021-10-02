#include <base/getPageSize.h>
#include <unistd.h>


Int64 getPageSize()
{
    return sysconf(_SC_PAGESIZE);
}
