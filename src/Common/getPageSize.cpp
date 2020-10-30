#include "getPageSize.h"

#include <unistd.h>

long getPageSize()
{
    return sysconf(_SC_PAGESIZE);
}
