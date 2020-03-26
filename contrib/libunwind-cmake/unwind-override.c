#include <libunwind.h>

int backtrace(void ** buffer, int size)
{
    return unw_backtrace(buffer, size);
}
