#include <libunwind.h>

#if defined(__APPLE__)
int backtrace(void ** buffer, int size);
#else
int backtrace(void ** buffer, int size)
{
    return unw_backtrace(buffer, size);
}
#endif
