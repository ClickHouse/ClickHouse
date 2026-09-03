#include <libunwind.h>

/// On MacOS this function will be replaced with a dynamic symbol
/// from the system library.
#if !defined(OS_DARWIN)
int backtrace(void ** buffer, int size)
{
    return unw_backtrace(buffer, size);
}
#endif
