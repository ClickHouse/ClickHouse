#include <libunwind.h>

/// For simplicity let's unify the interface.
/// On MacOS this function will be replaced with a dynamic symbol
/// from the system library.
#if defined(__APPLE__)
int backtrace(void ** buffer, int size);
#else
int backtrace(void ** buffer, int size)
{
    return unw_backtrace(buffer, size);
}
#endif
