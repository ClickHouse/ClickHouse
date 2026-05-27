#include <base/getPageSize.h>
#include <unistd.h>
#include <cstdlib>

static Int64 getPageSizeImpl()
{
    Int64 page_size = sysconf(_SC_PAGESIZE);
    if (page_size < 0)
        abort();
    return page_size;
}

/// Function-local static (instead of a namespace-scope variable with a dynamic
/// initializer) so the initialization survives LTO. With a namespace-scope
/// variable, LTO has been observed to DCE the `__cxx_global_var_init` for
/// this TU on the musl/aarch64 static build, leaving `staticPageSize` at 0
/// and breaking the first `aligned_alloc(page_size, ...)` at startup.
Int64 getPageSize()
{
    static const Int64 page_size = getPageSizeImpl();
    return page_size;
}
