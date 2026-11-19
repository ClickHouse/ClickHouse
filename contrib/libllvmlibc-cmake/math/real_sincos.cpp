/// Under sanitizers compiler-rt intercepts `sincos`, and in a static musl build the interceptor
/// calls `__real_sincos`. That name must resolve to llvm-libc's correctly rounded `sincos`, not
/// to musl's: `sin` and `cos` are not intercepted and come from llvm-libc, and `sincos(x)` has
/// to produce the same bits as `sin(x)` and `cos(x)`. See CMakeLists.txt in this directory and
/// the renaming step in contrib/compiler-rt-cmake/CMakeLists.txt.
#include "src/math/sincos.h"

extern "C" void __real_sincos(double x, double * sinx, double * cosx) /// NOLINT(bugprone-reserved-identifier)
{
    LIBC_NAMESPACE::sincos(x, sinx, cosx);
}
