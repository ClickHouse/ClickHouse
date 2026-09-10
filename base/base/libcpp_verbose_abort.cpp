#include <version>

/// Custom assertion handler for hardened libcxx
/// See https://prereleases.llvm.org/16.0.0/rc3/projects/libcxx/docs/UsingLibcxx.html#enabling-the-safe-libc-mode
void std::__libcpp_verbose_abort(char const * format, ...)
{
    va_list list;
    va_start(list, format);
    std::vfprintf(stderr, format, list);
    va_end(list);

    std::abort();
}
