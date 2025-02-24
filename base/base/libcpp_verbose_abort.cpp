#include <version>

void std::__libcpp_verbose_abort(char const* format, ...) {
    va_list list;
    va_start(list, format);
    std::vfprintf(stderr, format, list);
    va_end(list);

    std::abort();
}
