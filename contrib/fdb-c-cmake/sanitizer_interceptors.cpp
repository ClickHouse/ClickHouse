#include <dlfcn.h>

#if __has_feature(memory_sanitizer)
#    include <sanitizer/msan_interface.h>
#    define REAL(func) reinterpret_cast<decltype(&func)>(dlsym(RTLD_NEXT, #func))

extern "C" {
int dladdr1(const void * __address, Dl_info * __info, void ** __extra_info, int __flags)
{
    auto res = REAL(dladdr1)(__address, __info, __extra_info, __flags);
    if (res != 0)
    {
        __msan_unpoison(__info, sizeof(Dl_info));
        __msan_unpoison_string(__info->dli_fname);
    }
    return res;
}
}
#endif