#pragma once

#define __msan_unpoison(X, Y)
#if defined(__has_feature)
#   if __has_feature(memory_sanitizer)
#       undef __msan_unpoison
#       include <sanitizer/msan_interface.h>
#   endif
#endif
