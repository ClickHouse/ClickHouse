#pragma once

#include <base/defines.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-id-macro"
#endif

#undef __msan_unpoison
#undef __msan_test_shadow
#undef __msan_print_shadow
#undef __msan_unpoison_string

#define __msan_unpoison(X, Y) /// NOLINT
#define __msan_test_shadow(X, Y) (false) /// NOLINT
#define __msan_print_shadow(X, Y) /// NOLINT
#define __msan_unpoison_string(X) /// NOLINT

#if defined(ch_has_feature)
#    if ch_has_feature(memory_sanitizer)
#        undef __msan_unpoison
#        undef __msan_test_shadow
#        undef __msan_print_shadow
#        undef __msan_unpoison_string
#        include <sanitizer/msan_interface.h>
#    endif
#endif

#ifdef __clang__
#pragma clang diagnostic pop
#endif
