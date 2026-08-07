#pragma once

#include <base/defines.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-id-macro"

#undef __msan_unpoison
#undef __msan_test_shadow
#undef __msan_print_shadow
#undef __msan_unpoison_string

#define __msan_unpoison(X, Y) /// NOLINT
/// Given a pointer and **its size**, unpoisons 15 bytes **at the end**
/// See memcmpSmall.h / memcpySmall.h
#define __msan_unpoison_overflow_15(X, Y) /// NOLINT
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
#        undef __msan_unpoison_overflow_15
#        define __msan_unpoison_overflow_15(PTR, PTR_SIZE) __msan_unpoison(&(PTR)[(PTR_SIZE)], 15)
#    endif
#endif

#pragma clang diagnostic pop
