# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

include(CheckCSourceRuns)

option(USE_CPU_EXTENSIONS "Whenever possible, use functions optimized for CPUs with specific extensions (ex: SSE, AVX)." ON)

# In the current (11/2/21) state of mingw64, the packaged gcc is not capable of emitting properly aligned avx2 instructions under certain circumstances.
# This leads to crashes for windows builds using mingw64 when invoking the avx2-enabled versions of certain functions.  Until we can find a better
# work-around, disable avx2 (and all other extensions) in mingw builds.
#
# https://gcc.gnu.org/bugzilla/show_bug.cgi?id=54412
#
if (MINGW)
    message(STATUS "MINGW detected!  Disabling avx2 and other CPU extensions")
    set(USE_CPU_EXTENSIONS OFF)
endif()

if(NOT CMAKE_CROSSCOMPILING)
    check_c_source_runs("
    #include <stdbool.h>
    bool foo(int a, int b, int *c) {
        return __builtin_mul_overflow(a, b, c);
    }

    int main() {
        int out;
        if (foo(1, 2, &out)) {
            return 0;
        }

        return 0;
    }" AWS_HAVE_GCC_OVERFLOW_MATH_EXTENSIONS)

    if (USE_CPU_EXTENSIONS)
        check_c_source_runs("
        int main() {
        int foo = 42;
        _mulx_u32(1, 2, &foo);
        return foo != 2;
        }" AWS_HAVE_MSVC_MULX)
    endif()

endif()

check_c_source_compiles("
    #include <Windows.h>
    #if WINAPI_FAMILY_PARTITION(WINAPI_PARTITION_DESKTOP)
    int main() {
        return 0;
    }
    #else
    it's not windows desktop
    #endif
" AWS_HAVE_WINAPI_DESKTOP)

check_c_source_compiles("
    int main() {
#if !(defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86))
#    error \"not intel\"
#endif
        return 0;
    }
" AWS_ARCH_INTEL)

check_c_source_compiles("
    int main() {
#if !(defined(__aarch64__) || defined(_M_ARM64))
#    error \"not arm64\"
#endif
        return 0;
    }
" AWS_ARCH_ARM64)

check_c_source_compiles("
    int main() {
#if !(defined(__arm__) || defined(_M_ARM))
#    error \"not arm\"
#endif
        return 0;
    }
" AWS_ARCH_ARM32)

check_c_source_compiles("
int main() {
    int foo = 42, bar = 24;
    __asm__ __volatile__(\"\":\"=r\"(foo):\"r\"(bar):\"memory\");
}" AWS_HAVE_GCC_INLINE_ASM)

check_c_source_compiles("
#include <sys/auxv.h>
int main() {
#ifdef __linux__
    getauxval(AT_HWCAP);
    getauxval(AT_HWCAP2);
#endif
    return 0;
}" AWS_HAVE_AUXV)

string(REGEX MATCH "^(aarch64|arm)" ARM_CPU "${CMAKE_SYSTEM_PROCESSOR}")
if(NOT LEGACY_COMPILER_SUPPORT OR ARM_CPU)
    check_c_source_compiles("
    #include <execinfo.h>
    int main() {
        backtrace(NULL, 0);
        return 0;
    }" AWS_HAVE_EXECINFO)
endif()

check_c_source_compiles("
#include <linux/if_link.h>
int main() {
    return 1;
}" AWS_HAVE_LINUX_IF_LINK_H)
