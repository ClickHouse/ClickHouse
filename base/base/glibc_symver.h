#pragma once

/// The oldest version of glibc symbols on each platform, from glibc/sysdeps/unix/sysv/linux/$ARCH/$BITS_OR_BYTE_ORDER/libc.abilist.
/// A reference `.symver name, symbol@GLIBC_SYMVER` binds to that exact libc symbol, even when this binary defines `symbol` too.
#if defined(__amd64__)
#    define GLIBC_SYMVER "GLIBC_2.2.5"
#elif defined(__aarch64__)
#    define GLIBC_SYMVER "GLIBC_2.17"
#elif defined(__riscv) && (__riscv_xlen == 64)
#    define GLIBC_SYMVER "GLIBC_2.27"
#elif (defined(__PPC64__) || defined(__powerpc64__)) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#    define GLIBC_SYMVER "GLIBC_2.17"
#elif (defined(__S390X__) || defined(__s390x__))
#    define GLIBC_SYMVER "GLIBC_2.2"
#elif defined(__loongarch64)
#    define GLIBC_SYMVER "GLIBC_2.36"
#elif defined(__e2k__)
#    define GLIBC_SYMVER "GLIBC_2.0"
#else
#    error Your platform is not supported.
#endif
