#pragma once

#if defined(ARCADIA_BUILD)
static constexpr bool IS_ARCADIA_BUILD = true;
#else
static constexpr bool IS_ARCADIA_BUILD = false;
#endif

#if defined(__FreeBSD__)
static constexpr bool IS_FREEBSD_BUILD = true;
#else
static constexpr bool IS_FREEBSD_BUILD = false;
#endif

#if defined(__ELF__)
static constexpr bool IS_ELF = true;
#else
static constexpr bool IS_ELF = false;
#endif

#if defined(__linux__)
static constexpr bool IS_LINUX_BUILD = true;
#else
static constexpr bool IS_LINUX_BUILD = false;
#endif

#if defined(SANITIZER)
static constexpr bool IS_BUILD_WITH_SANITIZER = true;
#else
static constexpr bool IS_BUILD_WITH_SANITIZER = false;
#endif

// TODO add variables for: archs (Darwin, x86_64, ...), compilers (Clang, GCC, ...), sanitizer types, ...
