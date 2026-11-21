/* config.h for building libpcap inside ClickHouse.
 *
 * This is a hand-written, minimal configuration for a Linux/glibc build that
 * supports ONLY reading capture files (pcap and pcapng). Live packet capture,
 * remote capture, and all optional capture backends are disabled; the build
 * uses the "null" capture backend (pcap-null.c).
 *
 * It intentionally leaves out every PCAP_SUPPORT_* / capture-backend define so
 * that pcap.c reduces to the offline-only code paths.
 */

#pragma once

/* We build against GNU libc. */
#define HAVE_GLIBC 1

/* glibc provides these. */
#define HAVE_ASPRINTF 1
#define HAVE_VASPRINTF 1
#define HAVE_SNPRINTF 1
#define HAVE_VSNPRINTF 1
#define HAVE_STRTOK_R 1
#define HAVE_VSYSLOG 1
#define HAVE_FSEEKO 1
#define HAVE_UNISTD_H 1

/* The flavour of `strerror_r` differs between glibc (GNU-style) and musl (POSIX-style),
 * so `HAVE_GNU_STRERROR_R` / `HAVE_POSIX_STRERROR_R` is defined by CMake instead.
 */

/* socklen_t is available. */
#define HAVE_SOCKLEN_T 1

/* struct msghdr has msg_control / msg_flags on Linux. */
#define HAVE_STRUCT_MSGHDR_MSG_CONTROL 1
#define HAVE_STRUCT_MSGHDR_MSG_FLAGS 1

/* ether_hostton and where it is declared (glibc: netinet/ether.h). */
#define HAVE_ETHER_HOSTTON 1
#define HAVE_DECL_ETHER_HOSTTON 1
#define NETINET_ETHER_H_DECLARES_ETHER_HOSTTON 1
#define HAVE_STRUCT_ETHER_ADDR 1

/*
 * The Linux flavors of `getnetbyname_r` / `getprotobyname_r` are a glibc extension
 * that musl does not provide, so `HAVE_LINUX_GETNETBYNAME_R` /
 * `HAVE_LINUX_GETPROTOBYNAME_R` are defined by CMake instead.
 */

/* Compiler atomics (clang / gcc). */
#define HAVE___ATOMIC_LOAD_N 1
#define HAVE___ATOMIC_STORE_N 1

/*
 * glibc before 2.38 does NOT provide strlcat / strlcpy, so we rely on the
 * bundled missing/strl{cat,cpy}.c implementations. Do not define HAVE_STRLCAT /
 * HAVE_STRLCPY here; the CMake wrapper compiles the fallbacks.
 */

/* We don't have <net/bpf.h> style BPF; the null backend does not need it. */

/* SKF_AD_VLAN_TAG_PRESENT is not relied upon (no live linux capture). */
#define HAVE_DECL_SKF_AD_VLAN_TAG_PRESENT 0

/* Enable large-file support so fseeko/ftello work on 32-bit hosts too. */
#define _LARGEFILE_SOURCE 1

/* Sizes, taken from the compiler so this header stays architecture-agnostic. */
#ifdef __SIZEOF_POINTER__
#define SIZEOF_VOID_P __SIZEOF_POINTER__
#endif
/* time_t is the same width as a long on the platforms we target (LP64/ILP32). */
#ifdef __SIZEOF_LONG__
#define SIZEOF_TIME_T __SIZEOF_LONG__
#endif

/* Package identification. */
#define PACKAGE_NAME "libpcap"
#define PACKAGE_TARNAME "libpcap"
#define PACKAGE_VERSION "1.10.5"
#define PACKAGE_STRING "libpcap 1.10.5"
#define PACKAGE_BUGREPORT "libpcap-workers@lists.tcpdump.org"
#define PACKAGE_URL "https://www.tcpdump.org/"
