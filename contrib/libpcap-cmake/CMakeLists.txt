# libpcap, configured to ONLY read capture files (pcap / pcapng).
#
# We build the "null" capture backend (pcap-null.c): no live capture, no remote
# capture, no optional backends. This is all that the PCAP input format needs.
#
# BPF filtering is intentionally not built. The read path (savefile.c /
# sf-pcap.c / sf-pcapng.c) never compiles or runs a filter, so we drop gencode.c
# together with the BPF grammar/scanner. This also avoids shipping the
# GPL-licensed bison output (grammar.c) in the tree. The filter engine
# (bpf_filter.c, bpf_image.c, bpf_dump.c) is dropped as well, because on Linux it
# includes <linux/filter.h>, which is missing from some cross-compilation
# sysroots (loongarch64). Minimal stubs (compile-stub.c) are linked so any
# residual reference resolves cleanly.
#
# This target is only enabled when the PCAP input format (libtins) is enabled;
# the option lives in libtins-cmake and libpcap is added before it.

if (NOT ENABLE_PCAP OR NOT OS_LINUX)
    message(STATUS "Not using libpcap (the PCAP input format is supported only on Linux)")
    return()
endif()

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/libpcap")

set(SRCS
    "${LIBRARY_DIR}/etherent.c"
    "${LIBRARY_DIR}/fmtutils.c"
    "${LIBRARY_DIR}/nametoaddr.c"
    "${LIBRARY_DIR}/optimize.c"
    "${LIBRARY_DIR}/pcap-common.c"
    "${LIBRARY_DIR}/pcap-util.c"
    "${LIBRARY_DIR}/pcap.c"
    "${LIBRARY_DIR}/savefile.c"
    "${LIBRARY_DIR}/sf-pcapng.c"
    "${LIBRARY_DIR}/sf-pcap.c"
    "${LIBRARY_DIR}/pcap-null.c"     # the "null" capture backend
    # glibc < 2.38 lacks strlcat/strlcpy; the bundled fallbacks are always safe.
    "${LIBRARY_DIR}/missing/strlcat.c"
    "${LIBRARY_DIR}/missing/strlcpy.c"
    # Minimal stubs in place of the removed BPF grammar/scanner and filter engine.
    "${CMAKE_CURRENT_SOURCE_DIR}/compile-stub.c"
)

add_library(_pcap ${SRCS})
add_library(ch_contrib::pcap ALIAS _pcap)

# config.h lives in linux/.
target_include_directories(_pcap PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/linux"
    "${LIBRARY_DIR}"
)

# Public headers (pcap/pcap.h etc.) for consumers such as libtins.
target_include_directories(_pcap SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}")

# Build as a static, self-contained archive; no live-capture / remote defines.
target_compile_definitions(_pcap PRIVATE HAVE_CONFIG_H BUILDING_PCAP pcap_EXPORTS)

# glibc provides the GNU-style `strerror_r` returning `char *`, while musl provides
# the POSIX one returning `int`. Getting this wrong makes `pcap_strerror` interpret
# an integer as a pointer.
if (USE_MUSL)
    target_compile_definitions(_pcap PRIVATE HAVE_POSIX_STRERROR_R=1)
else()
    target_compile_definitions(_pcap PRIVATE HAVE_GNU_STRERROR_R=1)
endif()

# `getnetbyname_r` and `getprotobyname_r` are glibc extensions; musl has neither.
# libpcap then falls back to the non-reentrant `getnetbyname` / `getprotobyname`.
if (NOT USE_MUSL)
    target_compile_definitions(_pcap PRIVATE HAVE_LINUX_GETNETBYNAME_R=1 HAVE_LINUX_GETPROTOBYNAME_R=1)
endif()
