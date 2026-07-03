# libpcap, configured to ONLY read capture files (pcap / pcapng).
#
# We build the "null" capture backend (pcap-null.c): no live capture, no remote
# capture, no optional backends. This is all that the PCAP input format needs.
#
# The BPF filter grammar/scanner (grammar.c / scanner.c) would normally be
# generated at build time by bison and flex. To avoid adding those tools to the
# build toolchain, the generated files are checked in under generated/. They
# must still link even though the PCAP format never calls pcap_compile().
#
# This target is only enabled when the PCAP input format (libtins) is enabled;
# the option lives in libtins-cmake and libpcap is added before it.

if (NOT ENABLE_PCAP)
    message(STATUS "Not using libpcap")
    return()
endif()

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/libpcap")
set(GENERATED_DIR "${CMAKE_CURRENT_SOURCE_DIR}/generated")

set(SRCS
    "${LIBRARY_DIR}/bpf_dump.c"
    "${LIBRARY_DIR}/bpf_filter.c"
    "${LIBRARY_DIR}/bpf_image.c"
    "${LIBRARY_DIR}/etherent.c"
    "${LIBRARY_DIR}/fmtutils.c"
    "${LIBRARY_DIR}/gencode.c"
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
    # Pre-generated BPF grammar/scanner (bison/flex output).
    "${GENERATED_DIR}/grammar.c"
    "${GENERATED_DIR}/scanner.c"
)

add_library(_pcap ${SRCS})
add_library(ch_contrib::pcap ALIAS _pcap)

# config.h lives in linux/; generated grammar.h / scanner.h live in generated/.
target_include_directories(_pcap PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/linux"
    "${GENERATED_DIR}"
    "${LIBRARY_DIR}"
)

# Public headers (pcap/pcap.h etc.) for consumers such as libtins.
target_include_directories(_pcap SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}")

# Build as a static, self-contained archive; no live-capture / remote defines.
target_compile_definitions(_pcap PRIVATE HAVE_CONFIG_H BUILDING_PCAP pcap_EXPORTS)
