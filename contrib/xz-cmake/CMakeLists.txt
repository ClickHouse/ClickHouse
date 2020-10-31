#############################################################################
#
# Very limited CMake support for building some parts of XZ Utils
#
# For now, this is indented to be useful to build static or shared liblzma
# on Windows with MSVC (to avoid the need to maintain Visual Studio project
# files). Building liblzma on a few other platforms should work too but it
# is somewhat experimental and not as portable as using ./configure.
#
# On some platforms this builds also xz and xzdec, but these are
# highly experimental and meant for testing only:
#   - No large file support on those 32-bit platforms that need it
#   - No replacement getopt_long(), libc must have it
#   - No sandboxing support
#   - No translations
#   - No xz symlinks are installed
#
# Other missing things:
#   - No xzgrep or other scripts or their symlinks
#   - No tests (no test failures either!)
#
# NOTE: Even if the code compiles without warnings, the end result may be
# different than via ./configure. Specifically, the list of #defines
# may be different (if so, probably this CMakeLists.txt got them wrong).
#
# This file provides the following installation components (if you only
# need liblzma, install only its components!):
#   - liblzma_Runtime
#   - liblzma_Development
#   - xz (on some platforms only)
#   - xzdec (on some platforms only)
#
# To find the target liblzma::liblzma from other packages, use the CONFIG
# option with find_package() to avoid a conflict with the FindLibLZMA module
# with case-insensitive file systems. For example, to require liblzma 5.2.5
# or a newer compatible version:
#
#     find_package(liblzma 5.2.5 REQUIRED CONFIG)
#     target_link_libraries(my_application liblzma::liblzma)
#
#############################################################################
#
# Author: Lasse Collin
#
# This file has been put into the public domain.
# You can do whatever you want with this file.
#
#############################################################################

# Define library directory, where sources and header files are located
SET(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/xz/src)

# Read file with version
file(READ ${LIBRARY_DIR}/liblzma/api/lzma/version.h XZ_VERSION)
string(REGEX REPLACE
"^.*\n\
#define LZMA_VERSION_MAJOR ([0-9]+)\n\
#define LZMA_VERSION_MINOR ([0-9]+)\n\
#define LZMA_VERSION_PATCH ([0-9]+)\n\
.*$"
       "\\1.\\2.\\3" XZ_VERSION "${XZ_VERSION}")

# Parse version
MESSAGE(STATUS "LZMA VERSION ${XZ_VERSION}")

# cd contrib/xz/src
# find . -name '*.c' | grep -vP 'deprecated|legacy|/xz/' | sort | sed 's/^\./        ${LIBRARY_DIR}/'
SET(Sources
        ${LIBRARY_DIR}/common/tuklib_cpucores.c
        ${LIBRARY_DIR}/common/tuklib_exit.c
        ${LIBRARY_DIR}/common/tuklib_mbstr_fw.c
        ${LIBRARY_DIR}/common/tuklib_mbstr_width.c
        ${LIBRARY_DIR}/common/tuklib_open_stdxxx.c
        ${LIBRARY_DIR}/common/tuklib_physmem.c
        ${LIBRARY_DIR}/common/tuklib_progname.c
        ${LIBRARY_DIR}/liblzma/check/check.c
        ${LIBRARY_DIR}/liblzma/check/crc32_fast.c
        ${LIBRARY_DIR}/liblzma/check/crc32_small.c
        ${LIBRARY_DIR}/liblzma/check/crc32_table.c
        ${LIBRARY_DIR}/liblzma/check/crc32_tablegen.c
        ${LIBRARY_DIR}/liblzma/check/crc64_fast.c
        ${LIBRARY_DIR}/liblzma/check/crc64_small.c
        ${LIBRARY_DIR}/liblzma/check/crc64_table.c
        ${LIBRARY_DIR}/liblzma/check/crc64_tablegen.c
        ${LIBRARY_DIR}/liblzma/check/sha256.c
        ${LIBRARY_DIR}/liblzma/common/alone_decoder.c
        ${LIBRARY_DIR}/liblzma/common/alone_encoder.c
        ${LIBRARY_DIR}/liblzma/common/auto_decoder.c
        ${LIBRARY_DIR}/liblzma/common/block_buffer_decoder.c
        ${LIBRARY_DIR}/liblzma/common/block_buffer_encoder.c
        ${LIBRARY_DIR}/liblzma/common/block_decoder.c
        ${LIBRARY_DIR}/liblzma/common/block_encoder.c
        ${LIBRARY_DIR}/liblzma/common/block_header_decoder.c
        ${LIBRARY_DIR}/liblzma/common/block_header_encoder.c
        ${LIBRARY_DIR}/liblzma/common/block_util.c
        ${LIBRARY_DIR}/liblzma/common/common.c
        ${LIBRARY_DIR}/liblzma/common/easy_buffer_encoder.c
        ${LIBRARY_DIR}/liblzma/common/easy_decoder_memusage.c
        ${LIBRARY_DIR}/liblzma/common/easy_encoder.c
        ${LIBRARY_DIR}/liblzma/common/easy_encoder_memusage.c
        ${LIBRARY_DIR}/liblzma/common/easy_preset.c
        ${LIBRARY_DIR}/liblzma/common/file_info.c
        ${LIBRARY_DIR}/liblzma/common/filter_buffer_decoder.c
        ${LIBRARY_DIR}/liblzma/common/filter_buffer_encoder.c
        ${LIBRARY_DIR}/liblzma/common/filter_common.c
        ${LIBRARY_DIR}/liblzma/common/filter_decoder.c
        ${LIBRARY_DIR}/liblzma/common/filter_encoder.c
        ${LIBRARY_DIR}/liblzma/common/filter_flags_decoder.c
        ${LIBRARY_DIR}/liblzma/common/filter_flags_encoder.c
        ${LIBRARY_DIR}/liblzma/common/hardware_cputhreads.c
        ${LIBRARY_DIR}/liblzma/common/hardware_physmem.c
        ${LIBRARY_DIR}/liblzma/common/index.c
        ${LIBRARY_DIR}/liblzma/common/index_decoder.c
        ${LIBRARY_DIR}/liblzma/common/index_encoder.c
        ${LIBRARY_DIR}/liblzma/common/index_hash.c
        ${LIBRARY_DIR}/liblzma/common/outqueue.c
        ${LIBRARY_DIR}/liblzma/common/stream_buffer_decoder.c
        ${LIBRARY_DIR}/liblzma/common/stream_buffer_encoder.c
        ${LIBRARY_DIR}/liblzma/common/stream_decoder.c
        ${LIBRARY_DIR}/liblzma/common/stream_encoder.c
        ${LIBRARY_DIR}/liblzma/common/stream_encoder_mt.c
        ${LIBRARY_DIR}/liblzma/common/stream_flags_common.c
        ${LIBRARY_DIR}/liblzma/common/stream_flags_decoder.c
        ${LIBRARY_DIR}/liblzma/common/stream_flags_encoder.c
        ${LIBRARY_DIR}/liblzma/common/vli_decoder.c
        ${LIBRARY_DIR}/liblzma/common/vli_encoder.c
        ${LIBRARY_DIR}/liblzma/common/vli_size.c
        ${LIBRARY_DIR}/liblzma/delta/delta_common.c
        ${LIBRARY_DIR}/liblzma/delta/delta_decoder.c
        ${LIBRARY_DIR}/liblzma/delta/delta_encoder.c
        ${LIBRARY_DIR}/liblzma/lz/lz_decoder.c
        ${LIBRARY_DIR}/liblzma/lz/lz_encoder.c
        ${LIBRARY_DIR}/liblzma/lz/lz_encoder_mf.c
        ${LIBRARY_DIR}/liblzma/lzma/fastpos_table.c
        ${LIBRARY_DIR}/liblzma/lzma/fastpos_tablegen.c
        ${LIBRARY_DIR}/liblzma/lzma/lzma2_decoder.c
        ${LIBRARY_DIR}/liblzma/lzma/lzma2_encoder.c
        ${LIBRARY_DIR}/liblzma/lzma/lzma_decoder.c
        ${LIBRARY_DIR}/liblzma/lzma/lzma_encoder.c
        ${LIBRARY_DIR}/liblzma/lzma/lzma_encoder_optimum_fast.c
        ${LIBRARY_DIR}/liblzma/lzma/lzma_encoder_optimum_normal.c
        ${LIBRARY_DIR}/liblzma/lzma/lzma_encoder_presets.c
        ${LIBRARY_DIR}/liblzma/rangecoder/price_table.c
        ${LIBRARY_DIR}/liblzma/rangecoder/price_tablegen.c
        ${LIBRARY_DIR}/liblzma/simple/arm.c
        ${LIBRARY_DIR}/liblzma/simple/armthumb.c
        ${LIBRARY_DIR}/liblzma/simple/ia64.c
        ${LIBRARY_DIR}/liblzma/simple/powerpc.c
        ${LIBRARY_DIR}/liblzma/simple/simple_coder.c
        ${LIBRARY_DIR}/liblzma/simple/simple_decoder.c
        ${LIBRARY_DIR}/liblzma/simple/simple_encoder.c
        ${LIBRARY_DIR}/liblzma/simple/sparc.c
        ${LIBRARY_DIR}/liblzma/simple/x86.c
        ${LIBRARY_DIR}/lzmainfo/lzmainfo.c
)

# cd contrib/xz/src
# find . -name '*.h' | grep -vP 'deprecated|legacy|/xz/' | sort | sed 's/^\./        ${LIBRARY_DIR}/'
SET(Headers
        ${LIBRARY_DIR}/common/mythread.h
        ${LIBRARY_DIR}/common/sysdefs.h
        ${LIBRARY_DIR}/common/tuklib_common.h
        ${LIBRARY_DIR}/common/tuklib_config.h
        ${LIBRARY_DIR}/common/tuklib_cpucores.h
        ${LIBRARY_DIR}/common/tuklib_exit.h
        ${LIBRARY_DIR}/common/tuklib_gettext.h
        ${LIBRARY_DIR}/common/tuklib_integer.h
        ${LIBRARY_DIR}/common/tuklib_mbstr.h
        ${LIBRARY_DIR}/common/tuklib_open_stdxxx.h
        ${LIBRARY_DIR}/common/tuklib_physmem.h
        ${LIBRARY_DIR}/common/tuklib_progname.h
        ${LIBRARY_DIR}/liblzma/api/lzma/base.h
        ${LIBRARY_DIR}/liblzma/api/lzma/bcj.h
        ${LIBRARY_DIR}/liblzma/api/lzma/block.h
        ${LIBRARY_DIR}/liblzma/api/lzma/check.h
        ${LIBRARY_DIR}/liblzma/api/lzma/container.h
        ${LIBRARY_DIR}/liblzma/api/lzma/delta.h
        ${LIBRARY_DIR}/liblzma/api/lzma/filter.h
        ${LIBRARY_DIR}/liblzma/api/lzma.h
        ${LIBRARY_DIR}/liblzma/api/lzma/hardware.h
        ${LIBRARY_DIR}/liblzma/api/lzma/index.h
        ${LIBRARY_DIR}/liblzma/api/lzma/index_hash.h
        ${LIBRARY_DIR}/liblzma/api/lzma/lzma12.h
        ${LIBRARY_DIR}/liblzma/api/lzma/stream_flags.h
        ${LIBRARY_DIR}/liblzma/api/lzma/version.h
        ${LIBRARY_DIR}/liblzma/api/lzma/vli.h
        ${LIBRARY_DIR}/liblzma/check/check.h
        ${LIBRARY_DIR}/liblzma/check/crc32_table_be.h
        ${LIBRARY_DIR}/liblzma/check/crc32_table_le.h
        ${LIBRARY_DIR}/liblzma/check/crc64_table_be.h
        ${LIBRARY_DIR}/liblzma/check/crc64_table_le.h
        ${LIBRARY_DIR}/liblzma/check/crc_macros.h
        ${LIBRARY_DIR}/liblzma/common/alone_decoder.h
        ${LIBRARY_DIR}/liblzma/common/block_buffer_encoder.h
        ${LIBRARY_DIR}/liblzma/common/block_decoder.h
        ${LIBRARY_DIR}/liblzma/common/block_encoder.h
        ${LIBRARY_DIR}/liblzma/common/common.h
        ${LIBRARY_DIR}/liblzma/common/easy_preset.h
        ${LIBRARY_DIR}/liblzma/common/filter_common.h
        ${LIBRARY_DIR}/liblzma/common/filter_decoder.h
        ${LIBRARY_DIR}/liblzma/common/filter_encoder.h
        ${LIBRARY_DIR}/liblzma/common/index_decoder.h
        ${LIBRARY_DIR}/liblzma/common/index_encoder.h
        ${LIBRARY_DIR}/liblzma/common/index.h
        ${LIBRARY_DIR}/liblzma/common/memcmplen.h
        ${LIBRARY_DIR}/liblzma/common/outqueue.h
        ${LIBRARY_DIR}/liblzma/common/stream_decoder.h
        ${LIBRARY_DIR}/liblzma/common/stream_flags_common.h
        ${LIBRARY_DIR}/liblzma/delta/delta_common.h
        ${LIBRARY_DIR}/liblzma/delta/delta_decoder.h
        ${LIBRARY_DIR}/liblzma/delta/delta_encoder.h
        ${LIBRARY_DIR}/liblzma/delta/delta_private.h
        ${LIBRARY_DIR}/liblzma/lz/lz_decoder.h
        ${LIBRARY_DIR}/liblzma/lz/lz_encoder.h
        ${LIBRARY_DIR}/liblzma/lz/lz_encoder_hash.h
        ${LIBRARY_DIR}/liblzma/lz/lz_encoder_hash_table.h
        ${LIBRARY_DIR}/liblzma/lzma/fastpos.h
        ${LIBRARY_DIR}/liblzma/lzma/lzma2_decoder.h
        ${LIBRARY_DIR}/liblzma/lzma/lzma2_encoder.h
        ${LIBRARY_DIR}/liblzma/lzma/lzma_common.h
        ${LIBRARY_DIR}/liblzma/lzma/lzma_decoder.h
        ${LIBRARY_DIR}/liblzma/lzma/lzma_encoder.h
        ${LIBRARY_DIR}/liblzma/lzma/lzma_encoder_private.h
        ${LIBRARY_DIR}/liblzma/rangecoder/price.h
        ${LIBRARY_DIR}/liblzma/rangecoder/range_common.h
        ${LIBRARY_DIR}/liblzma/rangecoder/range_decoder.h
        ${LIBRARY_DIR}/liblzma/rangecoder/range_encoder.h
        ${LIBRARY_DIR}/liblzma/simple/simple_coder.h
        ${LIBRARY_DIR}/liblzma/simple/simple_decoder.h
        ${LIBRARY_DIR}/liblzma/simple/simple_encoder.h
        ${LIBRARY_DIR}/liblzma/simple/simple_private.h
)

ADD_LIBRARY(liblzma ${Sources} ${Headers})

target_include_directories(liblzma PUBLIC
    ${LIBRARY_DIR}/liblzma/api
    ${LIBRARY_DIR}/liblzma/common
    ${LIBRARY_DIR}/liblzma/check
    ${LIBRARY_DIR}/liblzma/lz
    ${LIBRARY_DIR}/liblzma/rangecoder
    ${LIBRARY_DIR}/liblzma/lzma
    ${LIBRARY_DIR}/liblzma/delta
    ${LIBRARY_DIR}/liblzma/simple
    ${LIBRARY_DIR}/common
)