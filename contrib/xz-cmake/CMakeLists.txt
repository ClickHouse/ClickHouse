set (SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/xz")

# Author: Lasse Collin
#
# This file has been put into the public domain.
# You can do whatever you want with this file.
#
# The file was edited for ClickHouse

# Get the package version from version.h into XZ_VERSION variable.
file(READ ${SRC_DIR}/src/liblzma/api/lzma/version.h XZ_VERSION)
string(REGEX REPLACE
"^.*\n\
#define LZMA_VERSION_MAJOR ([0-9]+)\n\
#define LZMA_VERSION_MINOR ([0-9]+)\n\
#define LZMA_VERSION_PATCH ([0-9]+)\n\
.*$"
       "\\1.\\2.\\3" XZ_VERSION "${XZ_VERSION}")

# Definitions common to all targets:
add_compile_definitions(
    # Package info:
    PACKAGE_NAME="XZ Utils"
    PACKAGE_BUGREPORT="lasse.collin@tukaani.org"
    PACKAGE_URL="https://tukaani.org/xz/"

    # Features:
    HAVE_CHECK_CRC32
    HAVE_CHECK_CRC64
    HAVE_CHECK_SHA256
    HAVE_DECODERS
    HAVE_DECODER_ARM
    HAVE_DECODER_ARMTHUMB
    HAVE_DECODER_DELTA
    HAVE_DECODER_IA64
    HAVE_DECODER_LZMA1
    HAVE_DECODER_LZMA2
    HAVE_DECODER_POWERPC
    HAVE_DECODER_SPARC
    HAVE_DECODER_X86
    HAVE_ENCODERS
    HAVE_ENCODER_ARM
    HAVE_ENCODER_ARMTHUMB
    HAVE_ENCODER_DELTA
    HAVE_ENCODER_IA64
    HAVE_ENCODER_LZMA1
    HAVE_ENCODER_LZMA2
    HAVE_ENCODER_POWERPC
    HAVE_ENCODER_SPARC
    HAVE_ENCODER_X86
    HAVE_MF_BT2
    HAVE_MF_BT3
    HAVE_MF_BT4
    HAVE_MF_HC3
    HAVE_MF_HC4

    # Standard headers and types are available:
    HAVE_STDBOOL_H
    HAVE__BOOL
    HAVE_STDINT_H
    HAVE_INTTYPES_H

    HAVE___BUILTIN_BSWAPXX
    HAVE___BUILTIN_ASSUME_ALIGNED

    _GNU_SOURCE
    __EXTENSIONS__
    _POSIX_PTHREAD_SEMANTICS
    _TANDEM_SOURCE
    _ALL_SOURCE

    HAVE_CLOCK_GETTIME=1
    HAVE_DECL_CLOCK_MONOTONIC=1

    HAVE_PTHREAD_CONDATTR_SETCLOCK
    MYTHREAD_POSIX
)

if (OS_LINUX)
    add_compile_definitions(
        TUKLIB_CPUCORES_SCHED_GETAFFINITY
        TUKLIB_PHYSMEM_SYSCONF)
elseif (OS_FREEBSD)
    add_compile_definitions(
        TUKLIB_CPUCORES_CPUSET
        TUKLIB_PHYSMEM_SYSCTL)
elseif (OS_DARWIN)
    add_compile_definitions(
        TUKLIB_CPUCORES_SYSCTL
        TUKLIB_PHYSMEM_SYSCTL)
endif ()

if (ARCH_AMD64 OR ARCH_AARCH64)
    add_compile_definitions(TUKLIB_FAST_UNALIGNED_ACCESS=1)
endif ()

find_package(Threads REQUIRED)


add_library(_liblzma
    ${SRC_DIR}/src/common/mythread.h
    ${SRC_DIR}/src/common/sysdefs.h
    ${SRC_DIR}/src/common/tuklib_common.h
    ${SRC_DIR}/src/common/tuklib_config.h
    ${SRC_DIR}/src/common/tuklib_cpucores.c
    ${SRC_DIR}/src/common/tuklib_cpucores.h
    ${SRC_DIR}/src/common/tuklib_integer.h
    ${SRC_DIR}/src/common/tuklib_physmem.c
    ${SRC_DIR}/src/common/tuklib_physmem.h
    ${SRC_DIR}/src/liblzma/api/lzma.h
    ${SRC_DIR}/src/liblzma/api/lzma/base.h
    ${SRC_DIR}/src/liblzma/api/lzma/bcj.h
    ${SRC_DIR}/src/liblzma/api/lzma/block.h
    ${SRC_DIR}/src/liblzma/api/lzma/check.h
    ${SRC_DIR}/src/liblzma/api/lzma/container.h
    ${SRC_DIR}/src/liblzma/api/lzma/delta.h
    ${SRC_DIR}/src/liblzma/api/lzma/filter.h
    ${SRC_DIR}/src/liblzma/api/lzma/hardware.h
    ${SRC_DIR}/src/liblzma/api/lzma/index.h
    ${SRC_DIR}/src/liblzma/api/lzma/index_hash.h
    ${SRC_DIR}/src/liblzma/api/lzma/lzma12.h
    ${SRC_DIR}/src/liblzma/api/lzma/stream_flags.h
    ${SRC_DIR}/src/liblzma/api/lzma/version.h
    ${SRC_DIR}/src/liblzma/api/lzma/vli.h
    ${SRC_DIR}/src/liblzma/check/check.c
    ${SRC_DIR}/src/liblzma/check/check.h
    ${SRC_DIR}/src/liblzma/check/crc32_fast.c
    ${SRC_DIR}/src/liblzma/check/crc32_table.c
    ${SRC_DIR}/src/liblzma/check/crc32_table_be.h
    ${SRC_DIR}/src/liblzma/check/crc32_table_le.h
    ${SRC_DIR}/src/liblzma/check/crc64_fast.c
    ${SRC_DIR}/src/liblzma/check/crc64_table.c
    ${SRC_DIR}/src/liblzma/check/crc64_table_be.h
    ${SRC_DIR}/src/liblzma/check/crc64_table_le.h
    ${SRC_DIR}/src/liblzma/check/crc_macros.h
    ${SRC_DIR}/src/liblzma/check/sha256.c
    ${SRC_DIR}/src/liblzma/common/alone_decoder.c
    ${SRC_DIR}/src/liblzma/common/alone_decoder.h
    ${SRC_DIR}/src/liblzma/common/alone_encoder.c
    ${SRC_DIR}/src/liblzma/common/auto_decoder.c
    ${SRC_DIR}/src/liblzma/common/block_buffer_decoder.c
    ${SRC_DIR}/src/liblzma/common/block_buffer_encoder.c
    ${SRC_DIR}/src/liblzma/common/block_buffer_encoder.h
    ${SRC_DIR}/src/liblzma/common/block_decoder.c
    ${SRC_DIR}/src/liblzma/common/block_decoder.h
    ${SRC_DIR}/src/liblzma/common/block_encoder.c
    ${SRC_DIR}/src/liblzma/common/block_encoder.h
    ${SRC_DIR}/src/liblzma/common/block_header_decoder.c
    ${SRC_DIR}/src/liblzma/common/block_header_encoder.c
    ${SRC_DIR}/src/liblzma/common/block_util.c
    ${SRC_DIR}/src/liblzma/common/common.c
    ${SRC_DIR}/src/liblzma/common/common.h
    ${SRC_DIR}/src/liblzma/common/easy_buffer_encoder.c
    ${SRC_DIR}/src/liblzma/common/easy_decoder_memusage.c
    ${SRC_DIR}/src/liblzma/common/easy_encoder.c
    ${SRC_DIR}/src/liblzma/common/easy_encoder_memusage.c
    ${SRC_DIR}/src/liblzma/common/easy_preset.c
    ${SRC_DIR}/src/liblzma/common/easy_preset.h
    ${SRC_DIR}/src/liblzma/common/file_info.c
    ${SRC_DIR}/src/liblzma/common/filter_buffer_decoder.c
    ${SRC_DIR}/src/liblzma/common/filter_buffer_encoder.c
    ${SRC_DIR}/src/liblzma/common/filter_common.c
    ${SRC_DIR}/src/liblzma/common/filter_common.h
    ${SRC_DIR}/src/liblzma/common/filter_decoder.c
    ${SRC_DIR}/src/liblzma/common/filter_decoder.h
    ${SRC_DIR}/src/liblzma/common/filter_encoder.c
    ${SRC_DIR}/src/liblzma/common/filter_encoder.h
    ${SRC_DIR}/src/liblzma/common/filter_flags_decoder.c
    ${SRC_DIR}/src/liblzma/common/filter_flags_encoder.c
    ${SRC_DIR}/src/liblzma/common/hardware_cputhreads.c
    ${SRC_DIR}/src/liblzma/common/hardware_physmem.c
    ${SRC_DIR}/src/liblzma/common/index.c
    ${SRC_DIR}/src/liblzma/common/index.h
    ${SRC_DIR}/src/liblzma/common/index_decoder.c
    ${SRC_DIR}/src/liblzma/common/index_decoder.h
    ${SRC_DIR}/src/liblzma/common/index_encoder.c
    ${SRC_DIR}/src/liblzma/common/index_encoder.h
    ${SRC_DIR}/src/liblzma/common/index_hash.c
    ${SRC_DIR}/src/liblzma/common/memcmplen.h
    ${SRC_DIR}/src/liblzma/common/outqueue.c
    ${SRC_DIR}/src/liblzma/common/outqueue.h
    ${SRC_DIR}/src/liblzma/common/stream_buffer_decoder.c
    ${SRC_DIR}/src/liblzma/common/stream_buffer_encoder.c
    ${SRC_DIR}/src/liblzma/common/stream_decoder.c
    ${SRC_DIR}/src/liblzma/common/stream_decoder.h
    ${SRC_DIR}/src/liblzma/common/stream_encoder.c
    ${SRC_DIR}/src/liblzma/common/stream_encoder_mt.c
    ${SRC_DIR}/src/liblzma/common/stream_flags_common.c
    ${SRC_DIR}/src/liblzma/common/stream_flags_common.h
    ${SRC_DIR}/src/liblzma/common/stream_flags_decoder.c
    ${SRC_DIR}/src/liblzma/common/stream_flags_encoder.c
    ${SRC_DIR}/src/liblzma/common/vli_decoder.c
    ${SRC_DIR}/src/liblzma/common/vli_encoder.c
    ${SRC_DIR}/src/liblzma/common/vli_size.c
    ${SRC_DIR}/src/liblzma/delta/delta_common.c
    ${SRC_DIR}/src/liblzma/delta/delta_common.h
    ${SRC_DIR}/src/liblzma/delta/delta_decoder.c
    ${SRC_DIR}/src/liblzma/delta/delta_decoder.h
    ${SRC_DIR}/src/liblzma/delta/delta_encoder.c
    ${SRC_DIR}/src/liblzma/delta/delta_encoder.h
    ${SRC_DIR}/src/liblzma/delta/delta_private.h
    ${SRC_DIR}/src/liblzma/lz/lz_decoder.c
    ${SRC_DIR}/src/liblzma/lz/lz_decoder.h
    ${SRC_DIR}/src/liblzma/lz/lz_encoder.c
    ${SRC_DIR}/src/liblzma/lz/lz_encoder.h
    ${SRC_DIR}/src/liblzma/lz/lz_encoder_hash.h
    ${SRC_DIR}/src/liblzma/lz/lz_encoder_hash_table.h
    ${SRC_DIR}/src/liblzma/lz/lz_encoder_mf.c
    ${SRC_DIR}/src/liblzma/lzma/fastpos.h
    ${SRC_DIR}/src/liblzma/lzma/fastpos_table.c
    ${SRC_DIR}/src/liblzma/lzma/lzma2_decoder.c
    ${SRC_DIR}/src/liblzma/lzma/lzma2_decoder.h
    ${SRC_DIR}/src/liblzma/lzma/lzma2_encoder.c
    ${SRC_DIR}/src/liblzma/lzma/lzma2_encoder.h
    ${SRC_DIR}/src/liblzma/lzma/lzma_common.h
    ${SRC_DIR}/src/liblzma/lzma/lzma_decoder.c
    ${SRC_DIR}/src/liblzma/lzma/lzma_decoder.h
    ${SRC_DIR}/src/liblzma/lzma/lzma_encoder.c
    ${SRC_DIR}/src/liblzma/lzma/lzma_encoder.h
    ${SRC_DIR}/src/liblzma/lzma/lzma_encoder_optimum_fast.c
    ${SRC_DIR}/src/liblzma/lzma/lzma_encoder_optimum_normal.c
    ${SRC_DIR}/src/liblzma/lzma/lzma_encoder_presets.c
    ${SRC_DIR}/src/liblzma/lzma/lzma_encoder_private.h
    ${SRC_DIR}/src/liblzma/rangecoder/price.h
    ${SRC_DIR}/src/liblzma/rangecoder/price_table.c
    ${SRC_DIR}/src/liblzma/rangecoder/range_common.h
    ${SRC_DIR}/src/liblzma/rangecoder/range_decoder.h
    ${SRC_DIR}/src/liblzma/rangecoder/range_encoder.h
    ${SRC_DIR}/src/liblzma/simple/arm.c
    ${SRC_DIR}/src/liblzma/simple/armthumb.c
    ${SRC_DIR}/src/liblzma/simple/ia64.c
    ${SRC_DIR}/src/liblzma/simple/powerpc.c
    ${SRC_DIR}/src/liblzma/simple/simple_coder.c
    ${SRC_DIR}/src/liblzma/simple/simple_coder.h
    ${SRC_DIR}/src/liblzma/simple/simple_decoder.c
    ${SRC_DIR}/src/liblzma/simple/simple_decoder.h
    ${SRC_DIR}/src/liblzma/simple/simple_encoder.c
    ${SRC_DIR}/src/liblzma/simple/simple_encoder.h
    ${SRC_DIR}/src/liblzma/simple/simple_private.h
    ${SRC_DIR}/src/liblzma/simple/sparc.c
    ${SRC_DIR}/src/liblzma/simple/x86.c
)

target_include_directories(_liblzma SYSTEM PRIVATE
    ${SRC_DIR}/src/liblzma/api
    ${SRC_DIR}/src/liblzma/common
    ${SRC_DIR}/src/liblzma/check
    ${SRC_DIR}/src/liblzma/lz
    ${SRC_DIR}/src/liblzma/rangecoder
    ${SRC_DIR}/src/liblzma/lzma
    ${SRC_DIR}/src/liblzma/delta
    ${SRC_DIR}/src/liblzma/simple
    ${SRC_DIR}/src/common
)
target_include_directories(_liblzma SYSTEM BEFORE PUBLIC ${SRC_DIR}/src/liblzma/api)

target_link_libraries(_liblzma Threads::Threads)

# Put the tuklib functions under the lzma_ namespace.
target_compile_definitions(_liblzma PRIVATE TUKLIB_SYMBOL_PREFIX=lzma_)

if (ENABLE_SSE2)
    target_compile_definitions(_liblzma PRIVATE HAVE_IMMINTRIN_H HAVE__MM_MOVEMASK_EPI8)
endif()

add_library(ch_contrib::xz ALIAS _liblzma)
