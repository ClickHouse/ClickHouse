set(LLAMA_CPP_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/llama-cpp")
set(LLAMA_CPP_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/llama-cpp")

set(GGML_SRCS 
    "${LLAMA_CPP_SOURCE_DIR}/ggml.c"
    "${LLAMA_CPP_SOURCE_DIR}/ggml-alloc.c"
    "${LLAMA_CPP_SOURCE_DIR}/ggml-backend.c"
    "${LLAMA_CPP_SOURCE_DIR}/ggml-quants.c"
)
set(GGML_HDRS
    "${LLAMA_CPP_SOURCE_DIR}/ggml.h"
    "${LLAMA_CPP_SOURCE_DIR}/ggml-alloc.h"
    "${LLAMA_CPP_SOURCE_DIR}/ggml-backend.h"
    "${LLAMA_CPP_SOURCE_DIR}/ggml-quants.h"
)

set(LLAMA_SRCS 
    "${LLAMA_CPP_SOURCE_DIR}/llama.cpp"
)
set(LLAMA_HDRS 
    "${LLAMA_CPP_SOURCE_DIR}/llama.h"
)

#
# GGML library
#

add_library(_ggml OBJECT
    ${GGML_SRCS} ${GGML_HDRS}
)
target_include_directories(_ggml PUBLIC ${LLAMA_CPP_SOURCE_DIR})
target_link_libraries(_ggml PUBLIC Threads::Threads)

# Posix conformance -- data types, macros and functions related to 
# controlling CPU affinity and ome memory allocation

if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    target_compile_definitions(_ggml PUBLIC _GNU_SOURCE)
endif()

if (CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
    target_compile_definitions(_ggml PUBLIC __BSD_VISIBLE)
endif()

if (
    CMAKE_SYSTEM_NAME MATCHES "Darwin" OR
    CMAKE_SYSTEM_NAME MATCHES "iOS" OR
    CMAKE_SYSTEM_NAME MATCHES "DragonFly"
)
    target_compile_definitions(_ggml PUBLIC _DARWIN_C_SOURCE)
endif()

#
# LLaMA library
#

add_library(_llama
    ${LLAMA_SRCS} ${LLAMA_HDRS}
)
target_include_directories(_llama PUBLIC ${LLAMA_CPP_SOURCE_DIR})
target_link_libraries(_llama PRIVATE
    _ggml
)

#
# Architecture-related optimization flags
#

set(LLAMA_ARCH_FLAGS "")

if ((${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm") OR (${CMAKE_SYSTEM_PROCESSOR} MATCHES "aarch64"))
    # ARM architecture

    check_cxx_compiler_flag(-mfp16-format=ieee COMPILER_SUPPORTS_FP16_FORMAT_I3E)
    if (NOT "${COMPILER_SUPPORTS_FP16_FORMAT_I3E}" STREQUAL "")
        list(APPEND LLAMA_ARCH_FLAGS -mfp16-format=ieee)
    endif()
    if (${CMAKE_SYSTEM_PROCESSOR} MATCHES "armv6")
        list(APPEND LLAMA_ARCH_FLAGS -mfpu=neon-fp-armv8 -mno-unaligned-access)
    endif()
    if (${CMAKE_SYSTEM_PROCESSOR} MATCHES "armv7")
        list(APPEND LLAMA_ARCH_FLAGS -mfpu=neon-fp-armv8 -mno-unaligned-access -funsafe-math-optimizations)
    endif()
    if (${CMAKE_SYSTEM_PROCESSOR} MATCHES "armv8")
        list(APPEND LLAMA_ARCH_FLAGS -mno-unaligned-access)
    endif()

elseif (${CMAKE_SYSTEM_PROCESSOR} MATCHES "^(x86_64|i686|AMD64)$")
    # X86 architecture
    message("X86")
    # TODO: Add LLAMA native flags

elseif (${CMAKE_SYSTEM_PROCESSOR} MATCHES "ppc64le")
    # PowerPC64LE CPU architecture (ClickHouse supports little-endian ppc64 only)

    list(APPEND ARCH_FLAGS -mcpu=powerpc64le)
endif()

target_compile_options(_ggml PRIVATE ${ARCH_FLAGS})
target_compile_options(_llama PRIVATE ${ARCH_FLAGS})

add_library(ch_contrib::llama ALIAS _llama)
