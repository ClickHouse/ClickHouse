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
    # x86 architecture

    list(APPEND LLAMA_ARCH_FLAGS -march=native)
    # TODO: Add LLAMA avx and fma optimizations

elseif (${CMAKE_SYSTEM_PROCESSOR} MATCHES "ppc64le")
    # PowerPC64LE CPU architecture (ClickHouse supports little-endian ppc64 only)

    list(APPEND LLAMA_ARCH_FLAGS -mcpu=powerpc64le)
endif()

#
# Posix conformance -- data types, macros and functions related to 
# controlling CPU affinity and memory allocation
#

set(LLAMA_COMPILE_DEFENITIONS "")
list(APPEND LLAMA_COMPILE_DEFENITIONS _XOPEN_SOURCE=600)

if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    list(APPEND LLAMA_COMPILE_DEFENITIONS _GNU_SOURCE)
endif()

if (CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
    list(APPEND LLAMA_COMPILE_DEFENITIONS __BSD_VISIBLE)
endif()

if (
    CMAKE_SYSTEM_NAME MATCHES "Darwin" OR
    CMAKE_SYSTEM_NAME MATCHES "iOS" OR
    CMAKE_SYSTEM_NAME MATCHES "DragonFly"
)
    list(APPEND LLAMA_COMPILE_DEFENITIONS _DARWIN_C_SOURCE)
endif()

#
# GGML library
#

add_library(_ggml OBJECT
    ${GGML_SRCS} ${GGML_HDRS}
)
target_include_directories(_ggml PUBLIC ${LLAMA_CPP_SOURCE_DIR})
target_link_libraries(_ggml PUBLIC Threads::Threads)


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

target_compile_definitions(_ggml PRIVATE ${LLAMA_COMPILE_DEFENITIONS})
target_compile_definitions(_llama PRIVATE ${LLAMA_COMPILE_DEFENITIONS})

target_compile_options(_ggml PRIVATE ${LLAMA_ARCH_FLAGS})
target_compile_options(_llama PRIVATE ${LLAMA_ARCH_FLAGS})

add_library(ch_contrib::llama ALIAS _llama)
