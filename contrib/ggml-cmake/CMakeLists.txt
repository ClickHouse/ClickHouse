set(GGML_DIR "${ClickHouse_SOURCE_DIR}/contrib/ggml")
set(GGML_CPP_SOURCE_DIR "${GGML_DIR}/src")
set(GGML_CPP_INCLUDE_DIR "${GGML_DIR}/include")

set(GGML_SRCS
    "${GGML_CPP_SOURCE_DIR}/ggml.c"
    "${GGML_CPP_SOURCE_DIR}/ggml-alloc.c"
    "${GGML_CPP_SOURCE_DIR}/ggml-backend.c"
    "${GGML_CPP_SOURCE_DIR}/ggml-quants.c"
    "${GGML_CPP_SOURCE_DIR}/ggml-aarch64.c"
)
set (GGML_PUBLIC_HDRS
    ${GGML_CPP_INCLUDE_DIR}/ggml.h
    ${GGML_CPP_INCLUDE_DIR}/ggml-alloc.h
    ${GGML_CPP_INCLUDE_DIR}/ggml-backend.h
)
set(GGML_HDRS
    "${GGML_CPP_SOURCE_DIR}/ggml-backend-impl.h"
    "${GGML_CPP_SOURCE_DIR}/ggml-impl.h"
    ${GGML_PUBLIC_HDRS}
)

#
# Architecture-related optimization flags
#

set(GGML_ARCH_FLAGS "")

if (ARCH_AARCH64)
    # ARM architecture

    if (${CMAKE_SYSTEM_PROCESSOR} MATCHES "armv6")
        list(APPEND GGML_ARCH_FLAGS -mfpu=neon-fp-armv8 -mno-unaligned-access)
    endif()
    if (${CMAKE_SYSTEM_PROCESSOR} MATCHES "armv7")
        list(APPEND GGML_ARCH_FLAGS -mfpu=neon-fp-armv8 -mno-unaligned-access -funsafe-math-optimizations)
    endif()
    if (${CMAKE_SYSTEM_PROCESSOR} MATCHES "armv8")
        list(APPEND GGML_ARCH_FLAGS -mno-unaligned-access)
    endif()

elseif (ARCH_AMD64)
    # x86 architecture

    macro(ggml_append_intrinsic intrin darwin_intrin)
        if (OS_DARWIN)
            execute_process(COMMAND sysctl hw.optional.${darwin_intrin} OUTPUT_VARIABLE GGML_${intrin})
        elseif(OS_LINUX)
            execute_process(COMMAND grep "${intrin} " /proc/cpuinfo OUTPUT_VARIABLE GGML_${intrin})
        endif()

        if ((OS_DARWIN AND "${GGML_${intrin}}" MATCHES "${darwin_intrin}") OR (OS_LINUX AND "${GGML_${intrin}}" MATCHES "${intrin}"))
            list(APPEND GGML_ARCH_FLAGS "-m${intrin}")
        endif()
    endmacro()

    ggml_append_intrinsic(f16c f16c)
    ggml_append_intrinsic(avx avx1_0)
    ggml_append_intrinsic(avx2 avx2_0)
    ggml_append_intrinsic(fma fma)
    # ggml_append_intrinsic(avx512f avx512f)
    # ggml_append_intrinsic(avx512vbmi avx512vbmi)

elseif (ARCH_PPC64LE)
    # PowerPC64LE CPU architecture (ClickHouse supports little-endian ppc64 only)

    list(APPEND GGML_ARCH_FLAGS -U__SSE2__=1) # build without SSE
    if (POWER9)
        list(APPEND GGML_ARCH_FLAGS -mpower9-vector)
    endif()
endif()

#
# Posix conformance -- data types, macros and functions related to
# controlling CPU affinity and memory allocation
#

set(GGML_COMPILE_DEFENITIONS "")
list(APPEND GGML_COMPILE_DEFENITIONS _XOPEN_SOURCE=600)

if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    list(APPEND GGML_COMPILE_DEFENITIONS _GNU_SOURCE)
endif()

if (CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
    list(APPEND GGML_COMPILE_DEFENITIONS __BSD_VISIBLE)
endif()

if (
    CMAKE_SYSTEM_NAME MATCHES "Darwin" OR
    CMAKE_SYSTEM_NAME MATCHES "iOS" OR
    CMAKE_SYSTEM_NAME MATCHES "DragonFly"
)
    list(APPEND GGML_COMPILE_DEFENITIONS _DARWIN_C_SOURCE)
endif()

#
# GGML library
#

add_library(_ggml
    ${GGML_SRCS} ${GGML_HDRS}
)
target_include_directories(_ggml PUBLIC ${GGML_CPP_INCLUDE_DIR} ${GGML_CPP_INCLUDE_DIR}/ggml)

target_compile_options(_ggml PRIVATE ${GGML_ARCH_FLAGS})
target_compile_definitions(_ggml PRIVATE ${GGML_COMPILE_DEFENITIONS})
set_target_properties(_ggml PROPERTIES PUBLIC_HEADER ${GGML_PUBLIC_HDRS})

#
# Dependencies
#

target_link_libraries(_ggml PUBLIC ${CMAKE_THREAD_LIBS_INIT})

find_library(MATH_LIB m) # -lm
if (MATH_LIB)
    target_link_libraries(_ggml PUBLIC ${MATH_LIB})
endif()

#
# ClickHouse alias
#

add_library(ch_contrib::ggml ALIAS _ggml)

