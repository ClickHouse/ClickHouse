## The Intel® QPL provides high performance implementations of data processing functions for existing hardware accelerator, and/or software path in case if hardware accelerator is not available.
set (UUID_DIR "${ClickHouse_SOURCE_DIR}/contrib/qpl-cmake")
set (QPL_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/qpl")
set (QPL_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/qpl/sources")
set (QPL_BINARY_DIR "${ClickHouse_BINARY_DIR}/build/contrib/qpl")
set (EFFICIENT_WAIT OFF)
set (BLOCK_ON_FAULT ON)
set (LOG_HW_INIT OFF)
set (SANITIZE_MEMORY OFF)
set (SANITIZE_THREADS OFF)
set (LIB_FUZZING_ENGINE OFF)
set (DYNAMIC_LOADING_LIBACCEL_CONFIG OFF)

function(GetLibraryVersion _content _outputVar)
    string(REGEX MATCHALL "QPL VERSION (.+) LANGUAGES" VERSION_REGEX "${_content}")
    SET(${_outputVar} ${CMAKE_MATCH_1} PARENT_SCOPE)
endfunction()

FILE(READ "${QPL_PROJECT_DIR}/CMakeLists.txt" HEADER_CONTENT)
GetLibraryVersion("${HEADER_CONTENT}" QPL_VERSION)

message(STATUS "Intel QPL version: ${QPL_VERSION}")

# There are 5 source subdirectories under $QPL_SRC_DIR: isal, c_api, core-sw, middle-layer, c_api.
# Generate 8 library targets: middle_layer_lib, isal, isal_asm, qplcore_px, qplcore_avx512, qplcore_sw_dispatcher, core_iaa, middle_layer_lib.
# Output ch_contrib::qpl by linking with 8 library targets.

# The qpl submodule comes with its own version of isal. It contains code which does not exist in upstream isal. It would be nice to link
# only upstream isal (ch_contrib::isal) but at this point we can't.

include("${QPL_PROJECT_DIR}/cmake/CompileOptions.cmake")

# check nasm compiler
include(CheckLanguage)
check_language(ASM_NASM)
if(NOT CMAKE_ASM_NASM_COMPILER)
    message(FATAL_ERROR "Please install NASM from 'https://www.nasm.us/' because NASM compiler can not be found!")
endif()

# [SUBDIR]isal
enable_language(ASM_NASM)

set(ISAL_C_SRC ${QPL_SRC_DIR}/isal/igzip/adler32_base.c
               ${QPL_SRC_DIR}/isal/igzip/huff_codes.c
               ${QPL_SRC_DIR}/isal/igzip/hufftables_c.c
               ${QPL_SRC_DIR}/isal/igzip/igzip.c
               ${QPL_SRC_DIR}/isal/igzip/igzip_base.c
               ${QPL_SRC_DIR}/isal/igzip/flatten_ll.c
               ${QPL_SRC_DIR}/isal/igzip/encode_df.c
               ${QPL_SRC_DIR}/isal/igzip/igzip_icf_base.c
               ${QPL_SRC_DIR}/isal/igzip/igzip_inflate.c
               ${QPL_SRC_DIR}/isal/igzip/igzip_icf_body.c
               ${QPL_SRC_DIR}/isal/crc/crc_base.c
               ${QPL_SRC_DIR}/isal/crc/crc64_base.c)

set(ISAL_ASM_SRC ${QPL_SRC_DIR}/isal/igzip/igzip_body.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_gen_icf_map_lh1_04.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_gen_icf_map_lh1_06.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_decode_block_stateless_04.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_finish.asm
                 ${QPL_SRC_DIR}/isal/igzip/encode_df_04.asm
                 ${QPL_SRC_DIR}/isal/igzip/encode_df_06.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_decode_block_stateless_01.asm
                 ${QPL_SRC_DIR}/isal/igzip/proc_heap.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_icf_body_h1_gr_bt.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_icf_finish.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_inflate_multibinary.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_update_histogram_01.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_update_histogram_04.asm
                 ${QPL_SRC_DIR}/isal/igzip/rfc1951_lookup.asm
                 ${QPL_SRC_DIR}/isal/igzip/adler32_sse.asm
                 ${QPL_SRC_DIR}/isal/igzip/adler32_avx2_4.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_deflate_hash.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_set_long_icf_fg_04.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_set_long_icf_fg_06.asm
                 ${QPL_SRC_DIR}/isal/igzip/igzip_multibinary.asm
                 ${QPL_SRC_DIR}/isal/igzip/stdmac.asm
                 ${QPL_SRC_DIR}/isal/crc/crc_multibinary.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_gzip_refl_by8.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_gzip_refl_by8_02.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_gzip_refl_by16_10.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_ieee_01.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_ieee_02.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_ieee_by4.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_ieee_by16_10.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_iscsi_00.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_iscsi_01.asm
                 ${QPL_SRC_DIR}/isal/crc/crc32_iscsi_by16_10.asm)

# Adding ISA-L library target
add_library(isal OBJECT ${ISAL_C_SRC})
add_library(isal_asm OBJECT ${ISAL_ASM_SRC})

set_property(GLOBAL APPEND PROPERTY QPL_LIB_DEPS
             $<TARGET_OBJECTS:isal>)

set_property(GLOBAL APPEND PROPERTY QPL_LIB_DEPS
             $<TARGET_OBJECTS:isal_asm>)

# Setting external and internal interfaces for ISA-L library
target_include_directories(isal
                        PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/isal/include>
                        PRIVATE ${QPL_SRC_DIR}/isal/include
                        PUBLIC ${QPL_SRC_DIR}/isal/igzip)

set_target_properties(isal PROPERTIES
                        CXX_STANDARD 11
                        C_STANDARD 99)

target_compile_options(isal PRIVATE
                       "$<$<C_COMPILER_ID:GNU>:${QPL_LINUX_TOOLCHAIN_REQUIRED_FLAGS}>"
                       "$<$<CONFIG:Debug>:>"
                       "$<$<CONFIG:Release>:>")

# AS_FEATURE_LEVEL=10 means "Check SIMD capabilities of the target system at runtime and use up to AVX512 if available".
# HAVE_KNOWS_AVX512 means rely on AVX512 being available on the target system.
target_compile_options(isal_asm PRIVATE "-I${QPL_SRC_DIR}/isal/include/"
                       PRIVATE "-I${QPL_SRC_DIR}/isal/igzip/"
                       PRIVATE "-I${QPL_SRC_DIR}/isal/crc/"
                       PRIVATE "-DHAVE_AS_KNOWS_AVX512"
                       PRIVATE "-DAS_FEATURE_LEVEL=10"
                       PRIVATE "-DQPL_LIB")

# Here must remove "-fno-sanitize=undefined" from COMPILE_OPTIONS.
# Otherwise nasm compiler would fail to proceed due to unrecognition of "-fno-sanitize=undefined"
if (SANITIZE STREQUAL "undefined")
    get_target_property(target_options isal_asm COMPILE_OPTIONS)
    list(REMOVE_ITEM target_options "-fno-sanitize=undefined")
    set_property(TARGET isal_asm PROPERTY COMPILE_OPTIONS ${target_options})
endif()

target_compile_definitions(isal PUBLIC
                           QPL_LIB
                           NDEBUG)

# [SUBDIR]core-sw
# Create set of libraries corresponding to supported platforms for SW fallback which are implemented by AVX512 and non-AVX512 instructions respectively.
# The upper level QPL API will check SIMD capabilities of the target system at runtime and decide to call AVX512 function or non-AVX512 function.
# Hence, here we don't need put ENABLE_AVX512 CMake switch.

get_list_of_supported_optimizations(PLATFORMS_LIST)

foreach(PLATFORM_ID IN LISTS PLATFORMS_LIST)
    # Find Core Sources
    file(GLOB SOURCES
        ${QPL_SRC_DIR}/core-sw/src/checksums/*.c
        ${QPL_SRC_DIR}/core-sw/src/filtering/*.c
        ${QPL_SRC_DIR}/core-sw/src/other/*.c
        ${QPL_SRC_DIR}/core-sw/src/compression/*.c)

    file(GLOB DATA_SOURCES
        ${QPL_SRC_DIR}/core-sw/src/data/*.c)

    # Create library
    add_library(qplcore_${PLATFORM_ID} OBJECT ${SOURCES})

    set_property(GLOBAL APPEND PROPERTY QPL_LIB_DEPS
                $<TARGET_OBJECTS:qplcore_${PLATFORM_ID}>)

    target_include_directories(qplcore_${PLATFORM_ID}
                                PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-sw>
                                PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-sw/include>
                                PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-sw/src/include>
                                PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-sw/src/compression/include>
                                PRIVATE $<TARGET_PROPERTY:isal,INTERFACE_INCLUDE_DIRECTORIES>)

    set_target_properties(qplcore_${PLATFORM_ID} PROPERTIES
                                $<$<C_COMPILER_ID:GNU>:C_STANDARD 17>)

    target_compile_options(qplcore_${PLATFORM_ID}
                            PRIVATE ${QPL_LINUX_TOOLCHAIN_REQUIRED_FLAGS}
                            PRIVATE "$<$<CONFIG:Debug>:>"
                            PRIVATE "$<$<CONFIG:Release>:-O3;-D_FORTIFY_SOURCE=2>")

    # Set specific compiler options and/or definitions based on a platform
    if (${PLATFORM_ID} MATCHES "avx512")
        target_compile_definitions(qplcore_${PLATFORM_ID} PRIVATE PLATFORM=2)
        target_compile_options(qplcore_${PLATFORM_ID} PRIVATE -march=skylake-avx512)
    else() # Create default px library
        target_compile_definitions(qplcore_${PLATFORM_ID} PRIVATE PLATFORM=0)
    endif()

    target_link_libraries(qplcore_${PLATFORM_ID} isal)
endforeach()

#
# Create dispatcher between platforms and auto-generated wrappers
#
file(GLOB SW_DISPATCHER_SOURCES ${QPL_SRC_DIR}/core-sw/dispatcher/*.cpp)

add_library(qplcore_sw_dispatcher OBJECT ${SW_DISPATCHER_SOURCES})

set_property(GLOBAL APPEND PROPERTY QPL_LIB_DEPS
             $<TARGET_OBJECTS:qplcore_sw_dispatcher>)

target_include_directories(qplcore_sw_dispatcher
                          PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-sw/dispatcher>)

# Generate kernel wrappers
generate_unpack_kernel_arrays(${QPL_BINARY_DIR} "${PLATFORMS_LIST}")

foreach(PLATFORM_ID IN LISTS PLATFORMS_LIST)
    file(GLOB GENERATED_${PLATFORM_ID}_TABLES_SRC ${QPL_BINARY_DIR}/generated/${PLATFORM_ID}_*.cpp)

    target_sources(qplcore_sw_dispatcher PRIVATE ${GENERATED_${PLATFORM_ID}_TABLES_SRC})

    # Set specific compiler options and/or definitions based on a platform
    if (${PLATFORM_ID} MATCHES "avx512")
        set_source_files_properties(${GENERATED_${PLATFORM_ID}_TABLES_SRC} PROPERTIES COMPILE_DEFINITIONS PLATFORM=2)
    else()
        set_source_files_properties(${GENERATED_${PLATFORM_ID}_TABLES_SRC} PROPERTIES COMPILE_DEFINITIONS PLATFORM=0)
    endif()

    target_include_directories(qplcore_sw_dispatcher
                                PUBLIC $<TARGET_PROPERTY:qplcore_${PLATFORM_ID},INTERFACE_INCLUDE_DIRECTORIES>)
endforeach()

set_target_properties(qplcore_sw_dispatcher PROPERTIES CXX_STANDARD 17)

# w/a for build compatibility with ISAL codebase
target_compile_definitions(qplcore_sw_dispatcher PUBLIC -DQPL_LIB)

target_compile_options(qplcore_sw_dispatcher
        PRIVATE $<$<C_COMPILER_ID:GNU>:${QPL_LINUX_TOOLCHAIN_REQUIRED_FLAGS};
                                       ${QPL_LINUX_TOOLCHAIN_DYNAMIC_LIBRARY_FLAGS};
                                       $<$<CONFIG:Release>:-O3;-D_FORTIFY_SOURCE=2>>
        PRIVATE $<$<COMPILE_LANG_AND_ID:CXX,GNU>:${QPL_LINUX_TOOLCHAIN_CPP_EMBEDDED_FLAGS}>)

# [SUBDIR]core-iaa
file(GLOB HW_PATH_SRC ${QPL_SRC_DIR}/core-iaa/sources/aecs/*.c
                      ${QPL_SRC_DIR}/core-iaa/sources/aecs/*.cpp
                      ${QPL_SRC_DIR}/core-iaa/sources/driver_loader/*.c
                      ${QPL_SRC_DIR}/core-iaa/sources/driver_loader/*.cpp
                      ${QPL_SRC_DIR}/core-iaa/sources/descriptors/*.c
                      ${QPL_SRC_DIR}/core-iaa/sources/descriptors/*.cpp
                      ${QPL_SRC_DIR}/core-iaa/sources/bit_rev.c)

# Create library
add_library(core_iaa OBJECT ${HW_PATH_SRC})

set_property(GLOBAL APPEND PROPERTY QPL_LIB_DEPS
             $<TARGET_OBJECTS:core_iaa>)

target_include_directories(core_iaa
        PRIVATE ${UUID_DIR}
        PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-iaa/include>
        PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-iaa/sources/include>
        PRIVATE $<BUILD_INTERFACE:${QPL_PROJECT_DIR}/include>  # status.h in own_checkers.h
        PRIVATE $<BUILD_INTERFACE:${QPL_PROJECT_DIR}/sources/c_api> # own_checkers.h
        PRIVATE $<TARGET_PROPERTY:qplcore_sw_dispatcher,INTERFACE_INCLUDE_DIRECTORIES>)

set_target_properties(core_iaa PROPERTIES
        $<$<C_COMPILER_ID:GNU>:C_STANDARD 17>
        CXX_STANDARD 17)

target_compile_options(core_iaa
        PRIVATE $<$<C_COMPILER_ID:GNU>:${QPL_LINUX_TOOLCHAIN_REQUIRED_FLAGS};
        $<$<CONFIG:Release>:-O3;-D_FORTIFY_SOURCE=2>>)

target_compile_features(core_iaa PRIVATE c_std_11)

target_compile_definitions(core_iaa PRIVATE QPL_BADARG_CHECK
        PRIVATE $<$<BOOL:${BLOCK_ON_FAULT}>: BLOCK_ON_FAULT_ENABLED>
        PRIVATE $<$<BOOL:${LOG_HW_INIT}>:LOG_HW_INIT>
        PRIVATE $<$<BOOL:${DYNAMIC_LOADING_LIBACCEL_CONFIG}>:DYNAMIC_LOADING_LIBACCEL_CONFIG>)

# [SUBDIR]middle-layer
file(GLOB MIDDLE_LAYER_SRC
        ${QPL_SRC_DIR}/middle-layer/analytics/*.cpp
        ${QPL_SRC_DIR}/middle-layer/c_wrapper/*.cpp
        ${QPL_SRC_DIR}/middle-layer/checksum/*.cpp
        ${QPL_SRC_DIR}/middle-layer/common/*.cpp
        ${QPL_SRC_DIR}/middle-layer/compression/*.cpp
        ${QPL_SRC_DIR}/middle-layer/compression/*/*.cpp
        ${QPL_SRC_DIR}/middle-layer/compression/*/*/*.cpp
        ${QPL_SRC_DIR}/middle-layer/dispatcher/*.cpp
        ${QPL_SRC_DIR}/middle-layer/other/*.cpp
        ${QPL_SRC_DIR}/middle-layer/util/*.cpp
        ${QPL_SRC_DIR}/middle-layer/inflate/*.cpp
        ${QPL_SRC_DIR}/core-iaa/sources/accelerator/*.cpp) # todo

add_library(middle_layer_lib OBJECT
        ${MIDDLE_LAYER_SRC})

set_property(GLOBAL APPEND PROPERTY QPL_LIB_DEPS
        $<TARGET_OBJECTS:middle_layer_lib>)

target_compile_options(middle_layer_lib
        PRIVATE $<$<C_COMPILER_ID:GNU>:${QPL_LINUX_TOOLCHAIN_REQUIRED_FLAGS};
                                       ${QPL_LINUX_TOOLCHAIN_DYNAMIC_LIBRARY_FLAGS};
                                       $<$<CONFIG:Release>:-O3;-D_FORTIFY_SOURCE=2>>
        PRIVATE $<$<COMPILE_LANG_AND_ID:CXX,GNU>:${QPL_LINUX_TOOLCHAIN_CPP_EMBEDDED_FLAGS}>)

target_compile_definitions(middle_layer_lib
        PUBLIC QPL_VERSION="${QPL_VERSION}"
        PUBLIC $<$<BOOL:${LOG_HW_INIT}>:LOG_HW_INIT>
        PUBLIC $<$<BOOL:${EFFICIENT_WAIT}>:QPL_EFFICIENT_WAIT>
        PUBLIC QPL_BADARG_CHECK
        PUBLIC $<$<BOOL:${DYNAMIC_LOADING_LIBACCEL_CONFIG}>:DYNAMIC_LOADING_LIBACCEL_CONFIG>)

set_target_properties(middle_layer_lib PROPERTIES CXX_STANDARD 17)

target_include_directories(middle_layer_lib
        PRIVATE ${UUID_DIR}
        PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/middle-layer>
        PUBLIC $<TARGET_PROPERTY:_qpl,INTERFACE_INCLUDE_DIRECTORIES>
        PUBLIC $<TARGET_PROPERTY:qplcore_sw_dispatcher,INTERFACE_INCLUDE_DIRECTORIES>
        PUBLIC $<TARGET_PROPERTY:isal,INTERFACE_INCLUDE_DIRECTORIES>
        PUBLIC $<TARGET_PROPERTY:core_iaa,INTERFACE_INCLUDE_DIRECTORIES>)

target_compile_definitions(middle_layer_lib PUBLIC -DQPL_LIB)

# [SUBDIR]c_api
file(GLOB_RECURSE QPL_C_API_SRC
        ${QPL_SRC_DIR}/c_api/*.c
        ${QPL_SRC_DIR}/c_api/*.cpp)

get_property(LIB_DEPS GLOBAL PROPERTY QPL_LIB_DEPS)

add_library(_qpl STATIC ${QPL_C_API_SRC} ${LIB_DEPS})

target_include_directories(_qpl
        PUBLIC $<BUILD_INTERFACE:${QPL_PROJECT_DIR}/include/> $<INSTALL_INTERFACE:include>
        PRIVATE $<TARGET_PROPERTY:middle_layer_lib,INTERFACE_INCLUDE_DIRECTORIES>
        PRIVATE $<BUILD_INTERFACE:${QPL_SRC_DIR}/c_api>)

set_target_properties(_qpl PROPERTIES
        $<$<C_COMPILER_ID:GNU>:C_STANDARD 17>
        CXX_STANDARD 17)

target_compile_options(_qpl
        PRIVATE $<$<C_COMPILER_ID:GNU>:${QPL_LINUX_TOOLCHAIN_REQUIRED_FLAGS};
                                       ${QPL_LINUX_TOOLCHAIN_DYNAMIC_LIBRARY_FLAGS};
                                       $<$<CONFIG:Release>:-O3;-D_FORTIFY_SOURCE=2>>
        PRIVATE $<$<COMPILE_LANG_AND_ID:CXX,GNU>:${QPL_LINUX_TOOLCHAIN_CPP_EMBEDDED_FLAGS}>)

target_compile_definitions(_qpl
        PRIVATE -DQPL_LIB
        PRIVATE -DQPL_BADARG_CHECK
        PRIVATE $<$<BOOL:${DYNAMIC_LOADING_LIBACCEL_CONFIG}>:DYNAMIC_LOADING_LIBACCEL_CONFIG>
        PUBLIC -DENABLE_QPL_COMPRESSION)

target_link_libraries(_qpl
        PRIVATE ch_contrib::accel-config
        PRIVATE ch_contrib::isal)

target_include_directories(_qpl SYSTEM BEFORE
        PUBLIC "${QPL_PROJECT_DIR}/include"
        PUBLIC ${UUID_DIR})

add_library (ch_contrib::qpl ALIAS _qpl)
