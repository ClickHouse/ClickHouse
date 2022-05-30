## The Intel® QPL provides high performance implementations of data processing functions for existing hardware accelerator, and/or software path in case if hardware accelerator is not available.

set (QPL_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/qpl")
set (QPL_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/qpl/sources")
set (QPL_BINARY_DIR "${ClickHouse_SOURCE_DIR}/build/contrib/qpl")
set (UUID_DIR "${ClickHouse_SOURCE_DIR}/contrib/qpl-cmake")

set (EFFICIENT_WAIT ON)
set (BLOCK_ON_FAULT ON)
set (LOG_HW_INIT OFF)
set (SANITIZE_MEMORY OFF)
set (SANITIZE_THREADS OFF)
set (LIB_FUZZING_ENGINE OFF)

function(GetLibraryVersion _content _outputVar)
    string(REGEX MATCHALL ".*Qpl VERSION+.* (.+).*LANGUAGES" VERSION_REGEX "${_content}")
    SET(${_outputVar} ${CMAKE_MATCH_1} PARENT_SCOPE)
endfunction()

FILE(READ "${QPL_PROJECT_DIR}/CMakeLists.txt" HEADER_CONTENT)
GetLibraryVersion("${HEADER_CONTENT}" QPL_VERSION)

message(STATUS "Intel QPL version: ${QPL_VERSION}")

include("${QPL_PROJECT_DIR}/cmake/CompileOptions.cmake")

# check nasm compiler
include(CheckLanguage)
check_language(ASM_NASM)
if(NOT CMAKE_ASM_NASM_COMPILER)
  message(FATAL_ERROR "Please install NASM from 'https://github.com/netwide-assembler/nasm' because NASM compiler could not be found on local machine!")
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

set(DISABLED_WARNINGS_FLAGS /wd4244 /wd4101 /wd4267 /wd4018 /wd4146 /wd4334 /wd4308)
set(FEATURE_FLAGS -DHAVE_AS_KNOWS_AVX512 -DAS_FEATURE_LEVEL=10)

# Adding ISA-L library target
add_library(isal OBJECT ${ISAL_C_SRC})
add_library(isal_asm OBJECT ${ISAL_ASM_SRC})

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
                       "$<$<C_COMPILER_ID:MSVC>:/WX;${DISABLED_WARNINGS_FLAGS};${QPL_WINDOWS_TOOLCHAIN_REQUIRED_FLAGS}>"
                       "$<$<CONFIG:Debug>:>"
                       "$<$<CONFIG:Release>:>")

target_compile_options(isal_asm PUBLIC "-I${QPL_SRC_DIR}/isal/include/"
                       PUBLIC "-I${QPL_SRC_DIR}/isal/igzip/"
                       PUBLIC "-I${QPL_SRC_DIR}/isal/crc/"
                       PUBLIC "-DHAVE_AS_KNOWS_AVX512"
                       PUBLIC "-DAS_FEATURE_LEVEL=10"
                       PUBLIC "-DQPL_LIB")

target_compile_definitions(isal PUBLIC
                           QPL_LIB
                           NDEBUG)

# [SUBDIR]core-sw
enable_language(C)

#
# Create avx512 library
#
#set(CMAKE_INCLUDE_CURRENT_DIR ON)

# Find Core Sources
file(GLOB SOURCES
     ${QPL_SRC_DIR}/core-sw/src/checksums/*.c
     ${QPL_SRC_DIR}/core-sw/src/filtering/*.c
     ${QPL_SRC_DIR}/core-sw/src/other/*.c
     ${QPL_SRC_DIR}/core-sw/src/compression/*.c)

file(GLOB DATA_SOURCES
     ${QPL_SRC_DIR}/core-sw/src/data/*.c)

# Create library
add_library(qplcore_avx512 OBJECT ${SOURCES})

target_compile_definitions(qplcore_avx512 PRIVATE PLATFORM=2)

target_include_directories(qplcore_avx512
                           PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-sw/include>
                           PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-sw/src/include>
                           PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-sw/src/compression/include>
                           PRIVATE $<TARGET_PROPERTY:isal,INTERFACE_INCLUDE_DIRECTORIES>)

set_target_properties(qplcore_avx512 PROPERTIES
        $<$<C_COMPILER_ID:MSVC>:C_STANDARD 18>
        $<$<C_COMPILER_ID:GNU>:C_STANDARD 17>)

target_link_libraries(qplcore_avx512 ${CMAKE_DL_LIBS} isal)

if (WIN32)
    target_compile_options(qplcore_avx512
                           PRIVATE ${QPL_WINDOWS_TOOLCHAIN_REQUIRED_FLAGS}
                           PRIVATE /arch:AVX512
                           PRIVATE "$<$<CONFIG:Debug>:>"
                           PRIVATE "$<$<CONFIG:Release>:-O2>")
else ()
    target_compile_options(qplcore_avx512
                           PRIVATE ${QPL_LINUX_TOOLCHAIN_REQUIRED_FLAGS}
                           PRIVATE -march=skylake-avx512
                           PRIVATE "$<$<CONFIG:Debug>:>"
                           PRIVATE "$<$<CONFIG:Release>:-O3;-D_FORTIFY_SOURCE=2>")
endif ()

target_compile_definitions(qplcore_avx512 PUBLIC QPL_BADARG_CHECK)

#
# Create px library
#
#set(CMAKE_INCLUDE_CURRENT_DIR ON)

# Create library
add_library(qplcore_px OBJECT ${SOURCES} ${DATA_SOURCES})

target_compile_definitions(qplcore_px PRIVATE PLATFORM=0)

target_include_directories(qplcore_px
                           PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-sw/include>
                           PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-sw/src/include>
                           PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-sw/src/compression/include>
                           PRIVATE $<TARGET_PROPERTY:isal,INTERFACE_INCLUDE_DIRECTORIES>)

set_target_properties(qplcore_px PROPERTIES
        $<$<C_COMPILER_ID:MSVC>:C_STANDARD 18>
        $<$<C_COMPILER_ID:GNU>:C_STANDARD 17>)

target_link_libraries(qplcore_px isal ${CMAKE_DL_LIBS})

if (WIN32)
    target_compile_options(qplcore_px
                           PRIVATE "$<$<CONFIG:Debug>:>"
                           PRIVATE ${QPL_WINDOWS_TOOLCHAIN_REQUIRED_FLAGS}
                           PRIVATE "$<$<CONFIG:Release>:-O2>")
else ()
    target_compile_options(qplcore_px
                           PRIVATE ${QPL_LINUX_TOOLCHAIN_REQUIRED_FLAGS}
                           PRIVATE "$<$<CONFIG:Debug>:>"
                           PRIVATE "$<$<CONFIG:Release>:-O3;-D_FORTIFY_SOURCE=2>")
endif ()

target_compile_definitions(qplcore_px PUBLIC QPL_BADARG_CHECK)

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

target_include_directories(core_iaa
        PRIVATE ${UUID_DIR}
        PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-iaa/include>
        PRIVATE $<BUILD_INTERFACE:${QPL_SRC_DIR}/core-iaa/sources/include>
        PRIVATE $<TARGET_PROPERTY:qplcore_avx512,INTERFACE_INCLUDE_DIRECTORIES>)

set_target_properties(core_iaa PROPERTIES
        $<$<C_COMPILER_ID:MSVC>:C_STANDARD 18>
        $<$<C_COMPILER_ID:GNU>:C_STANDARD 17>
        CXX_STANDARD 17)

if (WIN32)
    modify_standard_language_flag(LANGUAGE_NAME "CXX"
            FLAG_NAME "/GR"
            NEW_FLAG_VALUE "-")
endif ()

target_compile_options(core_iaa
        PRIVATE $<$<C_COMPILER_ID:GNU>:${QPL_LINUX_TOOLCHAIN_REQUIRED_FLAGS};
                                       $<$<CONFIG:Release>:-O3;-D_FORTIFY_SOURCE=2>>
        PRIVATE $<$<C_COMPILER_ID:MSVC>:${QPL_WINDOWS_TOOLCHAIN_REQUIRED_FLAGS};
                                        $<$<CONFIG:Release>:-O2>>)

target_compile_features(core_iaa PRIVATE c_std_11)

target_compile_definitions(core_iaa PRIVATE QPL_BADARG_CHECK
        PRIVATE $<$<BOOL:${BLOCK_ON_FAULT}>: BLOCK_ON_FAULT_ENABLED>
        PRIVATE $<$<BOOL:${LOG_HW_INIT}>:LOG_HW_INIT>)

# [SUBDIR]middle-layer
enable_language(CXX)

generate_unpack_kernel_arrays(${QPL_BINARY_DIR})

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

file(GLOB GENERATED_PX_TABLES_SRC ${QPL_BINARY_DIR}/generated/px_*.cpp)
file(GLOB GENERATED_AVX512_TABLES_SRC ${QPL_BINARY_DIR}/generated/avx512_*.cpp)

add_library(middle_layer_lib OBJECT
        ${GENERATED_PX_TABLES_SRC}
        ${GENERATED_AVX512_TABLES_SRC}
        ${MIDDLE_LAYER_SRC})

if (WIN32)
    modify_standard_language_flag(LANGUAGE_NAME "CXX"
            FLAG_NAME "/GR"
            NEW_FLAG_VALUE "-")
endif ()

target_compile_options(middle_layer_lib
        PRIVATE $<$<C_COMPILER_ID:GNU>:${QPL_LINUX_TOOLCHAIN_REQUIRED_FLAGS};
                                       ${QPL_LINUX_TOOLCHAIN_DYNAMIC_LIBRARY_FLAGS};
                                       $<$<CONFIG:Release>:-O3;-D_FORTIFY_SOURCE=2>>
        PRIVATE $<$<COMPILE_LANG_AND_ID:CXX,GNU>:${QPL_LINUX_TOOLCHAIN_CPP_EMBEDDED_FLAGS}>
        PRIVATE $<$<C_COMPILER_ID:MSVC>:${QPL_WINDOWS_TOOLCHAIN_REQUIRED_FLAGS};
                                        ${QPL_WINDOWS_TOOLCHAIN_DYNAMIC_LIBRARY_FLAGS}
                                        $<$<CONFIG:Release>:-O2>>
        PRIVATE $<$<COMPILE_LANG_AND_ID:CXX,MSVC>:${QPL_WINDOWS_TOOLCHAIN_CPP_EMBEDDED_FLAGS}>)

target_compile_definitions(middle_layer_lib
        PUBLIC QPL_VERSION="${QPL_VERSION}"
        PUBLIC $<$<C_COMPILER_ID:MSVC>:_ENABLE_EXTENDED_ALIGNED_STORAGE>
        PUBLIC $<$<BOOL:${LOG_HW_INIT}>:LOG_HW_INIT>
        PUBLIC $<$<BOOL:${EFFICIENT_WAIT}>:QPL_EFFICIENT_WAIT>
        PUBLIC QPL_BADARG_CHECK)

set_target_properties(middle_layer_lib PROPERTIES CXX_STANDARD 17)
set_source_files_properties(${GENERATED_PX_TABLES_SRC} PROPERTIES COMPILE_DEFINITIONS PLATFORM=0)
set_source_files_properties(${GENERATED_AVX512_TABLES_SRC} PROPERTIES COMPILE_DEFINITIONS PLATFORM=2)

target_include_directories(middle_layer_lib
        PRIVATE ${UUID_DIR}
        PUBLIC $<BUILD_INTERFACE:${QPL_SRC_DIR}/middle-layer>
        PUBLIC $<TARGET_PROPERTY:_qpl,INTERFACE_INCLUDE_DIRECTORIES>
        PUBLIC $<TARGET_PROPERTY:qplcore_px,INTERFACE_INCLUDE_DIRECTORIES>
        PUBLIC $<TARGET_PROPERTY:qplcore_avx512,INTERFACE_INCLUDE_DIRECTORIES>
        PUBLIC $<TARGET_PROPERTY:isal,INTERFACE_INCLUDE_DIRECTORIES>
        PUBLIC $<TARGET_PROPERTY:core_iaa,INTERFACE_INCLUDE_DIRECTORIES>)

target_compile_definitions(middle_layer_lib PUBLIC -DQPL_LIB)
# [SUBDIR]c_api
enable_language(C CXX)

file(GLOB_RECURSE QPL_C_API_SRC 
        ${QPL_SRC_DIR}/c_api/*.c
        ${QPL_SRC_DIR}/c_api/*.cpp)

add_library(_qpl STATIC ${QPL_C_API_SRC}
         $<TARGET_OBJECTS:middle_layer_lib>
        $<TARGET_OBJECTS:isal>
        $<TARGET_OBJECTS:isal_asm>
        $<TARGET_OBJECTS:qplcore_px>
        $<TARGET_OBJECTS:qplcore_avx512>
        $<TARGET_OBJECTS:core_iaa>
        $<TARGET_OBJECTS:middle_layer_lib>)

target_include_directories(_qpl
        PUBLIC $<BUILD_INTERFACE:${QPL_PROJECT_DIR}/include/>
        PRIVATE $<TARGET_PROPERTY:middle_layer_lib,INTERFACE_INCLUDE_DIRECTORIES>
        PRIVATE $<BUILD_INTERFACE:${QPL_SRC_DIR}/c_api>)

set_target_properties(_qpl PROPERTIES
        $<$<C_COMPILER_ID:MSVC>:C_STANDARD 18>
        $<$<C_COMPILER_ID:GNU>:C_STANDARD 17>
        CXX_STANDARD 17)

target_compile_options(_qpl
        PRIVATE $<$<C_COMPILER_ID:GNU>:${QPL_LINUX_TOOLCHAIN_REQUIRED_FLAGS};
                                       ${QPL_LINUX_TOOLCHAIN_DYNAMIC_LIBRARY_FLAGS};
                                       $<$<CONFIG:Release>:-O3;-D_FORTIFY_SOURCE=2>>
        PRIVATE $<$<COMPILE_LANG_AND_ID:CXX,GNU>:${QPL_LINUX_TOOLCHAIN_CPP_EMBEDDED_FLAGS}>
        PRIVATE $<$<C_COMPILER_ID:MSVC>:${QPL_WINDOWS_TOOLCHAIN_REQUIRED_FLAGS};
                                        ${QPL_WINDOWS_TOOLCHAIN_DYNAMIC_LIBRARY_FLAGS}
                                        $<$<CONFIG:Release>:-O2>>
        PRIVATE $<$<COMPILE_LANG_AND_ID:CXX,MSVC>:${QPL_WINDOWS_TOOLCHAIN_CPP_EMBEDDED_FLAGS}>)

target_compile_definitions(_qpl
        PUBLIC -DQPL_LIB
        PUBLIC -DQPL_BADARG_CHECK
        PUBLIC $<$<C_COMPILER_ID:MSVC>:_ENABLE_EXTENDED_ALIGNED_STORAGE>)

target_link_libraries(_qpl
        PRIVATE ${CMAKE_DL_LIBS})

add_library (ch_contrib::qpl ALIAS _qpl)
target_include_directories(_qpl PUBLIC "${QPL_PROJECT_DIR}/include")
