option(ENABLE_WASMEDGE "Enable WasmEdge engine" ${ENABLE_LIBRARIES})
if (NOT ENABLE_WASMEDGE)
    message (STATUS "Not using wasmedge")
    return()
endif()

# USE_MUSL:
#   For some reason, WasmEdge build fails with MUSL libc like below error:
#   contrib/wasmedge/lib/host/wasi/inode-linux.cpp:666:31: error: use of undeclared identifier 'O_SYMLINK'
#     666 |   const int OFlags = O_PATH | O_SYMLINK;
#   Should be possible to fix, but for now let's rely on WasmTime, since it probably will be main engine.
if (USE_MUSL OR ARCH_PPC64LE OR OS_DARWIN OR ARCH_S390X OR OS_FREEBSD OR ARCH_LOONGARCH64)
    message (STATUS "Unsupported architecture for WasmEdge")
    return()
endif()


set (SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/wasmedge")
set (CURRENT_DIR_BINARY "${ClickHouse_BINARY_DIR}/contrib/wasmedge")

SET(WASMEDGE_USE_LLVM 0)

file (GLOB_RECURSE SRCS
    ${SOURCE_DIR}/lib/api/*.cpp
    ${SOURCE_DIR}/lib/common/*.cpp
    ${SOURCE_DIR}/lib/executor/*.cpp
    ${SOURCE_DIR}/lib/host/*.cpp
    ${SOURCE_DIR}/lib/loader/*.cpp
    ${SOURCE_DIR}/lib/plugin/*.cpp
    ${SOURCE_DIR}/lib/po/*.cpp
    ${SOURCE_DIR}/lib/system/*.cpp
    ${SOURCE_DIR}/lib/validator/*.cpp
    ${SOURCE_DIR}/lib/vm/*.cpp
)

if (WASMEDGE_USE_LLVM)
    file (GLOB_RECURSE WASMEDGE_LLVM_SRCS
        ${SOURCE_DIR}/lib/llvm/*.cpp
    )
    list(APPEND SRCS ${WASMEDGE_LLVM_SRCS})
endif()

add_library(_wasmedge ${SRCS})
set_property(TARGET _wasmedge PROPERTY CXX_STANDARD 17)

SET (HAVE_MMAP 1)

SET (CPACK_PACKAGE_VERSION "0.1.0")
string(REGEX MATCH "^([0-9]+)[.]([0-9]+)[.]([0-9]+)*"
    WASMEDGE_VERSION_STRING
    "${CPACK_PACKAGE_VERSION}"
)

string(REPLACE "." ";" WASMEDGE_VERSION_LIST ${WASMEDGE_VERSION_STRING})
list(GET WASMEDGE_VERSION_LIST 0 WASMEDGE_VERSION_MAJOR)
list(GET WASMEDGE_VERSION_LIST 1 WASMEDGE_VERSION_MINOR)
list(GET WASMEDGE_VERSION_LIST 2 WASMEDGE_VERSION_PATCH)
set(WASMEDGE_API_VERSION "3")

configure_file("${SOURCE_DIR}/include/api/wasmedge/version.h.in" "${CURRENT_DIR_BINARY}/api/wasmedge/version.h")
configure_file("${SOURCE_DIR}/include/common/enum.inc" "${CURRENT_DIR_BINARY}/api/wasmedge/enum.inc" COPYONLY)
configure_file("${SOURCE_DIR}/include/common/enum_configure.h" "${CURRENT_DIR_BINARY}/api/wasmedge/enum_configure.h" COPYONLY)
configure_file("${SOURCE_DIR}/include/common/enum_errcode.h" "${CURRENT_DIR_BINARY}/api/wasmedge/enum_errcode.h" COPYONLY)
configure_file("${SOURCE_DIR}/include/common/enum_types.h" "${CURRENT_DIR_BINARY}/api/wasmedge/enum_types.h" COPYONLY)

configure_file("${SOURCE_DIR}/include/common/config.h.in" "${CURRENT_DIR_BINARY}/common/config.h")
configure_file("${SOURCE_DIR}/include/common/version.h.in" "${CURRENT_DIR_BINARY}/common/version.h")

unset(WASMEDGE_VERSION_STRING)
unset(WASMEDGE_VERSION_LIST)
unset(WASMEDGE_VERSION_MAJOR)
unset(WASMEDGE_VERSION_MINOR)
unset(WASMEDGE_VERSION_PATCH)
unset(WASMEDGE_API_VERSION)
unset(HAVE_MMAP)

target_include_directories(_wasmedge PUBLIC ${SOURCE_DIR}/include/api)
target_include_directories(_wasmedge PUBLIC ${CURRENT_DIR_BINARY}/api)
target_include_directories(_wasmedge PRIVATE ${CURRENT_DIR_BINARY})
target_include_directories(_wasmedge PRIVATE ${SOURCE_DIR}/include)
target_include_directories(_wasmedge PRIVATE ${SOURCE_DIR}/thirdparty)

target_compile_definitions(_wasmedge PRIVATE SPDLOG_FMT_EXTERNAL)
target_compile_definitions(_wasmedge PRIVATE FMT_HEADER_ONLY)
target_include_directories(_wasmedge PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/spdlog/include")
target_include_directories(_wasmedge PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/fmtlib/include")

target_compile_options (_wasmedge PRIVATE -Wno-invalid-specialization)

if (WASMEDGE_USE_LLVM)
    target_compile_definitions(_wasmedge PUBLIC -DWASMEDGE_USE_LLVM)
    target_link_libraries(_wasmedge PUBLIC ch_contrib::llvm)
endif()

add_library(ch_contrib::wasmedge ALIAS _wasmedge)
