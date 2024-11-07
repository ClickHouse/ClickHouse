add_compile_options("$<$<OR:$<COMPILE_LANGUAGE:C>,$<COMPILE_LANGUAGE:CXX>>:${COVERAGE_FLAGS}>")

if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

# TODO: Remove this. We like to compile with C++23 (set by top-level CMakeLists) but Clang crashes with our libcxx
# when instantiated from JSON.cpp. Try again when libcxx(abi) and Clang are upgraded to 16.
set (CMAKE_CXX_STANDARD 20)

configure_file(GitHash.cpp.in GitHash.generated.cpp)

set (SRCS
    argsToConfig.cpp
    cgroupsv2.cpp
    coverage.cpp
    demangle.cpp
    Decimal.cpp
    getAvailableMemoryAmount.cpp
    getFQDNOrHostName.cpp
    getMemoryAmount.cpp
    getPageSize.cpp
    getThreadId.cpp
    int8_to_string.cpp
    itoa.cpp
    JSON.cpp
    mremap.cpp
    phdr_cache.cpp
    preciseExp10.cpp
    shift10.cpp
    sleep.cpp
    terminalColors.cpp
    errnoToString.cpp
    StringRef.cpp
    safeExit.cpp
    throwError.cpp
    Numa.cpp
    GitHash.generated.cpp
)

add_library (common ${SRCS})

if (WITH_COVERAGE)
    target_compile_definitions(common PUBLIC WITH_COVERAGE=1)
else ()
    target_compile_definitions(common PUBLIC WITH_COVERAGE=0)
endif ()

if (TARGET ch_contrib::crc32_s390x)
    target_link_libraries(common PUBLIC ch_contrib::crc32_s390x)
endif()

if (TARGET ch_contrib::numactl)
    target_link_libraries(common PUBLIC ch_contrib::numactl)
endif()

target_include_directories(common PUBLIC .. "${CMAKE_CURRENT_BINARY_DIR}/..")

target_link_libraries (common
    PUBLIC
        ch_contrib::cityhash
        boost::headers_only
        boost::system
        Poco::Net
        Poco::Net::SSL
        Poco::Util
        Poco::Foundation
        ch_contrib::replxx
        ch_contrib::cctz
        ch_contrib::fmt
        ch_contrib::magic_enum
)

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
