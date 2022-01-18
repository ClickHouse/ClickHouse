set (SRCS
    argsToConfig.cpp
    coverage.cpp
    DateLUT.cpp
    DateLUTImpl.cpp
    demangle.cpp
    getFQDNOrHostName.cpp
    getMemoryAmount.cpp
    getPageSize.cpp
    getThreadId.cpp
    JSON.cpp
    LineReader.cpp
    mremap.cpp
    phdr_cache.cpp
    preciseExp10.cpp
    setTerminalEcho.cpp
    shift10.cpp
    sleep.cpp
    terminalColors.cpp
    errnoToString.cpp
    getResource.cpp
    StringRef.cpp
)

if (ENABLE_REPLXX)
    list (APPEND SRCS ReplxxLineReader.cpp)
elseif (ENABLE_READLINE)
    list (APPEND SRCS ReadlineLineReader.cpp)
endif ()

if (USE_DEBUG_HELPERS)
    set (INCLUDE_DEBUG_HELPERS "-include \"${ClickHouse_SOURCE_DIR}/base/common/iostream_debug_helpers.h\"")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${INCLUDE_DEBUG_HELPERS}")
endif ()

add_library (common ${SRCS})

if (WITH_COVERAGE)
    target_compile_definitions(common PUBLIC WITH_COVERAGE=1)
else ()
    target_compile_definitions(common PUBLIC WITH_COVERAGE=0)
endif ()

if (USE_INTERNAL_CCTZ)
    set_source_files_properties(DateLUTImpl.cpp PROPERTIES COMPILE_DEFINITIONS USE_INTERNAL_CCTZ)
endif()

target_include_directories(common PUBLIC .. "${CMAKE_CURRENT_BINARY_DIR}/..")

if (OS_DARWIN AND NOT MAKE_STATIC_LIBRARIES)
    target_link_libraries(common PUBLIC -Wl,-U,_inside_main)
endif()

# Allow explicit fallback to readline
if (NOT ENABLE_REPLXX AND ENABLE_READLINE)
    message (STATUS "Attempt to fallback to readline explicitly")
    set (READLINE_PATHS "/usr/local/opt/readline/lib")
    # First try find custom lib for macos users (default lib without history support)
    find_library (READLINE_LIB NAMES readline PATHS ${READLINE_PATHS} NO_DEFAULT_PATH)
    if (NOT READLINE_LIB)
        find_library (READLINE_LIB NAMES readline PATHS ${READLINE_PATHS})
    endif ()

    set(READLINE_INCLUDE_PATHS "/usr/local/opt/readline/include")
    find_path (READLINE_INCLUDE_DIR NAMES readline/readline.h PATHS ${READLINE_INCLUDE_PATHS} NO_DEFAULT_PATH)
    if (NOT READLINE_INCLUDE_DIR)
        find_path (READLINE_INCLUDE_DIR NAMES readline/readline.h PATHS ${READLINE_INCLUDE_PATHS})
    endif ()
    if (READLINE_INCLUDE_DIR AND READLINE_LIB)
        target_link_libraries(common PUBLIC ${READLINE_LIB})
        target_compile_definitions(common PUBLIC USE_READLINE=1)
        message (STATUS "Using readline: ${READLINE_INCLUDE_DIR} : ${READLINE_LIB}")
    endif ()
endif ()

target_link_libraries (common
    PUBLIC
        ${CITYHASH_LIBRARIES}
        boost::headers_only
        boost::system
        Poco::Net
        Poco::Net::SSL
        Poco::Util
        Poco::Foundation
        replxx
        cctz
        fmt
)

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
