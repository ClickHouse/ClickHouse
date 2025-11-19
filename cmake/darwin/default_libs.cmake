set (DEFAULT_LIBS "-nodefaultlibs")

set (DEFAULT_LIBS "${DEFAULT_LIBS} ${COVERAGE_OPTION} -lc -lm -lpthread -ldl")

message(STATUS "Default libraries: ${DEFAULT_LIBS}")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

# Minimal supported SDK version
set(CMAKE_OSX_DEPLOYMENT_TARGET 10.15)

add_library(Threads::Threads INTERFACE IMPORTED)
set_target_properties(Threads::Threads PROPERTIES INTERFACE_LINK_LIBRARIES pthread)

if(NOT CMAKE_CROSSCOMPILING)
    execute_process(
        COMMAND readlink -f /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk
        OUTPUT_VARIABLE ACTUAL_SDK_PATH
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    string(REGEX MATCH "MacOSX([0-9]+\\.[0-9]+)" _ ${ACTUAL_SDK_PATH})
    set(OS_DARWIN_SDK_VERSION ${CMAKE_MATCH_1})
    if(${OS_DARWIN_SDK_VERSION} MATCHES "^[0-9]+\\.[0-9]+")
        message(STATUS "Detected OSX SDK Version: ${OS_DARWIN_SDK_VERSION}")
    else ()
        message(WARNING "Unexpected OSX SDK Version: ${OS_DARWIN_SDK_VERSION}")
    endif()
endif()

include (cmake/unwind.cmake)
include (cmake/cxx.cmake)
