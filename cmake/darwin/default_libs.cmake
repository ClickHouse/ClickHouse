set (DEFAULT_LIBS "-nodefaultlibs")

set (DEFAULT_LIBS "${DEFAULT_LIBS} ${COVERAGE_OPTION} -lc -lm -lpthread -ldl")

message(STATUS "Default libraries: ${DEFAULT_LIBS}")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

# Minimal supported SDK version
set(CMAKE_OSX_DEPLOYMENT_TARGET 10.15)

# Unfortunately '-pthread' doesn't work with '-nodefaultlibs'.
# Just make sure we have pthreads at all.
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

if(NOT CMAKE_CROSSCOMPILING)
    execute_process(
        COMMAND xcrun --sdk macosx --show-sdk-version
        OUTPUT_VARIABLE OS_DARWIN_SDK_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    if(${OS_DARWIN_SDK_VERSION} MATCHES "^[0-9]+\\.[0-9]+")
        message(STATUS "Detected OSX SDK Version: ${OS_DARWIN_SDK_VERSION}")
    else ()
        message(WARNING "Unexpected OSX SDK Version: ${OS_DARWIN_SDK_VERSION}")
    endif()
endif()

include (cmake/unwind.cmake)
include (cmake/cxx.cmake)
