set (DEFAULT_LIBS "-nodefaultlibs")

set (DEFAULT_LIBS "${DEFAULT_LIBS} -lc -lm -lpthread -ldl")

message(STATUS "Default libraries: ${DEFAULT_LIBS}")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

# Minimal supported SDK version
set(CMAKE_OSX_DEPLOYMENT_TARGET 10.15)

add_library(Threads::Threads INTERFACE IMPORTED)
set_target_properties(Threads::Threads PROPERTIES INTERFACE_LINK_LIBRARIES pthread)

include (cmake/unwind.cmake)
include (cmake/cxx.cmake)

if (NOT SANITIZE STREQUAL "thread")
    # Replaces pthread_rwlock, which loses wakeups when waiters receive signals
    # (e.g. from the query profiler), permanently deadlocking the process.
    # Excluded only under TSan, which interposes these functions itself; the other
    # sanitizers run the query profiler too, so they need the workaround as well.
    # See base/darwin-compatibility/pthread_rwlock_shim.c and FB24027930.
    add_subdirectory(base/darwin-compatibility)
endif ()
