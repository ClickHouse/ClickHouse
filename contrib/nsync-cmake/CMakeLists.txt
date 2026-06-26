set(NSYNC_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/nsync")

set(NSYNC_CPP_SOURCES
    "${NSYNC_SOURCE_DIR}/internal/common.c"
    "${NSYNC_SOURCE_DIR}/internal/counter.c"
    "${NSYNC_SOURCE_DIR}/internal/cv.c"
    "${NSYNC_SOURCE_DIR}/internal/debug.c"
    "${NSYNC_SOURCE_DIR}/internal/dll.c"
    "${NSYNC_SOURCE_DIR}/internal/mu.c"
    "${NSYNC_SOURCE_DIR}/internal/mu_wait.c"
    "${NSYNC_SOURCE_DIR}/internal/note.c"
    "${NSYNC_SOURCE_DIR}/internal/once.c"
    "${NSYNC_SOURCE_DIR}/internal/sem_wait.c"
    "${NSYNC_SOURCE_DIR}/internal/time_internal.c"
    "${NSYNC_SOURCE_DIR}/internal/wait.c"
    "${NSYNC_SOURCE_DIR}/platform/posix/src/per_thread_waiter.c"
    "${NSYNC_SOURCE_DIR}/platform/c++11/src/yield.cc"
    "${NSYNC_SOURCE_DIR}/platform/c++11/src/time_rep_timespec.cc"
    "${NSYNC_SOURCE_DIR}/platform/c++11/src/nsync_panic.cc"
)

set(NSYNC_PRIVATE_INCLUDE_DIRS
    "${NSYNC_SOURCE_DIR}/internal"
    "${NSYNC_SOURCE_DIR}/platform/c++11"
    "${NSYNC_SOURCE_DIR}/platform/posix"
)

set(NSYNC_PUBLIC_COMPILE_DEFINITIONS
    NSYNC_ATOMIC_CPP11
    NSYNC_USE_CPP11_TIMEPOINT
)

if (OS_LINUX)
    list(PREPEND NSYNC_CPP_SOURCES "${NSYNC_SOURCE_DIR}/platform/linux/src/nsync_semaphore_futex.c")
    list(PREPEND NSYNC_PRIVATE_INCLUDE_DIRS "${NSYNC_SOURCE_DIR}/platform/c++11.futex")
elseif (OS_DARWIN)
    list(APPEND NSYNC_CPP_SOURCES
        "${NSYNC_SOURCE_DIR}/platform/c++11/src/nsync_semaphore_mutex.cc"
        "${NSYNC_SOURCE_DIR}/platform/posix/src/clock_gettime.c"
    )
    list(APPEND NSYNC_PRIVATE_INCLUDE_DIRS "${NSYNC_SOURCE_DIR}/platform/macos")
    list(APPEND NSYNC_PUBLIC_COMPILE_DEFINITIONS _DARWIN_C_SOURCE)
elseif (OS_FREEBSD)
    list(APPEND NSYNC_CPP_SOURCES "${NSYNC_SOURCE_DIR}/platform/c++11/src/nsync_semaphore_mutex.cc")
    list(APPEND NSYNC_PRIVATE_INCLUDE_DIRS "${NSYNC_SOURCE_DIR}/platform/freebsd")
else()
    list(APPEND NSYNC_CPP_SOURCES "${NSYNC_SOURCE_DIR}/platform/c++11/src/nsync_semaphore_mutex.cc")
endif()

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    list(APPEND NSYNC_PRIVATE_INCLUDE_DIRS "${NSYNC_SOURCE_DIR}/platform/clang")
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    list(APPEND NSYNC_PRIVATE_INCLUDE_DIRS "${NSYNC_SOURCE_DIR}/platform/gcc")
elseif (MSVC)
    list(APPEND NSYNC_PRIVATE_INCLUDE_DIRS "${NSYNC_SOURCE_DIR}/platform/msvc")
endif()

if (ARCH_AMD64)
    list(APPEND NSYNC_PRIVATE_INCLUDE_DIRS "${NSYNC_SOURCE_DIR}/platform/x86_64")
elseif (ARCH_AARCH64)
    list(APPEND NSYNC_PRIVATE_INCLUDE_DIRS "${NSYNC_SOURCE_DIR}/platform/aarch64")
endif()

set_source_files_properties(${NSYNC_CPP_SOURCES} PROPERTIES LANGUAGE CXX)

add_library(nsync_cpp ${NSYNC_CPP_SOURCES})

target_include_directories(nsync_cpp SYSTEM
    PUBLIC "${NSYNC_SOURCE_DIR}/public"
    PRIVATE ${NSYNC_PRIVATE_INCLUDE_DIRS}
)

target_compile_definitions(nsync_cpp PUBLIC ${NSYNC_PUBLIC_COMPILE_DEFINITIONS})

add_library(ch_contrib::nsync ALIAS nsync_cpp)
