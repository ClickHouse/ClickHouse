enable_language (ASM)

if (COMPILER_CLANG)
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-builtin-requires-header")
endif ()

set (GLIBC_COMPATIBILITY_SOURCES
glibc-compatibility.c
musl/pipe2.c
musl/fallocate.c
musl/longjmp.s
musl/vasprintf.c
musl/lgamma.c
musl/posix_spawn.c
musl/futimens.c
musl/syscall.s
musl/syscall_ret.c
musl/sched_cpucount.c
musl/glob.c
musl/exp2f.c
musl/pwritev.c
musl/getrandom.c
musl/fcntl.c
musl/timespec_get.c
musl/sched_getcpu.c
)

if (MAKE_STATIC_LIBRARIES)
    set (GLIBC_COMPATIBILITY_SOURCES ${GLIBC_COMPATIBILITY_SOURCES}
        libcxxabi/cxa_thread_atexit.cpp)
endif ()

add_library (glibc-compatibility ${GLIBC_COMPATIBILITY_SOURCES})

target_include_directories(glibc-compatibility PRIVATE libcxxabi)

# glibc-compatibility does not depend on any libraries but is linked to all libraries implicitly.
# Avoid linking of the library to itself.
set_target_properties(glibc-compatibility PROPERTIES LINK_LIBRARIES "")

# Garbage. Rough explanation: some libraries want to install itself and CMake forces us to also install the glibc-compatibility library.
install(TARGETS glibc-compatibility EXPORT CapnProtoTargets ARCHIVE DESTINATION "/tmp")
install(TARGETS glibc-compatibility EXPORT protobuf-targets ARCHIVE DESTINATION "/tmp")
install(TARGETS glibc-compatibility EXPORT double-conversionTargets ARCHIVE DESTINATION "/tmp")

if(ENABLE_TESTS)
    add_subdirectory(tests)
endif()
