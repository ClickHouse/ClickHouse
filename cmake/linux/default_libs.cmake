# Set standard, system and compiler libraries explicitly.
# This is intended for more control of what we are linking.

set (DEFAULT_LIBS "-nodefaultlibs")

# We need builtins from Clang's RT even without libcxx - for ubsan+int128.
# See https://bugs.llvm.org/show_bug.cgi?id=16404
if (COMPILER_CLANG AND NOT (CMAKE_CROSSCOMPILING AND ARCH_AARCH64))
    execute_process (COMMAND ${CMAKE_CXX_COMPILER} --print-file-name=libclang_rt.builtins-${CMAKE_SYSTEM_PROCESSOR}.a OUTPUT_VARIABLE BUILTINS_LIBRARY OUTPUT_STRIP_TRAILING_WHITESPACE)
else ()
    set (BUILTINS_LIBRARY "-lgcc")
endif ()

if (OS_ANDROID)
# pthread and rt are included in libc
set (DEFAULT_LIBS "${DEFAULT_LIBS} ${BUILTINS_LIBRARY} ${COVERAGE_OPTION} -lc -lm -ldl")
else ()
set (DEFAULT_LIBS "${DEFAULT_LIBS} ${BUILTINS_LIBRARY} ${COVERAGE_OPTION} -lc -lm -lrt -lpthread -ldl")
endif ()

message(STATUS "Default libraries: ${DEFAULT_LIBS}")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

# glibc-compatibility library relies to constant version of libc headers
# (because minor changes in function attributes between different glibc versions will introduce incompatibilities)
# This is for x86_64. For other architectures we have separate toolchains.
if (ARCH_AMD64 AND NOT_UNBUNDLED)
    set(CMAKE_C_STANDARD_INCLUDE_DIRECTORIES ${ClickHouse_SOURCE_DIR}/contrib/libc-headers/x86_64-linux-gnu ${ClickHouse_SOURCE_DIR}/contrib/libc-headers)
    set(CMAKE_CXX_STANDARD_INCLUDE_DIRECTORIES ${ClickHouse_SOURCE_DIR}/contrib/libc-headers/x86_64-linux-gnu ${ClickHouse_SOURCE_DIR}/contrib/libc-headers)
endif ()

# Global libraries

add_library(global-libs INTERFACE)

# Unfortunately '-pthread' doesn't work with '-nodefaultlibs'.
# Just make sure we have pthreads at all.
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

if (NOT OS_ANDROID)
    # Our compatibility layer doesn't build under Android, many errors in musl.
    add_subdirectory(base/glibc-compatibility)
endif ()

include (cmake/find/unwind.cmake)
include (cmake/find/cxx.cmake)

add_library(global-group INTERFACE)
target_link_libraries(global-group INTERFACE
    -Wl,--start-group
    $<TARGET_PROPERTY:global-libs,INTERFACE_LINK_LIBRARIES>
    -Wl,--end-group
)

link_libraries(global-group)

# FIXME: remove when all contribs will get custom cmake lists
install(
    TARGETS global-group global-libs
    EXPORT global
)
