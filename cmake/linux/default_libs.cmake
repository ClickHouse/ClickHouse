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

set (DEFAULT_LIBS "${DEFAULT_LIBS} ${BUILTINS_LIBRARY} ${COVERAGE_OPTION} -lc -lm -lrt -lpthread -ldl")

message(STATUS "Default libraries: ${DEFAULT_LIBS}")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

if (COMPILER_GCC)
    execute_process (COMMAND ${CMAKE_CXX_COMPILER} --print-file-name=include OUTPUT_VARIABLE COMPILER_GCC_INCLUDE_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process (COMMAND ${CMAKE_CXX_COMPILER} --print-file-name=include-fixed OUTPUT_VARIABLE COMPILER_GCC_INCLUDE_FIXED_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)
    set (COMPILER_INCLUDE_DIRS ${COMPILER_GCC_INCLUDE_DIR} ${COMPILER_GCC_INCLUDE_FIXED_DIR})
elseif (COMPILER_CLANG)
    execute_process (COMMAND ${CMAKE_CXX_COMPILER} --print-file-name=include OUTPUT_VARIABLE COMPILER_CLANG_INCLUDE_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)
    set (COMPILER_INCLUDE_DIRS ${COMPILER_CLANG_INCLUDE_DIR})
endif ()

# Global libraries

add_library(global-libs INTERFACE)

# Unfortunately '-pthread' doesn't work with '-nodefaultlibs'.
# Just make sure we have pthreads at all.
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# glibc-compatibility library relies to fixed version of libc headers
# (because minor changes in function attributes between different glibc versions will introduce incompatibilities)
# This is for x86_64. For other architectures we have separate toolchains.
if (ARCH_AMD64)
    set(CMAKE_C_STANDARD_INCLUDE_DIRECTORIES
        ${ClickHouse_SOURCE_DIR}/contrib/libc-headers/x86_64-linux-gnu
        ${ClickHouse_SOURCE_DIR}/contrib/libc-headers
        ${COMPILER_INCLUDE_DIRS})

    set(CMAKE_CXX_STANDARD_INCLUDE_DIRECTORIES
        ${ClickHouse_SOURCE_DIR}/contrib/libc-headers/x86_64-linux-gnu
        ${ClickHouse_SOURCE_DIR}/contrib/libc-headers
        ${COMPILER_INCLUDE_DIRS})

    # Disable unwanted includes to get more isolated build. The build should not depend on the percularities of the user environment.
    # NOTE It is very similar to using custom "toolchain". And using custom toolchain is more simple but is less convenient for users.
    # This is also very helpful to avoid mess with system libraries (e.g. using wrong version of zlib.h)
    # This will also help for further migration to musl-libc.

    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -nostdinc")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -nostdinc")
endif ()

add_subdirectory(libs/libglibc-compatibility)
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
