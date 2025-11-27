# Set standard, system and compiler libraries explicitly.
# This is intended for more control of what we are linking.

set (DEFAULT_LIBS "-nodefaultlibs")

# We need builtins from Clang
execute_process (COMMAND
    ${CMAKE_CXX_COMPILER} --target=${CMAKE_CXX_COMPILER_TARGET} --print-libgcc-file-name --rtlib=compiler-rt
    OUTPUT_VARIABLE BUILTINS_LIBRARY
    COMMAND_ERROR_IS_FATAL ANY
    OUTPUT_STRIP_TRAILING_WHITESPACE)

if (NOT EXISTS "${BUILTINS_LIBRARY}")
    # In the past we used to fallback into using libgcc, which then required sysroot to include gcc libraries.
    # Now we build the compiler-rt from source instead so we are independent of the target system's gcc.
    include (cmake/build_clang_builtin.cmake)
    build_clang_builtin(${CMAKE_CXX_COMPILER_TARGET} BUILTINS_LIBRARY)
elseif (SANITIZE STREQUAL undefined)
    # Apparently, in clang-19, the UBSan support library for C++ was moved out into ubsan_standalone_cxx.a, so we have to include both.
    string(REPLACE "builtins.a" "ubsan_standalone_cxx.a" EXTRA_BUILTINS_LIBRARY "${BUILTINS_LIBRARY}")
endif ()

if (OS_ANDROID)
    # pthread and rt are included in libc
    set (DEFAULT_LIBS "${DEFAULT_LIBS} ${BUILTINS_LIBRARY} ${EXTRA_BUILTINS_LIBRARY} ${COVERAGE_OPTION} -lc -lm -ldl")
elseif (USE_MUSL)
    set (DEFAULT_LIBS "${DEFAULT_LIBS} ${BUILTINS_LIBRARY} ${EXTRA_BUILTINS_LIBRARY} ${COVERAGE_OPTION} -static -lc")
else ()
    set (DEFAULT_LIBS "${DEFAULT_LIBS} ${BUILTINS_LIBRARY} ${EXTRA_BUILTINS_LIBRARY} ${COVERAGE_OPTION} -lc -lm -lrt -lpthread -ldl")
endif ()

message(STATUS "Default libraries: ${DEFAULT_LIBS}")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

add_library(Threads::Threads INTERFACE IMPORTED)
set_target_properties(Threads::Threads PROPERTIES INTERFACE_LINK_LIBRARIES pthread)

include (cmake/unwind.cmake)
include (cmake/cxx.cmake)

if (NOT OS_ANDROID)
    if (NOT USE_MUSL)
        disable_dummy_launchers_if_needed()
        # Our compatibility layer doesn't build under Android, many errors in musl.
        add_subdirectory(base/glibc-compatibility)
        enable_dummy_launchers_if_needed()
    endif ()
    add_subdirectory(base/harmful)
endif ()
