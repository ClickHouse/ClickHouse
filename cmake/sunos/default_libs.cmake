# Set standard, system and compiler libraries explicitly for illumos/SunOS.
# This is intended for more control of what we are linking.

# illumos requires these defines to expose POSIX APIs in system headers:
# - _REENTRANT: thread-safe libc functions
# - _POSIX_PTHREAD_SEMANTICS: POSIX-compliant function signatures
# - __EXTENSIONS__: expose non-standard but necessary APIs
add_compile_definitions(_REENTRANT _POSIX_PTHREAD_SEMANTICS __EXTENSIONS__)

set (DEFAULT_LIBS "-nodefaultlibs")

set (BUILTINS_LIBRARY "-lgcc_s")
if (USE_SYSTEM_COMPILER_RT)
    # Try to find the clang builtins library
    execute_process(COMMAND
        ${CMAKE_CXX_COMPILER} --print-libgcc-file-name --rtlib=compiler-rt
        OUTPUT_VARIABLE BUILTINS_LIBRARY
        COMMAND_ERROR_IS_FATAL ANY
        OUTPUT_STRIP_TRAILING_WHITESPACE)
endif ()

set (DEFAULT_LIBS "${DEFAULT_LIBS} ${BUILTINS_LIBRARY} -lc -lm -lrt -lpthread -ldl -lsocket -lnsl -lsendfile -lproc -lumem")

message(STATUS "Default libraries: ${DEFAULT_LIBS}")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

add_library(Threads::Threads INTERFACE IMPORTED)
set_target_properties(Threads::Threads PROPERTIES INTERFACE_LINK_LIBRARIES pthread)

include (cmake/unwind.cmake)
include (cmake/cxx.cmake)
