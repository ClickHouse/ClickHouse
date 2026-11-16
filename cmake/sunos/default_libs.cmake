# Set standard, system and compiler libraries explicitly for illumos/SunOS.
# This is intended for more control of what we are linking.

# illumos requires these defines to expose POSIX APIs in system headers:
# - _REENTRANT: thread-safe libc functions
# - _POSIX_PTHREAD_SEMANTICS: POSIX-compliant function signatures
# - __EXTENSIONS__: expose non-standard but necessary APIs
add_compile_definitions(_REENTRANT _POSIX_PTHREAD_SEMANTICS __EXTENSIONS__)

set (DEFAULT_LIBS "-nodefaultlibs")

# On SunOS/illumos, building compiler-rt from source is not yet supported
# (no toolchain file in contrib/compiler-rt-cmake/), so we use -lgcc_s.
set (BUILTINS_LIBRARY "-lgcc_s")

set (DEFAULT_LIBS "${DEFAULT_LIBS} ${BUILTINS_LIBRARY} -lc -lm -lrt -lpthread -ldl -lsocket -lnsl -lsendfile -lproc -lumem")

message(STATUS "Default libraries: ${DEFAULT_LIBS}")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

add_library(Threads::Threads INTERFACE IMPORTED)
set_target_properties(Threads::Threads PROPERTIES INTERFACE_LINK_LIBRARIES pthread)

include (cmake/unwind.cmake)
include (cmake/cxx.cmake)
