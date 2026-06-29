set (DEFAULT_LIBS "-nodefaultlibs")

# Wire compiler-rt runtimes (builtins/sanitizers/XRay) into the link flags.
include (cmake/compiler_rt_link.cmake)

set (DEFAULT_LIBS "${DEFAULT_LIBS} -lc -lm -lrt -lpthread")

message(STATUS "Default libraries: ${DEFAULT_LIBS}")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

add_library(Threads::Threads INTERFACE IMPORTED)
set_target_properties(Threads::Threads PROPERTIES INTERFACE_LINK_LIBRARIES pthread)

include (cmake/unwind.cmake)
include (cmake/cxx.cmake)
