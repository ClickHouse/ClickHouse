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
