set (DEFAULT_LIBS "-nodefaultlibs")

set (DEFAULT_LIBS "${DEFAULT_LIBS} ${COVERAGE_OPTION} -lc -lm -lpthread -ldl")

message(STATUS "Default libraries: ${DEFAULT_LIBS}")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

# Minimal supported SDK version
set(CMAKE_OSX_DEPLOYMENT_TARGET 10.15)

set(THREADS_PREFER_PTHREAD_FLAG ON)

include (cmake/unwind.cmake)
include (cmake/cxx.cmake)
link_libraries(global-group)

target_link_libraries(global-group INTERFACE
    $<TARGET_PROPERTY:global-libs,INTERFACE_LINK_LIBRARIES>
)

# FIXME: remove when all contribs will get custom cmake lists
install(
    TARGETS global-group global-libs
    EXPORT global
)
