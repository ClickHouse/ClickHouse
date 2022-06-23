set(protobuf_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/protobuf")
set(protobuf_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/protobuf")

set(protobuf_WITH_ZLIB 0 CACHE INTERNAL "" FORCE) # actually will use zlib, but skip find
set(protobuf_BUILD_TESTS OFF CACHE INTERNAL "" FORCE)

if (MAKE_STATIC_LIBRARIES)
    set(protobuf_BUILD_SHARED_LIBS OFF CACHE INTERNAL "" FORCE)
else ()
    set(protobuf_BUILD_SHARED_LIBS ON CACHE INTERNAL "" FORCE)
endif ()

add_subdirectory("${protobuf_SOURCE_DIR}/cmake" "${protobuf_BINARY_DIR}")

# We don't want to stop compilation on warnings in protobuf's headers.
# The following line overrides the value assigned by the command target_include_directories() in libprotobuf.cmake 
set_property(TARGET libprotobuf PROPERTY INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${protobuf_SOURCE_DIR}/src")
