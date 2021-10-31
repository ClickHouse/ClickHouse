set(protobuf_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/protobuf")
set(protobuf_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/protobuf")

set(protobuf_WITH_ZLIB 0 CACHE INTERNAL "" FORCE) # actually will use zlib, but skip find
set(protobuf_BUILD_TESTS OFF CACHE INTERNAL "" FORCE)

if (MAKE_STATIC_LIBRARIES)
    set(protobuf_BUILD_SHARED_LIBS OFF CACHE INTERNAL "" FORCE)
else ()
    set(protobuf_BUILD_SHARED_LIBS ON CACHE INTERNAL "" FORCE)
endif ()

if (CMAKE_CROSSCOMPILING)
    # Will build 'protoc' for host arch instead of cross-compiling
    set(protobuf_BUILD_PROTOC_BINARIES OFF CACHE INTERNAL "" FORCE)
endif ()

add_subdirectory("${protobuf_SOURCE_DIR}/cmake" "${protobuf_BINARY_DIR}")

if (ENABLE_FUZZING)
    # `protoc` will be built with sanitizer and it could fail during ClickHouse build
    # It easily reproduces in oss-fuzz building pipeline
    # To avoid this we can try to build `protoc` without any sanitizer with option `-fno-sanitize=all`, but
    # it this case we will face with linker errors, because libcxx still will be built with sanitizer
    # So, we can simply suppress all of these failures with a combination this flag and an environment variable
    # export MSAN_OPTIONS=exit_code=0
    target_compile_options(protoc PRIVATE "-fsanitize-recover=all")
endif()

# We don't want to stop compilation on warnings in protobuf's headers.
# The following line overrides the value assigned by the command target_include_directories() in libprotobuf.cmake
set_property(TARGET libprotobuf PROPERTY INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${protobuf_SOURCE_DIR}/src")

if (CMAKE_CROSSCOMPILING)
    # Build 'protoc' for host arch
    set (PROTOC_BUILD_DIR "${protobuf_BINARY_DIR}/build")

    if (NOT EXISTS "${PROTOC_BUILD_DIR}/protoc")

        # This is quite ugly but I cannot make dependencies work propery.

        execute_process(
            COMMAND mkdir -p ${PROTOC_BUILD_DIR}
            COMMAND_ECHO STDOUT)

        execute_process(
            COMMAND ${CMAKE_COMMAND}
                "-G${CMAKE_GENERATOR}"
                "-DCMAKE_MAKE_PROGRAM=${CMAKE_MAKE_PROGRAM}"
                "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
                "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
                "-Dprotobuf_BUILD_TESTS=0"
                "-Dprotobuf_BUILD_CONFORMANCE=0"
                "-Dprotobuf_BUILD_EXAMPLES=0"
                "-Dprotobuf_BUILD_PROTOC_BINARIES=1"
                "${protobuf_SOURCE_DIR}/cmake"
            WORKING_DIRECTORY "${PROTOC_BUILD_DIR}"
            COMMAND_ECHO STDOUT)

        execute_process(
            COMMAND ${CMAKE_COMMAND} --build "${PROTOC_BUILD_DIR}"
            COMMAND_ECHO STDOUT)
    endif ()

#    add_custom_command (
#        OUTPUT ${PROTOC_BUILD_DIR}
#        COMMAND mkdir -p ${PROTOC_BUILD_DIR})
#
#    add_custom_command (
#        OUTPUT "${PROTOC_BUILD_DIR}/CMakeCache.txt"
#
#        COMMAND ${CMAKE_COMMAND}
#             -G"${CMAKE_GENERATOR}"
#             -DCMAKE_MAKE_PROGRAM="${CMAKE_MAKE_PROGRAM}"
#             -DCMAKE_C_COMPILER="${CMAKE_C_COMPILER}"
#             -DCMAKE_CXX_COMPILER="${CMAKE_CXX_COMPILER}"
#             -Dprotobuf_BUILD_TESTS=0
#             -Dprotobuf_BUILD_CONFORMANCE=0
#             -Dprotobuf_BUILD_EXAMPLES=0
#             -Dprotobuf_BUILD_PROTOC_BINARIES=1
#             "${protobuf_SOURCE_DIR}/cmake"
#
#         DEPENDS "${PROTOC_BUILD_DIR}"
#         WORKING_DIRECTORY "${PROTOC_BUILD_DIR}"
#         COMMENT "Configuring 'protoc' for host architecture."
#         USES_TERMINAL)
#
#     add_custom_command (
#         OUTPUT "${PROTOC_BUILD_DIR}/protoc"
#         COMMAND ${CMAKE_COMMAND} --build "${PROTOC_BUILD_DIR}"
#         DEPENDS "${PROTOC_BUILD_DIR}/CMakeCache.txt"
#         COMMENT "Building 'protoc' for host architecture."
#         USES_TERMINAL)
#
#     add_custom_target (protoc-host DEPENDS "${PROTOC_BUILD_DIR}/protoc")

    add_executable(protoc IMPORTED GLOBAL)
    set_target_properties (protoc PROPERTIES IMPORTED_LOCATION "${PROTOC_BUILD_DIR}/protoc")
    add_dependencies(protoc "${PROTOC_BUILD_DIR}/protoc")

endif ()
