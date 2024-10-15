set(ENABLE_GRPC_DEFAULT ${ENABLE_LIBRARIES})
option(ENABLE_GRPC "Use gRPC" ${ENABLE_GRPC_DEFAULT})

if(NOT ENABLE_GRPC)
  message(STATUS "Not using gRPC")
  return()
endif()

set(CMAKE_CXX_STANDARD 17)

set(_gRPC_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/grpc")
set(_gRPC_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/grpc")

if(TARGET OpenSSL::SSL)
  set(gRPC_USE_UNSECURE_LIBRARIES FALSE)
else()
  set(gRPC_USE_UNSECURE_LIBRARIES TRUE)
endif()

include(grpc.cmake)
include(protobuf_generate_grpc.cmake)

set(gRPC_CPP_PLUGIN $<TARGET_FILE:grpc_cpp_plugin>)
set(gRPC_PYTHON_PLUGIN $<TARGET_FILE:grpc_python_plugin>)

set(gRPC_INCLUDE_DIRS "${ClickHouse_SOURCE_DIR}/contrib/grpc/include")
if(gRPC_USE_UNSECURE_LIBRARIES)
  set(gRPC_LIBRARIES grpc_unsecure grpc++_unsecure)
else()
  set(gRPC_LIBRARIES grpc grpc++)
endif()
add_library(_ch_contrib_grpc INTERFACE)
target_link_libraries(_ch_contrib_grpc INTERFACE ${gRPC_LIBRARIES})
target_include_directories(_ch_contrib_grpc SYSTEM INTERFACE ${gRPC_INCLUDE_DIRS})
add_library(ch_contrib::grpc ALIAS _ch_contrib_grpc)

# Here we are trying to build a binary tool grpc_cpp_plugin in case of cross-compilation.
# We need this file only during compilation process itself so we need it for our "host"
# platform, not "target" platform.
# If we are doing normal compilation this executable will be produced in grpc.cmake.
#
# All code inside this block looks so weird because cmake fundametally doesn't
# support different toolchains for different targets. So we just running it
# in "bash script" fashion with different (actually without, i.e. default) toolchain.
#
# FIXME Sorry, I don't know cmake.
if (NOT CMAKE_HOST_SYSTEM_NAME STREQUAL CMAKE_SYSTEM_NAME
    OR NOT CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL CMAKE_SYSTEM_PROCESSOR)

    # First we need to build openssl for host plaform
    set(OPENSSL_BUILD_DIR "${_gRPC_BINARY_DIR}/build_openssl")

    set(OPENSSL_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/openssl-cmake")

    execute_process(
        COMMAND mkdir -p ${OPENSSL_BUILD_DIR}
        COMMAND_ECHO STDOUT
        COMMAND_ERROR_IS_FATAL ANY
    )

    if (CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "amd64|x86_64")
        set (HOST_ARCH_AMD64 1)
    elseif (CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^(aarch64.*|AARCH64.*|arm64.*|ARM64.*)")
        set (HOST_ARCH_AARCH64 1)
    elseif (CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^(powerpc64le.*|ppc64le.*|PPC64LE.*)")
        set (HOST_ARCH_PPC64LE 1)
    elseif (CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^(s390x.*|S390X.*)")
        set (HOST_ARCH_S390X 1)
    elseif (CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "riscv64")
        set (HOST_ARCH_RISCV64 1)
    endif ()

    if (CMAKE_HOST_SYSTEM_NAME MATCHES "Linux")
        set (HOST_OS_LINUX 1)
    elseif (CMAKE_HOST_SYSTEM_NAME MATCHES "Darwin")
        set (HOST_OS_DARWIN 1)
    endif ()
    execute_process(
        COMMAND ${CMAKE_COMMAND}
            "-G${CMAKE_GENERATOR}"
            "-DCMAKE_MAKE_PROGRAM=${CMAKE_MAKE_PROGRAM}"
            "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
            "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
            "-DARCH_AMD64=${HOST_ARCH_AMD64}"
            "-DARCH_AARCH64=${HOST_ARCH_AARCH64}"
            "-DARCH_PPC64LE=${HOST_ARCH_PPC64LE}"
            "-DARCH_S390X=${HOST_ARCH_S390X}"
            "-DARCH_RISCV64=${HOST_ARCH_RISCV64}"
            "-DOS_DARWIN=${HOST_OS_DARWIN}"
            "-DOPENSSL_AUX_BUILD_FOR_CROSS_COMPILATION=1"
            "-DClickHouse_BINARY_DIR=${ClickHouse_BINARY_DIR}"
            "-DClickHouse_SOURCE_DIR=${ClickHouse_SOURCE_DIR}"
            "${OPENSSL_SOURCE_DIR}"
        WORKING_DIRECTORY "${OPENSSL_BUILD_DIR}"
        COMMAND_ECHO STDOUT
        COMMAND_ERROR_IS_FATAL ANY
    )

    execute_process(
        COMMAND ${CMAKE_COMMAND} --build "${OPENSSL_BUILD_DIR}"
        COMMAND_ECHO STDOUT
        COMMAND_ERROR_IS_FATAL ANY
    )

    execute_process(
        COMMAND ${CMAKE_COMMAND} --install "${OPENSSL_BUILD_DIR}"
        COMMAND_ECHO STDOUT
        COMMAND_ERROR_IS_FATAL ANY
    )

    # It's not important on which file we depend, we just want to specify right order
    add_library(openssl_for_grpc STATIC IMPORTED GLOBAL)
    set_target_properties (openssl_for_grpc PROPERTIES IMPORTED_LOCATION "${OPENSSL_BUILD_DIR}/libssl.a")
    add_dependencies(openssl_for_grpc "${OPENSSL_BUILD_DIR}/libssl.a")

    # Okay, openssl ready, let's build grpc_cpp_plugin
    set (GRPC_CPP_PLUGIN_BUILD_DIR "${_gRPC_BINARY_DIR}/build")

    execute_process(
        COMMAND mkdir -p ${GRPC_CPP_PLUGIN_BUILD_DIR}
        COMMAND_ECHO STDOUT
        COMMAND_ERROR_IS_FATAL ANY
    )

    set(abseil_source_dir "${ClickHouse_SOURCE_DIR}/contrib/abseil-cpp")
    set(protobuf_source_dir "${ClickHouse_SOURCE_DIR}/contrib/google-protobuf")
    set(re2_source_dir "${ClickHouse_SOURCE_DIR}/contrib/re2")
    set(ssl_source_dir "${ClickHouse_SOURCE_DIR}/contrib/openssl-cmake")
    set(zlib_source_dir "${ClickHouse_SOURCE_DIR}/contrib/zlib-ng")
    # For some reason config exists only in this directory
    set(zlib_config_source_dir "${ClickHouse_BINARY_DIR}/contrib/zlib-ng-cmake")
    set(cares_source_dir "${ClickHouse_SOURCE_DIR}/contrib/c-ares")

    execute_process(
        COMMAND ${CMAKE_COMMAND}
            "-G${CMAKE_GENERATOR}"
            "-DCMAKE_MAKE_PROGRAM=${CMAKE_MAKE_PROGRAM}"
            "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
            "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
            "-DABSL_ROOT_DIR=${abseil_source_dir}"
            "-DCMAKE_CXX_STANDARD_INCLUDE_DIRECTORIES=${zlib_config_source_dir}"
            "-DgRPC_INSTALL=0"
            "-DABSL_ENABLE_INSTALL=1"
            "-DPROTOBUF_ROOT_DIR=${protobuf_source_dir}"
            "-DRE2_ROOT_DIR=${re2_source_dir}"
            "-DCARES_ROOT_DIR=${cares_source_dir}"
            "-DOPENSSL_ROOT_DIR=${OPENSSL_BUILD_DIR}"
            "-DOPENSSL_INCLUDE_DIR=${OPENSSL_BUILD_DIR}/include"
            "-DZLIB_ROOT_DIR=${zlib_source_dir}"
            "-DgRPC_SSL_PROVIDER=package"
            "${_gRPC_SOURCE_DIR}"
        WORKING_DIRECTORY "${GRPC_CPP_PLUGIN_BUILD_DIR}"
        COMMAND_ECHO STDOUT
        COMMAND_ERROR_IS_FATAL ANY
    )

    execute_process(
        COMMAND ${CMAKE_COMMAND} --build "${GRPC_CPP_PLUGIN_BUILD_DIR}"
        COMMAND_ECHO STDOUT
        COMMAND_ERROR_IS_FATAL ANY
    )

    add_executable(grpc_cpp_plugin IMPORTED GLOBAL)
    set_target_properties (grpc_cpp_plugin PROPERTIES IMPORTED_LOCATION "${GRPC_CPP_PLUGIN_BUILD_DIR}/grpc_cpp_plugin")
    add_dependencies(grpc_cpp_plugin "${GRPC_CPP_PLUGIN_BUILD_DIR}/grpc_cpp_plugin")
    add_dependencies(grpc_cpp_plugin openssl_for_grpc)
endif()
