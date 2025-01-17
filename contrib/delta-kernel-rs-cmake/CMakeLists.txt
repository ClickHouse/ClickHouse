set(DELTA_KERNEL_RS_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/delta-kernel-rs")
set(DELTA_KERNEL_RS_BINARY_DIR "${CMAKE_BINARY_DIR}/contrib/delta-kernel-rs")
#
#set (SRCS
#)
#
#add_library(_delta_kernel_rs ${SRCS})
#target_include_directories(_delta_kernel_rs SYSTEM PUBLIC ../delta-kernel-rs/include)
#add_library(ch_contrib::delta_kernel_rs ALIAS _delta_kernel_rs)

if (NOT ENABLE_LIBRARIES)
  set(DEFAULT_ENABLE_RUST FALSE)
else()
  #list (APPEND CMAKE_MODULE_PATH "${ClickHouse_SOURCE_DIR}/contrib/delta-kernel-rs/cmake")
  #find_package(Rust)
  #set(DEFAULT_ENABLE_RUST ${Rust_FOUND})
endif()

#option(ENABLE_RUST "Enable rust" ${DEFAULT_ENABLE_RUST})
#if(NOT ENABLE_RUST)
#  message(STATUS "Not using rust")
#  return()
#endif()

message(STATUS "Checking Rust toolchain for current target")

if(DEFINED CMAKE_TOOLCHAIN_FILE)
    if(CMAKE_TOOLCHAIN_FILE MATCHES "ppc64le")
        set(Rust_CARGO_TARGET "powerpc64le-unknown-linux-gnu")
    elseif((CMAKE_TOOLCHAIN_FILE MATCHES "linux/toolchain-x86_64") AND (CMAKE_TOOLCHAIN_FILE MATCHES "musl"))
        set(Rust_CARGO_TARGET "x86_64-unknown-linux-musl")
    elseif(CMAKE_TOOLCHAIN_FILE MATCHES "linux/toolchain-x86_64")
        set(Rust_CARGO_TARGET "x86_64-unknown-linux-gnu")
    elseif((CMAKE_TOOLCHAIN_FILE MATCHES "linux/toolchain-aarch64") AND (CMAKE_TOOLCHAIN_FILE MATCHES "musl"))
        set(Rust_CARGO_TARGET "aarch64-unknown-linux-musl")
    elseif(CMAKE_TOOLCHAIN_FILE MATCHES "linux/toolchain-aarch64")
        set(Rust_CARGO_TARGET "aarch64-unknown-linux-gnu")
    elseif((CMAKE_TOOLCHAIN_FILE MATCHES "darwin") AND (CMAKE_TOOLCHAIN_FILE MATCHES "x86_64"))
        set(Rust_CARGO_TARGET "x86_64-apple-darwin")
    elseif((CMAKE_TOOLCHAIN_FILE MATCHES "darwin") AND (CMAKE_TOOLCHAIN_FILE MATCHES "aarch64"))
        set(Rust_CARGO_TARGET "aarch64-apple-darwin")
    elseif((CMAKE_TOOLCHAIN_FILE MATCHES "freebsd") AND (CMAKE_TOOLCHAIN_FILE MATCHES "x86_64"))
        set(Rust_CARGO_TARGET "x86_64-unknown-freebsd")
    elseif(CMAKE_TOOLCHAIN_FILE MATCHES "linux/toolchain-riscv64")
        set(Rust_CARGO_TARGET "riscv64gc-unknown-linux-gnu")
    else()
        message(FATAL_ERROR "Unsupported rust target")
    endif()
    message(STATUS "Switched Rust target to ${Rust_CARGO_TARGET}")
endif ()


# Propagate arch to rust build for CI
set(RUST_PLATFORM_TARGET "")
if("${OS_NAME}" STREQUAL "linux")
  if("${OS_ARCH}" STREQUAL "arm64")
    set(RUST_PLATFORM_TARGET "aarch64-unknown-linux-gnu")
  else()
    set(RUST_PLATFORM_TARGET "x86_64-unknown-linux-gnu")
  endif()
elseif("${OS_NAME}" STREQUAL "osx")
  # TODO: clean up upstream; we are not correctly setting OS_ARCH for cross
  # compile
  if("${OSX_BUILD_ARCH}" STREQUAL "arm64")
    set(RUST_PLATFORM_TARGET "aarch64-apple-darwin")
  elseif("${OSX_BUILD_ARCH}" STREQUAL "x86_64")
    set(RUST_PLATFORM_TARGET "x86_64-apple-darwin")
  elseif("${OS_ARCH}" STREQUAL "arm64")
    set(RUST_PLATFORM_TARGET "aarch64-apple-darwin")
  else()
    set(RUST_PLATFORM_TARGET "x86_64-apple-darwin")
  endif()
endif()

include(ExternalProject)
set(EXTENSION_SOURCES ${DELTA_KERNEL_RS_SOURCE_DIR}/delta_extension.cpp)

add_compile_definitions(DEFINE_DEFAULT_ENGINE)
set(DEFINE_DEFAULT_ENGINE 1)

set(TARGET_NAME delta)
project(${TARGET_NAME})

set(KERNEL_NAME delta_kernel)
add_library(_delta_kernel_rs STATIC ${EXTENSION_SOURCES})
#target_compile_definitions(_delta_kernel_rs -)
ExternalProject_Add(
  _delta_kernel_rs_target
  CONFIGURE_COMMAND ""
  GIT_REPOSITORY "file://${DELTA_KERNEL_RS_SOURCE_DIR}"  # Submodule path
  GIT_TAG 08f0764a00e89f42136fd478823d28278adc7ee8
  UPDATE_COMMAND ""
  BUILD_IN_SOURCE 1
  # Build debug build
  BUILD_COMMAND
      ${CMAKE_COMMAND} -E env CARGO_TARGET_DIR=${DELTA_KERNEL_RS_BINARY_DIR}/target/ffi-headers
      cargo build --package delta_kernel_ffi --workspace --all-features --target-dir "${DELTA_KERNEL_RS_BINARY_DIR}/target"
  # Build release build
  COMMAND
      ${CMAKE_COMMAND} -E env CARGO_TARGET_DIR=${DELTA_KERNEL_RS_BINARY_DIR}/target/ffi-headers
      cargo build --package delta_kernel_ffi --workspace --all-features --release --target-dir "${DELTA_KERNEL_RS_BINARY_DIR}/target"
  # Build DATs
  COMMAND
      ${CMAKE_COMMAND} -E env CARGO_TARGET_DIR=${DELTA_KERNEL_RS_BINARY_DIR}/target/ffi-headers
      cargo build --manifest-path=${DELTA_KERNEL_RS_SOURCE_DIR}/ffi/Cargo.toml
  BUILD_BYPRODUCTS "${DELTA_KERNEL_RS_BINARY_DIR}/target/debug/libdelta_kernel_ffi.a"
  BUILD_BYPRODUCTS "${DELTA_KERNEL_RS_BINARY_DIR}/target/release/libdelta_kernel_ffi.a"
  BUILD_BYPRODUCTS "${DELTA_KERNEL_RS_BINARY_DIR}/target/ffi-headers/delta_kernel_ffi.h"
  BUILD_BYPRODUCTS "${DELTA_KERNEL_RS_BINARY_DIR}/target/ffi-headers/delta_kernel_ffi.hpp"
  INSTALL_COMMAND ""
  LOG_BUILD ON)

#target_compile_options(_delta_kernel_rs PRIVATE -Wno-return-type-c-linkage)

add_dependencies(_delta_kernel_rs _delta_kernel_rs_target)
target_link_libraries(_delta_kernel_rs PUBLIC "${DELTA_KERNEL_RS_BINARY_DIR}/target/debug/libdelta_kernel_ffi.a")
target_include_directories(_delta_kernel_rs INTERFACE "${DELTA_KERNEL_RS_BINARY_DIR}/target/ffi-headers/")

#target_include_directories(_delta_kernel_rs PUBLIC ${DELTA_KERNEL_RS_SOURCE_DIR}/target/ffi-headers)

#add_library(ch_contrib::delta_kernel_rs STATIC IMPORTED)
#add_dependencies(ch_contrib::delta_kernel_rs _delta_kernel_rs_target)
#set_target_properties(ch_contrib::delta_kernel_rs PROPERTIES IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/debug/libdelta_kernel_ffi.a")

add_library(ch_contrib::delta_kernel_rs ALIAS _delta_kernel_rs)
