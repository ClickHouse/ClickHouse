set(DELTA_KERNEL_RS_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/delta-kernel-rs")
set(DELTA_KERNEL_RS_BINARY_DIR "${CMAKE_BINARY_DIR}/contrib/delta-kernel-rs")
set(DELTA_KERNEL_RS_CMAKE_DIR "${ClickHouse_SOURCE_DIR}/contrib/delta-kernel-rs-cmake")

include(ExternalProject)


set(ENABLE_DELTA_KERNEL_RS TRUE)
if (NOT ENABLE_LIBRARIES)
  set(ENABLE_DELTA_KERNEL_RS FALSE)
else()
  list (APPEND CMAKE_MODULE_PATH "${ClickHouse_SOURCE_DIR}/contrib/delta-kernel-rs/cmake")
  find_package(Rust)
  set(DEFAULT_ENABLE_RUST ${Rust_FOUND})
endif()

option(ENABLE_DELTA_KERNEL_RS "Enable delta-kernel-rs" ${DELTA_KERNEL_RS})
if (NOT ENABLE_DELTA_KERNEL_RS)
  message(STATUS "Not using delta-kernel-rs")
  return()
endif()

file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/empty.cpp" CONTENT " ")
set(SOURCES "${CMAKE_CURRENT_BINARY_DIR}/empty.cpp")

add_library(_delta_kernel_rs STATIC ${SOURCES})

set(DELTA_KERNEL_TARGET_PATH ${DELTA_KERNEL_RS_BINARY_DIR}/target4)

ExternalProject_Add(
  _delta_kernel_rs_target
  CONFIGURE_COMMAND ""
  GIT_REPOSITORY "file://${DELTA_KERNEL_RS_SOURCE_DIR}"
  GIT_TAG 08f0764a00e89f42136fd478823d28278adc7ee8
  UPDATE_COMMAND ""
  BUILD_IN_SOURCE 1
  BUILD_COMMAND
      ${CMAKE_COMMAND} -E env CARGO_TARGET_DIR=${DELTA_KERNEL_TARGET_PATH}
      cargo build
      --package delta_kernel_ffi
      --workspace
      --all-features
      --target-dir "${DELTA_KERNEL_TARGET_PATH}"
      --manifest-path=${DELTA_KERNEL_RS_SOURCE_DIR}/ffi/Cargo.toml
  BUILD_BYPRODUCTS "${DELTA_KERNEL_TARGET_PATH}/debug/libdelta_kernel_ffi.a"
  #BUILD_BYPRODUCTS "${DELTA_KERNEL_TARGET_PATH}/release/libdelta_kernel_ffi.a"
  BUILD_BYPRODUCTS "${DELTA_KERNEL_TARGET_PATH}/delta_kernel_ffi.hpp"
  INSTALL_COMMAND ""
  LOG_BUILD ON)

#target_compile_options(_delta_kernel_rs PRIVATE -Wno-return-type-c-linkage)

add_dependencies(_delta_kernel_rs _delta_kernel_rs_target)
target_link_libraries(_delta_kernel_rs PRIVATE "${DELTA_KERNEL_TARGET_PATH}/debug/libdelta_kernel_ffi.a")
target_include_directories(_delta_kernel_rs INTERFACE "${DELTA_KERNEL_TARGET_PATH}/")
add_library(ch_contrib::delta_kernel_rs ALIAS _delta_kernel_rs)
