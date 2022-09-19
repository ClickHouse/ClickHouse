if (NOT ENABLE_LIBRARIES)
  option(ENABLE_RUST "Enable rust" FALSE)
else()
  list (APPEND CMAKE_MODULE_PATH "${ClickHouse_SOURCE_DIR}/contrib/corrosion/cmake")
  find_package(Rust)
  option(ENABLE_RUST "Enable rust" $Rust_FOUND)
endif()

if(NOT ENABLE_RUST)
  message(STATUS "Not using rust")
  return()
endif()

message(STATUS "Checking Rust toolchain for current target")

if(CMAKE_TOOLCHAIN_FILE MATCHES "linux/toolchain-x86_64")
    set(Rust_CARGO_TARGET "x86_64-unknown-linux-gnu")
endif()

if(CMAKE_TOOLCHAIN_FILE MATCHES "linux/toolchain-aarch64")
    set(Rust_CARGO_TARGET "aarch64-unknown-linux-gnu")
endif()

if((CMAKE_TOOLCHAIN_FILE MATCHES "darwin") AND (CMAKE_TOOLCHAIN_FILE MATCHES "aarch64"))
    set(Rust_CARGO_TARGET "aarch64-apple-darwin")
    message(STATUS "Rust is not available on aarch64-apple-darwin")
    set(ENABLE_RUST FALSE)
    return()
endif()

if((CMAKE_TOOLCHAIN_FILE MATCHES "darwin") AND (CMAKE_TOOLCHAIN_FILE MATCHES "x86_64"))
    set(Rust_CARGO_TARGET "x86_64-apple-darwin")
endif()

if((CMAKE_TOOLCHAIN_FILE MATCHES "freebsd") AND (CMAKE_TOOLCHAIN_FILE MATCHES "x86_64"))
    set(Rust_CARGO_TARGET "x86_64-unknown-freebsd")
endif()

if(CMAKE_TOOLCHAIN_FILE MATCHES "ppc64le")
    set(Rust_CARGO_TARGET "powerpc64le-unknown-linux-gnu")
endif()

message(STATUS "Switched Rust target to ${Rust_CARGO_TARGET}")

# Define function corrosion_import_crate()
include ("${ClickHouse_SOURCE_DIR}/contrib/corrosion/cmake/Corrosion.cmake")
