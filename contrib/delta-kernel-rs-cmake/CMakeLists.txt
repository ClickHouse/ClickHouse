set(DELTA_KERNEL_RS_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/delta-kernel-rs")
set(DELTA_KERNEL_RS_BINARY_DIR "${CMAKE_BINARY_DIR}/contrib/delta-kernel-rs")
set(DELTA_KERNEL_RS_CMAKE_DIR "${ClickHouse_SOURCE_DIR}/contrib/delta-kernel-rs-cmake")

include(ExternalProject)


set(ENABLE_DELTA_KERNEL_RS TRUE)
# TODO: For now only build Linux x86 and ARM in Release of Debug
if (NOT ENABLE_LIBRARIES OR NOT OS_LINUX OR (NOT ARCH_AMD64 AND NOT ARCH_AARCH64) OR SANITIZE OR SANITIZE_COVERAGE)
  set(ENABLE_DELTA_KERNEL_RS FALSE)
else()
  list (APPEND CMAKE_MODULE_PATH "${ClickHouse_SOURCE_DIR}/contrib/delta-kernel-rs/cmake")
  find_package(Rust)
  set(DEFAULT_ENABLE_RUST ${Rust_FOUND})
endif()

option(ENABLE_RUST "Enable rust" ${DEFAULT_ENABLE_RUST})
if(NOT ENABLE_RUST)
  message(STATUS "Not using rust")
  return()
endif()

option(ENABLE_DELTA_KERNEL_RS "Enable delta-kernel-rs" ${DELTA_KERNEL_RS})
if (NOT ENABLE_DELTA_KERNEL_RS)
  message(STATUS "Not using delta-kernel-rs")
  return()
endif()

file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/empty.cpp" CONTENT " ")
set(SOURCES "${CMAKE_CURRENT_BINARY_DIR}/empty.cpp")

add_library(_delta_kernel_rs STATIC ${SOURCES})

set(DELTA_KERNEL_TARGET_PATH ${DELTA_KERNEL_RS_BINARY_DIR}/target6)

if(ARCH_AMD64)
    if(OS_DARWIN)
        set(PLATFORM_DIRECTORY darwin_x86_64)
        add_definitions(-DL_ENDIAN)
    else()
        set(PLATFORM_DIRECTORY linux_x86_64)
        add_definitions(-DAES_ASM -DBSAES_ASM -DCMLL_ASM -DECP_NISTZ256_ASM -DGHASH_ASM -DKECCAK1600_ASM -DMD5_ASM -DOPENSSL_BN_ASM_GF2m -DOPENSSL_BN_ASM_MONT -DOPENSSL_BN_ASM_MONT5 -DOPENSSL_CPUID_OBJ -DOPENSSL_IA32_SSE2 -DPOLY1305_ASM -DRC4_ASM -DSHA1_ASM -DSHA256_ASM -DSHA512_ASM -DVPAES_ASM -DWHIRLPOOL_ASM -DX25519_ASM -DL_ENDIAN)
    endif()
elseif(ARCH_AARCH64)
    if(OS_DARWIN)
        set(PLATFORM_DIRECTORY darwin_aarch64)
        add_definitions(-DL_ENDIAN)
    else()
        set(PLATFORM_DIRECTORY linux_aarch64)
        add_definitions(-DBSAES_ASM -DECP_NISTZ256_ASM -DECP_SM2P256_ASM -DKECCAK1600_ASM -DMD5_ASM -DOPENSSL_BN_ASM_MONT -DOPENSSL_CPUID_OBJ -DOPENSSL_SM3_ASM -DPOLY1305_ASM -DSHA1_ASM -DSHA256_ASM -DSHA512_ASM -DSM4_ASM -DVPAES_ASM -DVPSM4_ASM -DL_ENDIAN)
    endif()
elseif(ARCH_PPC64LE)
    set(PLATFORM_DIRECTORY linux_ppc64le)
    add_definitions(-DOPENSSL_CPUID_OBJ -DL_ENDIAN)
elseif(ARCH_S390X)
    set(PLATFORM_DIRECTORY linux_s390x)
    add_definitions(-DAES_CTR_ASM -DOPENSSL_CPUID_OBJ -DB_ENDIAN)
elseif(ARCH_RISCV64)
    set(PLATFORM_DIRECTORY linux_riscv64)
    add_definitions(-DOPENSSL_CPUID_OBJ -DL_ENDIAN)
elseif(ARCH_LOONGARCH64)
    set(PLATFORM_DIRECTORY linux_loongarch64)
    add_definitions(-DOPENSSL_CPUID_OBJ -DL_ENDIAN)
endif()

file(COPY ${ClickHouse_SOURCE_DIR}/contrib/openssl-cmake/${PLATFORM_DIRECTORY}/include/ DESTINATION ${DELTA_KERNEL_RS_CMAKE_DIR}/${PLATFORM_DIRECTORY})
file(COPY ${ClickHouse_SOURCE_DIR}/contrib/openssl/include/ DESTINATION ${DELTA_KERNEL_RS_CMAKE_DIR}/${PLATFORM_DIRECTORY})

if ("${CMAKE_BUILD_TYPE_UC}" STREQUAL "DEBUG")
    SET(SSLNAME "ssld")
    SET(CRYPTONAME "cryptod")
else()
    SET(SSLNAME "ssl")
    SET(CRYPTONAME "crypto")
endif()


ExternalProject_Add(
  _delta_kernel_rs_target
  CONFIGURE_COMMAND ""
  GIT_REPOSITORY "file://${DELTA_KERNEL_RS_SOURCE_DIR}"
  GIT_TAG 7254023ab8d58eddbb70fb065f7b5996455bcfc9
  GIT_REMOTE_UPDATE_STRATEGY CHECKOUT
  UPDATE_COMMAND ""
  BUILD_IN_SOURCE 1
  BUILD_COMMAND
      ${CMAKE_COMMAND} -E env CARGO_TARGET_DIR=${DELTA_KERNEL_TARGET_PATH} OPENSSL_LIBS=${SSLNAME}:${CRYPTONAME} OPENSSL_STATIC=1 OPENSSL_LIB_DIR=${CMAKE_BINARY_DIR}/contrib/openssl-cmake/ OPENSSL_INCLUDE_DIR=${DELTA_KERNEL_RS_CMAKE_DIR}/${PLATFORM_DIRECTORY}
      cargo build --verbose
      --package delta_kernel_ffi
      --all-features
      --verbose
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
#target_link_libraries(_delta_kernel_rs "${CMAKE_BINARY_DIR}/contrib/openssl-cmake/libssld.a")
target_link_libraries (_delta_kernel_rs PRIVATE OpenSSL::Crypto OpenSSL::SSL)
target_include_directories(_delta_kernel_rs INTERFACE "${DELTA_KERNEL_TARGET_PATH}/")
add_library(ch_contrib::delta_kernel_rs ALIAS _delta_kernel_rs)
