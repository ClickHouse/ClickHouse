# Needed for:
# - securely connecting to an external server, e.g. clickhouse-client --host ... --secure
# - lots of thirdparty libraries
option(ENABLE_SSL "Enable ssl" ${ENABLE_LIBRARIES})

if(NOT ENABLE_SSL)
  message(STATUS "Not using openssl")
  return()
endif()

# Copyright (c) 2019 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

# This file is created by generate_build_files.py and edited accordingly.

cmake_minimum_required(VERSION 3.5)

project(BoringSSL LANGUAGES C CXX)

set(BORINGSSL_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/boringssl")

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(CLANG 1)
endif()

if(CMAKE_COMPILER_IS_GNUCXX OR CLANG)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -fno-common -fno-exceptions -fno-rtti")
  if(APPLE AND CLANG)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
  endif()

  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-common -std=c11")
endif()

# pthread_rwlock_t requires a feature flag.
if(NOT WIN32)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_XOPEN_SOURCE=700")
endif()

if(WIN32)
  add_definitions(-D_HAS_EXCEPTIONS=0)
  add_definitions(-DWIN32_LEAN_AND_MEAN)
  add_definitions(-DNOMINMAX)
  # Allow use of fopen.
  add_definitions(-D_CRT_SECURE_NO_WARNINGS)
  # VS 2017 and higher supports STL-only warning suppressions.
  # A bug in CMake < 3.13.0 may cause the space in this value to
  # cause issues when building with NASM. In that case, update CMake.
  add_definitions("-D_STL_EXTRA_DISABLED_WARNINGS=4774 4987")
endif()

add_definitions(-DBORINGSSL_IMPLEMENTATION)

# CMake's iOS support uses Apple's multiple-architecture toolchain. It takes an
# architecture list from CMAKE_OSX_ARCHITECTURES, leaves CMAKE_SYSTEM_PROCESSOR
# alone, and expects all architecture-specific logic to be conditioned within
# the source files rather than the build. This does not work for our assembly
# files, so we fix CMAKE_SYSTEM_PROCESSOR and only support single-architecture
# builds.
if(NOT OPENSSL_NO_ASM AND CMAKE_OSX_ARCHITECTURES)
  list(LENGTH CMAKE_OSX_ARCHITECTURES NUM_ARCHES)
  if(NOT NUM_ARCHES EQUAL 1)
    message(FATAL_ERROR "Universal binaries not supported.")
  endif()
  list(GET CMAKE_OSX_ARCHITECTURES 0 CMAKE_SYSTEM_PROCESSOR)
endif()

if(OPENSSL_NO_ASM)
  add_definitions(-DOPENSSL_NO_ASM)
  set(ARCH "generic")
elseif(${CMAKE_SYSTEM_PROCESSOR} STREQUAL "x86_64")
  set(ARCH "x86_64")
elseif(${CMAKE_SYSTEM_PROCESSOR} STREQUAL "amd64")
  set(ARCH "x86_64")
elseif(${CMAKE_SYSTEM_PROCESSOR} STREQUAL "AMD64")
  # cmake reports AMD64 on Windows, but we might be building for 32-bit.
  if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    set(ARCH "x86_64")
  else()
    set(ARCH "x86")
  endif()
elseif(${CMAKE_SYSTEM_PROCESSOR} STREQUAL "x86")
  set(ARCH "x86")
elseif(${CMAKE_SYSTEM_PROCESSOR} STREQUAL "i386")
  # cmake uses `uname -p` to set the system processor, but Solaris
  # systems support multiple architectures.
  if((${CMAKE_SYSTEM_NAME} STREQUAL "SunOS") AND CMAKE_SIZEOF_VOID_P EQUAL 8)
    set(ARCH "x86_64")
  else()
    set(ARCH "x86")
  endif()
elseif(${CMAKE_SYSTEM_PROCESSOR} STREQUAL "i686")
  set(ARCH "x86")
elseif(${CMAKE_SYSTEM_PROCESSOR} STREQUAL "aarch64")
  set(ARCH "aarch64")
elseif(${CMAKE_SYSTEM_PROCESSOR} STREQUAL "arm64")
  set(ARCH "aarch64")
# Apple A12 Bionic chipset which is added in iPhone XS/XS Max/XR uses arm64e architecture.
elseif(${CMAKE_SYSTEM_PROCESSOR} STREQUAL "arm64e")
  set(ARCH "aarch64")
elseif(${CMAKE_SYSTEM_PROCESSOR} MATCHES "^arm*")
  set(ARCH "arm")
elseif(${CMAKE_SYSTEM_PROCESSOR} STREQUAL "mips")
  # Just to avoid the “unknown processor” error.
  set(ARCH "generic")
elseif(${CMAKE_SYSTEM_PROCESSOR} STREQUAL "ppc64le")
  set(ARCH "ppc64le")
else()
  message(FATAL_ERROR "Unknown processor:" ${CMAKE_SYSTEM_PROCESSOR})
endif()

if(NOT OPENSSL_NO_ASM)
  if(UNIX)
    enable_language(ASM)

    # Clang's integerated assembler does not support debug symbols.
    if(NOT CMAKE_ASM_COMPILER_ID MATCHES "Clang")
      set(CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} -Wa,-g")
    endif()

    # CMake does not add -isysroot and -arch flags to assembly.
    if(APPLE)
      if(CMAKE_OSX_SYSROOT)
        set(CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} -isysroot \"${CMAKE_OSX_SYSROOT}\"")
      endif()
      foreach(arch ${CMAKE_OSX_ARCHITECTURES})
        set(CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} -arch ${arch}")
      endforeach()
    endif()
  else()
    set(CMAKE_ASM_NASM_FLAGS "${CMAKE_ASM_NASM_FLAGS} -gcv8")
    enable_language(ASM_NASM)
  endif()
endif()

if(BUILD_SHARED_LIBS)
  add_definitions(-DBORINGSSL_SHARED_LIBRARY)
  # Enable position-independent code globally. This is needed because
  # some library targets are OBJECT libraries.
  set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
endif()

set(
  CRYPTO_ios_aarch64_SOURCES

  ios-aarch64/crypto/chacha/chacha-armv8.S
  ios-aarch64/crypto/fipsmodule/aesv8-armx64.S
  ios-aarch64/crypto/fipsmodule/armv8-mont.S
  ios-aarch64/crypto/fipsmodule/ghash-neon-armv8.S
  ios-aarch64/crypto/fipsmodule/ghashv8-armx64.S
  ios-aarch64/crypto/fipsmodule/sha1-armv8.S
  ios-aarch64/crypto/fipsmodule/sha256-armv8.S
  ios-aarch64/crypto/fipsmodule/sha512-armv8.S
  ios-aarch64/crypto/fipsmodule/vpaes-armv8.S
  ios-aarch64/crypto/test/trampoline-armv8.S
)

set(
  CRYPTO_ios_arm_SOURCES

  ios-arm/crypto/chacha/chacha-armv4.S
  ios-arm/crypto/fipsmodule/aesv8-armx32.S
  ios-arm/crypto/fipsmodule/armv4-mont.S
  ios-arm/crypto/fipsmodule/bsaes-armv7.S
  ios-arm/crypto/fipsmodule/ghash-armv4.S
  ios-arm/crypto/fipsmodule/ghashv8-armx32.S
  ios-arm/crypto/fipsmodule/sha1-armv4-large.S
  ios-arm/crypto/fipsmodule/sha256-armv4.S
  ios-arm/crypto/fipsmodule/sha512-armv4.S
  ios-arm/crypto/fipsmodule/vpaes-armv7.S
  ios-arm/crypto/test/trampoline-armv4.S
)

set(
  CRYPTO_linux_aarch64_SOURCES

  linux-aarch64/crypto/chacha/chacha-armv8.S
  linux-aarch64/crypto/fipsmodule/aesv8-armx64.S
  linux-aarch64/crypto/fipsmodule/armv8-mont.S
  linux-aarch64/crypto/fipsmodule/ghash-neon-armv8.S
  linux-aarch64/crypto/fipsmodule/ghashv8-armx64.S
  linux-aarch64/crypto/fipsmodule/sha1-armv8.S
  linux-aarch64/crypto/fipsmodule/sha256-armv8.S
  linux-aarch64/crypto/fipsmodule/sha512-armv8.S
  linux-aarch64/crypto/fipsmodule/vpaes-armv8.S
  linux-aarch64/crypto/test/trampoline-armv8.S
)

set(
  CRYPTO_linux_arm_SOURCES

  linux-arm/crypto/chacha/chacha-armv4.S
  linux-arm/crypto/fipsmodule/aesv8-armx32.S
  linux-arm/crypto/fipsmodule/armv4-mont.S
  linux-arm/crypto/fipsmodule/bsaes-armv7.S
  linux-arm/crypto/fipsmodule/ghash-armv4.S
  linux-arm/crypto/fipsmodule/ghashv8-armx32.S
  linux-arm/crypto/fipsmodule/sha1-armv4-large.S
  linux-arm/crypto/fipsmodule/sha256-armv4.S
  linux-arm/crypto/fipsmodule/sha512-armv4.S
  linux-arm/crypto/fipsmodule/vpaes-armv7.S
  linux-arm/crypto/test/trampoline-armv4.S
  "${BORINGSSL_SOURCE_DIR}/crypto/curve25519/asm/x25519-asm-arm.S"
  "${BORINGSSL_SOURCE_DIR}/crypto/poly1305/poly1305_arm_asm.S"
)

set(
  CRYPTO_linux_ppc64le_SOURCES

  linux-ppc64le/crypto/fipsmodule/aesp8-ppc.S
  linux-ppc64le/crypto/fipsmodule/ghashp8-ppc.S
  linux-ppc64le/crypto/test/trampoline-ppc.S
)

set(
  CRYPTO_linux_x86_SOURCES

  linux-x86/crypto/chacha/chacha-x86.S
  linux-x86/crypto/fipsmodule/aesni-x86.S
  linux-x86/crypto/fipsmodule/bn-586.S
  linux-x86/crypto/fipsmodule/co-586.S
  linux-x86/crypto/fipsmodule/ghash-ssse3-x86.S
  linux-x86/crypto/fipsmodule/ghash-x86.S
  linux-x86/crypto/fipsmodule/md5-586.S
  linux-x86/crypto/fipsmodule/sha1-586.S
  linux-x86/crypto/fipsmodule/sha256-586.S
  linux-x86/crypto/fipsmodule/sha512-586.S
  linux-x86/crypto/fipsmodule/vpaes-x86.S
  linux-x86/crypto/fipsmodule/x86-mont.S
  linux-x86/crypto/test/trampoline-x86.S
)

set(
  CRYPTO_linux_x86_64_SOURCES

  linux-x86_64/crypto/chacha/chacha-x86_64.S
  linux-x86_64/crypto/cipher_extra/aes128gcmsiv-x86_64.S
  linux-x86_64/crypto/cipher_extra/chacha20_poly1305_x86_64.S
  linux-x86_64/crypto/fipsmodule/aesni-gcm-x86_64.S
  linux-x86_64/crypto/fipsmodule/aesni-x86_64.S
  linux-x86_64/crypto/fipsmodule/ghash-ssse3-x86_64.S
  linux-x86_64/crypto/fipsmodule/ghash-x86_64.S
  linux-x86_64/crypto/fipsmodule/md5-x86_64.S
  linux-x86_64/crypto/fipsmodule/p256-x86_64-asm.S
  linux-x86_64/crypto/fipsmodule/p256_beeu-x86_64-asm.S
  linux-x86_64/crypto/fipsmodule/rdrand-x86_64.S
  linux-x86_64/crypto/fipsmodule/rsaz-avx2.S
  linux-x86_64/crypto/fipsmodule/sha1-x86_64.S
  linux-x86_64/crypto/fipsmodule/sha256-x86_64.S
  linux-x86_64/crypto/fipsmodule/sha512-x86_64.S
  linux-x86_64/crypto/fipsmodule/vpaes-x86_64.S
  linux-x86_64/crypto/fipsmodule/x86_64-mont.S
  linux-x86_64/crypto/fipsmodule/x86_64-mont5.S
  linux-x86_64/crypto/test/trampoline-x86_64.S
  "${BORINGSSL_SOURCE_DIR}/crypto/hrss/asm/poly_rq_mul.S"
)

set(
  CRYPTO_mac_x86_SOURCES

  mac-x86/crypto/chacha/chacha-x86.S
  mac-x86/crypto/fipsmodule/aesni-x86.S
  mac-x86/crypto/fipsmodule/bn-586.S
  mac-x86/crypto/fipsmodule/co-586.S
  mac-x86/crypto/fipsmodule/ghash-ssse3-x86.S
  mac-x86/crypto/fipsmodule/ghash-x86.S
  mac-x86/crypto/fipsmodule/md5-586.S
  mac-x86/crypto/fipsmodule/sha1-586.S
  mac-x86/crypto/fipsmodule/sha256-586.S
  mac-x86/crypto/fipsmodule/sha512-586.S
  mac-x86/crypto/fipsmodule/vpaes-x86.S
  mac-x86/crypto/fipsmodule/x86-mont.S
  mac-x86/crypto/test/trampoline-x86.S
)

set(
  CRYPTO_mac_x86_64_SOURCES

  mac-x86_64/crypto/chacha/chacha-x86_64.S
  mac-x86_64/crypto/cipher_extra/aes128gcmsiv-x86_64.S
  mac-x86_64/crypto/cipher_extra/chacha20_poly1305_x86_64.S
  mac-x86_64/crypto/fipsmodule/aesni-gcm-x86_64.S
  mac-x86_64/crypto/fipsmodule/aesni-x86_64.S
  mac-x86_64/crypto/fipsmodule/ghash-ssse3-x86_64.S
  mac-x86_64/crypto/fipsmodule/ghash-x86_64.S
  mac-x86_64/crypto/fipsmodule/md5-x86_64.S
  mac-x86_64/crypto/fipsmodule/p256-x86_64-asm.S
  mac-x86_64/crypto/fipsmodule/p256_beeu-x86_64-asm.S
  mac-x86_64/crypto/fipsmodule/rdrand-x86_64.S
  mac-x86_64/crypto/fipsmodule/rsaz-avx2.S
  mac-x86_64/crypto/fipsmodule/sha1-x86_64.S
  mac-x86_64/crypto/fipsmodule/sha256-x86_64.S
  mac-x86_64/crypto/fipsmodule/sha512-x86_64.S
  mac-x86_64/crypto/fipsmodule/vpaes-x86_64.S
  mac-x86_64/crypto/fipsmodule/x86_64-mont.S
  mac-x86_64/crypto/fipsmodule/x86_64-mont5.S
  mac-x86_64/crypto/test/trampoline-x86_64.S
)

set(
  CRYPTO_win_aarch64_SOURCES

  win-aarch64/crypto/chacha/chacha-armv8.S
  win-aarch64/crypto/fipsmodule/aesv8-armx64.S
  win-aarch64/crypto/fipsmodule/armv8-mont.S
  win-aarch64/crypto/fipsmodule/ghash-neon-armv8.S
  win-aarch64/crypto/fipsmodule/ghashv8-armx64.S
  win-aarch64/crypto/fipsmodule/sha1-armv8.S
  win-aarch64/crypto/fipsmodule/sha256-armv8.S
  win-aarch64/crypto/fipsmodule/sha512-armv8.S
  win-aarch64/crypto/fipsmodule/vpaes-armv8.S
  win-aarch64/crypto/test/trampoline-armv8.S
)

set(
  CRYPTO_win_x86_SOURCES

  win-x86/crypto/chacha/chacha-x86.asm
  win-x86/crypto/fipsmodule/aesni-x86.asm
  win-x86/crypto/fipsmodule/bn-586.asm
  win-x86/crypto/fipsmodule/co-586.asm
  win-x86/crypto/fipsmodule/ghash-ssse3-x86.asm
  win-x86/crypto/fipsmodule/ghash-x86.asm
  win-x86/crypto/fipsmodule/md5-586.asm
  win-x86/crypto/fipsmodule/sha1-586.asm
  win-x86/crypto/fipsmodule/sha256-586.asm
  win-x86/crypto/fipsmodule/sha512-586.asm
  win-x86/crypto/fipsmodule/vpaes-x86.asm
  win-x86/crypto/fipsmodule/x86-mont.asm
  win-x86/crypto/test/trampoline-x86.asm
)

set(
  CRYPTO_win_x86_64_SOURCES

  win-x86_64/crypto/chacha/chacha-x86_64.asm
  win-x86_64/crypto/cipher_extra/aes128gcmsiv-x86_64.asm
  win-x86_64/crypto/cipher_extra/chacha20_poly1305_x86_64.asm
  win-x86_64/crypto/fipsmodule/aesni-gcm-x86_64.asm
  win-x86_64/crypto/fipsmodule/aesni-x86_64.asm
  win-x86_64/crypto/fipsmodule/ghash-ssse3-x86_64.asm
  win-x86_64/crypto/fipsmodule/ghash-x86_64.asm
  win-x86_64/crypto/fipsmodule/md5-x86_64.asm
  win-x86_64/crypto/fipsmodule/p256-x86_64-asm.asm
  win-x86_64/crypto/fipsmodule/p256_beeu-x86_64-asm.asm
  win-x86_64/crypto/fipsmodule/rdrand-x86_64.asm
  win-x86_64/crypto/fipsmodule/rsaz-avx2.asm
  win-x86_64/crypto/fipsmodule/sha1-x86_64.asm
  win-x86_64/crypto/fipsmodule/sha256-x86_64.asm
  win-x86_64/crypto/fipsmodule/sha512-x86_64.asm
  win-x86_64/crypto/fipsmodule/vpaes-x86_64.asm
  win-x86_64/crypto/fipsmodule/x86_64-mont.asm
  win-x86_64/crypto/fipsmodule/x86_64-mont5.asm
  win-x86_64/crypto/test/trampoline-x86_64.asm
)

if(APPLE AND ARCH STREQUAL "aarch64")
  set(CRYPTO_ARCH_SOURCES ${CRYPTO_ios_aarch64_SOURCES})
elseif(APPLE AND ARCH STREQUAL "arm")
  set(CRYPTO_ARCH_SOURCES ${CRYPTO_ios_arm_SOURCES})
elseif(APPLE)
  set(CRYPTO_ARCH_SOURCES ${CRYPTO_mac_${ARCH}_SOURCES})
elseif(UNIX)
  set(CRYPTO_ARCH_SOURCES ${CRYPTO_linux_${ARCH}_SOURCES})
elseif(WIN32)
  set(CRYPTO_ARCH_SOURCES ${CRYPTO_win_${ARCH}_SOURCES})
endif()

add_library(
  _crypto

  ${CRYPTO_ARCH_SOURCES}
  err_data.c
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/a_bitstr.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/a_bool.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/a_d2i_fp.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/a_dup.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/a_enum.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/a_gentm.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/a_i2d_fp.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/a_int.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/a_mbstr.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/a_object.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/a_octet.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/a_print.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/a_strex.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/a_strnid.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/a_time.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/a_type.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/a_utctm.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/a_utf8.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/asn1_lib.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/asn1_par.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/asn_pack.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/f_enum.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/f_int.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/f_string.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/tasn_dec.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/tasn_enc.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/tasn_fre.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/tasn_new.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/tasn_typ.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/tasn_utl.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/asn1/time_support.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/base64/base64.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/bio/bio.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/bio/bio_mem.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/bio/connect.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/bio/fd.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/bio/file.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/bio/hexdump.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/bio/pair.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/bio/printf.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/bio/socket.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/bio/socket_helper.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/blake2/blake2.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/bn_extra/bn_asn1.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/bn_extra/convert.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/buf/buf.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/bytestring/asn1_compat.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/bytestring/ber.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/bytestring/cbb.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/bytestring/cbs.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/bytestring/unicode.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/chacha/chacha.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cipher_extra/cipher_extra.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cipher_extra/derive_key.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cipher_extra/e_aesccm.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cipher_extra/e_aesctrhmac.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cipher_extra/e_aesgcmsiv.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cipher_extra/e_chacha20poly1305.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cipher_extra/e_null.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cipher_extra/e_rc2.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cipher_extra/e_rc4.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cipher_extra/e_tls.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cipher_extra/tls_cbc.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cmac/cmac.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/conf/conf.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cpu-aarch64-fuchsia.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cpu-aarch64-linux.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cpu-aarch64-win.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cpu-arm-linux.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cpu-arm.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cpu-intel.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/cpu-ppc64le.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/crypto.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/curve25519/curve25519.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/curve25519/spake25519.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/dh_extra/dh_asn1.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/dh_extra/params.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/digest_extra/digest_extra.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/dsa/dsa.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/dsa/dsa_asn1.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/ec_extra/ec_asn1.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/ec_extra/ec_derive.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/ec_extra/hash_to_curve.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/ecdh_extra/ecdh_extra.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/ecdsa_extra/ecdsa_asn1.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/engine/engine.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/err/err.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/evp/digestsign.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/evp/evp.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/evp/evp_asn1.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/evp/evp_ctx.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/evp/p_dsa_asn1.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/evp/p_ec.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/evp/p_ec_asn1.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/evp/p_ed25519.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/evp/p_ed25519_asn1.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/evp/p_rsa.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/evp/p_rsa_asn1.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/evp/p_x25519.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/evp/p_x25519_asn1.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/evp/pbkdf.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/evp/print.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/evp/scrypt.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/evp/sign.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/ex_data.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/fipsmodule/bcm.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/fipsmodule/fips_shared_support.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/hkdf/hkdf.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/hpke/hpke.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/hrss/hrss.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/lhash/lhash.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/mem.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/obj/obj.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/obj/obj_xref.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/pem/pem_all.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/pem/pem_info.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/pem/pem_lib.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/pem/pem_oth.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/pem/pem_pk8.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/pem/pem_pkey.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/pem/pem_x509.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/pem/pem_xaux.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/pkcs7/pkcs7.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/pkcs7/pkcs7_x509.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/pkcs8/p5_pbev2.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/pkcs8/pkcs8.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/pkcs8/pkcs8_x509.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/poly1305/poly1305.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/poly1305/poly1305_arm.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/poly1305/poly1305_vec.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/pool/pool.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/rand_extra/deterministic.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/rand_extra/forkunsafe.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/rand_extra/fuchsia.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/rand_extra/passive.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/rand_extra/rand_extra.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/rand_extra/windows.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/rc4/rc4.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/refcount_c11.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/refcount_lock.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/rsa_extra/rsa_asn1.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/rsa_extra/rsa_print.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/siphash/siphash.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/stack/stack.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/thread.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/thread_none.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/thread_pthread.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/thread_win.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/trust_token/pmbtoken.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/trust_token/trust_token.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/trust_token/voprf.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/a_digest.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/a_sign.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/a_verify.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/algorithm.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/asn1_gen.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/by_dir.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/by_file.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/i2d_pr.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/name_print.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/rsa_pss.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/t_crl.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/t_req.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/t_x509.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/t_x509a.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509_att.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509_cmp.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509_d2.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509_def.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509_ext.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509_lu.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509_obj.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509_req.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509_set.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509_trs.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509_txt.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509_v3.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509_vfy.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509_vpm.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509cset.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509name.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509rset.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x509spki.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x_algor.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x_all.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x_attrib.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x_crl.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x_exten.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x_info.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x_name.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x_pkey.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x_pubkey.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x_req.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x_sig.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x_spki.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x_val.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x_x509.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509/x_x509a.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/pcy_cache.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/pcy_data.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/pcy_lib.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/pcy_map.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/pcy_node.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/pcy_tree.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_akey.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_akeya.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_alt.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_bcons.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_bitst.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_conf.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_cpols.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_crld.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_enum.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_extku.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_genn.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_ia5.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_info.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_int.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_lib.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_ncons.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_ocsp.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_pci.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_pcia.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_pcons.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_pmaps.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_prn.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_purp.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_skey.c"
  "${BORINGSSL_SOURCE_DIR}/crypto/x509v3/v3_utl.c"
)

add_library(
  _ssl

  "${BORINGSSL_SOURCE_DIR}/ssl/bio_ssl.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/d1_both.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/d1_lib.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/d1_pkt.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/d1_srtp.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/dtls_method.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/dtls_record.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/encrypted_client_hello.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/extensions.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/handoff.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/handshake.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/handshake_client.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/handshake_server.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/s3_both.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/s3_lib.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/s3_pkt.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/ssl_aead_ctx.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/ssl_asn1.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/ssl_buffer.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/ssl_cert.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/ssl_cipher.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/ssl_file.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/ssl_key_share.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/ssl_lib.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/ssl_privkey.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/ssl_session.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/ssl_stat.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/ssl_transcript.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/ssl_versions.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/ssl_x509.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/t1_enc.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/tls13_both.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/tls13_client.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/tls13_enc.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/tls13_server.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/tls_method.cc"
  "${BORINGSSL_SOURCE_DIR}/ssl/tls_record.cc"

  "${BORINGSSL_SOURCE_DIR}/decrepit/ssl/ssl_decrepit.c"
  "${BORINGSSL_SOURCE_DIR}/decrepit/cfb/cfb.c"
  "${BORINGSSL_SOURCE_DIR}/decrepit/bio/base64_bio.c"
)

add_executable(
  bssl

  "${BORINGSSL_SOURCE_DIR}/tool/args.cc"
  "${BORINGSSL_SOURCE_DIR}/tool/ciphers.cc"
  "${BORINGSSL_SOURCE_DIR}/tool/client.cc"
  "${BORINGSSL_SOURCE_DIR}/tool/const.cc"
  "${BORINGSSL_SOURCE_DIR}/tool/digest.cc"
  "${BORINGSSL_SOURCE_DIR}/tool/fd.cc"
  "${BORINGSSL_SOURCE_DIR}/tool/file.cc"
  "${BORINGSSL_SOURCE_DIR}/tool/generate_ech.cc"
  "${BORINGSSL_SOURCE_DIR}/tool/generate_ed25519.cc"
  "${BORINGSSL_SOURCE_DIR}/tool/genrsa.cc"
  "${BORINGSSL_SOURCE_DIR}/tool/pkcs12.cc"
  "${BORINGSSL_SOURCE_DIR}/tool/rand.cc"
  "${BORINGSSL_SOURCE_DIR}/tool/server.cc"
  "${BORINGSSL_SOURCE_DIR}/tool/sign.cc"
  "${BORINGSSL_SOURCE_DIR}/tool/speed.cc"
  "${BORINGSSL_SOURCE_DIR}/tool/tool.cc"
  "${BORINGSSL_SOURCE_DIR}/tool/transport_common.cc"
)

target_link_libraries(_ssl _crypto)
target_link_libraries(bssl _ssl)

if(NOT WIN32 AND NOT ANDROID)
  target_link_libraries(_crypto pthread)
endif()

# NOTE: that ClickHouse does not support WIN32 anyway.
if(WIN32)
  target_link_libraries(bssl ws2_32)
endif()

target_include_directories(_crypto SYSTEM PUBLIC "${BORINGSSL_SOURCE_DIR}/include")
target_include_directories(_ssl SYSTEM PUBLIC "${BORINGSSL_SOURCE_DIR}/include")

target_compile_options(_crypto PRIVATE -Wno-gnu-anonymous-struct)

add_library(OpenSSL::Crypto ALIAS _crypto)
add_library(OpenSSL::SSL ALIAS _ssl)
