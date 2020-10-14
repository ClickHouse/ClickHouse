# Copyright (c) 2019 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

# This file is created by generate_build_files.py. Do not edit manually.

cmake_minimum_required(VERSION 3.0)

project(BoringSSL LANGUAGES C CXX)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(CLANG 1)
endif()

if(CMAKE_COMPILER_IS_GNUCXX OR CLANG)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -fvisibility=hidden -fno-common -fno-exceptions -fno-rtti")
  if(APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
  endif()

  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fvisibility=hidden -fno-common")
  if((CMAKE_C_COMPILER_VERSION VERSION_GREATER "4.8.99") OR CLANG)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c11")
  else()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99")
  endif()
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
  if(NOT ${NUM_ARCHES} EQUAL 1)
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
  set(ARCH "x86")
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

include_directories(src/include)

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
  src/crypto/curve25519/asm/x25519-asm-arm.S
  src/crypto/poly1305/poly1305_arm_asm.S
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
  src/crypto/hrss/asm/poly_rq_mul.S
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

if(APPLE AND ${ARCH} STREQUAL "aarch64")
  set(CRYPTO_ARCH_SOURCES ${CRYPTO_ios_aarch64_SOURCES})
elseif(APPLE AND ${ARCH} STREQUAL "arm")
  set(CRYPTO_ARCH_SOURCES ${CRYPTO_ios_arm_SOURCES})
elseif(APPLE)
  set(CRYPTO_ARCH_SOURCES ${CRYPTO_mac_${ARCH}_SOURCES})
elseif(UNIX)
  set(CRYPTO_ARCH_SOURCES ${CRYPTO_linux_${ARCH}_SOURCES})
elseif(WIN32)
  set(CRYPTO_ARCH_SOURCES ${CRYPTO_win_${ARCH}_SOURCES})
endif()

add_library(
  crypto

  ${CRYPTO_ARCH_SOURCES}
  err_data.c
  src/crypto/asn1/a_bitstr.c
  src/crypto/asn1/a_bool.c
  src/crypto/asn1/a_d2i_fp.c
  src/crypto/asn1/a_dup.c
  src/crypto/asn1/a_enum.c
  src/crypto/asn1/a_gentm.c
  src/crypto/asn1/a_i2d_fp.c
  src/crypto/asn1/a_int.c
  src/crypto/asn1/a_mbstr.c
  src/crypto/asn1/a_object.c
  src/crypto/asn1/a_octet.c
  src/crypto/asn1/a_print.c
  src/crypto/asn1/a_strnid.c
  src/crypto/asn1/a_time.c
  src/crypto/asn1/a_type.c
  src/crypto/asn1/a_utctm.c
  src/crypto/asn1/a_utf8.c
  src/crypto/asn1/asn1_lib.c
  src/crypto/asn1/asn1_par.c
  src/crypto/asn1/asn_pack.c
  src/crypto/asn1/f_enum.c
  src/crypto/asn1/f_int.c
  src/crypto/asn1/f_string.c
  src/crypto/asn1/tasn_dec.c
  src/crypto/asn1/tasn_enc.c
  src/crypto/asn1/tasn_fre.c
  src/crypto/asn1/tasn_new.c
  src/crypto/asn1/tasn_typ.c
  src/crypto/asn1/tasn_utl.c
  src/crypto/asn1/time_support.c
  src/crypto/base64/base64.c
  src/crypto/bio/bio.c
  src/crypto/bio/bio_mem.c
  src/crypto/bio/connect.c
  src/crypto/bio/fd.c
  src/crypto/bio/file.c
  src/crypto/bio/hexdump.c
  src/crypto/bio/pair.c
  src/crypto/bio/printf.c
  src/crypto/bio/socket.c
  src/crypto/bio/socket_helper.c
  src/crypto/bn_extra/bn_asn1.c
  src/crypto/bn_extra/convert.c
  src/crypto/buf/buf.c
  src/crypto/bytestring/asn1_compat.c
  src/crypto/bytestring/ber.c
  src/crypto/bytestring/cbb.c
  src/crypto/bytestring/cbs.c
  src/crypto/bytestring/unicode.c
  src/crypto/chacha/chacha.c
  src/crypto/cipher_extra/cipher_extra.c
  src/crypto/cipher_extra/derive_key.c
  src/crypto/cipher_extra/e_aesccm.c
  src/crypto/cipher_extra/e_aesctrhmac.c
  src/crypto/cipher_extra/e_aesgcmsiv.c
  src/crypto/cipher_extra/e_chacha20poly1305.c
  src/crypto/cipher_extra/e_null.c
  src/crypto/cipher_extra/e_rc2.c
  src/crypto/cipher_extra/e_rc4.c
  src/crypto/cipher_extra/e_tls.c
  src/crypto/cipher_extra/tls_cbc.c
  src/crypto/cmac/cmac.c
  src/crypto/conf/conf.c
  src/crypto/cpu-aarch64-fuchsia.c
  src/crypto/cpu-aarch64-linux.c
  src/crypto/cpu-arm-linux.c
  src/crypto/cpu-arm.c
  src/crypto/cpu-intel.c
  src/crypto/cpu-ppc64le.c
  src/crypto/crypto.c
  src/crypto/curve25519/curve25519.c
  src/crypto/curve25519/spake25519.c
  src/crypto/dh/check.c
  src/crypto/dh/dh.c
  src/crypto/dh/dh_asn1.c
  src/crypto/dh/params.c
  src/crypto/digest_extra/digest_extra.c
  src/crypto/dsa/dsa.c
  src/crypto/dsa/dsa_asn1.c
  src/crypto/ec_extra/ec_asn1.c
  src/crypto/ec_extra/ec_derive.c
  src/crypto/ec_extra/hash_to_curve.c
  src/crypto/ecdh_extra/ecdh_extra.c
  src/crypto/ecdsa_extra/ecdsa_asn1.c
  src/crypto/engine/engine.c
  src/crypto/err/err.c
  src/crypto/evp/digestsign.c
  src/crypto/evp/evp.c
  src/crypto/evp/evp_asn1.c
  src/crypto/evp/evp_ctx.c
  src/crypto/evp/p_dsa_asn1.c
  src/crypto/evp/p_ec.c
  src/crypto/evp/p_ec_asn1.c
  src/crypto/evp/p_ed25519.c
  src/crypto/evp/p_ed25519_asn1.c
  src/crypto/evp/p_rsa.c
  src/crypto/evp/p_rsa_asn1.c
  src/crypto/evp/p_x25519.c
  src/crypto/evp/p_x25519_asn1.c
  src/crypto/evp/pbkdf.c
  src/crypto/evp/print.c
  src/crypto/evp/scrypt.c
  src/crypto/evp/sign.c
  src/crypto/ex_data.c
  src/crypto/fipsmodule/bcm.c
  src/crypto/fipsmodule/fips_shared_support.c
  src/crypto/fipsmodule/is_fips.c
  src/crypto/hkdf/hkdf.c
  src/crypto/hpke/hpke.c
  src/crypto/hrss/hrss.c
  src/crypto/lhash/lhash.c
  src/crypto/mem.c
  src/crypto/obj/obj.c
  src/crypto/obj/obj_xref.c
  src/crypto/pem/pem_all.c
  src/crypto/pem/pem_info.c
  src/crypto/pem/pem_lib.c
  src/crypto/pem/pem_oth.c
  src/crypto/pem/pem_pk8.c
  src/crypto/pem/pem_pkey.c
  src/crypto/pem/pem_x509.c
  src/crypto/pem/pem_xaux.c
  src/crypto/pkcs7/pkcs7.c
  src/crypto/pkcs7/pkcs7_x509.c
  src/crypto/pkcs8/p5_pbev2.c
  src/crypto/pkcs8/pkcs8.c
  src/crypto/pkcs8/pkcs8_x509.c
  src/crypto/poly1305/poly1305.c
  src/crypto/poly1305/poly1305_arm.c
  src/crypto/poly1305/poly1305_vec.c
  src/crypto/pool/pool.c
  src/crypto/rand_extra/deterministic.c
  src/crypto/rand_extra/forkunsafe.c
  src/crypto/rand_extra/fuchsia.c
  src/crypto/rand_extra/rand_extra.c
  src/crypto/rand_extra/windows.c
  src/crypto/rc4/rc4.c
  src/crypto/refcount_c11.c
  src/crypto/refcount_lock.c
  src/crypto/rsa_extra/rsa_asn1.c
  src/crypto/rsa_extra/rsa_print.c
  src/crypto/siphash/siphash.c
  src/crypto/stack/stack.c
  src/crypto/thread.c
  src/crypto/thread_none.c
  src/crypto/thread_pthread.c
  src/crypto/thread_win.c
  src/crypto/trust_token/pmbtoken.c
  src/crypto/trust_token/trust_token.c
  src/crypto/trust_token/voprf.c
  src/crypto/x509/a_digest.c
  src/crypto/x509/a_sign.c
  src/crypto/x509/a_strex.c
  src/crypto/x509/a_verify.c
  src/crypto/x509/algorithm.c
  src/crypto/x509/asn1_gen.c
  src/crypto/x509/by_dir.c
  src/crypto/x509/by_file.c
  src/crypto/x509/i2d_pr.c
  src/crypto/x509/rsa_pss.c
  src/crypto/x509/t_crl.c
  src/crypto/x509/t_req.c
  src/crypto/x509/t_x509.c
  src/crypto/x509/t_x509a.c
  src/crypto/x509/x509.c
  src/crypto/x509/x509_att.c
  src/crypto/x509/x509_cmp.c
  src/crypto/x509/x509_d2.c
  src/crypto/x509/x509_def.c
  src/crypto/x509/x509_ext.c
  src/crypto/x509/x509_lu.c
  src/crypto/x509/x509_obj.c
  src/crypto/x509/x509_r2x.c
  src/crypto/x509/x509_req.c
  src/crypto/x509/x509_set.c
  src/crypto/x509/x509_trs.c
  src/crypto/x509/x509_txt.c
  src/crypto/x509/x509_v3.c
  src/crypto/x509/x509_vfy.c
  src/crypto/x509/x509_vpm.c
  src/crypto/x509/x509cset.c
  src/crypto/x509/x509name.c
  src/crypto/x509/x509rset.c
  src/crypto/x509/x509spki.c
  src/crypto/x509/x_algor.c
  src/crypto/x509/x_all.c
  src/crypto/x509/x_attrib.c
  src/crypto/x509/x_crl.c
  src/crypto/x509/x_exten.c
  src/crypto/x509/x_info.c
  src/crypto/x509/x_name.c
  src/crypto/x509/x_pkey.c
  src/crypto/x509/x_pubkey.c
  src/crypto/x509/x_req.c
  src/crypto/x509/x_sig.c
  src/crypto/x509/x_spki.c
  src/crypto/x509/x_val.c
  src/crypto/x509/x_x509.c
  src/crypto/x509/x_x509a.c
  src/crypto/x509v3/pcy_cache.c
  src/crypto/x509v3/pcy_data.c
  src/crypto/x509v3/pcy_lib.c
  src/crypto/x509v3/pcy_map.c
  src/crypto/x509v3/pcy_node.c
  src/crypto/x509v3/pcy_tree.c
  src/crypto/x509v3/v3_akey.c
  src/crypto/x509v3/v3_akeya.c
  src/crypto/x509v3/v3_alt.c
  src/crypto/x509v3/v3_bcons.c
  src/crypto/x509v3/v3_bitst.c
  src/crypto/x509v3/v3_conf.c
  src/crypto/x509v3/v3_cpols.c
  src/crypto/x509v3/v3_crld.c
  src/crypto/x509v3/v3_enum.c
  src/crypto/x509v3/v3_extku.c
  src/crypto/x509v3/v3_genn.c
  src/crypto/x509v3/v3_ia5.c
  src/crypto/x509v3/v3_info.c
  src/crypto/x509v3/v3_int.c
  src/crypto/x509v3/v3_lib.c
  src/crypto/x509v3/v3_ncons.c
  src/crypto/x509v3/v3_ocsp.c
  src/crypto/x509v3/v3_pci.c
  src/crypto/x509v3/v3_pcia.c
  src/crypto/x509v3/v3_pcons.c
  src/crypto/x509v3/v3_pmaps.c
  src/crypto/x509v3/v3_prn.c
  src/crypto/x509v3/v3_purp.c
  src/crypto/x509v3/v3_skey.c
  src/crypto/x509v3/v3_utl.c
)

add_library(
  ssl

  src/ssl/bio_ssl.cc
  src/ssl/d1_both.cc
  src/ssl/d1_lib.cc
  src/ssl/d1_pkt.cc
  src/ssl/d1_srtp.cc
  src/ssl/dtls_method.cc
  src/ssl/dtls_record.cc
  src/ssl/handoff.cc
  src/ssl/handshake.cc
  src/ssl/handshake_client.cc
  src/ssl/handshake_server.cc
  src/ssl/s3_both.cc
  src/ssl/s3_lib.cc
  src/ssl/s3_pkt.cc
  src/ssl/ssl_aead_ctx.cc
  src/ssl/ssl_asn1.cc
  src/ssl/ssl_buffer.cc
  src/ssl/ssl_cert.cc
  src/ssl/ssl_cipher.cc
  src/ssl/ssl_file.cc
  src/ssl/ssl_key_share.cc
  src/ssl/ssl_lib.cc
  src/ssl/ssl_privkey.cc
  src/ssl/ssl_session.cc
  src/ssl/ssl_stat.cc
  src/ssl/ssl_transcript.cc
  src/ssl/ssl_versions.cc
  src/ssl/ssl_x509.cc
  src/ssl/t1_enc.cc
  src/ssl/t1_lib.cc
  src/ssl/tls13_both.cc
  src/ssl/tls13_client.cc
  src/ssl/tls13_enc.cc
  src/ssl/tls13_server.cc
  src/ssl/tls_method.cc
  src/ssl/tls_record.cc
)

add_executable(
  bssl

  src/tool/args.cc
  src/tool/ciphers.cc
  src/tool/client.cc
  src/tool/const.cc
  src/tool/digest.cc
  src/tool/fd.cc
  src/tool/file.cc
  src/tool/generate_ed25519.cc
  src/tool/genrsa.cc
  src/tool/pkcs12.cc
  src/tool/rand.cc
  src/tool/server.cc
  src/tool/sign.cc
  src/tool/speed.cc
  src/tool/tool.cc
  src/tool/transport_common.cc
)

target_link_libraries(bssl ssl crypto)

if(NOT WIN32 AND NOT ANDROID)
  target_link_libraries(crypto pthread)
endif()

if(WIN32)
  target_link_libraries(bssl ws2_32)
endif()

