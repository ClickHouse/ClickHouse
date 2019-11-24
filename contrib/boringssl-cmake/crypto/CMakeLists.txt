set(BORINGSSL_CRYPTO_SOURCE_DIR ${BORINGSSL_SOURCE_DIR}/crypto)

include_directories(${BORINGSSL_CRYPTO_SOURCE_DIR}../include)

if(NOT OPENSSL_NO_ASM)
  if(UNIX)
    if(${ARCH} STREQUAL "aarch64")
      # The "armx" Perl scripts look for "64" in the style argument
      # in order to decide whether to generate 32- or 64-bit asm.
      if(APPLE)
        set(PERLASM_STYLE ios64)
      else()
        set(PERLASM_STYLE linux64)
      endif()
    elseif(${ARCH} STREQUAL "arm")
      if(APPLE)
        set(PERLASM_STYLE ios32)
      else()
        set(PERLASM_STYLE linux32)
      endif()
    elseif(${ARCH} STREQUAL "ppc64le")
      set(PERLASM_STYLE linux64le)
    else()
      if(${ARCH} STREQUAL "x86")
        set(PERLASM_FLAGS "-fPIC -DOPENSSL_IA32_SSE2")
      endif()
      if(APPLE)
        set(PERLASM_STYLE macosx)
      else()
        set(PERLASM_STYLE elf)
      endif()
    endif()
    set(ASM_EXT S)
    enable_language(ASM)
    set(CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} -Wa,--noexecstack")

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
    if(${ARCH} STREQUAL "x86_64")
      set(PERLASM_STYLE nasm)
    else()
      set(PERLASM_STYLE win32n)
      set(PERLASM_FLAGS "-DOPENSSL_IA32_SSE2")
    endif()
    set(CMAKE_ASM_NASM_FLAGS "${CMAKE_ASM_NASM_FLAGS} -gcv8")

    # On Windows, we use the NASM output, specifically built with Yasm.
    set(ASM_EXT asm)
    enable_language(ASM_NASM)
  endif()
endif()

function(perlasm dest src)
  get_filename_component(dir ${dest} DIRECTORY)
  if ("${dir}" STREQUAL "")
    set(dir ".")
  endif()

  add_custom_command(
    OUTPUT ${dest}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${dir}
    COMMAND ${PERL_EXECUTABLE} ${BORINGSSL_SOURCE_DIR}/${src} ${PERLASM_STYLE} ${PERLASM_FLAGS} ${ARGN} ${dest}
    DEPENDS
    ${src}
    ${BORINGSSL_CRYPTO_SOURCE_DIR}/perlasm/arm-xlate.pl
    ${BORINGSSL_CRYPTO_SOURCE_DIR}/perlasm/ppc-xlate.pl
    ${BORINGSSL_CRYPTO_SOURCE_DIR}/perlasm/x86_64-xlate.pl
    ${BORINGSSL_CRYPTO_SOURCE_DIR}/perlasm/x86asm.pl
    ${BORINGSSL_CRYPTO_SOURCE_DIR}/perlasm/x86gas.pl
    ${BORINGSSL_CRYPTO_SOURCE_DIR}/perlasm/x86masm.pl
    ${BORINGSSL_CRYPTO_SOURCE_DIR}/perlasm/x86nasm.pl
    WORKING_DIRECTORY .
  )
endfunction()

add_subdirectory(${BORINGSSL_CRYPTO_SOURCE_DIR}/fipsmodule ${BORINGSSL_CRYPTO_SOURCE_DIR}/fipsmodule)
add_subdirectory(${BORINGSSL_CRYPTO_SOURCE_DIR}/test ${BORINGSSL_CRYPTO_SOURCE_DIR}/test)

if(FIPS_DELOCATE OR FIPS_SHARED)
  SET_SOURCE_FILES_PROPERTIES(${BORINGSSL_CRYPTO_SOURCE_DIR}/fipsmodule/bcm.o PROPERTIES EXTERNAL_OBJECT true)
  SET_SOURCE_FILES_PROPERTIES(${BORINGSSL_CRYPTO_SOURCE_DIR}/fipsmodule/bcm.o PROPERTIES GENERATED true)

  set(
    CRYPTO_FIPS_OBJECTS

    ${BORINGSSL_CRYPTO_SOURCE_DIR}/fipsmodule/bcm.o
  )
endif()

if(${ARCH} STREQUAL "arm")
  set(
    CRYPTO_ARCH_SOURCES

    ${BORINGSSL_CRYPTO_SOURCE_DIR}/chacha/chacha-armv4.${ASM_EXT}
    ${BORINGSSL_CRYPTO_SOURCE_DIR}/curve25519/asm/x25519-asm-arm.S
    ${BORINGSSL_CRYPTO_SOURCE_DIR}/poly1305/poly1305_arm_asm.S
    ${BORINGSSL_CRYPTO_SOURCE_DIR}/test/trampoline-armv4.${ASM_EXT}
  )
endif()

if(${ARCH} STREQUAL "aarch64")
  set(
    CRYPTO_ARCH_SOURCES

    ${BORINGSSL_CRYPTO_SOURCE_DIR}/chacha/chacha-armv8.${ASM_EXT}
    ${BORINGSSL_CRYPTO_SOURCE_DIR}/test/trampoline-armv8.${ASM_EXT}
  )
endif()

if(${ARCH} STREQUAL "x86")
  set(
    CRYPTO_ARCH_SOURCES

    ${BORINGSSL_CRYPTO_SOURCE_DIR}/chacha/chacha-x86.${ASM_EXT}
    ${BORINGSSL_CRYPTO_SOURCE_DIR}/test/trampoline-x86.${ASM_EXT}
  )
endif()

if(${ARCH} STREQUAL "x86_64")
  set(
    CRYPTO_ARCH_SOURCES

    ${BORINGSSL_CRYPTO_SOURCE_DIR}/chacha/chacha-x86_64.${ASM_EXT}
    ${BORINGSSL_CRYPTO_SOURCE_DIR}/cipher_extra/aes128gcmsiv-x86_64.${ASM_EXT}
    ${BORINGSSL_CRYPTO_SOURCE_DIR}/cipher_extra/chacha20_poly1305_x86_64.${ASM_EXT}
    ${BORINGSSL_CRYPTO_SOURCE_DIR}/hrss/asm/poly_rq_mul.S
    ${BORINGSSL_CRYPTO_SOURCE_DIR}/test/trampoline-x86_64.${ASM_EXT}
  )
endif()

perlasm(${BORINGSSL_CRYPTO_SOURCE_DIR}/chacha/chacha-armv4.${ASM_EXT} ${BORINGSSL_CRYPTO_SOURCE_DIR}/chacha/asm/chacha-armv4.pl)
perlasm(${BORINGSSL_CRYPTO_SOURCE_DIR}/chacha/chacha-armv8.${ASM_EXT} ${BORINGSSL_CRYPTO_SOURCE_DIR}/chacha/asm/chacha-armv8.pl)
perlasm(${BORINGSSL_CRYPTO_SOURCE_DIR}/chacha/chacha-x86.${ASM_EXT} ${BORINGSSL_CRYPTO_SOURCE_DIR}/chacha/asm/chacha-x86.pl)
perlasm(${BORINGSSL_CRYPTO_SOURCE_DIR}/chacha/chacha-x86_64.${ASM_EXT} ${BORINGSSL_CRYPTO_SOURCE_DIR}/chacha/asm/chacha-x86_64.pl)
perlasm(${BORINGSSL_CRYPTO_SOURCE_DIR}/cipher_extra/aes128gcmsiv-x86_64.${ASM_EXT} ${BORINGSSL_CRYPTO_SOURCE_DIR}/cipher_extra/asm/aes128gcmsiv-x86_64.pl)
perlasm(${BORINGSSL_CRYPTO_SOURCE_DIR}/cipher_extra/chacha20_poly1305_x86_64.${ASM_EXT} ${BORINGSSL_CRYPTO_SOURCE_DIR}/cipher_extra/asm/chacha20_poly1305_x86_64.pl)
perlasm(${BORINGSSL_CRYPTO_SOURCE_DIR}/test/trampoline-armv4.${ASM_EXT} ${BORINGSSL_CRYPTO_SOURCE_DIR}/test/asm/trampoline-armv4.pl)
perlasm(${BORINGSSL_CRYPTO_SOURCE_DIR}/test/trampoline-armv8.${ASM_EXT} ${BORINGSSL_CRYPTO_SOURCE_DIR}/test/asm/trampoline-armv8.pl)
perlasm(${BORINGSSL_CRYPTO_SOURCE_DIR}/test/trampoline-x86.${ASM_EXT} ${BORINGSSL_CRYPTO_SOURCE_DIR}/test/asm/trampoline-x86.pl)
perlasm(${BORINGSSL_CRYPTO_SOURCE_DIR}/test/trampoline-x86_64.${ASM_EXT} ${BORINGSSL_CRYPTO_SOURCE_DIR}/test/asm/trampoline-x86_64.pl)

add_custom_command(
  OUTPUT ${BORINGSSL_CRYPTO_SOURCE_DIR}/err_data.c
  COMMAND ${GO_EXECUTABLE} run err_data_generate.go > ${BORINGSSL_CRYPTO_SOURCE_DIR}/err_data.c
  DEPENDS
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/err_data_generate.go
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/asn1.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/bio.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/bn.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/cipher.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/conf.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/dh.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/digest.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/dsa.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/ecdh.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/ecdsa.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/ec.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/engine.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/evp.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/hkdf.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/obj.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/pem.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/pkcs7.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/pkcs8.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/rsa.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/ssl.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/x509.errordata
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/x509v3.errordata
  WORKING_DIRECTORY ${BORINGSSL_SOURCE_DIR}/err
)

add_library(
  boringssl_crypto

  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/a_bitstr.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/a_bool.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/a_d2i_fp.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/a_dup.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/a_enum.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/a_gentm.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/a_i2d_fp.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/a_int.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/a_mbstr.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/a_object.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/a_octet.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/a_print.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/a_strnid.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/a_time.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/a_type.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/a_utctm.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/a_utf8.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/asn1_lib.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/asn1_par.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/asn_pack.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/f_enum.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/f_int.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/f_string.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/tasn_dec.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/tasn_enc.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/tasn_fre.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/tasn_new.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/tasn_typ.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/tasn_utl.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/asn1/time_support.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/base64/base64.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/bio/bio.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/bio/bio_mem.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/bio/connect.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/bio/fd.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/bio/file.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/bio/hexdump.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/bio/pair.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/bio/printf.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/bio/socket.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/bio/socket_helper.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/bn_extra/bn_asn1.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/bn_extra/convert.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/buf/buf.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/bytestring/asn1_compat.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/bytestring/ber.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/bytestring/cbb.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/bytestring/cbs.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/bytestring/unicode.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/chacha/chacha.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/cipher_extra/cipher_extra.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/cipher_extra/derive_key.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/cipher_extra/e_aesccm.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/cipher_extra/e_aesctrhmac.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/cipher_extra/e_aesgcmsiv.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/cipher_extra/e_chacha20poly1305.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/cipher_extra/e_null.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/cipher_extra/e_rc2.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/cipher_extra/e_rc4.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/cipher_extra/e_tls.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/cipher_extra/tls_cbc.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/cmac/cmac.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/conf/conf.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/cpu-aarch64-fuchsia.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/cpu-aarch64-linux.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/cpu-arm-linux.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/cpu-arm.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/cpu-intel.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/cpu-ppc64le.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/crypto.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/curve25519/spake25519.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/dh/dh.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/dh/params.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/dh/check.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/dh/dh_asn1.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/digest_extra/digest_extra.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/dsa/dsa.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/dsa/dsa_asn1.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/ecdh_extra/ecdh_extra.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/ecdsa_extra/ecdsa_asn1.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/ec_extra/ec_asn1.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/ec_extra/ec_derive.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err/err.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/err_data.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/engine/engine.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/evp/digestsign.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/evp/evp.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/evp/evp_asn1.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/evp/evp_ctx.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/evp/p_dsa_asn1.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/evp/p_ec.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/evp/p_ec_asn1.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/evp/p_ed25519.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/evp/p_ed25519_asn1.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/evp/p_rsa.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/evp/p_rsa_asn1.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/evp/p_x25519.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/evp/p_x25519_asn1.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/evp/pbkdf.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/evp/print.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/evp/scrypt.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/evp/sign.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/ex_data.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/hkdf/hkdf.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/hrss/hrss.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/lhash/lhash.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/mem.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/obj/obj.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/obj/obj_xref.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/pem/pem_all.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/pem/pem_info.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/pem/pem_lib.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/pem/pem_oth.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/pem/pem_pk8.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/pem/pem_pkey.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/pem/pem_x509.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/pem/pem_xaux.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/pkcs7/pkcs7.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/pkcs7/pkcs7_x509.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/pkcs8/pkcs8.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/pkcs8/pkcs8_x509.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/pkcs8/p5_pbev2.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/poly1305/poly1305.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/poly1305/poly1305_arm.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/poly1305/poly1305_vec.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/pool/pool.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/rand_extra/deterministic.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/rand_extra/forkunsafe.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/rand_extra/fuchsia.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/rand_extra/rand_extra.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/rand_extra/windows.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/rc4/rc4.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/refcount_c11.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/refcount_lock.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/rsa_extra/rsa_asn1.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/rsa_extra/rsa_print.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/stack/stack.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/siphash/siphash.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/thread.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/thread_none.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/thread_pthread.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/thread_win.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/a_digest.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/a_sign.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/a_strex.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/a_verify.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/algorithm.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/asn1_gen.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/by_dir.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/by_file.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/i2d_pr.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/rsa_pss.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/t_crl.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/t_req.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/t_x509.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/t_x509a.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509_att.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509_cmp.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509_d2.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509_def.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509_ext.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509_lu.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509_obj.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509_r2x.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509_req.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509_set.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509_trs.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509_txt.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509_v3.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509_vfy.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509_vpm.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509cset.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509name.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509rset.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x509spki.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x_algor.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x_all.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x_attrib.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x_crl.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x_exten.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x_info.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x_name.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x_pkey.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x_pubkey.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x_req.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x_sig.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x_spki.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x_val.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x_x509.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509/x_x509a.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/pcy_cache.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/pcy_data.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/pcy_lib.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/pcy_map.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/pcy_node.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/pcy_tree.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_akey.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_akeya.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_alt.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_bcons.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_bitst.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_conf.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_cpols.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_crld.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_enum.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_extku.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_genn.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_ia5.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_info.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_int.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_lib.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_ncons.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_ocsp.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_pci.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_pcia.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_pcons.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_pku.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_pmaps.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_prn.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_purp.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_skey.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_sxnet.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/x509v3/v3_utl.c
  ${BORINGSSL_CRYPTO_SOURCE_DIR}/../third_party/fiat/curve25519.c

  $<TARGET_OBJECTS:fipsmodule>

  ${CRYPTO_ARCH_SOURCES}
  ${CRYPTO_FIPS_OBJECTS}
)

if(FIPS_SHARED)
  set(EXTRA_INJECT_HASH_ARGS)
  if(ANDROID)
    set(EXTRA_INJECT_HASH_ARGS "-sha256")
  endif()
  # Rewrite libcrypto.so to inject the correct module hash value. This assumes
  # UNIX-style library naming, but we only support FIPS mode on Linux anyway.
  add_custom_command(
    TARGET boringssl_crypto POST_BUILD
    COMMAND ${GO_EXECUTABLE} run
    ${BORINGSSL_SOURCE_DIR}/../util/fipstools/inject_hash/inject_hash.go
    -o libcrypto.so -in-object libcrypto.so ${EXTRA_INJECT_HASH_ARGS}
    # The DEPENDS argument to a POST_BUILD rule appears to be ignored. Thus
    # go_executable isn't used (as it doesn't get built), but we list this
    # dependency anyway in case it starts working in some CMake version.
    DEPENDS ${BORINGSSL_SOURCE_DIR}/../util/fipstools/inject_hash/inject_hash.go
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  )
endif()

add_dependencies(boringssl_crypto global_target)

if(FIPS_DELOCATE OR FIPS_SHARED)
  add_dependencies(boringssl_crypto bcm_o_target)
endif()

SET_TARGET_PROPERTIES(boringssl_crypto PROPERTIES LINKER_LANGUAGE C)

if(NOT MSVC AND NOT ANDROID)
  target_link_libraries(boringssl_crypto pthread)
endif()

# Every target depends on crypto, so we add libcxx as a dependency here to
# simplify injecting it everywhere.
if(USE_CUSTOM_LIBCXX)
  target_link_libraries(boringssl_crypto libcxx)
endif()