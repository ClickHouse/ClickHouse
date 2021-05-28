set(OPENSSL_SOURCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/openssl)
set(OPENSSL_BINARY_DIR ${ClickHouse_BINARY_DIR}/contrib/openssl)

set(OPENSSLDIR "/etc/ssl" CACHE PATH "Set the default openssl directory")
set(OPENSSL_ENGINESDIR "/usr/lib/engines-3" CACHE PATH "Set the default openssl directory for engines")
set(OPENSSL_MODULESDIR "/usr/local/lib/ossl-modules" CACHE PATH "Set the default openssl directory for modules")

# How these lists were generated?
# I compiled the original OpenSSL with it's original build system and copied the list of source files from build commands.

add_definitions(-DOPENSSLDIR="${OPENSSLDIR}" -DENGINESDIR="${OPENSSL_ENGINESDIR}" -DMODULESDIR="${OPENSSL_MODULESDIR}")

if(ARCH_AMD64)
    add_definitions(-DAES_ASM -DBSAES_ASM -DECP_NISTZ256_ASM -DGHASH_ASM -DKECCAK1600_ASM -DMD5_ASM -DOPENSSL_BN_ASM_GF2m -DOPENSSL_BN_ASM_MONT -DOPENSSL_BN_ASM_MONT5 -DOPENSSL_CPUID_OBJ -DOPENSSL_IA32_SSE2 -DPOLY1305_ASM -DSHA1_ASM -DSHA256_ASM -DSHA512_ASM -DVPAES_ASM -DWHIRLPOOL_ASM -DX25519_ASM -DOPENSSL_USE_NODELETE -DL_ENDIAN)
elseif(ARCH_AARCH64)
    add_definitions(-DECP_NISTZ256_ASM -DKECCAK1600_ASM -DOPENSSL_BN_ASM_MONT -DOPENSSL_CPUID_OBJ -DPOLY1305_ASM -DSHA1_ASM -DSHA256_ASM -DSHA512_ASM -DVPAES_ASM -DOPENSSL_USE_NODELETE -DL_ENDIAN)
endif()

enable_language(ASM)

if (COMPILER_CLANG)
    add_definitions(-Wno-unused-command-line-argument)
endif ()

if (ARCH_AMD64)
    if (OS_DARWIN)
        set (OPENSSL_SYSTEM "macosx")
    endif ()

    macro(perl_generate_asm FILE_IN FILE_OUT)
        get_filename_component(DIRNAME ${FILE_OUT} DIRECTORY)
        file(MAKE_DIRECTORY ${DIRNAME})
        add_custom_command(OUTPUT ${FILE_OUT}
            COMMAND /usr/bin/env perl ${FILE_IN} ${OPENSSL_SYSTEM} ${FILE_OUT}
            # ASM code has broken unwind tables (CFI), strip them.
            # Otherwise asynchronous unwind (that we use for query profiler)
            #  will lead to segfault while trying to interpret wrong "CFA expression".
            COMMAND sed -i -e '/^\.cfi_/d' ${FILE_OUT})
    endmacro()

    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/aes/asm/aes-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/aes/aes-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/aes/asm/aesni-mb-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/aes/aesni-mb-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/aes/asm/aesni-sha1-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/aes/aesni-sha1-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/aes/asm/aesni-sha256-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/aes/aesni-sha256-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/aes/asm/aesni-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/aes/aesni-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/aes/asm/bsaes-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/aes/bsaes-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/aes/asm/vpaes-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/aes/vpaes-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/bn/asm/rsaz-avx2.pl ${OPENSSL_BINARY_DIR}/crypto/bn/rsaz-avx2.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/bn/asm/rsaz-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/bn/rsaz-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/bn/asm/x86_64-gf2m.pl ${OPENSSL_BINARY_DIR}/crypto/bn/x86_64-gf2m.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/bn/asm/x86_64-mont.pl ${OPENSSL_BINARY_DIR}/crypto/bn/x86_64-mont.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/bn/asm/x86_64-mont5.pl ${OPENSSL_BINARY_DIR}/crypto/bn/x86_64-mont5.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/camellia/asm/cmll-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/camellia/cmll-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/chacha/asm/chacha-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/chacha/chacha-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/ec/asm/ecp_nistz256-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/ec/ecp_nistz256-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/ec/asm/x25519-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/ec/x25519-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/x86_64cpuid.pl ${OPENSSL_BINARY_DIR}/crypto/x86_64cpuid.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/md5/asm/md5-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/md5/md5-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/modes/asm/aesni-gcm-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/modes/aesni-gcm-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/modes/asm/ghash-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/modes/ghash-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/poly1305/asm/poly1305-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/poly1305/poly1305-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/rc4/asm/rc4-md5-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/rc4/rc4-md5-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/rc4/asm/rc4-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/rc4/rc4-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/sha/asm/keccak1600-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/sha/keccak1600-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/sha/asm/sha1-mb-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/sha/sha1-mb-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/sha/asm/sha1-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/sha/sha1-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/sha/asm/sha256-mb-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/sha/sha256-mb-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/sha/asm/sha512-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/sha/sha256-x86_64.s)   # This is not a mistake
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/sha/asm/sha512-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/sha/sha512-x86_64.s)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/whrlpool/asm/wp-x86_64.pl ${OPENSSL_BINARY_DIR}/crypto/whrlpool/wp-x86_64.s)

elseif (ARCH_AARCH64)

    macro(perl_generate_asm FILE_IN FILE_OUT)
        get_filename_component(DIRNAME ${FILE_OUT} DIRECTORY)
        file(MAKE_DIRECTORY ${DIRNAME})
        add_custom_command(OUTPUT ${FILE_OUT}
            COMMAND /usr/bin/env perl ${FILE_IN} "linux64" ${FILE_OUT})
            # Hope that the ASM code for AArch64 doesn't have broken CFI. Otherwise, add the same sed as for x86_64.
    endmacro()

    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/aes/asm/aesv8-armx.pl ${OPENSSL_BINARY_DIR}/crypto/aes/aesv8-armx.S)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/aes/asm/vpaes-armv8.pl ${OPENSSL_BINARY_DIR}/crypto/aes/vpaes-armv8.S)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/bn/asm/armv8-mont.pl ${OPENSSL_BINARY_DIR}/crypto/bn/armv8-mont.S)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/chacha/asm/chacha-armv8.pl ${OPENSSL_BINARY_DIR}/crypto/chacha/chacha-armv8.S)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/ec/asm/ecp_nistz256-armv8.pl ${OPENSSL_BINARY_DIR}/crypto/ec/ecp_nistz256-armv8.S)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/arm64cpuid.pl ${OPENSSL_BINARY_DIR}/crypto/arm64cpuid.S)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/modes/asm/ghashv8-armx.pl ${OPENSSL_BINARY_DIR}/crypto/modes/ghashv8-armx.S)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/poly1305/asm/poly1305-armv8.pl ${OPENSSL_BINARY_DIR}/crypto/poly1305/poly1305-armv8.S)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/sha/asm/keccak1600-armv8.pl ${OPENSSL_BINARY_DIR}/crypto/sha/keccak1600-armv8.S)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/sha/asm/sha1-armv8.pl ${OPENSSL_BINARY_DIR}/crypto/sha/sha1-armv8.S)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/sha/asm/sha512-armv8.pl ${OPENSSL_BINARY_DIR}/crypto/sha/sha256-armv8.S)   # This is not a mistake
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/sha/asm/sha512-armv8.pl ${OPENSSL_BINARY_DIR}/crypto/sha/sha512-armv8.S)
    perl_generate_asm(${OPENSSL_SOURCE_DIR}/crypto/modes/asm/aes-gcm-armv8_64.pl ${OPENSSL_BINARY_DIR}/crypto/modes/asm/aes-gcm-armv8_64.S)

endif ()

set(CRYPTO_SRCS
${OPENSSL_SOURCE_DIR}/crypto/aes/aes_cfb.c
${OPENSSL_SOURCE_DIR}/crypto/aes/aes_ecb.c
${OPENSSL_SOURCE_DIR}/crypto/aes/aes_ige.c
${OPENSSL_SOURCE_DIR}/crypto/aes/aes_misc.c
${OPENSSL_SOURCE_DIR}/crypto/aes/aes_ofb.c
${OPENSSL_SOURCE_DIR}/crypto/aes/aes_wrap.c
${OPENSSL_SOURCE_DIR}/crypto/aria/aria.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_bitstr.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_d2i_fp.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_digest.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_dup.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_gentm.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_i2d_fp.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_int.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_mbstr.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_object.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_octet.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_print.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_sign.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_strex.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_strnid.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_time.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_type.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_utctm.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_utf8.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/a_verify.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/ameth_lib.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/asn1_err.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/asn1_gen.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/asn1_item_list.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/asn1_lib.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/asn1_par.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/asn_mime.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/asn_moid.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/asn_mstbl.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/asn_pack.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/bio_asn1.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/bio_ndef.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/d2i_param.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/d2i_pr.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/d2i_pu.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/evp_asn1.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/f_int.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/f_string.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/i2d_param.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/i2d_pr.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/i2d_pu.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/n_pkey.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/nsseq.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/p5_pbe.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/p5_pbev2.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/p5_scrypt.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/p8_pkey.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/t_bitst.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/t_pkey.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/t_spki.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/tasn_dec.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/tasn_enc.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/tasn_fre.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/tasn_new.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/tasn_prn.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/tasn_scn.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/tasn_typ.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/tasn_utl.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/x_algor.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/x_bignum.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/x_info.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/x_int64.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/x_long.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/x_pkey.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/x_sig.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/x_spki.c
${OPENSSL_SOURCE_DIR}/crypto/asn1/x_val.c
${OPENSSL_SOURCE_DIR}/crypto/async/arch/async_null.c
${OPENSSL_SOURCE_DIR}/crypto/async/arch/async_posix.c
${OPENSSL_SOURCE_DIR}/crypto/async/arch/async_win.c
${OPENSSL_SOURCE_DIR}/crypto/async/async.c
${OPENSSL_SOURCE_DIR}/crypto/async/async_err.c
${OPENSSL_SOURCE_DIR}/crypto/async/async_wait.c
${OPENSSL_SOURCE_DIR}/crypto/bf/bf_cfb64.c
${OPENSSL_SOURCE_DIR}/crypto/bf/bf_ecb.c
${OPENSSL_SOURCE_DIR}/crypto/bf/bf_enc.c
${OPENSSL_SOURCE_DIR}/crypto/bf/bf_ofb64.c
${OPENSSL_SOURCE_DIR}/crypto/bf/bf_skey.c
${OPENSSL_SOURCE_DIR}/crypto/bio/b_addr.c
${OPENSSL_SOURCE_DIR}/crypto/bio/b_dump.c
${OPENSSL_SOURCE_DIR}/crypto/bio/b_print.c
${OPENSSL_SOURCE_DIR}/crypto/bio/b_sock.c
${OPENSSL_SOURCE_DIR}/crypto/bio/b_sock2.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bf_buff.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bf_lbuf.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bf_nbio.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bf_null.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bf_prefix.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bio_cb.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bio_err.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bio_lib.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bio_meth.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bss_acpt.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bss_bio.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bss_conn.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bss_dgram.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bss_fd.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bss_file.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bss_log.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bss_mem.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bss_null.c
${OPENSSL_SOURCE_DIR}/crypto/bio/bss_sock.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_add.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_blind.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_const.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_conv.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_ctx.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_depr.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_dh.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_div.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_err.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_exp.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_exp2.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_gcd.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_gf2m.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_intern.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_kron.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_lib.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_mod.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_mont.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_mpi.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_mul.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_nist.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_prime.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_print.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_rand.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_recp.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_rsa_fips186_4.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_shift.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_sqr.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_sqrt.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_srp.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_word.c
${OPENSSL_SOURCE_DIR}/crypto/bn/bn_x931p.c
${OPENSSL_SOURCE_DIR}/crypto/buffer/buf_err.c
${OPENSSL_SOURCE_DIR}/crypto/buffer/buffer.c
${OPENSSL_SOURCE_DIR}/crypto/camellia/cmll_cfb.c
${OPENSSL_SOURCE_DIR}/crypto/camellia/cmll_ctr.c
${OPENSSL_SOURCE_DIR}/crypto/camellia/cmll_ecb.c
${OPENSSL_SOURCE_DIR}/crypto/camellia/cmll_misc.c
${OPENSSL_SOURCE_DIR}/crypto/camellia/cmll_ofb.c
${OPENSSL_SOURCE_DIR}/crypto/cast/c_cfb64.c
${OPENSSL_SOURCE_DIR}/crypto/cast/c_ecb.c
${OPENSSL_SOURCE_DIR}/crypto/cast/c_enc.c
${OPENSSL_SOURCE_DIR}/crypto/cast/c_ofb64.c
${OPENSSL_SOURCE_DIR}/crypto/cast/c_skey.c
${OPENSSL_SOURCE_DIR}/crypto/cmac/cm_ameth.c
${OPENSSL_SOURCE_DIR}/crypto/cmac/cmac.c
${OPENSSL_SOURCE_DIR}/crypto/cmp/cmp_asn.c
${OPENSSL_SOURCE_DIR}/crypto/cmp/cmp_ctx.c
${OPENSSL_SOURCE_DIR}/crypto/cmp/cmp_err.c
${OPENSSL_SOURCE_DIR}/crypto/cmp/cmp_hdr.c
${OPENSSL_SOURCE_DIR}/crypto/cmp/cmp_msg.c
${OPENSSL_SOURCE_DIR}/crypto/cmp/cmp_protect.c
${OPENSSL_SOURCE_DIR}/crypto/cmp/cmp_status.c
${OPENSSL_SOURCE_DIR}/crypto/cmp/cmp_util.c
${OPENSSL_SOURCE_DIR}/crypto/cms/cms_asn1.c
${OPENSSL_SOURCE_DIR}/crypto/cms/cms_att.c
${OPENSSL_SOURCE_DIR}/crypto/cms/cms_cd.c
${OPENSSL_SOURCE_DIR}/crypto/cms/cms_dd.c
${OPENSSL_SOURCE_DIR}/crypto/cms/cms_enc.c
${OPENSSL_SOURCE_DIR}/crypto/cms/cms_env.c
${OPENSSL_SOURCE_DIR}/crypto/cms/cms_err.c
${OPENSSL_SOURCE_DIR}/crypto/cms/cms_ess.c
${OPENSSL_SOURCE_DIR}/crypto/cms/cms_io.c
${OPENSSL_SOURCE_DIR}/crypto/cms/cms_kari.c
${OPENSSL_SOURCE_DIR}/crypto/cms/cms_lib.c
${OPENSSL_SOURCE_DIR}/crypto/cms/cms_pwri.c
${OPENSSL_SOURCE_DIR}/crypto/cms/cms_sd.c
${OPENSSL_SOURCE_DIR}/crypto/cms/cms_smime.c
${OPENSSL_SOURCE_DIR}/crypto/comp/c_zlib.c
${OPENSSL_SOURCE_DIR}/crypto/comp/comp_err.c
${OPENSSL_SOURCE_DIR}/crypto/comp/comp_lib.c
${OPENSSL_SOURCE_DIR}/crypto/conf/conf_api.c
${OPENSSL_SOURCE_DIR}/crypto/conf/conf_def.c
${OPENSSL_SOURCE_DIR}/crypto/conf/conf_err.c
${OPENSSL_SOURCE_DIR}/crypto/conf/conf_lib.c
${OPENSSL_SOURCE_DIR}/crypto/conf/conf_mall.c
${OPENSSL_SOURCE_DIR}/crypto/conf/conf_mod.c
${OPENSSL_SOURCE_DIR}/crypto/conf/conf_sap.c
${OPENSSL_SOURCE_DIR}/crypto/conf/conf_ssl.c
${OPENSSL_SOURCE_DIR}/crypto/crmf/crmf_asn.c
${OPENSSL_SOURCE_DIR}/crypto/crmf/crmf_err.c
${OPENSSL_SOURCE_DIR}/crypto/crmf/crmf_lib.c
${OPENSSL_SOURCE_DIR}/crypto/crmf/crmf_pbm.c
${OPENSSL_SOURCE_DIR}/crypto/ct/ct_b64.c
${OPENSSL_SOURCE_DIR}/crypto/ct/ct_err.c
${OPENSSL_SOURCE_DIR}/crypto/ct/ct_log.c
${OPENSSL_SOURCE_DIR}/crypto/ct/ct_oct.c
${OPENSSL_SOURCE_DIR}/crypto/ct/ct_policy.c
${OPENSSL_SOURCE_DIR}/crypto/ct/ct_prn.c
${OPENSSL_SOURCE_DIR}/crypto/ct/ct_sct.c
${OPENSSL_SOURCE_DIR}/crypto/ct/ct_sct_ctx.c
${OPENSSL_SOURCE_DIR}/crypto/ct/ct_vfy.c
${OPENSSL_SOURCE_DIR}/crypto/ct/ct_x509v3.c
${OPENSSL_SOURCE_DIR}/crypto/des/cbc_cksm.c
${OPENSSL_SOURCE_DIR}/crypto/des/cbc_enc.c
${OPENSSL_SOURCE_DIR}/crypto/des/cfb64ede.c
${OPENSSL_SOURCE_DIR}/crypto/des/cfb64enc.c
${OPENSSL_SOURCE_DIR}/crypto/des/cfb_enc.c
${OPENSSL_SOURCE_DIR}/crypto/des/des_enc.c
${OPENSSL_SOURCE_DIR}/crypto/des/ecb3_enc.c
${OPENSSL_SOURCE_DIR}/crypto/des/ecb_enc.c
${OPENSSL_SOURCE_DIR}/crypto/des/fcrypt.c
${OPENSSL_SOURCE_DIR}/crypto/des/fcrypt_b.c
${OPENSSL_SOURCE_DIR}/crypto/des/ofb64ede.c
${OPENSSL_SOURCE_DIR}/crypto/des/ofb64enc.c
${OPENSSL_SOURCE_DIR}/crypto/des/ofb_enc.c
${OPENSSL_SOURCE_DIR}/crypto/des/pcbc_enc.c
${OPENSSL_SOURCE_DIR}/crypto/des/qud_cksm.c
${OPENSSL_SOURCE_DIR}/crypto/des/rand_key.c
${OPENSSL_SOURCE_DIR}/crypto/des/set_key.c
${OPENSSL_SOURCE_DIR}/crypto/des/str2key.c
${OPENSSL_SOURCE_DIR}/crypto/des/xcbc_enc.c
${OPENSSL_SOURCE_DIR}/crypto/dh/dh_ameth.c
${OPENSSL_SOURCE_DIR}/crypto/dh/dh_asn1.c
${OPENSSL_SOURCE_DIR}/crypto/dh/dh_check.c
${OPENSSL_SOURCE_DIR}/crypto/dh/dh_depr.c
${OPENSSL_SOURCE_DIR}/crypto/dh/dh_err.c
${OPENSSL_SOURCE_DIR}/crypto/dh/dh_gen.c
${OPENSSL_SOURCE_DIR}/crypto/dh/dh_group_params.c
${OPENSSL_SOURCE_DIR}/crypto/dh/dh_kdf.c
${OPENSSL_SOURCE_DIR}/crypto/dh/dh_key.c
${OPENSSL_SOURCE_DIR}/crypto/dh/dh_lib.c
${OPENSSL_SOURCE_DIR}/crypto/dh/dh_meth.c
${OPENSSL_SOURCE_DIR}/crypto/dh/dh_pmeth.c
${OPENSSL_SOURCE_DIR}/crypto/dh/dh_prn.c
${OPENSSL_SOURCE_DIR}/crypto/dh/dh_rfc5114.c
${OPENSSL_SOURCE_DIR}/crypto/dsa/dsa_aid.c
${OPENSSL_SOURCE_DIR}/crypto/dsa/dsa_ameth.c
${OPENSSL_SOURCE_DIR}/crypto/dsa/dsa_asn1.c
${OPENSSL_SOURCE_DIR}/crypto/dsa/dsa_depr.c
${OPENSSL_SOURCE_DIR}/crypto/dsa/dsa_err.c
${OPENSSL_SOURCE_DIR}/crypto/dsa/dsa_gen.c
${OPENSSL_SOURCE_DIR}/crypto/dsa/dsa_key.c
${OPENSSL_SOURCE_DIR}/crypto/dsa/dsa_lib.c
${OPENSSL_SOURCE_DIR}/crypto/dsa/dsa_meth.c
${OPENSSL_SOURCE_DIR}/crypto/dsa/dsa_ossl.c
${OPENSSL_SOURCE_DIR}/crypto/dsa/dsa_pmeth.c
${OPENSSL_SOURCE_DIR}/crypto/dsa/dsa_prn.c
${OPENSSL_SOURCE_DIR}/crypto/dsa/dsa_sign.c
${OPENSSL_SOURCE_DIR}/crypto/dsa/dsa_vrf.c
${OPENSSL_SOURCE_DIR}/crypto/dso/dso_dl.c
${OPENSSL_SOURCE_DIR}/crypto/dso/dso_dlfcn.c
${OPENSSL_SOURCE_DIR}/crypto/dso/dso_err.c
${OPENSSL_SOURCE_DIR}/crypto/dso/dso_lib.c
${OPENSSL_SOURCE_DIR}/crypto/dso/dso_openssl.c
${OPENSSL_SOURCE_DIR}/crypto/dso/dso_vms.c
${OPENSSL_SOURCE_DIR}/crypto/dso/dso_win32.c
${OPENSSL_SOURCE_DIR}/crypto/ec/curve448/arch_32/f_impl.c
${OPENSSL_SOURCE_DIR}/crypto/ec/curve448/curve448.c
${OPENSSL_SOURCE_DIR}/crypto/ec/curve448/curve448_tables.c
${OPENSSL_SOURCE_DIR}/crypto/ec/curve448/eddsa.c
${OPENSSL_SOURCE_DIR}/crypto/ec/curve448/f_generic.c
${OPENSSL_SOURCE_DIR}/crypto/ec/curve448/scalar.c
${OPENSSL_SOURCE_DIR}/crypto/ec/curve25519.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ec2_oct.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ec2_smpl.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ec_ameth.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ec_asn1.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ec_check.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ec_curve.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ec_cvt.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ec_err.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ec_key.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ec_kmeth.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ec_lib.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ec_mult.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ec_oct.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ec_pmeth.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ec_print.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ecdh_kdf.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ecdh_ossl.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ecdsa_ossl.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ecdsa_sign.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ecdsa_vrf.c
${OPENSSL_SOURCE_DIR}/crypto/ec/eck_prn.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ecp_mont.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ecp_nist.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ecp_nistp224.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ecp_nistp256.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ecp_nistp521.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ecp_nistputil.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ecp_nistz256.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ecp_oct.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ecp_smpl.c
${OPENSSL_SOURCE_DIR}/crypto/ec/ecx_meth.c
${OPENSSL_SOURCE_DIR}/crypto/engine/eng_all.c
${OPENSSL_SOURCE_DIR}/crypto/engine/eng_cnf.c
${OPENSSL_SOURCE_DIR}/crypto/engine/eng_ctrl.c
${OPENSSL_SOURCE_DIR}/crypto/engine/eng_dyn.c
${OPENSSL_SOURCE_DIR}/crypto/engine/eng_err.c
${OPENSSL_SOURCE_DIR}/crypto/engine/eng_fat.c
${OPENSSL_SOURCE_DIR}/crypto/engine/eng_init.c
${OPENSSL_SOURCE_DIR}/crypto/engine/eng_lib.c
${OPENSSL_SOURCE_DIR}/crypto/engine/eng_list.c
${OPENSSL_SOURCE_DIR}/crypto/engine/eng_openssl.c
${OPENSSL_SOURCE_DIR}/crypto/engine/eng_pkey.c
${OPENSSL_SOURCE_DIR}/crypto/engine/eng_rdrand.c
${OPENSSL_SOURCE_DIR}/crypto/engine/eng_table.c
${OPENSSL_SOURCE_DIR}/crypto/engine/tb_asnmth.c
${OPENSSL_SOURCE_DIR}/crypto/engine/tb_cipher.c
${OPENSSL_SOURCE_DIR}/crypto/engine/tb_dh.c
${OPENSSL_SOURCE_DIR}/crypto/engine/tb_digest.c
${OPENSSL_SOURCE_DIR}/crypto/engine/tb_dsa.c
${OPENSSL_SOURCE_DIR}/crypto/engine/tb_eckey.c
${OPENSSL_SOURCE_DIR}/crypto/engine/tb_pkmeth.c
${OPENSSL_SOURCE_DIR}/crypto/engine/tb_rand.c
${OPENSSL_SOURCE_DIR}/crypto/engine/tb_rsa.c
${OPENSSL_SOURCE_DIR}/crypto/err/err.c
${OPENSSL_SOURCE_DIR}/crypto/err/err_all.c
${OPENSSL_SOURCE_DIR}/crypto/err/err_blocks.c
${OPENSSL_SOURCE_DIR}/crypto/err/err_prn.c
${OPENSSL_SOURCE_DIR}/crypto/ess/ess_asn1.c
${OPENSSL_SOURCE_DIR}/crypto/ess/ess_err.c
${OPENSSL_SOURCE_DIR}/crypto/ess/ess_lib.c
${OPENSSL_SOURCE_DIR}/crypto/evp/bio_b64.c
${OPENSSL_SOURCE_DIR}/crypto/evp/bio_enc.c
${OPENSSL_SOURCE_DIR}/crypto/evp/bio_md.c
${OPENSSL_SOURCE_DIR}/crypto/evp/bio_ok.c
${OPENSSL_SOURCE_DIR}/crypto/evp/c_allc.c
${OPENSSL_SOURCE_DIR}/crypto/evp/c_alld.c
${OPENSSL_SOURCE_DIR}/crypto/evp/cmeth_lib.c
${OPENSSL_SOURCE_DIR}/crypto/evp/digest.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_aes.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_aes_cbc_hmac_sha1.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_aes_cbc_hmac_sha256.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_aria.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_bf.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_camellia.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_cast.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_chacha20_poly1305.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_des.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_des3.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_idea.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_null.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_old.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_rc2.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_rc4.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_rc4_hmac_md5.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_rc5.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_seed.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_sm4.c
${OPENSSL_SOURCE_DIR}/crypto/evp/e_xcbc_d.c
${OPENSSL_SOURCE_DIR}/crypto/evp/encode.c
${OPENSSL_SOURCE_DIR}/crypto/evp/evp_cnf.c
${OPENSSL_SOURCE_DIR}/crypto/evp/evp_enc.c
${OPENSSL_SOURCE_DIR}/crypto/evp/evp_err.c
${OPENSSL_SOURCE_DIR}/crypto/evp/evp_fetch.c
${OPENSSL_SOURCE_DIR}/crypto/evp/evp_key.c
${OPENSSL_SOURCE_DIR}/crypto/evp/evp_lib.c
${OPENSSL_SOURCE_DIR}/crypto/evp/evp_pbe.c
${OPENSSL_SOURCE_DIR}/crypto/evp/evp_pkey.c
${OPENSSL_SOURCE_DIR}/crypto/evp/evp_utils.c
${OPENSSL_SOURCE_DIR}/crypto/evp/exchange.c
${OPENSSL_SOURCE_DIR}/crypto/evp/kdf_lib.c
${OPENSSL_SOURCE_DIR}/crypto/evp/kdf_meth.c
${OPENSSL_SOURCE_DIR}/crypto/evp/keymgmt_lib.c
${OPENSSL_SOURCE_DIR}/crypto/evp/keymgmt_meth.c
${OPENSSL_SOURCE_DIR}/crypto/evp/legacy_blake2.c
${OPENSSL_SOURCE_DIR}/crypto/evp/legacy_md4.c
${OPENSSL_SOURCE_DIR}/crypto/evp/legacy_md5.c
${OPENSSL_SOURCE_DIR}/crypto/evp/legacy_md5_sha1.c
${OPENSSL_SOURCE_DIR}/crypto/evp/legacy_mdc2.c
${OPENSSL_SOURCE_DIR}/crypto/evp/legacy_sha.c
${OPENSSL_SOURCE_DIR}/crypto/evp/legacy_ripemd.c
${OPENSSL_SOURCE_DIR}/crypto/evp/legacy_wp.c
${OPENSSL_SOURCE_DIR}/crypto/evp/m_null.c
${OPENSSL_SOURCE_DIR}/crypto/evp/m_sigver.c
${OPENSSL_SOURCE_DIR}/crypto/evp/mac_lib.c
${OPENSSL_SOURCE_DIR}/crypto/evp/mac_meth.c
${OPENSSL_SOURCE_DIR}/crypto/evp/names.c
${OPENSSL_SOURCE_DIR}/crypto/evp/p5_crpt.c
${OPENSSL_SOURCE_DIR}/crypto/evp/p5_crpt2.c
${OPENSSL_SOURCE_DIR}/crypto/evp/p_dec.c
${OPENSSL_SOURCE_DIR}/crypto/evp/p_enc.c
${OPENSSL_SOURCE_DIR}/crypto/evp/p_lib.c
${OPENSSL_SOURCE_DIR}/crypto/evp/p_open.c
${OPENSSL_SOURCE_DIR}/crypto/evp/p_seal.c
${OPENSSL_SOURCE_DIR}/crypto/evp/p_sign.c
${OPENSSL_SOURCE_DIR}/crypto/evp/p_verify.c
${OPENSSL_SOURCE_DIR}/crypto/evp/pbe_scrypt.c
${OPENSSL_SOURCE_DIR}/crypto/evp/pkey_kdf.c
${OPENSSL_SOURCE_DIR}/crypto/evp/pkey_mac.c
${OPENSSL_SOURCE_DIR}/crypto/evp/pmeth_fn.c
${OPENSSL_SOURCE_DIR}/crypto/evp/pmeth_gn.c
${OPENSSL_SOURCE_DIR}/crypto/evp/pmeth_lib.c
${OPENSSL_SOURCE_DIR}/crypto/evp/signature.c
${OPENSSL_SOURCE_DIR}/crypto/ffc/ffc_params.c
${OPENSSL_SOURCE_DIR}/crypto/hmac/hm_ameth.c
${OPENSSL_SOURCE_DIR}/crypto/hmac/hmac.c
${OPENSSL_SOURCE_DIR}/crypto/idea/i_cbc.c
${OPENSSL_SOURCE_DIR}/crypto/idea/i_cfb64.c
${OPENSSL_SOURCE_DIR}/crypto/idea/i_ecb.c
${OPENSSL_SOURCE_DIR}/crypto/idea/i_ofb64.c
${OPENSSL_SOURCE_DIR}/crypto/idea/i_skey.c
${OPENSSL_SOURCE_DIR}/crypto/kdf/kdf_err.c
${OPENSSL_SOURCE_DIR}/crypto/lhash/lh_stats.c
${OPENSSL_SOURCE_DIR}/crypto/lhash/lhash.c
${OPENSSL_SOURCE_DIR}/crypto/asn1_dsa.c
${OPENSSL_SOURCE_DIR}/crypto/bsearch.c
${OPENSSL_SOURCE_DIR}/crypto/context.c
${OPENSSL_SOURCE_DIR}/crypto/core_algorithm.c
${OPENSSL_SOURCE_DIR}/crypto/core_fetch.c
${OPENSSL_SOURCE_DIR}/crypto/core_namemap.c
${OPENSSL_SOURCE_DIR}/crypto/cpt_err.c
${OPENSSL_SOURCE_DIR}/crypto/cryptlib.c
${OPENSSL_SOURCE_DIR}/crypto/ctype.c
${OPENSSL_SOURCE_DIR}/crypto/cversion.c
${OPENSSL_SOURCE_DIR}/crypto/ebcdic.c
${OPENSSL_SOURCE_DIR}/crypto/ex_data.c
${OPENSSL_SOURCE_DIR}/crypto/getenv.c
${OPENSSL_SOURCE_DIR}/crypto/info.c
${OPENSSL_SOURCE_DIR}/crypto/init.c
${OPENSSL_SOURCE_DIR}/crypto/initthread.c
${OPENSSL_SOURCE_DIR}/crypto/mem.c
${OPENSSL_SOURCE_DIR}/crypto/mem_sec.c
${OPENSSL_SOURCE_DIR}/crypto/o_dir.c
${OPENSSL_SOURCE_DIR}/crypto/o_fips.c
${OPENSSL_SOURCE_DIR}/crypto/o_fopen.c
${OPENSSL_SOURCE_DIR}/crypto/o_init.c
${OPENSSL_SOURCE_DIR}/crypto/o_str.c
${OPENSSL_SOURCE_DIR}/crypto/o_time.c
${OPENSSL_SOURCE_DIR}/crypto/packet.c
${OPENSSL_SOURCE_DIR}/crypto/param_build.c
${OPENSSL_SOURCE_DIR}/crypto/params.c
${OPENSSL_SOURCE_DIR}/crypto/params_from_text.c
${OPENSSL_SOURCE_DIR}/crypto/provider.c
${OPENSSL_SOURCE_DIR}/crypto/provider_conf.c
${OPENSSL_SOURCE_DIR}/crypto/provider_core.c
${OPENSSL_SOURCE_DIR}/crypto/provider_predefined.c
${OPENSSL_SOURCE_DIR}/crypto/sparse_array.c
${OPENSSL_SOURCE_DIR}/crypto/self_test_core.c
${OPENSSL_SOURCE_DIR}/crypto/threads_none.c
${OPENSSL_SOURCE_DIR}/crypto/threads_pthread.c
${OPENSSL_SOURCE_DIR}/crypto/threads_win.c
${OPENSSL_SOURCE_DIR}/crypto/trace.c
${OPENSSL_SOURCE_DIR}/crypto/uid.c
${OPENSSL_SOURCE_DIR}/crypto/md4/md4_dgst.c
${OPENSSL_SOURCE_DIR}/crypto/md4/md4_one.c
${OPENSSL_SOURCE_DIR}/crypto/md5/md5_dgst.c
${OPENSSL_SOURCE_DIR}/crypto/md5/md5_one.c
${OPENSSL_SOURCE_DIR}/crypto/md5/md5_sha1.c
${OPENSSL_SOURCE_DIR}/crypto/mdc2/mdc2_one.c
${OPENSSL_SOURCE_DIR}/crypto/mdc2/mdc2dgst.c
${OPENSSL_SOURCE_DIR}/crypto/modes/cbc128.c
${OPENSSL_SOURCE_DIR}/crypto/modes/ccm128.c
${OPENSSL_SOURCE_DIR}/crypto/modes/cfb128.c
${OPENSSL_SOURCE_DIR}/crypto/modes/ctr128.c
${OPENSSL_SOURCE_DIR}/crypto/modes/cts128.c
${OPENSSL_SOURCE_DIR}/crypto/modes/gcm128.c
${OPENSSL_SOURCE_DIR}/crypto/modes/ocb128.c
${OPENSSL_SOURCE_DIR}/crypto/modes/ofb128.c
${OPENSSL_SOURCE_DIR}/crypto/modes/siv128.c
${OPENSSL_SOURCE_DIR}/crypto/modes/wrap128.c
${OPENSSL_SOURCE_DIR}/crypto/modes/xts128.c
${OPENSSL_SOURCE_DIR}/crypto/objects/o_names.c
${OPENSSL_SOURCE_DIR}/crypto/objects/obj_dat.c
${OPENSSL_SOURCE_DIR}/crypto/objects/obj_err.c
${OPENSSL_SOURCE_DIR}/crypto/objects/obj_lib.c
${OPENSSL_SOURCE_DIR}/crypto/objects/obj_xref.c
${OPENSSL_SOURCE_DIR}/crypto/ocsp/ocsp_asn.c
${OPENSSL_SOURCE_DIR}/crypto/ocsp/ocsp_cl.c
${OPENSSL_SOURCE_DIR}/crypto/ocsp/ocsp_err.c
${OPENSSL_SOURCE_DIR}/crypto/ocsp/ocsp_ext.c
${OPENSSL_SOURCE_DIR}/crypto/ocsp/ocsp_ht.c
${OPENSSL_SOURCE_DIR}/crypto/ocsp/ocsp_lib.c
${OPENSSL_SOURCE_DIR}/crypto/ocsp/ocsp_prn.c
${OPENSSL_SOURCE_DIR}/crypto/ocsp/ocsp_srv.c
${OPENSSL_SOURCE_DIR}/crypto/ocsp/ocsp_vfy.c
${OPENSSL_SOURCE_DIR}/crypto/ocsp/v3_ocsp.c
${OPENSSL_SOURCE_DIR}/crypto/pem/pem_all.c
${OPENSSL_SOURCE_DIR}/crypto/pem/pem_err.c
${OPENSSL_SOURCE_DIR}/crypto/pem/pem_info.c
${OPENSSL_SOURCE_DIR}/crypto/pem/pem_lib.c
${OPENSSL_SOURCE_DIR}/crypto/pem/pem_oth.c
${OPENSSL_SOURCE_DIR}/crypto/pem/pem_pk8.c
${OPENSSL_SOURCE_DIR}/crypto/pem/pem_pkey.c
${OPENSSL_SOURCE_DIR}/crypto/pem/pem_sign.c
${OPENSSL_SOURCE_DIR}/crypto/pem/pem_x509.c
${OPENSSL_SOURCE_DIR}/crypto/pem/pem_xaux.c
${OPENSSL_SOURCE_DIR}/crypto/pem/pvkfmt.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs12/p12_add.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs12/p12_asn.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs12/p12_attr.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs12/p12_crpt.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs12/p12_crt.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs12/p12_decr.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs12/p12_init.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs12/p12_key.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs12/p12_kiss.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs12/p12_mutl.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs12/p12_npas.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs12/p12_p8d.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs12/p12_p8e.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs12/p12_sbag.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs12/p12_utl.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs12/pk12err.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs7/bio_pk7.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs7/pk7_asn1.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs7/pk7_attr.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs7/pk7_doit.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs7/pk7_lib.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs7/pk7_mime.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs7/pk7_smime.c
${OPENSSL_SOURCE_DIR}/crypto/pkcs7/pkcs7err.c
${OPENSSL_SOURCE_DIR}/crypto/poly1305/poly1305.c
${OPENSSL_SOURCE_DIR}/crypto/poly1305/poly1305_ameth.c
${OPENSSL_SOURCE_DIR}/crypto/property/defn_cache.c
${OPENSSL_SOURCE_DIR}/crypto/property/property.c
${OPENSSL_SOURCE_DIR}/crypto/property/property_err.c
${OPENSSL_SOURCE_DIR}/crypto/property/property_parse.c
${OPENSSL_SOURCE_DIR}/crypto/property/property_string.c
${OPENSSL_SOURCE_DIR}/crypto/rand/drbg_ctr.c
${OPENSSL_SOURCE_DIR}/crypto/rand/drbg_hash.c
${OPENSSL_SOURCE_DIR}/crypto/rand/drbg_hmac.c
${OPENSSL_SOURCE_DIR}/crypto/rand/drbg_lib.c
${OPENSSL_SOURCE_DIR}/crypto/rand/rand_crng_test.c
${OPENSSL_SOURCE_DIR}/crypto/rand/rand_egd.c
${OPENSSL_SOURCE_DIR}/crypto/rand/rand_err.c
${OPENSSL_SOURCE_DIR}/crypto/rand/rand_lib.c
${OPENSSL_SOURCE_DIR}/crypto/rand/rand_unix.c
${OPENSSL_SOURCE_DIR}/crypto/rand/rand_vms.c
${OPENSSL_SOURCE_DIR}/crypto/rand/rand_vxworks.c
${OPENSSL_SOURCE_DIR}/crypto/rand/rand_win.c
${OPENSSL_SOURCE_DIR}/crypto/rand/randfile.c
${OPENSSL_SOURCE_DIR}/crypto/rc2/rc2_cbc.c
${OPENSSL_SOURCE_DIR}/crypto/rc2/rc2_ecb.c
${OPENSSL_SOURCE_DIR}/crypto/rc2/rc2_skey.c
${OPENSSL_SOURCE_DIR}/crypto/rc2/rc2cfb64.c
${OPENSSL_SOURCE_DIR}/crypto/rc2/rc2ofb64.c
${OPENSSL_SOURCE_DIR}/crypto/ripemd/rmd_dgst.c
${OPENSSL_SOURCE_DIR}/crypto/ripemd/rmd_one.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_ameth.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_asn1.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_chk.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_crpt.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_depr.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_err.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_gen.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_lib.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_meth.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_mp.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_none.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_oaep.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_ossl.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_pk1.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_pmeth.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_prn.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_pss.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_saos.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_sign.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_sp800_56b_check.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_sp800_56b_gen.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_ssl.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_x931.c
${OPENSSL_SOURCE_DIR}/crypto/rsa/rsa_x931g.c
${OPENSSL_SOURCE_DIR}/crypto/seed/seed.c
${OPENSSL_SOURCE_DIR}/crypto/seed/seed_cbc.c
${OPENSSL_SOURCE_DIR}/crypto/seed/seed_cfb.c
${OPENSSL_SOURCE_DIR}/crypto/seed/seed_ecb.c
${OPENSSL_SOURCE_DIR}/crypto/seed/seed_ofb.c
${OPENSSL_SOURCE_DIR}/crypto/serializer/serializer_err.c
${OPENSSL_SOURCE_DIR}/crypto/serializer/serializer_lib.c
${OPENSSL_SOURCE_DIR}/crypto/serializer/serializer_meth.c
${OPENSSL_SOURCE_DIR}/crypto/serializer/serializer_pkey.c
${OPENSSL_SOURCE_DIR}/crypto/sha/sha1_one.c
${OPENSSL_SOURCE_DIR}/crypto/sha/sha1dgst.c
${OPENSSL_SOURCE_DIR}/crypto/sha/sha256.c
${OPENSSL_SOURCE_DIR}/crypto/sha/sha3.c
${OPENSSL_SOURCE_DIR}/crypto/sha/sha512.c
${OPENSSL_SOURCE_DIR}/crypto/siphash/siphash.c
${OPENSSL_SOURCE_DIR}/crypto/siphash/siphash_ameth.c
${OPENSSL_SOURCE_DIR}/crypto/sm2/sm2_crypt.c
${OPENSSL_SOURCE_DIR}/crypto/sm2/sm2_err.c
${OPENSSL_SOURCE_DIR}/crypto/sm2/sm2_pmeth.c
${OPENSSL_SOURCE_DIR}/crypto/sm2/sm2_sign.c
${OPENSSL_SOURCE_DIR}/crypto/sm3/sm3.c
${OPENSSL_SOURCE_DIR}/crypto/sm3/legacy_sm3.c
${OPENSSL_SOURCE_DIR}/crypto/sm4/sm4.c
${OPENSSL_SOURCE_DIR}/crypto/srp/srp_lib.c
${OPENSSL_SOURCE_DIR}/crypto/srp/srp_vfy.c
${OPENSSL_SOURCE_DIR}/crypto/stack/stack.c
${OPENSSL_SOURCE_DIR}/crypto/store/loader_file.c
${OPENSSL_SOURCE_DIR}/crypto/store/store_err.c
${OPENSSL_SOURCE_DIR}/crypto/store/store_init.c
${OPENSSL_SOURCE_DIR}/crypto/store/store_lib.c
${OPENSSL_SOURCE_DIR}/crypto/store/store_register.c
${OPENSSL_SOURCE_DIR}/crypto/store/store_strings.c
${OPENSSL_SOURCE_DIR}/crypto/ts/ts_asn1.c
${OPENSSL_SOURCE_DIR}/crypto/ts/ts_conf.c
${OPENSSL_SOURCE_DIR}/crypto/ts/ts_err.c
${OPENSSL_SOURCE_DIR}/crypto/ts/ts_lib.c
${OPENSSL_SOURCE_DIR}/crypto/ts/ts_req_print.c
${OPENSSL_SOURCE_DIR}/crypto/ts/ts_req_utils.c
${OPENSSL_SOURCE_DIR}/crypto/ts/ts_rsp_print.c
${OPENSSL_SOURCE_DIR}/crypto/ts/ts_rsp_sign.c
${OPENSSL_SOURCE_DIR}/crypto/ts/ts_rsp_utils.c
${OPENSSL_SOURCE_DIR}/crypto/ts/ts_rsp_verify.c
${OPENSSL_SOURCE_DIR}/crypto/ts/ts_verify_ctx.c
${OPENSSL_SOURCE_DIR}/crypto/txt_db/txt_db.c
${OPENSSL_SOURCE_DIR}/crypto/ui/ui_err.c
${OPENSSL_SOURCE_DIR}/crypto/ui/ui_lib.c
${OPENSSL_SOURCE_DIR}/crypto/ui/ui_null.c
${OPENSSL_SOURCE_DIR}/crypto/ui/ui_openssl.c
${OPENSSL_SOURCE_DIR}/crypto/ui/ui_util.c
${OPENSSL_SOURCE_DIR}/crypto/whrlpool/wp_dgst.c
${OPENSSL_SOURCE_DIR}/crypto/x509/by_dir.c
${OPENSSL_SOURCE_DIR}/crypto/x509/by_file.c
${OPENSSL_SOURCE_DIR}/crypto/x509/by_store.c
${OPENSSL_SOURCE_DIR}/crypto/x509/pcy_cache.c
${OPENSSL_SOURCE_DIR}/crypto/x509/pcy_data.c
${OPENSSL_SOURCE_DIR}/crypto/x509/pcy_lib.c
${OPENSSL_SOURCE_DIR}/crypto/x509/pcy_map.c
${OPENSSL_SOURCE_DIR}/crypto/x509/pcy_node.c
${OPENSSL_SOURCE_DIR}/crypto/x509/pcy_tree.c
${OPENSSL_SOURCE_DIR}/crypto/x509/t_crl.c
${OPENSSL_SOURCE_DIR}/crypto/x509/t_req.c
${OPENSSL_SOURCE_DIR}/crypto/x509/t_x509.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_addr.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_admis.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_akey.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_akeya.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_alt.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_asid.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_bcons.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_bitst.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_conf.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_cpols.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_crld.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_enum.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_extku.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_genn.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_ia5.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_info.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_int.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_lib.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_ncons.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_pci.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_pcia.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_pcons.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_pku.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_pmaps.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_prn.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_purp.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_skey.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_sxnet.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_tlsf.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3_utl.c
${OPENSSL_SOURCE_DIR}/crypto/x509/v3err.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509_att.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509_cmp.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509_d2.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509_def.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509_err.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509_ext.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509_lu.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509_meth.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509_obj.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509_r2x.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509_req.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509_set.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509_trs.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509_txt.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509_v3.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509_vfy.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509_vpm.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509cset.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509name.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509rset.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509spki.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x509type.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x_all.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x_attrib.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x_crl.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x_exten.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x_name.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x_pubkey.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x_req.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x_x509.c
${OPENSSL_SOURCE_DIR}/crypto/x509/x_x509a.c
${OPENSSL_SOURCE_DIR}/providers/implementations/asymciphers/rsa_enc.c
${OPENSSL_SOURCE_DIR}/providers/defltprov.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_null.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aes.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aes_cbc_hmac_sha.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aes_cbc_hmac_sha1_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aes_cbc_hmac_sha256_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aes_ccm.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aes_ccm_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aes_gcm.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aes_gcm_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aes_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aes_ocb.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aes_ocb_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aes_siv.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aes_siv_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aes_wrp.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aes_xts.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aes_xts_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aria.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aria_ccm.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aria_ccm_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aria_gcm.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aria_gcm_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aria_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_blowfish.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_blowfish_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_camellia.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_camellia_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_cast5.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_cast5_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_chacha20.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_chacha20_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_chacha20_poly1305.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_chacha20_poly1305_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_des.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_des_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_desx.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_desx_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_idea.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_idea_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_rc2.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_rc2_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_rc4.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_rc4_hmac_md5.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_rc4_hmac_md5_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_rc4_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_seed.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_seed_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_sm4.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_sm4_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_tdes.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_tdes_default.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_tdes_default_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_tdes_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_tdes_wrap.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_tdes_wrap_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/digests/blake2_prov.c
${OPENSSL_SOURCE_DIR}/providers/implementations/digests/blake2b_prov.c
${OPENSSL_SOURCE_DIR}/providers/implementations/digests/blake2s_prov.c
${OPENSSL_SOURCE_DIR}/providers/implementations/digests/md5_prov.c
${OPENSSL_SOURCE_DIR}/providers/implementations/digests/md5_sha1_prov.c
${OPENSSL_SOURCE_DIR}/providers/implementations/digests/sha2_prov.c
${OPENSSL_SOURCE_DIR}/providers/implementations/digests/sha3_prov.c
${OPENSSL_SOURCE_DIR}/providers/implementations/digests/sm3_prov.c
${OPENSSL_SOURCE_DIR}/providers/implementations/exchange/dh_exch.c
${OPENSSL_SOURCE_DIR}/providers/implementations/kdfs/hkdf.c
${OPENSSL_SOURCE_DIR}/providers/implementations/kdfs/kbkdf.c
${OPENSSL_SOURCE_DIR}/providers/implementations/kdfs/krb5kdf.c
${OPENSSL_SOURCE_DIR}/providers/implementations/kdfs/pbkdf2.c
${OPENSSL_SOURCE_DIR}/providers/implementations/kdfs/scrypt.c
${OPENSSL_SOURCE_DIR}/providers/implementations/kdfs/sshkdf.c
${OPENSSL_SOURCE_DIR}/providers/implementations/kdfs/sskdf.c
${OPENSSL_SOURCE_DIR}/providers/implementations/kdfs/tls1_prf.c
${OPENSSL_SOURCE_DIR}/providers/implementations/kdfs/x942kdf.c
${OPENSSL_SOURCE_DIR}/providers/implementations/keymgmt/dh_kmgmt.c
${OPENSSL_SOURCE_DIR}/providers/implementations/keymgmt/dsa_kmgmt.c
${OPENSSL_SOURCE_DIR}/providers/implementations/keymgmt/rsa_kmgmt.c
${OPENSSL_SOURCE_DIR}/providers/implementations/macs/blake2b_mac.c
${OPENSSL_SOURCE_DIR}/providers/implementations/macs/blake2s_mac.c
${OPENSSL_SOURCE_DIR}/providers/implementations/macs/cmac_prov.c
${OPENSSL_SOURCE_DIR}/providers/implementations/macs/gmac_prov.c
${OPENSSL_SOURCE_DIR}/providers/implementations/macs/hmac_prov.c
${OPENSSL_SOURCE_DIR}/providers/implementations/macs/kmac_prov.c
${OPENSSL_SOURCE_DIR}/providers/implementations/macs/poly1305_prov.c
${OPENSSL_SOURCE_DIR}/providers/implementations/macs/siphash_prov.c
${OPENSSL_SOURCE_DIR}/providers/implementations/serializers/serializer_common.c
${OPENSSL_SOURCE_DIR}/providers/implementations/serializers/serializer_dh.c
${OPENSSL_SOURCE_DIR}/providers/implementations/serializers/serializer_dh_param.c
${OPENSSL_SOURCE_DIR}/providers/implementations/serializers/serializer_dh_priv.c
${OPENSSL_SOURCE_DIR}/providers/implementations/serializers/serializer_dh_pub.c
${OPENSSL_SOURCE_DIR}/providers/implementations/serializers/serializer_dsa.c
${OPENSSL_SOURCE_DIR}/providers/implementations/serializers/serializer_dsa_param.c
${OPENSSL_SOURCE_DIR}/providers/implementations/serializers/serializer_dsa_priv.c
${OPENSSL_SOURCE_DIR}/providers/implementations/serializers/serializer_dsa_pub.c
${OPENSSL_SOURCE_DIR}/providers/implementations/serializers/serializer_rsa.c
${OPENSSL_SOURCE_DIR}/providers/implementations/serializers/serializer_rsa_priv.c
${OPENSSL_SOURCE_DIR}/providers/implementations/serializers/serializer_rsa_pub.c
${OPENSSL_SOURCE_DIR}/providers/implementations/signature/dsa.c
${OPENSSL_SOURCE_DIR}/providers/common/bio_prov.c
${OPENSSL_SOURCE_DIR}/providers/common/provider_err.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/ciphercommon.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/ciphercommon_block.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/ciphercommon_ccm.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/ciphercommon_ccm_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/ciphercommon_gcm.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/ciphercommon_gcm_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/ciphercommon_hw.c
${OPENSSL_SOURCE_DIR}/providers/implementations/digests/digestcommon.c
${OPENSSL_SOURCE_DIR}/providers/common/nid_to_name.c
${OPENSSL_SOURCE_DIR}/providers/common/provider_util.c
${OPENSSL_SOURCE_DIR}/providers/implementations/ciphers/cipher_aes_xts_fips.c
${OPENSSL_SOURCE_DIR}/providers/implementations/kdfs/pbkdf2_fips.c
)

if (ARCH_AMD64)
    set (CRYPTO_SRCS ${CRYPTO_SRCS}
        ${OPENSSL_SOURCE_DIR}/crypto/bn/asm/x86_64-gcc.c
        ${OPENSSL_SOURCE_DIR}/crypto/bn/rsaz_exp.c
        ${OPENSSL_BINARY_DIR}/crypto/aes/aes-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/aes/aesni-mb-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/aes/aesni-sha1-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/aes/aesni-sha256-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/aes/aesni-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/aes/bsaes-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/aes/vpaes-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/bn/rsaz-avx2.s
        ${OPENSSL_BINARY_DIR}/crypto/bn/rsaz-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/bn/x86_64-gf2m.s
        ${OPENSSL_BINARY_DIR}/crypto/bn/x86_64-mont.s
        ${OPENSSL_BINARY_DIR}/crypto/bn/x86_64-mont5.s
        ${OPENSSL_BINARY_DIR}/crypto/camellia/cmll-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/chacha/chacha-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/ec/ecp_nistz256-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/ec/x25519-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/x86_64cpuid.s
        ${OPENSSL_BINARY_DIR}/crypto/md5/md5-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/modes/aesni-gcm-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/modes/ghash-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/poly1305/poly1305-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/rc4/rc4-md5-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/rc4/rc4-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/sha/keccak1600-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/sha/sha1-mb-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/sha/sha1-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/sha/sha256-mb-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/sha/sha256-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/sha/sha512-x86_64.s
        ${OPENSSL_BINARY_DIR}/crypto/whrlpool/wp-x86_64.s)
elseif (ARCH_AARCH64)
    set (CRYPTO_SRCS ${CRYPTO_SRCS}
        ${OPENSSL_SOURCE_DIR}/crypto/aes/aes_cbc.c
        ${OPENSSL_SOURCE_DIR}/crypto/aes/aes_core.c
        ${OPENSSL_SOURCE_DIR}/crypto/bn/bn_asm.c
        ${OPENSSL_SOURCE_DIR}/crypto/camellia/camellia.c
        ${OPENSSL_SOURCE_DIR}/crypto/camellia/cmll_cbc.c
        ${OPENSSL_SOURCE_DIR}/crypto/armcap.c
        ${OPENSSL_SOURCE_DIR}/crypto/rc4/rc4_enc.c
        ${OPENSSL_SOURCE_DIR}/crypto/rc4/rc4_skey.c
        ${OPENSSL_SOURCE_DIR}/crypto/whrlpool/wp_block.c
        ${OPENSSL_BINARY_DIR}/crypto/aes/aesv8-armx.S
        ${OPENSSL_BINARY_DIR}/crypto/aes/vpaes-armv8.S
        ${OPENSSL_BINARY_DIR}/crypto/bn/armv8-mont.S
        ${OPENSSL_BINARY_DIR}/crypto/chacha/chacha-armv8.S
        ${OPENSSL_BINARY_DIR}/crypto/ec/ecp_nistz256-armv8.S
        ${OPENSSL_BINARY_DIR}/crypto/arm64cpuid.S
        ${OPENSSL_BINARY_DIR}/crypto/modes/ghashv8-armx.S
        ${OPENSSL_BINARY_DIR}/crypto/poly1305/poly1305-armv8.S
        ${OPENSSL_BINARY_DIR}/crypto/sha/keccak1600-armv8.S
        ${OPENSSL_BINARY_DIR}/crypto/sha/sha1-armv8.S
        ${OPENSSL_BINARY_DIR}/crypto/sha/sha256-armv8.S
        ${OPENSSL_BINARY_DIR}/crypto/sha/sha512-armv8.S
        ${OPENSSL_BINARY_DIR}/crypto/modes/asm/aes-gcm-armv8_64.S)
endif ()

set(SSL_SRCS
${OPENSSL_SOURCE_DIR}/crypto/packet.c
${OPENSSL_SOURCE_DIR}/ssl/bio_ssl.c
${OPENSSL_SOURCE_DIR}/ssl/d1_lib.c
${OPENSSL_SOURCE_DIR}/ssl/d1_msg.c
${OPENSSL_SOURCE_DIR}/ssl/d1_srtp.c
${OPENSSL_SOURCE_DIR}/ssl/methods.c
${OPENSSL_SOURCE_DIR}/ssl/pqueue.c
${OPENSSL_SOURCE_DIR}/ssl/s3_cbc.c
${OPENSSL_SOURCE_DIR}/ssl/s3_enc.c
${OPENSSL_SOURCE_DIR}/ssl/s3_lib.c
${OPENSSL_SOURCE_DIR}/ssl/s3_msg.c
${OPENSSL_SOURCE_DIR}/ssl/ssl_asn1.c
${OPENSSL_SOURCE_DIR}/ssl/ssl_cert.c
${OPENSSL_SOURCE_DIR}/ssl/ssl_ciph.c
${OPENSSL_SOURCE_DIR}/ssl/ssl_conf.c
${OPENSSL_SOURCE_DIR}/ssl/ssl_err.c
${OPENSSL_SOURCE_DIR}/ssl/ssl_init.c
${OPENSSL_SOURCE_DIR}/ssl/ssl_lib.c
${OPENSSL_SOURCE_DIR}/ssl/ssl_mcnf.c
${OPENSSL_SOURCE_DIR}/ssl/ssl_rsa.c
${OPENSSL_SOURCE_DIR}/ssl/ssl_sess.c
${OPENSSL_SOURCE_DIR}/ssl/ssl_stat.c
${OPENSSL_SOURCE_DIR}/ssl/ssl_txt.c
${OPENSSL_SOURCE_DIR}/ssl/ssl_utst.c
${OPENSSL_SOURCE_DIR}/ssl/t1_enc.c
${OPENSSL_SOURCE_DIR}/ssl/t1_lib.c
${OPENSSL_SOURCE_DIR}/ssl/t1_trce.c
${OPENSSL_SOURCE_DIR}/ssl/tls13_enc.c
${OPENSSL_SOURCE_DIR}/ssl/tls_srp.c
${OPENSSL_SOURCE_DIR}/ssl/record/dtls1_bitmap.c
${OPENSSL_SOURCE_DIR}/ssl/record/rec_layer_d1.c
${OPENSSL_SOURCE_DIR}/ssl/record/rec_layer_s3.c
${OPENSSL_SOURCE_DIR}/ssl/record/ssl3_buffer.c
${OPENSSL_SOURCE_DIR}/ssl/record/ssl3_record.c
${OPENSSL_SOURCE_DIR}/ssl/record/ssl3_record_tls13.c
${OPENSSL_SOURCE_DIR}/ssl/statem/extensions.c
${OPENSSL_SOURCE_DIR}/ssl/statem/extensions_clnt.c
${OPENSSL_SOURCE_DIR}/ssl/statem/extensions_cust.c
${OPENSSL_SOURCE_DIR}/ssl/statem/extensions_srvr.c
${OPENSSL_SOURCE_DIR}/ssl/statem/statem.c
${OPENSSL_SOURCE_DIR}/ssl/statem/statem_clnt.c
${OPENSSL_SOURCE_DIR}/ssl/statem/statem_dtls.c
${OPENSSL_SOURCE_DIR}/ssl/statem/statem_lib.c
${OPENSSL_SOURCE_DIR}/ssl/statem/statem_srvr.c
)

add_library(crypto ${CRYPTO_SRCS})
add_library(ssl ${SSL_SRCS})

if (ARCH_AMD64)
    target_include_directories(crypto
        SYSTEM PUBLIC linux_x86_64/include
        PRIVATE linux_x86_64/include_private)
elseif (ARCH_AARCH64)
    target_include_directories(crypto
        SYSTEM PUBLIC linux_aarch64/include
        PRIVATE linux_aarch64/include_private)
endif ()

target_include_directories(crypto
SYSTEM PUBLIC ${OPENSSL_SOURCE_DIR}/include
PRIVATE ${OPENSSL_SOURCE_DIR}/providers/common/include
PRIVATE ${OPENSSL_SOURCE_DIR}/providers/implementations/include
PRIVATE ${OPENSSL_SOURCE_DIR}/crypto
PRIVATE ${OPENSSL_SOURCE_DIR}/crypto/include
PRIVATE ${OPENSSL_SOURCE_DIR}/crypto/ec/curve448
PRIVATE ${OPENSSL_SOURCE_DIR}/crypto/ec/curve448/arch_32
PRIVATE ${OPENSSL_SOURCE_DIR})

target_include_directories(ssl
PRIVATE ${OPENSSL_SOURCE_DIR})

target_link_libraries(ssl crypto)
