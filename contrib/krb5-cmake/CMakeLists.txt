set (ENABLE_KRB5_DEFAULT 1)
if (NOT CMAKE_SYSTEM_NAME MATCHES "Linux" AND NOT (CMAKE_SYSTEM_NAME MATCHES "Darwin" AND NOT CMAKE_CROSSCOMPILING))
    message (WARNING "krb5 disabled in non-Linux and non-native-Darwin environments")
    set (ENABLE_KRB5_DEFAULT 0)
endif ()
OPTION(ENABLE_KRB5 "Enable krb5" ${ENABLE_KRB5_DEFAULT})

if (NOT ENABLE_KRB5)
    message(STATUS "Not using krb5")
    return()
endif ()

find_program(AWK_PROGRAM awk)
if(NOT AWK_PROGRAM)
    message(FATAL_ERROR "You need the awk program to build ClickHouse with krb5 enabled.")
endif()

set(KRB5_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/krb5/src")

set(ALL_SRCS
    "${KRB5_SOURCE_DIR}/util/et/et_name.c"
    "${KRB5_SOURCE_DIR}/util/et/com_err.c"
    "${KRB5_SOURCE_DIR}/util/et/error_message.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_inq_names.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_rel_name.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_unwrap_aead.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_set_name_attr.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_glue.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_imp_cred.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/gssd_pname_to_uid.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_authorize_localname.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_prf.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_acquire_cred_with_pw.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_set_cred_option.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_map_name_to_any.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_inq_cred.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_rel_cred.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_seal.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_delete_sec_context.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_context_time.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_get_name_attr.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_mech_invoke.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_unwrap_iov.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_exp_sec_context.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_init_sec_context.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_accept_sec_context.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_verify.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_sign.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_mechname.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_mechattr.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_complete_auth_token.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_wrap_aead.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_inq_cred_oid.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_rel_buffer.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_initialize.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_export_name_comp.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_set_context_option.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_acquire_cred.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_acquire_cred_imp_name.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_imp_name.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_inq_name.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_set_neg_mechs.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_inq_context.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_export_cred.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_oid_ops.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_inq_context_oid.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_del_name_attr.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_decapsulate_token.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_compare_name.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_rel_name_mapping.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_imp_sec_context.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_dup_name.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_export_name.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_wrap_iov.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_rel_oid_set.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_unseal.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_store_cred.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_buffer_set.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_canon_name.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_dsp_status.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_dsp_name.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_dsp_name_ext.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_saslname.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_process_context.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_encapsulate_token.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue/g_negoex.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/delete_sec_context.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/lucid_context.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/duplicate_name.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/get_tkt_flags.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/set_allowable_enctypes.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/k5sealiov.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/gssapi_err_krb5.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/canon_name.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/inq_cred.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/export_sec_context.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/inq_names.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/prf.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/k5sealv3iov.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/store_cred.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/import_name.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/export_name.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/naming_exts.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/s4u_gss_glue.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/rel_name.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/k5unsealiov.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/gssapi_krb5.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/disp_status.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/import_cred.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/k5seal.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/accept_sec_context.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/import_sec_context.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/process_context_token.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/disp_name.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/wrap_size_limit.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/krb5_gss_glue.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/util_crypt.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/set_ccache.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/export_cred.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/rel_oid.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/val_cred.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/context_time.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/cred_store.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/iakerb.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/copy_ccache.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/init_sec_context.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/indicate_mechs.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/inq_context.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/util_seed.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/util_seqnum.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/compare_name.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/ser_sctx.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/k5sealv3.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/acquire_cred.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/k5unseal.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/rel_cred.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/util_cksum.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/generic/disp_com_err_status.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/generic/gssapi_generic.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/generic/rel_oid_set.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/generic/oid_ops.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/generic/util_buffer.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/generic/util_buffer_set.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/generic/util_set.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/generic/util_token.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/generic/gssapi_err_generic.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/generic/disp_major_status.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/generic/util_seqstate.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/generic/util_errmap.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/generic/rel_buffer.c"

    "${KRB5_SOURCE_DIR}/lib/gssapi/spnego/spnego_mech.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/spnego/negoex_util.c"
    "${KRB5_SOURCE_DIR}/lib/gssapi/spnego/negoex_ctx.c"

    # "${KRB5_SOURCE_DIR}/lib/gssapi/spnego/negoex_trace.c"

    "${KRB5_SOURCE_DIR}/lib/crypto/krb/prng.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/enc_dk_cmac.c"
    # "${KRB5_SOURCE_DIR}/lib/crypto/krb/crc32.c"
    # "${KRB5_SOURCE_DIR}/lib/crypto/krb/checksum_cbc.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/enctype_util.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/enc_etm.c"
    # "${KRB5_SOURCE_DIR}/lib/crypto/krb/combine_keys.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/default_state.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/decrypt_iov.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/checksum_dk_cmac.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/etypes.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/old_api_glue.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/cksumtypes.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/prf_cmac.c"
    # "${KRB5_SOURCE_DIR}/lib/crypto/krb/enc_old.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/decrypt.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/prf_dk.c"
    # "${KRB5_SOURCE_DIR}/lib/crypto/krb/s2k_des.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/checksum_unkeyed.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/crypto_length.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/block_size.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/string_to_key.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/verify_checksum.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/crypto_libinit.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/derive.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/random_to_key.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/verify_checksum_iov.c"
    # "${KRB5_SOURCE_DIR}/lib/crypto/krb/checksum_confounder.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/checksum_length.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/enc_dk_hmac.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/make_checksum.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/prf_des.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/prf.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/coll_proof_cksum.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/enc_rc4.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/cf2.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/aead.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/encrypt_iov.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/cksumtype_to_string.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/key.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/enc_raw.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/keylengths.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/checksum_hmac_md5.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/keyed_cksum.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/keyed_checksum_types.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/prf_aes2.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/state.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/checksum_dk_hmac.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/encrypt.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/checksum_etm.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/make_random_key.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/string_to_cksumtype.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/mandatory_sumtype.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/make_checksum_iov.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/s2k_rc4.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/valid_cksumtype.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/nfold.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/prng_fortuna.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/encrypt_length.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/cmac.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/keyblocks.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/prf_rc4.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb/s2k_pbkdf2.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/openssl/enc_provider/aes.c"
    # "${KRB5_SOURCE_DIR}/lib/crypto/openssl/enc_provider/des.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/openssl/enc_provider/rc4.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/openssl/enc_provider/des3.c"
    #"${KRB5_SOURCE_DIR}/lib/crypto/openssl/enc_provider/camellia.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/openssl/sha256.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/openssl/hmac.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/openssl/pbkdf2.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/openssl/init.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/openssl/stubs.c"
    # "${KRB5_SOURCE_DIR}/lib/crypto/openssl/hash_provider/hash_crc32.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/openssl/hash_provider/hash_evp.c"
    "${KRB5_SOURCE_DIR}/lib/crypto/openssl/des/des_keys.c"
    "${KRB5_SOURCE_DIR}/util/support/fake-addrinfo.c"
    "${KRB5_SOURCE_DIR}/util/support/k5buf.c"
    "${KRB5_SOURCE_DIR}/util/support/hex.c"
    "${KRB5_SOURCE_DIR}/util/support/threads.c"
    "${KRB5_SOURCE_DIR}/util/support/utf8.c"
    "${KRB5_SOURCE_DIR}/util/support/hashtab.c"
    "${KRB5_SOURCE_DIR}/util/support/dir_filenames.c"
    "${KRB5_SOURCE_DIR}/util/support/base64.c"
    "${KRB5_SOURCE_DIR}/util/support/strerror_r.c"
    "${KRB5_SOURCE_DIR}/util/support/plugins.c"
    "${KRB5_SOURCE_DIR}/util/support/path.c"
    "${KRB5_SOURCE_DIR}/util/support/init-addrinfo.c"
    "${KRB5_SOURCE_DIR}/util/support/json.c"
    "${KRB5_SOURCE_DIR}/util/support/errors.c"
    "${KRB5_SOURCE_DIR}/util/support/utf8_conv.c"
    "${KRB5_SOURCE_DIR}/util/support/strlcpy.c"
    "${KRB5_SOURCE_DIR}/util/support/gmt_mktime.c"
    "${KRB5_SOURCE_DIR}/util/support/zap.c"
    "${KRB5_SOURCE_DIR}/util/support/bcmp.c"
    "${KRB5_SOURCE_DIR}/util/support/secure_getenv.c"
    "${KRB5_SOURCE_DIR}/util/profile/prof_tree.c"
    "${KRB5_SOURCE_DIR}/util/profile/prof_file.c"
    "${KRB5_SOURCE_DIR}/util/profile/prof_parse.c"
    "${KRB5_SOURCE_DIR}/util/profile/prof_get.c"
    "${KRB5_SOURCE_DIR}/util/profile/prof_set.c"
    "${KRB5_SOURCE_DIR}/util/profile/prof_err.c"
    "${KRB5_SOURCE_DIR}/util/profile/prof_init.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/fwd_tgt.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/conv_creds.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/fast.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/ser_adata.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/copy_tick.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/enc_keyhelper.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/ser_actx.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/init_ctx.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/preauth2.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/copy_princ.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/parse_host_string.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/pr_to_salt.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/rd_req.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/pac_sign.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/copy_addrs.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/conv_princ.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/rd_rep.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/str_conv.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/gic_opt.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/recvauth.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/copy_cksum.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/ai_authdata.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/ser_ctx.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/appdefault.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/bld_princ.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/in_tkt_sky.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/copy_creds.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/auth_con.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/copy_key.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/kdc_rep_dc.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/mk_cred.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/gic_keytab.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/rd_req_dec.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/set_realm.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/preauth_sam2.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/libdef_parse.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/privsafe.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/ser_auth.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/val_renew.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/addr_order.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/authdata_dec.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/walk_rtree.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/gen_subkey.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/copy_auth.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/chpw.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/mk_req.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/allow_weak.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/mk_rep.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/mk_priv.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/s4u_authdata.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/preauth_otp.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/init_keyblock.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/ser_addr.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/encrypt_tk.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/s4u_creds.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/srv_dec_tkt.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/rd_priv.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/authdata_enc.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/authdata_exp.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/decode_kdc.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/decrypt_tk.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/enc_helper.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/mk_req_ext.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/ser_key.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/preauth_encts.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/send_tgs.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/ser_cksum.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/tgtname.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/encode_kdc.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/rd_cred.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/rd_safe.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/preauth_pkinit.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/srv_rcache.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/chk_trans.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/etype_list.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/get_creds.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/ser_princ.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/gic_pwd.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/authdata.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/gen_save_subkey.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/vfy_increds.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/addr_comp.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/kfree.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/response_items.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/serialize.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/cammac_util.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/gc_via_tkt.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/copy_ctx.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/sendauth.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/addr_srch.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/mk_safe.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/preauth_ec.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/bld_pr_ext.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/random_str.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/sname_match.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/princ_comp.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/get_in_tkt.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/gen_seqnum.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/cp_key_cnt.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/mk_error.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/copy_athctr.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/deltat.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/get_etype_info.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/plugin.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/kerrs.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/vic_opt.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/unparse.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/parse.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/rd_error.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/pac.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/valid_times.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/copy_data.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb/padata.c"


    "${KRB5_SOURCE_DIR}/lib/krb5/os/hostrealm.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/thread_safe.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/krbfileio.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/toffset.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/hostaddr.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/ustime.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/timeofday.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/ccdefname.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/full_ipadr.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/read_pwd.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/trace.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/localauth_k5login.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/localauth_rule.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/localaddr.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/hostrealm_dns.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/hostrealm_domain.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/sn2princ.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/net_write.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/gen_rname.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/net_read.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/accessor.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/hostrealm_profile.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/c_ustime.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/expand_path.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/port2ip.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/changepw.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/unlck_file.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/gen_port.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/localauth_an2ln.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/genaddrs.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/init_os_ctx.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/localauth.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/locate_kdc.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/prompter.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/ktdefname.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/realm_dom.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/dnssrv.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/mk_faddr.c"
    # "${KRB5_SOURCE_DIR}/lib/krb5/os/dnsglue.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/sendto_kdc.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/hostrealm_registry.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/write_msg.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/localauth_names.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/read_msg.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/os/lock_file.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/ccache/ccselect.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/ccache/ccselect_realm.c"
    # "${KRB5_SOURCE_DIR}/lib/krb5/ccache/ser_cc.c"

    "${KRB5_SOURCE_DIR}/lib/krb5/ccache/ccdefops.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/ccache/cc_retr.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/ccache/ccselect_k5identity.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/ccache/cccopy.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/ccache/ccfns.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/ccache/cc_file.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/ccache/ccbase.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/ccache/cccursor.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/ccache/ccdefault.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/ccache/cc_memory.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/ccache/ccmarshal.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/ccache/ccselect_hostname.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/ccache/cc_dir.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/ccache/cc_keyring.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/ccache/cc_kcm.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/keytab/ktadd.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/keytab/ktbase.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/keytab/ktdefault.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/keytab/kt_memory.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/keytab/ktfns.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/keytab/ktremove.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/keytab/read_servi.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/keytab/kt_file.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/keytab/read_servi.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/keytab/ktfr_entry.c"



    "${KRB5_SOURCE_DIR}/lib/krb5/error_tables/k5e1_err.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/error_tables/kdb5_err.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/error_tables/asn1_err.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/error_tables/krb5_err.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/error_tables/krb524_err.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/error_tables/kv5m_err.c"



    "${KRB5_SOURCE_DIR}/lib/krb5/rcache/rc_base.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/rcache/rc_dfl.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/rcache/rc_file2.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/rcache/rc_none.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/rcache/memrcache.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/unicode/ucdata/ucdata.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/unicode/ucstr.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/asn.1/asn1_encode.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/asn.1/asn1_k_encode.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/asn.1/ldap_key_seq.c"
    "${KRB5_SOURCE_DIR}/lib/krb5/krb5_libinit.c"
)

add_custom_command(
    OUTPUT "${KRB5_SOURCE_DIR}/util/et/compile_et"
    COMMAND /bin/sh
    ./config_script
    ./compile_et.sh
    "/usr/local/share/et"
    ${AWK_PROGRAM}
    sed
    >
    compile_et
    DEPENDS "${KRB5_SOURCE_DIR}/util/et/compile_et.sh" "${KRB5_SOURCE_DIR}/util/et/config_script"
    WORKING_DIRECTORY "${KRB5_SOURCE_DIR}/util/et"
)

file(GLOB_RECURSE ET_FILES
    "${KRB5_SOURCE_DIR}/*.et"
)

function(preprocess_et out_var)
  set(result)
  foreach(in_f ${ARGN})
    string(REPLACE
        .et
        .c
        F_C
        ${in_f}
    )
    string(REPLACE
        .et
        .h
        F_H
        ${in_f}
    )

    get_filename_component(ET_PATH ${in_f} DIRECTORY)

    add_custom_command(OUTPUT ${F_C} ${F_H}
      COMMAND perl "${KRB5_SOURCE_DIR}/util/et/compile_et" -d "${KRB5_SOURCE_DIR}/util/et" ${in_f}
      DEPENDS ${in_f} "${KRB5_SOURCE_DIR}/util/et/compile_et"
      WORKING_DIRECTORY ${ET_PATH}
      VERBATIM
      )
    list(APPEND result ${F_C})
  endforeach()
  set(${out_var} "${result}" PARENT_SCOPE)
endfunction()

add_custom_command(
    OUTPUT "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/error_map.h"
    COMMAND perl
    -I../../../util
    ../../../util/gen-map.pl
    -oerror_map.h
    NAME=gsserrmap
    KEY=OM_uint32
    VALUE=char*
    COMPARE=compare_OM_uint32
    FREEVALUE=free_string
    WORKING_DIRECTORY "${KRB5_SOURCE_DIR}/lib/gssapi/krb5"
)


add_custom_target(
    ERROR_MAP_H
    DEPENDS "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/error_map.h"
    VERBATIM
)

add_custom_command(
    OUTPUT "${KRB5_SOURCE_DIR}/lib/gssapi/generic/errmap.h"
    COMMAND perl -w -I../../../util  ../../../util/gen.pl bimap errmap.h NAME=mecherrmap LEFT=OM_uint32 RIGHT=struct\ mecherror LEFTPRINT=print_OM_uint32 RIGHTPRINT=mecherror_print LEFTCMP=cmp_OM_uint32 RIGHTCMP=mecherror_cmp
    WORKING_DIRECTORY "${KRB5_SOURCE_DIR}/lib/gssapi/generic"
)

add_custom_target(
    ERRMAP_H
    DEPENDS "${KRB5_SOURCE_DIR}/lib/gssapi/generic/errmap.h"
    VERBATIM
)

add_custom_target(
    KRB_5_H
    DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/include/krb5/krb5.h"
    VERBATIM
)

add_library(_krb5)

add_dependencies(
    _krb5
    ERRMAP_H
    ERROR_MAP_H
    KRB_5_H
)

preprocess_et(processed_et_files ${ET_FILES})

if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
    add_custom_command(
        OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/include_private/kcmrpc.h" "${CMAKE_CURRENT_BINARY_DIR}/include_private/kcmrpc.c"
        COMMAND mig -header kcmrpc.h -user kcmrpc.c -sheader /dev/null -server /dev/null -I"${KRB5_SOURCE_DIR}/lib/krb5/ccache" "${KRB5_SOURCE_DIR}/lib/krb5/ccache/kcmrpc.defs"
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/include_private"
    )

    list(APPEND ALL_SRCS "${CMAKE_CURRENT_BINARY_DIR}/include_private/kcmrpc.c")
endif()

target_sources(_krb5 PRIVATE
    ${ALL_SRCS}
)

file(MAKE_DIRECTORY
    "${CMAKE_CURRENT_BINARY_DIR}/include/gssapi"
)

file(GLOB GSSAPI_GENERIC_HEADERS
    "${KRB5_SOURCE_DIR}/lib/gssapi/generic/*.h"
    "${KRB5_SOURCE_DIR}/lib/gssapi/generic/gssapi.hin"
)

file(COPY ${GSSAPI_GENERIC_HEADERS}
    DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/include/gssapi/"
)

file(RENAME
    "${CMAKE_CURRENT_BINARY_DIR}/include/gssapi/gssapi.hin"
    "${CMAKE_CURRENT_BINARY_DIR}/include/gssapi/gssapi.h"
)

file(COPY "${KRB5_SOURCE_DIR}/lib/gssapi/krb5/gssapi_krb5.h"
    DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/include/gssapi/"
)

file(COPY "${KRB5_SOURCE_DIR}/util/et/com_err.h"
    DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/include/"
)

file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/osconf.h"
    DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/include_private/"
)

file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/profile.h"
    DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/include_private/"
)

string(TOLOWER "${CMAKE_SYSTEM_NAME}" _system_name)

file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/autoconf_${_system_name}.h"
    DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/include_private/"
)

file(RENAME
    "${CMAKE_CURRENT_BINARY_DIR}/include_private/autoconf_${_system_name}.h"
    "${CMAKE_CURRENT_BINARY_DIR}/include_private/autoconf.h"
)

file(MAKE_DIRECTORY
    "${CMAKE_CURRENT_BINARY_DIR}/include/krb5"
)

SET(KRBHDEP
    "${KRB5_SOURCE_DIR}/include/krb5/krb5.hin"
    "${KRB5_SOURCE_DIR}/lib/krb5/error_tables/krb5_err.h"
    "${KRB5_SOURCE_DIR}/lib/krb5/error_tables/k5e1_err.h"
    "${KRB5_SOURCE_DIR}/lib/krb5/error_tables/kdb5_err.h"
    "${KRB5_SOURCE_DIR}/lib/krb5/error_tables/kv5m_err.h"
    "${KRB5_SOURCE_DIR}/lib/krb5/error_tables/krb524_err.h"
    "${KRB5_SOURCE_DIR}/lib/krb5/error_tables/asn1_err.h"
)

# cmake < 3.18 does not have 'cat' command
add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/include/krb5/krb5.h"
    COMMAND cat ${KRBHDEP} > "${CMAKE_CURRENT_BINARY_DIR}/include/krb5/krb5.h"
    DEPENDS ${KRBHDEP}
)



target_include_directories(_krb5 SYSTEM BEFORE PUBLIC
    "${KRB5_SOURCE_DIR}/include"
    "${CMAKE_CURRENT_BINARY_DIR}/include"
)

target_include_directories(_krb5 PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}/include_private" # For autoconf.h and other generated headers.
    ${KRB5_SOURCE_DIR}
    "${KRB5_SOURCE_DIR}/include"
    "${KRB5_SOURCE_DIR}/lib/gssapi/mechglue"
    "${KRB5_SOURCE_DIR}/lib/"
    "${KRB5_SOURCE_DIR}/lib/gssapi"
    "${KRB5_SOURCE_DIR}/lib/gssapi/generic"
    "${KRB5_SOURCE_DIR}/lib/gssapi/krb5"
    "${KRB5_SOURCE_DIR}/lib/gssapi/spnego"
    "${KRB5_SOURCE_DIR}/util/et"
    "${KRB5_SOURCE_DIR}/lib/crypto/openssl"
    "${KRB5_SOURCE_DIR}/lib/crypto/krb"
    "${KRB5_SOURCE_DIR}/util/profile"
    "${KRB5_SOURCE_DIR}/lib/krb5/ccache/ccapi"
    "${KRB5_SOURCE_DIR}/lib/krb5/ccache"
    "${KRB5_SOURCE_DIR}/lib/krb5/keytab"
    "${KRB5_SOURCE_DIR}/lib/krb5/rcache"
    "${KRB5_SOURCE_DIR}/lib/krb5/unicode"
    "${KRB5_SOURCE_DIR}/lib/krb5/os"
)

target_compile_definitions(_krb5 PRIVATE
    KRB5_PRIVATE
    _GSS_STATIC_LINK=1
    KRB5_DEPRECATED=1
    LOCALEDIR="/usr/local/share/locale"
    BINDIR="/usr/local/bin"
    SBINDIR="/usr/local/sbin"
    LIBDIR="/usr/local/lib"
)

target_link_libraries(_krb5 PRIVATE OpenSSL::Crypto OpenSSL::SSL)

add_library(ch_contrib::krb5 ALIAS _krb5)
