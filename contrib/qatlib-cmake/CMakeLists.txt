# Intel® QuickAssist Technology Library (QATlib).

message(STATUS "Intel QATlib ON")
set(LIBQAT_ROOT_DIR "${ClickHouse_SOURCE_DIR}/contrib/qatlib")
set(LIBQAT_DIR "${LIBQAT_ROOT_DIR}/quickassist/lookaside/access_layer/src")
set(LIBOSAL_DIR "${LIBQAT_ROOT_DIR}/quickassist/utilities/osal/src")
set(OPENSSL_DIR "${ClickHouse_SOURCE_DIR}/contrib/openssl")

# Build 3 libraries: _qatmgr, _osal, _qatlib
# Produce ch_contrib::qatlib by linking these libraries.

# _qatmgr

SET(LIBQATMGR_sources ${LIBQAT_DIR}/qat_direct/vfio/qat_mgr_client.c
    ${LIBQAT_DIR}/qat_direct/vfio/qat_mgr_lib.c
    ${LIBQAT_DIR}/qat_direct/vfio/qat_log.c
    ${LIBQAT_DIR}/qat_direct/vfio/vfio_lib.c
    ${LIBQAT_DIR}/qat_direct/vfio/adf_pfvf_proto.c
    ${LIBQAT_DIR}/qat_direct/vfio/adf_pfvf_vf_msg.c
    ${LIBQAT_DIR}/qat_direct/vfio/adf_vfio_pf.c)

add_library(_qatmgr ${LIBQATMGR_sources})

target_include_directories(_qatmgr PRIVATE
    ${LIBQAT_ROOT_DIR}/quickassist/lookaside/access_layer/src/qat_direct/vfio
    ${LIBQAT_ROOT_DIR}/quickassist/lookaside/access_layer/include
    ${LIBQAT_ROOT_DIR}/quickassist/include
    ${LIBQAT_ROOT_DIR}/quickassist/utilities/osal/include
    ${LIBQAT_ROOT_DIR}/quickassist/utilities/osal/src/linux/user_space/include
    ${LIBQAT_ROOT_DIR}/quickassist/qat/drivers/crypto/qat/qat_common
    ${LIBQAT_ROOT_DIR}/quickassist/lookaside/access_layer/src/qat_direct/include
    ${LIBQAT_ROOT_DIR}/quickassist/lookaside/access_layer/src/qat_direct/common/include
    ${ClickHouse_SOURCE_DIR}/contrib/sysroot/linux-x86_64-musl/include)

target_compile_definitions(_qatmgr PRIVATE -DUSER_SPACE)
target_compile_options(_qatmgr PRIVATE -Wno-error=int-conversion)

# _osal

SET(LIBOSAL_sources
    ${LIBOSAL_DIR}/linux/user_space/OsalSemaphore.c
    ${LIBOSAL_DIR}/linux/user_space/OsalThread.c
    ${LIBOSAL_DIR}/linux/user_space/OsalMutex.c
    ${LIBOSAL_DIR}/linux/user_space/OsalSpinLock.c
    ${LIBOSAL_DIR}/linux/user_space/OsalAtomic.c
    ${LIBOSAL_DIR}/linux/user_space/OsalServices.c
    ${LIBOSAL_DIR}/linux/user_space/OsalUsrKrnProxy.c
    ${LIBOSAL_DIR}/linux/user_space/OsalCryptoInterface.c)

add_library(_osal ${LIBOSAL_sources})

target_include_directories(_osal PRIVATE
    ${LIBQAT_ROOT_DIR}/quickassist/utilities/osal/src/linux/user_space
    ${LIBQAT_ROOT_DIR}/quickassist/utilities/osal/include
    ${LIBQAT_ROOT_DIR}/quickassist/utilities/osal/src/linux/user_space/include
    ${OPENSSL_DIR}/include
    ${ClickHouse_SOURCE_DIR}/contrib/openssl-cmake/linux_x86_64/include
    ${ClickHouse_SOURCE_DIR}/contrib/qatlib-cmake/include)

target_compile_definitions(_osal PRIVATE -DOSAL_ENSURE_ON -DUSE_OPENSSL)

# _qatlib
SET(LIBQAT_sources
    ${LIBQAT_DIR}/common/compression/dc_buffers.c
    ${LIBQAT_DIR}/common/compression/dc_chain.c
    ${LIBQAT_DIR}/common/compression/dc_datapath.c
    ${LIBQAT_DIR}/common/compression/dc_dp.c
    ${LIBQAT_DIR}/common/compression/dc_header_footer.c
    ${LIBQAT_DIR}/common/compression/dc_header_footer_lz4.c
    ${LIBQAT_DIR}/common/compression/dc_session.c
    ${LIBQAT_DIR}/common/compression/dc_stats.c
    ${LIBQAT_DIR}/common/compression/dc_err_sim.c
    ${LIBQAT_DIR}/common/compression/dc_ns_datapath.c
    ${LIBQAT_DIR}/common/compression/dc_ns_header_footer.c
    ${LIBQAT_DIR}/common/compression/dc_crc32.c
    ${LIBQAT_DIR}/common/compression/dc_crc64.c
    ${LIBQAT_DIR}/common/compression/dc_xxhash32.c
    ${LIBQAT_DIR}/common/compression/icp_sal_dc_err_sim.c
    ${LIBQAT_DIR}/common/crypto/asym/diffie_hellman/lac_dh_control_path.c
    ${LIBQAT_DIR}/common/crypto/asym/diffie_hellman/lac_dh_data_path.c
    ${LIBQAT_DIR}/common/crypto/asym/diffie_hellman/lac_dh_interface_check.c
    ${LIBQAT_DIR}/common/crypto/asym/diffie_hellman/lac_dh_stats.c
    ${LIBQAT_DIR}/common/crypto/asym/dsa/lac_dsa.c
    ${LIBQAT_DIR}/common/crypto/asym/dsa/lac_dsa_interface_check.c
    ${LIBQAT_DIR}/common/crypto/asym/ecc/lac_ec.c
    ${LIBQAT_DIR}/common/crypto/asym/ecc/lac_ec_common.c
    ${LIBQAT_DIR}/common/crypto/asym/ecc/lac_ec_montedwds.c
    ${LIBQAT_DIR}/common/crypto/asym/ecc/lac_ec_nist_curves.c
    ${LIBQAT_DIR}/common/crypto/asym/ecc/lac_ecdh.c
    ${LIBQAT_DIR}/common/crypto/asym/ecc/lac_ecdsa.c
    ${LIBQAT_DIR}/common/crypto/asym/ecc/lac_ecsm2.c
    ${LIBQAT_DIR}/common/crypto/asym/ecc/lac_kpt_ecdsa.c
    ${LIBQAT_DIR}/common/crypto/asym/large_number/lac_ln.c
    ${LIBQAT_DIR}/common/crypto/asym/large_number/lac_ln_interface_check.c
    ${LIBQAT_DIR}/common/crypto/asym/pke_common/lac_pke_mmp.c
    ${LIBQAT_DIR}/common/crypto/asym/pke_common/lac_pke_qat_comms.c
    ${LIBQAT_DIR}/common/crypto/asym/pke_common/lac_pke_utils.c
    ${LIBQAT_DIR}/common/crypto/asym/prime/lac_prime.c
    ${LIBQAT_DIR}/common/crypto/asym/prime/lac_prime_interface_check.c
    ${LIBQAT_DIR}/common/crypto/asym/rsa/lac_rsa.c
    ${LIBQAT_DIR}/common/crypto/asym/rsa/lac_rsa_control_path.c
    ${LIBQAT_DIR}/common/crypto/asym/rsa/lac_rsa_decrypt.c
    ${LIBQAT_DIR}/common/crypto/asym/rsa/lac_rsa_encrypt.c
    ${LIBQAT_DIR}/common/crypto/asym/rsa/lac_rsa_interface_check.c
    ${LIBQAT_DIR}/common/crypto/asym/rsa/lac_rsa_keygen.c
    ${LIBQAT_DIR}/common/crypto/asym/rsa/lac_rsa_stats.c
    ${LIBQAT_DIR}/common/crypto/asym/rsa/lac_kpt_rsa_decrypt.c
    ${LIBQAT_DIR}/common/crypto/sym/drbg/lac_sym_drbg_api.c
    ${LIBQAT_DIR}/common/crypto/sym/key/lac_sym_key.c
    ${LIBQAT_DIR}/common/crypto/sym/lac_sym_alg_chain.c
    ${LIBQAT_DIR}/common/crypto/sym/lac_sym_api.c
    ${LIBQAT_DIR}/common/crypto/sym/lac_sym_auth_enc.c
    ${LIBQAT_DIR}/common/crypto/sym/lac_sym_cb.c
    ${LIBQAT_DIR}/common/crypto/sym/lac_sym_cipher.c
    ${LIBQAT_DIR}/common/crypto/sym/lac_sym_compile_check.c
    ${LIBQAT_DIR}/common/crypto/sym/lac_sym_dp.c
    ${LIBQAT_DIR}/common/crypto/sym/lac_sym_hash.c
    ${LIBQAT_DIR}/common/crypto/sym/lac_sym_partial.c
    ${LIBQAT_DIR}/common/crypto/sym/lac_sym_queue.c
    ${LIBQAT_DIR}/common/crypto/sym/lac_sym_stats.c
    ${LIBQAT_DIR}/common/crypto/sym/nrbg/lac_sym_nrbg_api.c
    ${LIBQAT_DIR}/common/crypto/sym/qat/lac_sym_qat.c
    ${LIBQAT_DIR}/common/crypto/sym/qat/lac_sym_qat_cipher.c
    ${LIBQAT_DIR}/common/crypto/sym/qat/lac_sym_qat_constants_table.c
    ${LIBQAT_DIR}/common/crypto/sym/qat/lac_sym_qat_hash.c
    ${LIBQAT_DIR}/common/crypto/sym/qat/lac_sym_qat_hash_defs_lookup.c
    ${LIBQAT_DIR}/common/crypto/sym/qat/lac_sym_qat_key.c
    ${LIBQAT_DIR}/common/crypto/sym/lac_sym_hash_sw_precomputes.c
    ${LIBQAT_DIR}/common/crypto/kpt/provision/lac_kpt_provision.c
    ${LIBQAT_DIR}/common/ctrl/sal_compression.c
    ${LIBQAT_DIR}/common/ctrl/sal_create_services.c
    ${LIBQAT_DIR}/common/ctrl/sal_ctrl_services.c
    ${LIBQAT_DIR}/common/ctrl/sal_list.c
    ${LIBQAT_DIR}/common/ctrl/sal_crypto.c
    ${LIBQAT_DIR}/common/ctrl/sal_dc_chain.c
    ${LIBQAT_DIR}/common/ctrl/sal_instances.c
    ${LIBQAT_DIR}/common/qat_comms/sal_qat_cmn_msg.c
    ${LIBQAT_DIR}/common/utils/lac_buffer_desc.c
    ${LIBQAT_DIR}/common/utils/lac_log_message.c
    ${LIBQAT_DIR}/common/utils/lac_mem.c
    ${LIBQAT_DIR}/common/utils/lac_mem_pools.c
    ${LIBQAT_DIR}/common/utils/lac_sw_responses.c
    ${LIBQAT_DIR}/common/utils/lac_sync.c
    ${LIBQAT_DIR}/common/utils/sal_service_state.c
    ${LIBQAT_DIR}/common/utils/sal_statistics.c
    ${LIBQAT_DIR}/common/utils/sal_misc_error_stats.c
    ${LIBQAT_DIR}/common/utils/sal_string_parse.c
    ${LIBQAT_DIR}/common/utils/sal_user_process.c
    ${LIBQAT_DIR}/common/utils/sal_versions.c
    ${LIBQAT_DIR}/common/device/sal_dev_info.c
    ${LIBQAT_DIR}/user/sal_user.c
    ${LIBQAT_DIR}/user/sal_user_dyn_instance.c
    ${LIBQAT_DIR}/qat_direct/common/adf_process_proxy.c
    ${LIBQAT_DIR}/qat_direct/common/adf_user_cfg.c
    ${LIBQAT_DIR}/qat_direct/common/adf_user_device.c
    ${LIBQAT_DIR}/qat_direct/common/adf_user_dyn.c
    ${LIBQAT_DIR}/qat_direct/common/adf_user_ETring_mgr_dp.c
    ${LIBQAT_DIR}/qat_direct/common/adf_user_init.c
    ${LIBQAT_DIR}/qat_direct/common/adf_user_ring.c
    ${LIBQAT_DIR}/qat_direct/common/adf_user_transport_ctrl.c
    ${LIBQAT_DIR}/qat_direct/vfio/adf_vfio_cfg.c
    ${LIBQAT_DIR}/qat_direct/vfio/adf_vfio_ring.c
    ${LIBQAT_DIR}/qat_direct/vfio/adf_vfio_user_bundles.c
    ${LIBQAT_DIR}/qat_direct/vfio/adf_vfio_user_proxy.c
    ${LIBQAT_DIR}/common/compression/dc_crc_base.c)

add_library(_qatlib ${LIBQAT_sources})

target_include_directories(_qatlib PRIVATE
    ${CMAKE_SYSROOT}/usr/include
    ${LIBQAT_ROOT_DIR}/quickassist/lookaside/access_layer/include
    ${LIBQAT_ROOT_DIR}/quickassist/utilities/libusdm_drv
    ${LIBQAT_ROOT_DIR}/quickassist/utilities/osal/include
    ${LIBOSAL_DIR}/linux/user_space/include
    ${LIBQAT_ROOT_DIR}/quickassist/include
    ${LIBQAT_ROOT_DIR}/quickassist/include/lac
    ${LIBQAT_ROOT_DIR}/quickassist/include/dc
    ${LIBQAT_ROOT_DIR}/quickassist/qat/drivers/crypto/qat/qat_common
    ${LIBQAT_ROOT_DIR}/quickassist/lookaside/access_layer/src/common/compression/include
    ${LIBQAT_ROOT_DIR}/quickassist/lookaside/access_layer/src/common/crypto/sym/include
    ${LIBQAT_ROOT_DIR}/quickassist/lookaside/access_layer/src/common/crypto/asym/include
    ${LIBQAT_ROOT_DIR}/quickassist/lookaside/firmware/include
    ${LIBQAT_ROOT_DIR}/quickassist/lookaside/access_layer/src/common/include
    ${LIBQAT_ROOT_DIR}/quickassist/lookaside/access_layer/src/qat_direct/include
    ${LIBQAT_ROOT_DIR}/quickassist/lookaside/access_layer/src/qat_direct/common/include
    ${LIBQAT_ROOT_DIR}/quickassist/lookaside/access_layer/src/qat_direct/vfio
    ${LIBQAT_ROOT_DIR}/quickassist/utilities/osal/src/linux/user_space
    ${LIBQAT_ROOT_DIR}/quickassist/utilities/osal/src/linux/user_space/include
    ${ClickHouse_SOURCE_DIR}/contrib/sysroot/linux-x86_64-musl/include)

target_link_libraries(_qatlib PRIVATE _qatmgr _osal OpenSSL::SSL ch_contrib::isal)
target_compile_definitions(_qatlib PRIVATE -DUSER_SPACE -DLAC_BYTE_ORDER=__LITTLE_ENDIAN -DOSAL_ENSURE_ON)
target_link_options(_qatlib PRIVATE -pie -z relro -z now -z noexecstack)
target_compile_options(_qatlib PRIVATE -march=native)
add_library (ch_contrib::qatlib ALIAS _qatlib)

# _usdm

set(LIBUSDM_DIR "${ClickHouse_SOURCE_DIR}/contrib/qatlib/quickassist/utilities/libusdm_drv")
set(LIBUSDM_sources
        ${LIBUSDM_DIR}/user_space/vfio/qae_mem_utils_vfio.c
        ${LIBUSDM_DIR}/user_space/qae_mem_utils_common.c
        ${LIBUSDM_DIR}/user_space/vfio/qae_mem_hugepage_utils_vfio.c)

add_library(_usdm ${LIBUSDM_sources})

target_include_directories(_usdm PRIVATE
    ${ClickHouse_SOURCE_DIR}/contrib/sysroot/linux-x86_64-musl/include
    ${LIBUSDM_DIR}
    ${LIBUSDM_DIR}/include
    ${LIBUSDM_DIR}/user_space)

add_library (ch_contrib::usdm ALIAS _usdm)
