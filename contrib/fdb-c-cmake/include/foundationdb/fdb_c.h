#pragma once
#include <cstdint>
#include <string_view>
#include "internal/fdb_c_options.g.h"
#include "internal/fdb_c.h"

#define APPLY_FDB_C_APIS(M) \
    M(fdb_get_error) \
    M(fdb_error_predicate) \
    M(fdb_network_set_option) \
    M(fdb_setup_network) \
    M(fdb_run_network) \
    M(fdb_stop_network) \
    M(fdb_add_network_thread_completion_hook) \
    M(fdb_future_cancel) \
    M(fdb_future_release_memory) \
    M(fdb_future_destroy) \
    M(fdb_future_block_until_ready) \
    M(fdb_future_is_ready) \
    M(fdb_future_set_callback) \
    M(fdb_future_get_error) \
    M(fdb_future_get_int64) \
    M(fdb_future_get_uint64) \
    M(fdb_future_get_key) \
    M(fdb_future_get_value) \
    M(fdb_future_get_keyvalue_array) \
    M(fdb_future_get_mappedkeyvalue_array) \
    M(fdb_future_get_key_array) \
    M(fdb_future_get_string_array) \
    M(fdb_future_get_keyrange_array) \
    M(fdb_result_destroy) \
    M(fdb_result_get_keyvalue_array) \
    M(fdb_create_database) \
    M(fdb_database_destroy) \
    M(fdb_database_set_option) \
    M(fdb_database_open_tenant) \
    M(fdb_database_create_transaction) \
    M(fdb_database_reboot_worker) \
    M(fdb_database_force_recovery_with_data_loss) \
    M(fdb_database_create_snapshot) \
    M(fdb_database_get_main_thread_busyness) \
    M(fdb_database_get_server_protocol) \
    M(fdb_database_purge_blob_granules) \
    M(fdb_database_wait_purge_granules_complete) \
    M(fdb_tenant_create_transaction) \
    M(fdb_tenant_destroy) \
    M(fdb_transaction_destroy) \
    M(fdb_transaction_cancel) \
    M(fdb_transaction_set_option) \
    M(fdb_transaction_set_read_version) \
    M(fdb_transaction_get_read_version) \
    M(fdb_transaction_get) \
    M(fdb_transaction_get_key) \
    M(fdb_transaction_get_addresses_for_key) \
    M(fdb_transaction_get_range) \
    M(fdb_transaction_get_mapped_range) \
    M(fdb_transaction_set) \
    M(fdb_transaction_atomic_op) \
    M(fdb_transaction_clear) \
    M(fdb_transaction_clear_range) \
    M(fdb_transaction_watch) \
    M(fdb_transaction_commit) \
    M(fdb_transaction_get_committed_version) \
    M(fdb_transaction_get_approximate_size) \
    M(fdb_transaction_get_versionstamp) \
    M(fdb_transaction_on_error) \
    M(fdb_transaction_reset) \
    M(fdb_transaction_add_conflict_range) \
    M(fdb_transaction_get_estimated_range_size_bytes) \
    M(fdb_transaction_get_range_split_points) \
    M(fdb_transaction_get_blob_granule_ranges) \
    M(fdb_transaction_read_blob_granules) \
    M(fdb_select_api_version_impl) \
    M(fdb_get_max_api_version) \
    M(fdb_get_client_version) \
    M(fdb_delay)

#define APPLY_FDB_C_STRUCTS(M) \
    M(FDBKeyValue) \
    M(FDBKeySelector) \
    M(FDBGetRangeReqAndResult) \
    M(FDBMappedKeyValue) \
    M(FDBKeyRange) \
    M(FDBReadBlobGranuleContext)

#define FDB_C_API_TYPE(name) decltype(&foundationdb::api::name)

#define M(name) extern FDB_C_API_TYPE(name) name;
APPLY_FDB_C_APIS(M)
#undef M

#define M(name) using name = foundationdb::api::name;
APPLY_FDB_C_STRUCTS(M)
#undef M

void fdb_load_library_from_memory(const std::string_view &);

#define FDB_C_EMBED
