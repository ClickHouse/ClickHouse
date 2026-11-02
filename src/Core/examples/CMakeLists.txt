clickhouse_add_executable (string_pool string_pool.cpp)
target_link_libraries (string_pool PRIVATE dbms clickhouse_common_io clickhouse_common_config ch_contrib::sparsehash)

clickhouse_add_executable (field field.cpp)
target_link_libraries (field PRIVATE dbms)

clickhouse_add_executable (string_ref_hash string_ref_hash.cpp)
target_link_libraries (string_ref_hash PRIVATE clickhouse_common_io clickhouse_common_config)

if (TARGET ch_contrib::google_cloud_cpp)
    clickhouse_add_executable (gcloud_storage gcloud_storage.cpp)
    target_link_libraries (gcloud_storage PRIVATE clickhouse_common_io clickhouse_common_config dbms)
endif()

if (ENABLE_GOOGLE_CLOUD_CPP_KMS)
    clickhouse_add_executable (gcloud_kms gcloud_kms.cpp)
    target_link_libraries (gcloud_kms PRIVATE clickhouse_common_io clickhouse_common_config dbms)
endif()
