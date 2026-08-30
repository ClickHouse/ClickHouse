# ~~~
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ~~~

# File copied from google-cloud-cpp/google/cloud/storage/google_cloud_cpp_storage.cmake with minor modifications.

set(GOOGLE_CLOUD_CPP_STORAGE_DIR "${GOOGLE_CLOUD_CPP_DIR}/google/cloud/storage")

# the client library
add_library(
    google_cloud_cpp_storage # cmake-format: sort
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/auto_finalize.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/auto_finalize.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_access_control.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_access_control.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_autoclass.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_autoclass.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_billing.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_cors_entry.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_cors_entry.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_custom_placement_config.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_custom_placement_config.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_encryption.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_hierarchical_namespace.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_hierarchical_namespace.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_iam_configuration.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_iam_configuration.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_lifecycle.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_logging.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_logging.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_metadata.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_metadata.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_object_retention.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_object_retention.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_retention_policy.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_retention_policy.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_rpo.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_soft_delete_policy.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_soft_delete_policy.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_versioning.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/bucket_website.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/client.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/client.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/client_options.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/client_options.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/download_options.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/enable_object_retention.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/hash_mismatch_error.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/hashing_options.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/hashing_options.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/headers_map.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/hmac_key_metadata.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/hmac_key_metadata.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/iam_policy.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/iam_policy.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/idempotency_policy.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/idempotency_policy.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/include_folders_as_prefixes.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/access_control_common.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/access_control_common_parser.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/access_control_common_parser.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/access_token_credentials.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/access_token_credentials.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/base64.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/base64.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/binary_data_as_debug_string.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/bucket_access_control_parser.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/bucket_access_control_parser.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/bucket_acl_requests.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/bucket_acl_requests.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/bucket_metadata_parser.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/bucket_metadata_parser.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/bucket_requests.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/bucket_requests.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/common_metadata.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/common_metadata_parser.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/complex_option.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/compute_engine_util.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/compute_engine_util.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/connection_factory.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/connection_factory.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/connection_impl.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/connection_impl.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/const_buffer.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/const_buffer.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/crc32c.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/crc32c.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/curl/request.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/curl/request_builder.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/default_object_acl_requests.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/default_object_acl_requests.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/empty_response.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/empty_response.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/error_credentials.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/error_credentials.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/generate_message_boundary.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/generate_message_boundary.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/generic_object_request.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/generic_request.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/generic_stub.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/generic_stub_adapter.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/generic_stub_adapter.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/generic_stub_factory.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/generic_stub_factory.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/hash_function.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/hash_function.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/hash_function_impl.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/hash_function_impl.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/hash_validator.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/hash_validator.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/hash_validator_impl.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/hash_validator_impl.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/hash_values.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/hash_values.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/hmac_key_metadata_parser.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/hmac_key_metadata_parser.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/hmac_key_requests.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/hmac_key_requests.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/http_response.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/http_response.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/impersonate_service_account_credentials.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/impersonate_service_account_credentials.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/lifecycle_rule_parser.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/lifecycle_rule_parser.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/logging_stub.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/logging_stub.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/make_jwt_assertion.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/make_jwt_assertion.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/md5hash.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/md5hash.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/metadata_parser.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/metadata_parser.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/notification_metadata_parser.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/notification_metadata_parser.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/notification_requests.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/notification_requests.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/object_access_control_parser.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/object_access_control_parser.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/object_acl_requests.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/object_acl_requests.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/object_metadata_parser.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/object_metadata_parser.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/object_read_source.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/object_read_streambuf.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/object_read_streambuf.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/object_requests.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/object_requests.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/object_write_streambuf.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/object_write_streambuf.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/openssl/hash_function_impl.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/patch_builder.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/patch_builder.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/patch_builder_details.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/patch_builder_details.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/policy_document_request.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/policy_document_request.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/request_project_id.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/request_project_id.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/rest/object_read_source.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/rest/object_read_source.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/rest/request_builder.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/rest/request_builder.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/rest/stub.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/rest/stub.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/retry_object_read_source.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/retry_object_read_source.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/service_account_parser.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/service_account_parser.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/service_account_requests.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/service_account_requests.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/sign_blob_requests.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/sign_blob_requests.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/signed_url_requests.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/signed_url_requests.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/storage_connection.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/storage_connection.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/tracing_connection.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/tracing_connection.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/tracing_object_read_source.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/tracing_object_read_source.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/tuple_filter.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/unified_rest_credentials.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/unified_rest_credentials.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/well_known_parameters_impl.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/lifecycle_rule.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/lifecycle_rule.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/list_buckets_reader.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/list_buckets_reader.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/list_hmac_keys_reader.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/list_hmac_keys_reader.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/list_objects_and_prefixes_reader.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/list_objects_reader.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/list_objects_reader.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/notification_event_type.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/notification_metadata.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/notification_metadata.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/notification_payload_format.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/oauth2/anonymous_credentials.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/oauth2/anonymous_credentials.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/oauth2/authorized_user_credentials.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/oauth2/authorized_user_credentials.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/oauth2/compute_engine_credentials.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/oauth2/compute_engine_credentials.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/oauth2/credential_constants.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/oauth2/credentials.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/oauth2/credentials.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/oauth2/google_application_default_credentials_file.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/oauth2/google_application_default_credentials_file.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/oauth2/google_credentials.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/oauth2/google_credentials.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/oauth2/refreshing_credentials_wrapper.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/oauth2/refreshing_credentials_wrapper.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/oauth2/service_account_credentials.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/oauth2/service_account_credentials.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/object_access_control.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/object_access_control.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/object_metadata.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/object_metadata.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/object_read_stream.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/object_read_stream.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/object_retention.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/object_retention.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/object_rewriter.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/object_rewriter.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/object_stream.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/object_write_stream.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/object_write_stream.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/options.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/override_default_project.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/override_unlocked_retention.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/owner.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/parallel_upload.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/parallel_upload.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/policy_document.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/policy_document.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/project_team.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/retry_policy.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/service_account.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/service_account.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/signed_url_options.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/soft_deleted.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/storage_class.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/upload_options.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/user_ip_option.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/version.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/version.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/version_info.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/well_known_headers.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/well_known_headers.h
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/well_known_parameters.cc
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/well_known_parameters.h)
target_link_libraries(
    google_cloud_cpp_storage
    PUBLIC absl::cord
           absl::memory
           absl::strings
           absl::time
           absl::variant
           google-cloud-cpp::common
           google-cloud-cpp::rest_internal
           nlohmann_json::nlohmann_json
           Crc32c::crc32c
           CURL::libcurl
           Threads::Threads)
target_link_libraries(google_cloud_cpp_storage PUBLIC OpenSSL::Crypto)
target_include_directories(
    google_cloud_cpp_storage PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
                                    $<INSTALL_INTERFACE:include>)
target_compile_options(google_cloud_cpp_storage
                       PUBLIC ${GOOGLE_CLOUD_CPP_EXCEPTIONS_FLAG})

# GCC-7.x issues a warning (a member variable may be used without being
# initialized), in this file. GCC-8.0 no longer emits that diagnostic, and
# neither does Clang. On the assumption that this is a spurious warning we
# disable it for older versions of GCC.
if (${CMAKE_CXX_COMPILER_ID} STREQUAL "GNU" AND ${CMAKE_CXX_COMPILER_VERSION}
                                                VERSION_LESS 8.0)
    set_property(
        SOURCE list_objects_reader.cc
        APPEND_STRING
        PROPERTY COMPILE_FLAGS "-Wno-maybe-uninitialized")
endif ()

set_target_properties(
    google_cloud_cpp_storage
    PROPERTIES EXPORT_NAME "google-cloud-cpp::storage"
               VERSION ${PROJECT_VERSION}
               SOVERSION ${PROJECT_VERSION_MAJOR})
add_library(google-cloud-cpp::storage ALIAS google_cloud_cpp_storage)
