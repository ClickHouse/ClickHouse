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
    ${GOOGLE_CLOUD_CPP_STORAGE_DIR}/internal/win32/hash_function_impl.cc
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
if (WIN32)
    target_compile_definitions(google_cloud_cpp_storage
                               PRIVATE WIN32_LEAN_AND_MEAN)
    # We use `setsockopt()` directly, which requires the ws2_32 (Winsock2 for
    # Windows32?) library on Windows.
    target_link_libraries(google_cloud_cpp_storage PUBLIC ws2_32 bcrypt)
else ()
    target_link_libraries(google_cloud_cpp_storage PUBLIC OpenSSL::Crypto)
endif ()
google_cloud_cpp_add_common_options(google_cloud_cpp_storage)
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

# create_bazel_config(google_cloud_cpp_storage)

# # Export the CMake targets to make it easy to create configuration files.
# install(
#     EXPORT storage-targets
#     DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/google_cloud_cpp_storage"
#     COMPONENT google_cloud_cpp_development)

# install(
#     TARGETS google_cloud_cpp_storage
#     EXPORT storage-targets
#     RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
#             COMPONENT google_cloud_cpp_runtime
#     LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
#             COMPONENT google_cloud_cpp_runtime
#             NAMELINK_COMPONENT google_cloud_cpp_development
#     ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
#             COMPONENT google_cloud_cpp_development)

# google_cloud_cpp_install_headers(google_cloud_cpp_storage
#                                  include/google/cloud/storage)

# google_cloud_cpp_add_pkgconfig(
#     "storage"
#     "The Google Cloud Storage C++ Client Library"
#     "Provides C++ APIs to access Google Cloud Storage."
#     "google_cloud_cpp_common"
#     "google_cloud_cpp_rest_internal"
#     "libcurl"
#     "absl_cord"
#     "absl_strings"
#     "absl_str_format"
#     "absl_time"
#     "absl_variant"
#     NON_WIN32_REQUIRES
#     openssl
#     LIBS
#     crc32c
#     WIN32_LIBS
#     ws2_32
#     bcrypt)

# # Create and install the CMake configuration files.
# include(CMakePackageConfigHelpers)
# configure_file("config.cmake.in" "google_cloud_cpp_storage-config.cmake" @ONLY)
# write_basic_package_version_file(
#     "google_cloud_cpp_storage-config-version.cmake"
#     VERSION ${PROJECT_VERSION}
#     COMPATIBILITY ExactVersion)

# install(
#     FILES
#         "${CMAKE_CURRENT_BINARY_DIR}/google_cloud_cpp_storage-config.cmake"
#         "${CMAKE_CURRENT_BINARY_DIR}/google_cloud_cpp_storage-config-version.cmake"
#     DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/google_cloud_cpp_storage"
#     COMPONENT google_cloud_cpp_development)

# install(
#     FILES testing/mock_client.h
#     DESTINATION include/google/cloud/storage/testing
#     COMPONENT google_cloud_cpp_development)

# if (BUILD_TESTING)
#     add_library(
#         storage_client_testing # cmake-format: sort
#         testing/canonical_errors.h
#         testing/client_unit_test.cc
#         testing/client_unit_test.h
#         testing/constants.h
#         testing/mock_client.h
#         testing/mock_generic_stub.h
#         testing/mock_hash_function.h
#         testing/mock_hash_validator.h
#         testing/mock_http_request.cc
#         testing/mock_http_request.h
#         testing/mock_resume_policy.h
#         testing/mock_storage_stub.h
#         testing/object_integration_test.cc
#         testing/object_integration_test.h
#         testing/random_names.cc
#         testing/random_names.h
#         testing/remove_stale_buckets.cc
#         testing/remove_stale_buckets.h
#         testing/retry_http_request.cc
#         testing/retry_http_request.h
#         testing/retry_tests.cc
#         testing/retry_tests.h
#         testing/storage_integration_test.cc
#         testing/storage_integration_test.h
#         testing/temp_file.cc
#         testing/temp_file.h
#         testing/upload_hash_cases.cc
#         testing/upload_hash_cases.h
#         testing/write_base64.cc
#         testing/write_base64.h)
#     target_link_libraries(
#         storage_client_testing
#         PUBLIC absl::memory
#                google-cloud-cpp::storage
#                google_cloud_cpp_testing
#                nlohmann_json::nlohmann_json
#                GTest::gmock_main
#                GTest::gmock
#                GTest::gtest)
#     google_cloud_cpp_add_common_options(storage_client_testing)
#     target_include_directories(
#         storage_client_testing PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
#                                       $<INSTALL_INTERFACE:include>)
#     target_compile_options(storage_client_testing
#                            PUBLIC ${GOOGLE_CLOUD_CPP_EXCEPTIONS_FLAG})
#     create_bazel_config(storage_client_testing)

#     # List the unit tests, then setup the targets and dependencies.
#     set(storage_client_unit_tests
#         # cmake-format: sort
#         auto_finalize_test.cc
#         bucket_access_control_test.cc
#         bucket_cors_entry_test.cc
#         bucket_iam_configuration_test.cc
#         bucket_metadata_test.cc
#         bucket_object_retention_test.cc
#         bucket_soft_delete_policy_test.cc
#         client_bucket_acl_test.cc
#         client_bucket_test.cc
#         client_default_object_acl_test.cc
#         client_notifications_test.cc
#         client_object_acl_test.cc
#         client_object_copy_test.cc
#         client_object_test.cc
#         client_options_test.cc
#         client_service_account_test.cc
#         client_sign_policy_document_test.cc
#         client_sign_url_test.cc
#         client_test.cc
#         client_write_object_test.cc
#         compose_many_test.cc
#         delete_by_prefix_test.cc
#         hashing_options_test.cc
#         hmac_key_metadata_test.cc
#         idempotency_policy_test.cc
#         internal/access_control_common_parser_test.cc
#         internal/access_control_common_test.cc
#         internal/access_token_credentials_test.cc
#         internal/base64_test.cc
#         internal/bucket_acl_requests_test.cc
#         internal/bucket_requests_test.cc
#         internal/complex_option_test.cc
#         internal/compute_engine_util_test.cc
#         internal/connection_impl_bucket_acl_test.cc
#         internal/connection_impl_bucket_test.cc
#         internal/connection_impl_default_object_acl_test.cc
#         internal/connection_impl_notifications_test.cc
#         internal/connection_impl_object_acl_test.cc
#         internal/connection_impl_object_copy_test.cc
#         internal/connection_impl_object_test.cc
#         internal/connection_impl_service_account_test.cc
#         internal/connection_impl_sign_blob_test.cc
#         internal/connection_impl_test.cc
#         internal/const_buffer_test.cc
#         internal/crc32c_test.cc
#         internal/default_object_acl_requests_test.cc
#         internal/generate_message_boundary_test.cc
#         internal/generic_request_test.cc
#         internal/hash_function_impl_test.cc
#         internal/hash_validator_test.cc
#         internal/hash_values_test.cc
#         internal/hmac_key_requests_test.cc
#         internal/http_response_test.cc
#         internal/impersonate_service_account_credentials_test.cc
#         internal/logging_stub_test.cc
#         internal/make_jwt_assertion_test.cc
#         internal/md5hash_test.cc
#         internal/metadata_parser_test.cc
#         internal/notification_requests_test.cc
#         internal/object_acl_requests_test.cc
#         internal/object_read_streambuf_test.cc
#         internal/object_requests_test.cc
#         internal/object_write_streambuf_test.cc
#         internal/patch_builder_test.cc
#         internal/policy_document_request_test.cc
#         internal/request_project_id_test.cc
#         internal/rest/object_read_source_test.cc
#         internal/rest/request_builder_test.cc
#         internal/rest/stub_test.cc
#         internal/retry_object_read_source_test.cc
#         internal/service_account_requests_test.cc
#         internal/sign_blob_requests_test.cc
#         internal/signed_url_requests_test.cc
#         internal/tracing_connection_test.cc
#         internal/tracing_object_read_source_test.cc
#         internal/tuple_filter_test.cc
#         internal/unified_rest_credentials_test.cc
#         lifecycle_rule_test.cc
#         list_buckets_reader_test.cc
#         list_hmac_keys_reader_test.cc
#         list_objects_and_prefixes_reader_test.cc
#         list_objects_reader_test.cc
#         notification_metadata_test.cc
#         oauth2/anonymous_credentials_test.cc
#         oauth2/authorized_user_credentials_test.cc
#         oauth2/compute_engine_credentials_test.cc
#         oauth2/google_application_default_credentials_file_test.cc
#         oauth2/google_credentials_test.cc
#         oauth2/service_account_credentials_test.cc
#         object_access_control_test.cc
#         object_metadata_test.cc
#         object_retention_test.cc
#         object_stream_test.cc
#         parallel_uploads_test.cc
#         policy_document_test.cc
#         retry_policy_test.cc
#         service_account_test.cc
#         signed_url_options_test.cc
#         storage_class_test.cc
#         storage_iam_policy_test.cc
#         storage_version_test.cc
#         testing/remove_stale_buckets_test.cc
#         well_known_headers_test.cc
#         well_known_parameters_test.cc)

#     foreach (fname ${storage_client_unit_tests})
#         google_cloud_cpp_add_executable(target "storage" "${fname}")
#         target_link_libraries(
#             ${target}
#             PRIVATE absl::memory
#                     storage_client_testing
#                     google_cloud_cpp_testing
#                     google_cloud_cpp_testing_rest
#                     google-cloud-cpp::storage
#                     GTest::gmock_main
#                     GTest::gmock
#                     GTest::gtest
#                     CURL::libcurl
#                     nlohmann_json::nlohmann_json)
#         google_cloud_cpp_add_common_options(${target})
#         add_test(NAME ${target} COMMAND ${target})
#     endforeach ()
#     # Export the list of unit tests so the Bazel BUILD file can pick it up.
#     export_list_to_bazel("storage_client_unit_tests.bzl"
#                          "storage_client_unit_tests" YEAR "2018")

#     include(FindBenchmarkWithWorkarounds)

#     set(storage_client_benchmarks # cmake-format: sort
#                                   internal/crc32c_benchmark.cc)

#     # Export the list of benchmarks to a .bzl file so we do not need to maintain
#     # the list in two places.
#     export_list_to_bazel("storage_client_benchmarks.bzl"
#                          "storage_client_benchmarks" YEAR "2023")

#     # Generate a target for each benchmark.
#     foreach (fname IN LISTS storage_client_benchmarks)
#         google_cloud_cpp_add_executable(target "storage" "${fname}")
#         add_test(NAME ${target} COMMAND ${target})
#         target_link_libraries(
#             ${target} PRIVATE google-cloud-cpp::storage storage_client_testing
#                               benchmark::benchmark_main)
#         google_cloud_cpp_add_common_options(${target})
#     endforeach ()

#     add_subdirectory(tests)
#     add_subdirectory(benchmarks)
# endif ()
