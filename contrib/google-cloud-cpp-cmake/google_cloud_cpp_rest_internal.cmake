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

# File copied from google-cloud-cpp/google/cloud/google_cloud_cpp_grpc_utils.cmake with minor modifications.

set(GOOGLE_CLOUD_CPP_COMMON_DIR "${GOOGLE_CLOUD_CPP_DIR}/google/cloud")

# the library
add_library(
    google_cloud_cpp_rest_internal # cmake-format: sort
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/binary_data_as_debug_string.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/binary_data_as_debug_string.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/curl_handle.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/curl_handle.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/curl_handle_factory.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/curl_handle_factory.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/curl_http_payload.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/curl_http_payload.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/curl_impl.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/curl_impl.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/curl_options.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/curl_rest_client.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/curl_rest_client.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/curl_rest_response.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/curl_rest_response.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/curl_wrappers.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/curl_wrappers.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/curl_writev.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/curl_writev.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/external_account_source_format.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/external_account_source_format.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/external_account_token_source_aws.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/external_account_token_source_aws.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/external_account_token_source_file.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/external_account_token_source_file.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/external_account_token_source_url.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/external_account_token_source_url.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/http_payload.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/json_parsing.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/json_parsing.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/make_jwt_assertion.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/make_jwt_assertion.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_access_token_credentials.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_access_token_credentials.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_anonymous_credentials.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_anonymous_credentials.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_api_key_credentials.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_api_key_credentials.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_authorized_user_credentials.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_authorized_user_credentials.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_cached_credentials.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_cached_credentials.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_compute_engine_credentials.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_compute_engine_credentials.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_credential_constants.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_credentials.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_credentials.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_decorate_credentials.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_decorate_credentials.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_error_credentials.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_error_credentials.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_external_account_credentials.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_external_account_credentials.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_external_account_token_source.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_google_application_default_credentials_file.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_google_application_default_credentials_file.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_google_credentials.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_google_credentials.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_http_client_factory.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_impersonate_service_account_credentials.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_impersonate_service_account_credentials.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_logging_credentials.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_logging_credentials.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_minimal_iam_credentials_rest.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_minimal_iam_credentials_rest.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_refreshing_credentials_wrapper.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_refreshing_credentials_wrapper.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_service_account_credentials.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_service_account_credentials.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_universe_domain.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/oauth2_universe_domain.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/openssl/parse_service_account_p12_file.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/openssl/sign_using_sha256.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/parse_service_account_p12_file.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/populate_rest_options.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/populate_rest_options.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_carrier.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_carrier.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_client.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_context.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_context.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_lro_helpers.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_lro_helpers.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_opentelemetry.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_opentelemetry.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_options.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_parse_json_error.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_parse_json_error.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_request.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_request.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_response.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_response.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_retry_loop.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_set_metadata.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/rest_set_metadata.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/sign_using_sha256.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/tracing_http_payload.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/tracing_http_payload.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/tracing_rest_client.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/tracing_rest_client.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/tracing_rest_response.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/tracing_rest_response.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/unified_rest_credentials.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/unified_rest_credentials.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/win32/parse_service_account_p12_file.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/win32/sign_using_sha256.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/win32/win32_helpers.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/win32/win32_helpers.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/rest_options.h)
target_link_libraries(
    google_cloud_cpp_rest_internal
    PUBLIC absl::span google-cloud-cpp::common CURL::libcurl
           nlohmann_json::nlohmann_json)
if (WIN32)
    target_compile_definitions(google_cloud_cpp_rest_internal
                               PRIVATE WIN32_LEAN_AND_MEAN)
    # We use `setsockopt()` directly, which requires the ws2_32 (Winsock2 for
    # Windows32?) library on Windows.
    target_link_libraries(google_cloud_cpp_rest_internal PUBLIC ws2_32 bcrypt
                                                                crypt32)
else ()
    target_link_libraries(google_cloud_cpp_rest_internal PUBLIC OpenSSL::SSL
                                                                OpenSSL::Crypto)
endif ()
google_cloud_cpp_add_common_options(google_cloud_cpp_rest_internal)
target_include_directories(
    google_cloud_cpp_rest_internal
    PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
           $<INSTALL_INTERFACE:include>)
target_compile_options(google_cloud_cpp_rest_internal
                       PUBLIC ${GOOGLE_CLOUD_CPP_EXCEPTIONS_FLAG})
set_target_properties(
    google_cloud_cpp_rest_internal
    PROPERTIES EXPORT_NAME "google-cloud-cpp::rest_internal"
               VERSION ${PROJECT_VERSION}
               SOVERSION ${PROJECT_VERSION_MAJOR})
add_library(google-cloud-cpp::rest_internal ALIAS
            google_cloud_cpp_rest_internal)

# # Export the CMake targets to make it easy to create configuration files.
# install(
#     EXPORT google_cloud_cpp_rest_internal-targets
#     DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/google_cloud_cpp_rest_internal"
#     COMPONENT google_cloud_cpp_development)

# # Install the libraries and headers in the locations determined by
# # GNUInstallDirs
# install(
#     TARGETS google_cloud_cpp_rest_internal
#     EXPORT google_cloud_cpp_rest_internal-targets
#     RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
#             COMPONENT google_cloud_cpp_runtime
#     LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
#             COMPONENT google_cloud_cpp_runtime
#             NAMELINK_COMPONENT google_cloud_cpp_development
#     ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
#             COMPONENT google_cloud_cpp_development)

# google_cloud_cpp_install_headers(google_cloud_cpp_rest_internal
#                                  include/google/cloud)

# google_cloud_cpp_add_pkgconfig(
#     rest_internal
#     "REST library for the Google Cloud C++ Client Library"
#     "Provides REST Transport for the Google Cloud C++ Client Library."
#     "google_cloud_cpp_common"
#     "libcurl"
#     NON_WIN32_REQUIRES
#     openssl
#     WIN32_LIBS
#     ws2_32
#     bcrypt
#     crypt32)

# # Create and install the CMake configuration files.
# include(CMakePackageConfigHelpers)
# configure_file("config-rest.cmake.in"
#                "google_cloud_cpp_rest_internal-config.cmake" @ONLY)
# write_basic_package_version_file(
#     "google_cloud_cpp_rest_internal-config-version.cmake"
#     VERSION ${PROJECT_VERSION}
#     COMPATIBILITY ExactVersion)

# install(
#     FILES
#         "${CMAKE_CURRENT_BINARY_DIR}/google_cloud_cpp_rest_internal-config.cmake"
#         "${CMAKE_CURRENT_BINARY_DIR}/google_cloud_cpp_rest_internal-config-version.cmake"
#     DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/google_cloud_cpp_rest_internal"
#     COMPONENT google_cloud_cpp_development)

# create_bazel_config(google_cloud_cpp_rest_internal YEAR 2021)

# # Define the unit tests in a function so we have a new scope for variable names.
# function (google_cloud_cpp_rest_internal_add_test fname labels)
#     google_cloud_cpp_add_executable(target "common" "${fname}")
#     target_link_libraries(
#         ${target}
#         PRIVATE google-cloud-cpp::rest_internal
#                 google_cloud_cpp_testing
#                 google_cloud_cpp_testing_rest
#                 google-cloud-cpp::common
#                 absl::span
#                 GTest::gmock_main
#                 GTest::gmock
#                 GTest::gtest
#                 CURL::libcurl)
#     google_cloud_cpp_add_common_options(${target})
#     add_test(NAME ${target} COMMAND ${target})
#     set_tests_properties(${target} PROPERTIES LABELS "${labels}")
# endfunction ()

# if (BUILD_TESTING)
#     # List the unit tests, then setup the targets and dependencies.
#     set(google_cloud_cpp_rest_internal_unit_tests
#         # cmake-format: sort
#         internal/binary_data_as_debug_string_test.cc
#         internal/curl_handle_factory_test.cc
#         internal/curl_handle_test.cc
#         internal/curl_http_payload_test.cc
#         internal/curl_impl_test.cc
#         internal/curl_rest_client_test.cc
#         internal/curl_wrappers_disable_sigpipe_handler_test.cc
#         internal/curl_wrappers_enable_sigpipe_handler_test.cc
#         internal/curl_wrappers_locking_already_present_test.cc
#         internal/curl_wrappers_locking_disabled_test.cc
#         internal/curl_wrappers_locking_enabled_test.cc
#         internal/curl_wrappers_test.cc
#         internal/curl_writev_test.cc
#         internal/external_account_source_format_test.cc
#         internal/external_account_token_source_aws_test.cc
#         internal/external_account_token_source_file_test.cc
#         internal/external_account_token_source_url_test.cc
#         internal/json_parsing_test.cc
#         internal/make_jwt_assertion_test.cc
#         internal/oauth2_access_token_credentials_test.cc
#         internal/oauth2_anonymous_credentials_test.cc
#         internal/oauth2_api_key_credentials_test.cc
#         internal/oauth2_authorized_user_credentials_test.cc
#         internal/oauth2_cached_credentials_test.cc
#         internal/oauth2_compute_engine_credentials_test.cc
#         internal/oauth2_credentials_test.cc
#         internal/oauth2_external_account_credentials_test.cc
#         internal/oauth2_google_application_default_credentials_file_test.cc
#         internal/oauth2_google_credentials_test.cc
#         internal/oauth2_impersonate_service_account_credentials_test.cc
#         internal/oauth2_logging_credentials_test.cc
#         internal/oauth2_minimal_iam_credentials_rest_test.cc
#         internal/oauth2_refreshing_credentials_wrapper_test.cc
#         internal/oauth2_service_account_credentials_test.cc
#         internal/oauth2_universe_domain_test.cc
#         internal/populate_rest_options_test.cc
#         internal/rest_carrier_test.cc
#         internal/rest_context_test.cc
#         internal/rest_lro_helpers_test.cc
#         internal/rest_opentelemetry_test.cc
#         internal/rest_parse_json_error_test.cc
#         internal/rest_request_test.cc
#         internal/rest_response_test.cc
#         internal/rest_retry_loop_test.cc
#         internal/rest_set_metadata_test.cc
#         internal/tracing_http_payload_test.cc
#         internal/tracing_rest_client_test.cc
#         internal/tracing_rest_response_test.cc
#         internal/unified_rest_credentials_test.cc)

#     # List the emulator integration tests, then setup the targets and
#     # dependencies.
#     set(google_cloud_cpp_rest_internal_emulator_integration_tests
#         # cmake-format: sort
#         internal/curl_rest_client_integration_test.cc)

#     # List the production integration tests, then setup the targets and
#     # dependencies.
#     set(google_cloud_cpp_rest_internal_production_integration_tests
#         # cmake-format: sort
#         internal/external_account_integration_test.cc
#         internal/unified_rest_credentials_integration_test.cc)

#     # Export the list of unit and integration tests so the Bazel BUILD file can
#     # pick them up.
#     export_list_to_bazel("google_cloud_cpp_rest_internal_unit_tests.bzl"
#                          "google_cloud_cpp_rest_internal_unit_tests" YEAR 2021)
#     export_list_to_bazel(
#         "google_cloud_cpp_rest_internal_emulator_integration_tests.bzl"
#         "google_cloud_cpp_rest_internal_emulator_integration_tests" YEAR 2022)
#     export_list_to_bazel(
#         "google_cloud_cpp_rest_internal_production_integration_tests.bzl"
#         "google_cloud_cpp_rest_internal_production_integration_tests" YEAR 2022)

#     foreach (fname ${google_cloud_cpp_rest_internal_unit_tests})
#         google_cloud_cpp_rest_internal_add_test("${fname}" "")
#     endforeach ()

#     foreach (fname ${google_cloud_cpp_rest_internal_emulator_integration_tests})
#         google_cloud_cpp_rest_internal_add_test(
#             "${fname}" "integration-test;integration-test-emulator")
#     endforeach ()

#     foreach (fname
#              ${google_cloud_cpp_rest_internal_production_integration_tests})
#         google_cloud_cpp_rest_internal_add_test(
#             "${fname}" "integration-test;integration-test-production")
#     endforeach ()

#     set(google_cloud_cpp_rest_internal_benchmarks
#         # cmake-format: sortable
#         internal/curl_handle_factory_benchmark.cc)

#     # Export the list of benchmarks to a .bzl file so we do not need to maintain
#     # the list in two places.
#     export_list_to_bazel(
#         "google_cloud_cpp_rest_internal_benchmarks.bzl"
#         "google_cloud_cpp_rest_internal_benchmarks" YEAR "2022")

#     # Generate a target for each benchmark.
#     foreach (fname ${google_cloud_cpp_rest_internal_benchmarks})
#         google_cloud_cpp_add_executable(target "common" "${fname}")
#         add_test(NAME ${target} COMMAND ${target})
#         target_link_libraries(
#             ${target}
#             PRIVATE google-cloud-cpp::rest_internal google-cloud-cpp::common
#                     benchmark::benchmark_main)
#         google_cloud_cpp_add_common_options(${target})
#     endforeach ()
# endif ()
