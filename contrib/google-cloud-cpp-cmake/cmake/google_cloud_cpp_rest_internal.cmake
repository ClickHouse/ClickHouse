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
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/rest_options.h)
target_link_libraries(
    google_cloud_cpp_rest_internal
    PUBLIC absl::span google-cloud-cpp::common CURL::libcurl
           nlohmann_json::nlohmann_json)
target_link_libraries(google_cloud_cpp_rest_internal PUBLIC OpenSSL::SSL OpenSSL::Crypto)
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
