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

# File copied from contrib/google-cloud-cpp/google/cloud/google_cloud_cpp_common.cmake with minor modifications.

set(GOOGLE_CLOUD_CPP_COMMON_DIR "${GOOGLE_CLOUD_CPP_DIR}/google/cloud")

# Generate the version information from the CMake values.
# configure_file(${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/version_info.h.in
#                ${CMAKE_CURRENT_SOURCE_DIR}/internal/version_info.h)

# Create the file that captures build information. Having access to the compiler
# and build flags at runtime allows us to print better benchmark results.
string(TOUPPER "${CMAKE_BUILD_TYPE}" GOOGLE_CLOUD_CPP_BUILD_TYPE_UPPER)
configure_file(${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/build_info.cc.in internal/build_info.cc)

# the client library
add_library(
    google_cloud_cpp_common # cmake-format: sort
    ${CMAKE_CURRENT_BINARY_DIR}/internal/build_info.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/access_token.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/access_token.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/backoff_policy.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/common_options.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/credentials.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/credentials.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/experimental_tag.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/future.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/future_generic.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/future_void.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/idempotency.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/absl_str_cat_quiet.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/absl_str_join_quiet.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/absl_str_replace_quiet.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/algorithm.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/api_client_header.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/api_client_header.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/attributes.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/auth_header_error.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/auth_header_error.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/backoff_policy.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/backoff_policy.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/base64_transforms.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/base64_transforms.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/big_endian.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/build_info.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/call_context.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/clock.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/compiler_info.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/compiler_info.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/compute_engine_util.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/compute_engine_util.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/credentials_impl.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/credentials_impl.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/debug_future_status.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/debug_future_status.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/debug_string.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/debug_string.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/detect_gcp.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/detect_gcp_impl.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/detect_gcp_impl.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/diagnostics_pop.inc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/diagnostics_push.inc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/disable_deprecation_warnings.inc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/disable_msvc_crt_secure_warnings.inc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/error_context.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/error_context.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/filesystem.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/filesystem.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/format_time_point.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/format_time_point.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/future_base.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/future_coroutines.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/future_fwd.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/future_impl.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/future_impl.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/future_then_impl.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/future_then_impl.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/getenv.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/getenv.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/group_options.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/invocation_id_generator.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/invocation_id_generator.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/invoke_result.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/ios_flags_saver.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/log_impl.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/log_impl.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/make_status.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/make_status.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/noexcept_action.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/noexcept_action.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/non_constructible.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/opentelemetry.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/opentelemetry.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/opentelemetry_context.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/opentelemetry_context.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/pagination_range.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/parse_rfc3339.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/parse_rfc3339.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/populate_common_options.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/populate_common_options.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/port_platform.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/random.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/random.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/retry_info.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/retry_loop_helpers.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/retry_loop_helpers.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/retry_policy_impl.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/retry_policy_impl.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/service_endpoint.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/service_endpoint.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/sha256_hash.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/sha256_hash.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/sha256_hmac.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/sha256_hmac.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/sha256_type.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/status_payload_keys.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/status_payload_keys.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/status_utils.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/status_utils.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/strerror.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/strerror.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/subject_token.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/subject_token.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/throw_delegate.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/throw_delegate.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/timer_queue.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/timer_queue.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/trace_propagator.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/trace_propagator.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/traced_stream_range.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/tuple.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/type_list.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/type_traits.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/url_encode.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/url_encode.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/user_agent_prefix.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/user_agent_prefix.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/utility.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/version_info.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/kms_key_name.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/kms_key_name.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/location.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/location.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/log.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/log.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/no_await_tag.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/opentelemetry_options.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/optional.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/options.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/options.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/polling_policy.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/project.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/project.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/retry_policy.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/rpc_metadata.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/status.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/status.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/status_or.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/stream_range.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/terminate_handler.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/terminate_handler.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/tracing_options.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/tracing_options.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/universe_domain_options.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/version.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/version.h)
target_link_libraries(
    google_cloud_cpp_common
    PUBLIC absl::base
           absl::memory
           absl::optional
           absl::span
           absl::str_format
           absl::time
           absl::variant
           Threads::Threads)
target_link_libraries(google_cloud_cpp_common PUBLIC OpenSSL::Crypto ch_contrib::re2)

target_include_directories(
    google_cloud_cpp_common PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
                                   $<INSTALL_INTERFACE:include>)

# We're putting generated code into ${PROJECT_BINARY_DIR} (e.g. compiled
# protobufs or build info), so we need it on the include path, however we don't
# want it checked by linters so we mark it as SYSTEM.
target_include_directories(google_cloud_cpp_common SYSTEM
                           PUBLIC $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}>)
target_compile_options(google_cloud_cpp_common
                       PUBLIC ${GOOGLE_CLOUD_CPP_EXCEPTIONS_FLAG})

set_target_properties(
    google_cloud_cpp_common
    PROPERTIES EXPORT_NAME "google-cloud-cpp::common"
               VERSION ${PROJECT_VERSION}
               SOVERSION ${PROJECT_VERSION_MAJOR})
add_library(google-cloud-cpp::common ALIAS google_cloud_cpp_common)
