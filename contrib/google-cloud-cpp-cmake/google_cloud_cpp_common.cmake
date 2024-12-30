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

# File copied from google-cloud-cpp/google-cloud-cpp/google_cloud_cpp_common.cmake with minor modifications.

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
if (WIN32)
    target_compile_definitions(google_cloud_cpp_common
                               PRIVATE WIN32_LEAN_AND_MEAN)
    target_link_libraries(google_cloud_cpp_common PUBLIC bcrypt)
else ()
    target_link_libraries(google_cloud_cpp_common PUBLIC OpenSSL::Crypto ch_contrib::re2)
endif ()

google_cloud_cpp_add_common_options(google_cloud_cpp_common)
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

#create_bazel_config(google_cloud_cpp_common YEAR 2018)

# # Export the CMake targets to make it easy to create configuration files.
# install(
#     EXPORT google_cloud_cpp_common-targets
#     DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/google_cloud_cpp_common"
#     COMPONENT google_cloud_cpp_development)

# # Install the libraries and headers in the locations determined by
# # GNUInstallDirs
# install(
#     TARGETS google_cloud_cpp_common
#     EXPORT google_cloud_cpp_common-targets
#     RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
#             COMPONENT google_cloud_cpp_runtime
#     LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
#             COMPONENT google_cloud_cpp_runtime
#             NAMELINK_COMPONENT google_cloud_cpp_development
#     ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
#             COMPONENT google_cloud_cpp_development)

#google_cloud_cpp_install_headers(google_cloud_cpp_common include/google/cloud)

# google_cloud_cpp_add_pkgconfig(
#     "common"
#     "Google Cloud C++ Client Library Common Components"
#     "Common Components used by the Google Cloud C++ Client Libraries."
#     "absl_optional"
#     "absl_span"
#     "absl_strings"
#     "absl_time"
#     "absl_time_zone"
#     "absl_variant"
#     "${GOOGLE_CLOUD_CPP_OPENTELEMETRY_API}"
#     NON_WIN32_REQUIRES
#     openssl
#     WIN32_LIBS
#     bcrypt)

# Create and install the CMake configuration files.
# configure_file("config.cmake.in" "google_cloud_cpp_common-config.cmake" @ONLY)
# write_basic_package_version_file(
#     "google_cloud_cpp_common-config-version.cmake"
#     VERSION ${PROJECT_VERSION}
#     COMPATIBILITY ExactVersion)

# install(
#     FILES
#         "${CMAKE_CURRENT_BINARY_DIR}/google_cloud_cpp_common-config.cmake"
#         "${CMAKE_CURRENT_BINARY_DIR}/google_cloud_cpp_common-config-version.cmake"
#     DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/google_cloud_cpp_common"
#     COMPONENT google_cloud_cpp_development)

# if (GOOGLE_CLOUD_CPP_WITH_MOCKS)
#     # Create a header-only library for the mocks. We use a CMake `INTERFACE`
#     # library for these, a regular library would not work on macOS (where the
#     # library needs at least one .o file).
#     add_library(google_cloud_cpp_mocks INTERFACE)
#     set(google_cloud_cpp_mocks_hdrs
#         # cmake-format: sort
#         mocks/current_options.h mocks/mock_async_streaming_read_write_rpc.h
#         mocks/mock_stream_range.h)
#     export_list_to_bazel("google_cloud_cpp_mocks.bzl"
#                          "google_cloud_cpp_mocks_hdrs" YEAR "2022")
#     target_link_libraries(
#         google_cloud_cpp_mocks INTERFACE google-cloud-cpp::common GTest::gmock
#                                          GTest::gtest)
#     set_target_properties(google_cloud_cpp_mocks
#                           PROPERTIES EXPORT_NAME google-cloud-cpp::mocks)
#     target_include_directories(
#         google_cloud_cpp_mocks
#         INTERFACE $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
#                   $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}>
#                   $<INSTALL_INTERFACE:include>)
#     target_compile_options(google_cloud_cpp_mocks
#                            INTERFACE ${GOOGLE_CLOUD_CPP_EXCEPTIONS_FLAG})
#     add_library(google-cloud-cpp::mocks ALIAS google_cloud_cpp_mocks)

#     install(
#         FILES ${google_cloud_cpp_mocks_hdrs}
#         DESTINATION "include/google/cloud/mocks"
#         COMPONENT google_cloud_cpp_development)

#     # Export the CMake targets to make it easy to create configuration files.
#     install(
#         EXPORT google_cloud_cpp_mocks-targets
#         DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/google_cloud_cpp_mocks"
#         COMPONENT google_cloud_cpp_development)

#     install(
#         TARGETS google_cloud_cpp_mocks
#         EXPORT google_cloud_cpp_mocks-targets
#         COMPONENT google_cloud_cpp_development)

#     google_cloud_cpp_add_pkgconfig(
#         "mocks" "Google Cloud C++ Testing Library"
#         "Helpers for testing the Google Cloud C++ Client Libraries"
#         "google_cloud_cpp_common" "gmock")

#     # Create and install the CMake configuration files.
#     configure_file("mocks-config.cmake.in"
#                    "google_cloud_cpp_mocks-config.cmake" @ONLY)
#     write_basic_package_version_file(
#         "google_cloud_cpp_mocks-config-version.cmake"
#         VERSION ${PROJECT_VERSION}
#         COMPATIBILITY ExactVersion)

#     install(
#         FILES
#             "${CMAKE_CURRENT_BINARY_DIR}/google_cloud_cpp_mocks-config.cmake"
#             "${CMAKE_CURRENT_BINARY_DIR}/google_cloud_cpp_mocks-config-version.cmake"
#         DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/google_cloud_cpp_mocks"
#         COMPONENT google_cloud_cpp_development)
# endif ()

# if (BUILD_TESTING)
#     include(FindBenchmarkWithWorkarounds)

#     set(google_cloud_cpp_common_unit_tests
#         # cmake-format: sort
#         access_token_test.cc
#         common_options_test.cc
#         future_coroutines_test.cc
#         future_generic_test.cc
#         future_generic_then_test.cc
#         future_void_test.cc
#         future_void_then_test.cc
#         internal/algorithm_test.cc
#         internal/api_client_header_test.cc
#         internal/backoff_policy_test.cc
#         internal/base64_transforms_test.cc
#         internal/big_endian_test.cc
#         internal/call_context_test.cc
#         internal/clock_test.cc
#         internal/compiler_info_test.cc
#         internal/compute_engine_util_test.cc
#         internal/credentials_impl_test.cc
#         internal/debug_future_status_test.cc
#         internal/debug_string_test.cc
#         internal/detect_gcp_test.cc
#         internal/error_context_test.cc
#         internal/filesystem_test.cc
#         internal/format_time_point_test.cc
#         internal/future_impl_test.cc
#         internal/future_then_impl_test.cc
#         internal/group_options_test.cc
#         internal/invocation_id_generator_test.cc
#         internal/invoke_result_test.cc
#         internal/log_impl_test.cc
#         internal/make_status_test.cc
#         internal/noexcept_action_test.cc
#         internal/opentelemetry_context_test.cc
#         internal/opentelemetry_test.cc
#         internal/pagination_range_test.cc
#         internal/parse_rfc3339_test.cc
#         internal/populate_common_options_test.cc
#         internal/random_test.cc
#         internal/retry_loop_helpers_test.cc
#         internal/retry_policy_impl_test.cc
#         internal/service_endpoint_test.cc
#         internal/sha256_hash_test.cc
#         internal/sha256_hmac_test.cc
#         internal/status_payload_keys_test.cc
#         internal/status_utils_test.cc
#         internal/strerror_test.cc
#         internal/subject_token_test.cc
#         internal/throw_delegate_test.cc
#         internal/timer_queue_test.cc
#         internal/trace_propagator_test.cc
#         internal/traced_stream_range_test.cc
#         internal/tuple_test.cc
#         internal/type_list_test.cc
#         internal/url_encode_test.cc
#         internal/user_agent_prefix_test.cc
#         internal/utility_test.cc
#         kms_key_name_test.cc
#         location_test.cc
#         log_test.cc
#         mocks/current_options_test.cc
#         mocks/mock_stream_range_test.cc
#         options_test.cc
#         polling_policy_test.cc
#         project_test.cc
#         status_or_test.cc
#         status_test.cc
#         stream_range_test.cc
#         terminate_handler_test.cc
#         tracing_options_test.cc)

#     # Export the list of unit tests so the Bazel BUILD file can pick it up.
#     export_list_to_bazel("google_cloud_cpp_common_unit_tests.bzl"
#                          "google_cloud_cpp_common_unit_tests" YEAR "2018")

#     foreach (fname ${google_cloud_cpp_common_unit_tests})
#         google_cloud_cpp_add_executable(target "common" "${fname}")
#         target_link_libraries(
#             ${target}
#             PRIVATE google_cloud_cpp_testing
#                     google-cloud-cpp::common
#                     google-cloud-cpp::mocks
#                     absl::variant
#                     GTest::gmock_main
#                     GTest::gmock
#                     GTest::gtest)
#         google_cloud_cpp_add_common_options(${target})
#         add_test(NAME ${target} COMMAND ${target})
#     endforeach ()

#     set(google_cloud_cpp_common_benchmarks # cmake-format: sort
#                                            options_benchmark.cc)

#     # Export the list of benchmarks to a .bzl file so we do not need to maintain
#     # the list in two places.
#     export_list_to_bazel("google_cloud_cpp_common_benchmarks.bzl"
#                          "google_cloud_cpp_common_benchmarks" YEAR "2020")

#     # Generate a target for each benchmark.
#     foreach (fname ${google_cloud_cpp_common_benchmarks})
#         google_cloud_cpp_add_executable(target "common" "${fname}")
#         add_test(NAME ${target} COMMAND ${target})
#         target_link_libraries(${target} PRIVATE google-cloud-cpp::common
#                                                 benchmark::benchmark_main)
#         google_cloud_cpp_add_common_options(${target})
#     endforeach ()
# endif ()

# if (BUILD_TESTING AND GOOGLE_CLOUD_CPP_ENABLE_CXX_EXCEPTIONS)
#     google_cloud_cpp_add_samples_relative("common" "samples/")
# endif ()
