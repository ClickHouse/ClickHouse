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

# File copied from google-cloud-cpp/google-cloud-cpp/google_cloud_cpp_grpc_utils.cmake with minor modifications.

set(GOOGLE_CLOUD_CPP_COMMON_DIR "${GOOGLE_CLOUD_CPP_DIR}/google/cloud")

# the library
add_library(
    google_cloud_cpp_grpc_utils # cmake-format: sort
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/async_operation.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/async_streaming_read_write_rpc.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/background_threads.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/completion_queue.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/completion_queue.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/connection_options.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/connection_options.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/grpc_error_delegate.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/grpc_error_delegate.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/grpc_options.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/grpc_options.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/grpc_utils/async_operation.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/grpc_utils/completion_queue.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/grpc_utils/grpc_error_delegate.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/grpc_utils/version.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/iam_updater.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_connection_ready.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_connection_ready.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_long_running_operation.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_polling_loop.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_polling_loop.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_read_stream_impl.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_read_write_stream_auth.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_read_write_stream_impl.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_read_write_stream_logging.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_read_write_stream_timeout.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_read_write_stream_tracing.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_retry_loop.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_retry_unary_rpc.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_rpc_details.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_streaming_read_rpc.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_streaming_read_rpc_auth.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_streaming_read_rpc_impl.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_streaming_read_rpc_logging.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_streaming_read_rpc_timeout.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_streaming_read_rpc_tracing.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_streaming_write_rpc.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_streaming_write_rpc_auth.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_streaming_write_rpc_impl.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_streaming_write_rpc_logging.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_streaming_write_rpc_timeout.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/async_streaming_write_rpc_tracing.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/background_threads_impl.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/background_threads_impl.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/completion_queue_impl.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/debug_string_protobuf.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/debug_string_protobuf.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/debug_string_status.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/debug_string_status.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/default_completion_queue_impl.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/default_completion_queue_impl.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/extract_long_running_result.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/extract_long_running_result.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/grpc_access_token_authentication.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/grpc_access_token_authentication.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/grpc_api_key_authentication.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/grpc_api_key_authentication.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/grpc_async_access_token_cache.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/grpc_async_access_token_cache.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/grpc_channel_credentials_authentication.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/grpc_channel_credentials_authentication.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/grpc_impersonate_service_account.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/grpc_impersonate_service_account.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/grpc_metadata_view.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/grpc_opentelemetry.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/grpc_opentelemetry.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/grpc_request_metadata.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/grpc_request_metadata.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/grpc_service_account_authentication.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/grpc_service_account_authentication.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/log_wrapper.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/log_wrapper.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/minimal_iam_credentials_stub.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/minimal_iam_credentials_stub.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/populate_grpc_options.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/populate_grpc_options.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/resumable_streaming_read_rpc.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/retry_loop.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/routing_matcher.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/setup_context.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/streaming_read_rpc.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/streaming_read_rpc.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/streaming_read_rpc_logging.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/streaming_read_rpc_tracing.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/streaming_write_rpc.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/streaming_write_rpc_impl.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/streaming_write_rpc_impl.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/streaming_write_rpc_logging.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/streaming_write_rpc_tracing.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/time_utils.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/time_utils.h
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/unified_grpc_credentials.cc
    ${GOOGLE_CLOUD_CPP_COMMON_DIR}/internal/unified_grpc_credentials.h)
target_link_libraries(
    google_cloud_cpp_grpc_utils
    PUBLIC absl::function_ref
           absl::memory
           absl::time
           absl::variant
           google-cloud-cpp::iam_credentials_v1_iamcredentials_protos
           google-cloud-cpp::iam_v1_policy_protos
           google-cloud-cpp::longrunning_operations_protos
           google-cloud-cpp::iam_v1_iam_policy_protos
           google-cloud-cpp::rpc_error_details_protos
           google-cloud-cpp::rpc_status_protos
           google-cloud-cpp::common
           gRPC::grpc++
           gRPC::grpc)
google_cloud_cpp_add_common_options(google_cloud_cpp_grpc_utils)
target_include_directories(
    google_cloud_cpp_grpc_utils PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
                                       $<INSTALL_INTERFACE:include>)
target_compile_options(google_cloud_cpp_grpc_utils
                       PUBLIC ${GOOGLE_CLOUD_CPP_EXCEPTIONS_FLAG})
set_target_properties(
    google_cloud_cpp_grpc_utils
    PROPERTIES EXPORT_NAME "google-cloud-cpp::grpc_utils"
               VERSION ${PROJECT_VERSION}
               SOVERSION ${PROJECT_VERSION_MAJOR})
add_library(google-cloud-cpp::grpc_utils ALIAS google_cloud_cpp_grpc_utils)

#create_bazel_config(google_cloud_cpp_grpc_utils YEAR 2019)

# # Install the libraries and headers in the locations determined by
# # GNUInstallDirs
# install(
#     TARGETS
#     EXPORT grpc_utils-targets
#     RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
#     LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
#     ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
#             COMPONENT google_cloud_cpp_development)

# # Export the CMake targets to make it easy to create configuration files.
# install(
#     EXPORT grpc_utils-targets
#     DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/google_cloud_cpp_grpc_utils"
#     COMPONENT google_cloud_cpp_development)

# install(
#     TARGETS google_cloud_cpp_grpc_utils
#     EXPORT grpc_utils-targets
#     RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
#             COMPONENT google_cloud_cpp_runtime
#     LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
#             COMPONENT google_cloud_cpp_runtime
#             NAMELINK_COMPONENT google_cloud_cpp_development
#     ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
#             COMPONENT google_cloud_cpp_development)

# google_cloud_cpp_install_headers(google_cloud_cpp_grpc_utils
#                                  include/google/cloud)

# google_cloud_cpp_add_pkgconfig(
#     grpc_utils
#     "gRPC Utilities for the Google Cloud C++ Client Library"
#     "Provides gRPC Utilities for the Google Cloud C++ Client Library."
#     "google_cloud_cpp_common"
#     "google_cloud_cpp_iam_credentials_v1_iamcredentials_protos"
#     "google_cloud_cpp_iam_v1_policy_protos"
#     "google_cloud_cpp_iam_v1_iam_policy_protos"
#     "google_cloud_cpp_longrunning_operations_protos"
#     "google_cloud_cpp_rpc_status_protos"
#     "absl_function_ref"
#     "absl_strings"
#     "absl_time"
#     "absl_time_zone"
#     "absl_variant"
#     "openssl")

# # Create and install the CMake configuration files.
# configure_file("grpc_utils/config.cmake.in"
#                "google_cloud_cpp_grpc_utils-config.cmake" @ONLY)
# write_basic_package_version_file(
#     "google_cloud_cpp_grpc_utils-config-version.cmake"
#     VERSION ${PROJECT_VERSION}
#     COMPATIBILITY ExactVersion)

# install(
#     FILES
#         "${CMAKE_CURRENT_BINARY_DIR}/google_cloud_cpp_grpc_utils-config.cmake"
#         "${CMAKE_CURRENT_BINARY_DIR}/google_cloud_cpp_grpc_utils-config-version.cmake"
#     DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/google_cloud_cpp_grpc_utils"
#     COMPONENT google_cloud_cpp_development)

# function (google_cloud_cpp_grpc_utils_add_test fname labels)
#     google_cloud_cpp_add_executable(target "common" "${fname}")
#     target_link_libraries(
#         ${target}
#         PRIVATE google-cloud-cpp::grpc_utils
#                 google_cloud_cpp_testing_grpc
#                 google_cloud_cpp_testing
#                 google-cloud-cpp::common
#                 absl::variant
#                 GTest::gmock_main
#                 GTest::gmock
#                 GTest::gtest
#                 gRPC::grpc++
#                 gRPC::grpc)
#     google_cloud_cpp_add_common_options(${target})
#     add_test(NAME ${target} COMMAND ${target})
#     set_tests_properties(${target} PROPERTIES LABELS "${labels}")
# endfunction ()

# if (BUILD_TESTING)
#     include(FindBenchmarkWithWorkarounds)

#     # List the unit tests, then setup the targets and dependencies.
#     set(google_cloud_cpp_grpc_utils_unit_tests
#         # cmake-format: sort
#         completion_queue_test.cc
#         connection_options_test.cc
#         grpc_error_delegate_test.cc
#         grpc_options_test.cc
#         internal/async_connection_ready_test.cc
#         internal/async_long_running_operation_test.cc
#         internal/async_polling_loop_test.cc
#         internal/async_read_write_stream_auth_test.cc
#         internal/async_read_write_stream_impl_test.cc
#         internal/async_read_write_stream_logging_test.cc
#         internal/async_read_write_stream_timeout_test.cc
#         internal/async_read_write_stream_tracing_test.cc
#         internal/async_retry_loop_test.cc
#         internal/async_retry_unary_rpc_test.cc
#         internal/async_streaming_read_rpc_auth_test.cc
#         internal/async_streaming_read_rpc_impl_test.cc
#         internal/async_streaming_read_rpc_logging_test.cc
#         internal/async_streaming_read_rpc_timeout_test.cc
#         internal/async_streaming_read_rpc_tracing_test.cc
#         internal/async_streaming_write_rpc_auth_test.cc
#         internal/async_streaming_write_rpc_impl_test.cc
#         internal/async_streaming_write_rpc_logging_test.cc
#         internal/async_streaming_write_rpc_timeout_test.cc
#         internal/async_streaming_write_rpc_tracing_test.cc
#         internal/background_threads_impl_test.cc
#         internal/debug_string_protobuf_test.cc
#         internal/debug_string_status_test.cc
#         internal/extract_long_running_result_test.cc
#         internal/grpc_access_token_authentication_test.cc
#         internal/grpc_async_access_token_cache_test.cc
#         internal/grpc_channel_credentials_authentication_test.cc
#         internal/grpc_opentelemetry_test.cc
#         internal/grpc_request_metadata_test.cc
#         internal/grpc_service_account_authentication_test.cc
#         internal/log_wrapper_test.cc
#         internal/minimal_iam_credentials_stub_test.cc
#         internal/populate_grpc_options_test.cc
#         internal/resumable_streaming_read_rpc_test.cc
#         internal/retry_loop_test.cc
#         internal/routing_matcher_test.cc
#         internal/streaming_read_rpc_logging_test.cc
#         internal/streaming_read_rpc_test.cc
#         internal/streaming_read_rpc_tracing_test.cc
#         internal/streaming_write_rpc_logging_test.cc
#         internal/streaming_write_rpc_test.cc
#         internal/streaming_write_rpc_tracing_test.cc
#         internal/time_utils_test.cc
#         internal/unified_grpc_credentials_test.cc)

#     # List the unit tests, then setup the targets and dependencies.
#     set(google_cloud_cpp_grpc_utils_integration_tests
#         # cmake-format: sort
#         internal/grpc_impersonate_service_account_integration_test.cc)

#     # Export the list of unit and integration tests so the Bazel BUILD file can
#     # pick them up.
#     export_list_to_bazel("google_cloud_cpp_grpc_utils_unit_tests.bzl"
#                          "google_cloud_cpp_grpc_utils_unit_tests" YEAR "2019")
#     export_list_to_bazel(
#         "google_cloud_cpp_grpc_utils_integration_tests.bzl"
#         "google_cloud_cpp_grpc_utils_integration_tests" YEAR "2021")

#     foreach (fname ${google_cloud_cpp_grpc_utils_unit_tests})
#         google_cloud_cpp_grpc_utils_add_test("${fname}" "")
#     endforeach ()

#     # TODO(#12485) - remove dependency on bigtable in this integration test.
#     if (NOT bigtable IN_LIST GOOGLE_CLOUD_CPP_ENABLE)
#         list(REMOVE_ITEM google_cloud_cpp_grpc_utils_integration_tests
#              "internal/grpc_impersonate_service_account_integration_test.cc")
#     endif ()

#     foreach (fname ${google_cloud_cpp_grpc_utils_integration_tests})
#         google_cloud_cpp_add_executable(target "common" "${fname}")
#         target_link_libraries(
#             ${target}
#             PRIVATE google-cloud-cpp::grpc_utils
#                     google_cloud_cpp_testing_grpc
#                     google_cloud_cpp_testing
#                     google-cloud-cpp::common
#                     google-cloud-cpp::iam_credentials_v1_iamcredentials_protos
#                     absl::variant
#                     GTest::gmock_main
#                     GTest::gmock
#                     GTest::gtest
#                     gRPC::grpc++
#                     gRPC::grpc)
#         google_cloud_cpp_add_common_options(${target})
#         add_test(NAME ${target} COMMAND ${target})
#         set_tests_properties(${target} PROPERTIES LABELS
#                                                   "integration-test-production")
#         # TODO(12485) - remove dep on bigtable_protos
#         if (bigtable IN_LIST GOOGLE_CLOUD_CPP_ENABLE)
#             target_link_libraries(${target}
#                                   PRIVATE google-cloud-cpp::bigtable_protos)
#         endif ()
#     endforeach ()

#     set(google_cloud_cpp_grpc_utils_benchmarks # cmake-format: sortable
#                                                completion_queue_benchmark.cc)

#     # Export the list of benchmarks to a .bzl file so we do not need to maintain
#     # the list in two places.
#     export_list_to_bazel("google_cloud_cpp_grpc_utils_benchmarks.bzl"
#                          "google_cloud_cpp_grpc_utils_benchmarks" YEAR "2020")

#     # Generate a target for each benchmark.
#     foreach (fname ${google_cloud_cpp_grpc_utils_benchmarks})
#         google_cloud_cpp_add_executable(target "common" "${fname}")
#         add_test(NAME ${target} COMMAND ${target})
#         target_link_libraries(
#             ${target}
#             PRIVATE google-cloud-cpp::grpc_utils google-cloud-cpp::common
#                     benchmark::benchmark_main)
#         google_cloud_cpp_add_common_options(${target})
#     endforeach ()
# endif ()
