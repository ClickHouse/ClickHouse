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

# File copied from contrib/google-cloud-cpp/google/cloud/google_cloud_cpp_grpc_utils.cmake with minor modifications.

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
