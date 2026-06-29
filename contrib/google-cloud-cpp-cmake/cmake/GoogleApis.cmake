# ~~~
# Copyright 2020 Google LLC
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

# File copied from contrib/google-cloud-cpp/cmake/GoogleApis.cmake with minor modifications.

if (NOT GOOGLE_CLOUD_CPP_ENABLE_GRPC)
    return()
endif ()

include(GoogleapisConfig)

set(GOOGLE_CLOUD_CPP_GOOGLEAPIS_URL
    "https://github.com/googleapis/googleapis/archive/${_GOOGLE_CLOUD_CPP_GOOGLEAPIS_COMMIT_SHA}.tar.gz"
    "https://storage.googleapis.com/cloud-cpp-community-archive/github.com/googleapis/googleapis/archive/${_GOOGLE_CLOUD_CPP_GOOGLEAPIS_COMMIT_SHA}.tar.gz"
)
set(GOOGLE_CLOUD_CPP_GOOGLEAPIS_URL_HASH
    "${_GOOGLE_CLOUD_CPP_GOOGLEAPIS_SHA256}")
if (GOOGLE_CLOUD_CPP_OVERRIDE_GOOGLEAPIS_URL)
    set(GOOGLE_CLOUD_CPP_GOOGLEAPIS_URL
        ${GOOGLE_CLOUD_CPP_OVERRIDE_GOOGLEAPIS_URL})
endif ()
if (GOOGLE_CLOUD_CPP_OVERRIDE_GOOGLEAPIS_URL_HASH)
    set(GOOGLE_CLOUD_CPP_GOOGLEAPIS_URL_HASH
        "${GOOGLE_CLOUD_CPP_OVERRIDE_GOOGLEAPIS_URL_HASH}")
endif ()

set(EXTERNAL_GOOGLEAPIS_PROTO_FILES
    # cmake-format: sort
    "google/api/annotations.proto"
    "google/api/auth.proto"
    "google/api/backend.proto"
    "google/api/billing.proto"
    "google/api/client.proto"
    "google/api/config_change.proto"
    "google/api/consumer.proto"
    "google/api/context.proto"
    "google/api/control.proto"
    "google/api/distribution.proto"
    "google/api/documentation.proto"
    "google/api/endpoint.proto"
    "google/api/error_reason.proto"
    "google/api/field_behavior.proto"
    "google/api/field_info.proto"
    "google/api/http.proto"
    "google/api/httpbody.proto"
    "google/api/label.proto"
    "google/api/launch_stage.proto"
    "google/api/log.proto"
    "google/api/logging.proto"
    "google/api/metric.proto"
    "google/api/monitored_resource.proto"
    "google/api/monitoring.proto"
    "google/api/policy.proto"
    "google/api/quota.proto"
    "google/api/resource.proto"
    "google/api/routing.proto"
    "google/api/service.proto"
    "google/api/source_info.proto"
    "google/api/system_parameter.proto"
    "google/api/usage.proto"
    "google/api/visibility.proto"
    "google/cloud/extended_operations.proto"
    "google/cloud/location/locations.proto"
    # orgpolicy/v**1** is used *indirectly* by google/cloud/asset, therefore it
    # does not appear in protolists/asset.list. In addition, it is not compiled
    # by any other library. So, added manually.
    "google/cloud/orgpolicy/v1/orgpolicy.proto"
    # Some gRPC based authentication is implemented by the IAM Credentials
    # service.
    "google/iam/credentials/v1/common.proto"
    "google/iam/credentials/v1/iamcredentials.proto"
    # We expose google::iam::v1::Policy in our google::cloud::IAMUpdater
    "google/iam/v1/iam_policy.proto"
    "google/iam/v1/options.proto"
    "google/iam/v1/policy.proto"
    "google/longrunning/operations.proto"
    "google/rpc/code.proto"
    "google/rpc/context/attribute_context.proto"
    "google/rpc/error_details.proto"
    "google/rpc/status.proto"
    "google/type/calendar_period.proto"
    "google/type/color.proto"
    "google/type/date.proto"
    "google/type/datetime.proto"
    "google/type/dayofweek.proto"
    "google/type/decimal.proto"
    "google/type/expr.proto"
    "google/type/fraction.proto"
    "google/type/interval.proto"
    "google/type/latlng.proto"
    "google/type/localized_text.proto"
    "google/type/money.proto"
    "google/type/month.proto"
    "google/type/phone_number.proto"
    "google/type/postal_address.proto"
    "google/type/quaternion.proto"
    "google/type/timeofday.proto")

# Set EXTERNAL_GOOGLEAPIS_SOURCE in the parent directory, as it is used by all
# the generated libraries.  The Conan packages (https://conan.io), will need to
# patch this value.  Setting the value in a single place makes such patching
# easier.
set(EXTERNAL_GOOGLEAPIS_PREFIX "${PROJECT_BINARY_DIR}/external/googleapis")
set(EXTERNAL_GOOGLEAPIS_SOURCE
    "${EXTERNAL_GOOGLEAPIS_PREFIX}/src/googleapis_download"
    PARENT_SCOPE)
set(EXTERNAL_GOOGLEAPIS_SOURCE
    "${EXTERNAL_GOOGLEAPIS_PREFIX}/src/googleapis_download")

# Include the functions to compile proto files and maintain proto libraries.
include(CompileProtos)

set(EXTERNAL_GOOGLEAPIS_BYPRODUCTS)
foreach (proto ${EXTERNAL_GOOGLEAPIS_PROTO_FILES})
    list(APPEND EXTERNAL_GOOGLEAPIS_BYPRODUCTS
         "${EXTERNAL_GOOGLEAPIS_SOURCE}/${proto}")
endforeach ()

file(GLOB protolists "protolists/*.list")
foreach (file IN LISTS protolists)
    google_cloud_cpp_load_protolist(protos "${file}")
    foreach (proto IN LISTS protos)
        list(APPEND EXTERNAL_GOOGLEAPIS_BYPRODUCTS "${proto}")
    endforeach ()
endforeach ()

include(ExternalProject)

# -- The build needs protobuf files. The original build scripts download them from a remote server (see target 'googleapis_download').
#    This is too unreliable in the context of ClickHouse ... we instead ship the downloaded archive with the ClickHouse source and
#    extract it into the build directory directly.

# Dummy googleapis_download target. This needs to exist because lots of other targets depend on it
# We however trick it a little bit saying this target generates the ${EXTERNAL_GOOGLEAPIS_BYPRODUCTS} BYPRODUCTS when
# actually the following section is the one actually providing such BYPRODUCTS.
externalproject_add(
    googleapis_download
    EXCLUDE_FROM_ALL ON
    PREFIX "${EXTERNAL_GOOGLEAPIS_PREFIX}"
    PATCH_COMMAND ""
    DOWNLOAD_COMMAND ""
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
    BUILD_BYPRODUCTS ${EXTERNAL_GOOGLEAPIS_BYPRODUCTS}
    LOG_DOWNLOAD OFF)

# Command that extracts the tarball into the proper dir
# Note: The hash must match the Google Cloud Api version, otherwise funny things will happen.
# Find the right hash in "strip-prefix" in MODULE.bazel in the subrepository
message(STATUS "Extracting googleapis tarball")
set(PB_HASH "e60db19f11f94175ac682c5898cce0f77cc508ea")
set(PB_ARCHIVE "${PB_HASH}.tar.gz")
set(PB_DIR "googleapis-${PB_HASH}")

file(ARCHIVE_EXTRACT INPUT
    "${ClickHouse_SOURCE_DIR}/contrib/google-cloud-cpp-cmake/googleapis/${PB_ARCHIVE}"
    DESTINATION
    "${EXTERNAL_GOOGLEAPIS_PREFIX}/tmp")

file(REMOVE_RECURSE "${EXTERNAL_GOOGLEAPIS_SOURCE}")
file(RENAME
    "${EXTERNAL_GOOGLEAPIS_PREFIX}/tmp/${PB_DIR}"
    "${EXTERNAL_GOOGLEAPIS_SOURCE}"
)

google_cloud_cpp_find_proto_include_dir(PROTO_INCLUDE_DIR)

google_cloud_cpp_add_protos_property()

function (external_googleapis_short_name var proto)
    string(REPLACE "google/" "" short_name "${proto}")
    string(REPLACE "/" "_" short_name "${short_name}")
    string(REPLACE ".proto" "_protos" short_name "${short_name}")
    set("${var}"
        "${short_name}"
        PARENT_SCOPE)
endfunction ()

# Create a single source proto library.
#
# * proto: the filename for the proto source.
# * (optional) ARGN: proto libraries the new library depends on.
function (external_googleapis_add_library proto)
    external_googleapis_short_name(short_name "${proto}")
    google_cloud_cpp_grpcpp_library(
        google_cloud_cpp_${short_name} "${EXTERNAL_GOOGLEAPIS_SOURCE}/${proto}"
        PROTO_PATH_DIRECTORIES "${EXTERNAL_GOOGLEAPIS_SOURCE}"
        "${PROTO_INCLUDE_DIR}")

    external_googleapis_set_version_and_alias("${short_name}")

    set(public_deps)
    foreach (dep_short_name ${ARGN})
        list(APPEND public_deps "google-cloud-cpp::${dep_short_name}")
    endforeach ()
    list(LENGTH public_deps public_deps_length)
    if (public_deps_length EQUAL 0)
        target_link_libraries("google_cloud_cpp_${short_name}")
    else ()
        target_link_libraries("google_cloud_cpp_${short_name}"
                              PUBLIC ${public_deps})
    endif ()
endfunction ()

function (external_googleapis_set_version_and_alias short_name)
    add_dependencies("google_cloud_cpp_${short_name}" googleapis_download)
    set_target_properties(
        "google_cloud_cpp_${short_name}"
        PROPERTIES EXPORT_NAME google-cloud-cpp::${short_name}
                   VERSION "${PROJECT_VERSION}"
                   SOVERSION ${PROJECT_VERSION_MAJOR})
    add_library("google-cloud-cpp::${short_name}" ALIAS
                "google_cloud_cpp_${short_name}")
endfunction ()

if (GOOGLE_CLOUD_CPP_USE_INSTALLED_COMMON)
    return()
endif ()

# Avoid adding new proto libraries to this list as these libraries are always
# installed, regardless of whether or not they are needed. See #8022 for more
# details.
set(external_googleapis_installed_libraries_list
    # cmake-format: sort
    google_cloud_cpp_cloud_common_common_protos
    google_cloud_cpp_iam_credentials_v1_common_protos
    google_cloud_cpp_iam_credentials_v1_iamcredentials_protos
    google_cloud_cpp_iam_v1_iam_policy_protos
    google_cloud_cpp_iam_v1_options_protos
    google_cloud_cpp_iam_v1_policy_protos
    google_cloud_cpp_longrunning_operations_protos)

# These proto files cannot be added in the foreach() loop because they have
# dependencies.
set(PROTO_FILES_WITH_DEPENDENCIES
    # cmake-format: sort
    "google/api/annotations.proto"
    "google/api/auth.proto"
    "google/api/billing.proto"
    "google/api/client.proto"
    "google/api/control.proto"
    "google/api/distribution.proto"
    "google/api/endpoint.proto"
    "google/api/log.proto"
    "google/api/logging.proto"
    "google/api/metric.proto"
    "google/api/monitored_resource.proto"
    "google/api/monitoring.proto"
    "google/api/quota.proto"
    "google/api/service.proto"
    "google/api/usage.proto"
    "google/cloud/location/locations.proto"
    "google/rpc/status.proto")

# For some directories *most* (but not all) the proto files are simple enough
# that the libraries can be generated with a foreach() loop.
foreach (proto IN LISTS EXTERNAL_GOOGLEAPIS_PROTO_FILES)
    if (proto MATCHES "^google/api/"
        OR proto MATCHES "^google/type"
        OR proto MATCHES "^google/rpc/"
        OR proto MATCHES "^google/cloud/")
        external_googleapis_short_name(short_name "${proto}")
        list(APPEND external_googleapis_installed_libraries_list
             google_cloud_cpp_${short_name})
        list(FIND PROTO_FILES_WITH_DEPENDENCIES "${proto}" has_dependency)
        if (has_dependency EQUAL -1)
            external_googleapis_add_library("${proto}")
        endif ()
    endif ()
endforeach ()

# Out of order because they have dependencies.
external_googleapis_add_library("google/api/annotations.proto" api_http_protos)
external_googleapis_add_library("google/api/auth.proto" api_annotations_protos)
external_googleapis_add_library("google/api/client.proto"
                                api_launch_stage_protos)
external_googleapis_add_library("google/api/control.proto" api_policy_protos)
external_googleapis_add_library("google/api/metric.proto"
                                api_launch_stage_protos api_label_protos)
external_googleapis_add_library("google/api/billing.proto"
                                api_annotations_protos api_metric_protos)
external_googleapis_add_library("google/api/distribution.proto"
                                api_annotations_protos)
external_googleapis_add_library("google/api/endpoint.proto"
                                api_annotations_protos)
external_googleapis_add_library("google/api/log.proto" api_label_protos)
external_googleapis_add_library("google/api/logging.proto"
                                api_annotations_protos api_label_protos)
external_googleapis_add_library("google/api/monitored_resource.proto"
                                api_launch_stage_protos api_label_protos)
external_googleapis_add_library("google/api/monitoring.proto"
                                api_annotations_protos)
external_googleapis_add_library("google/api/quota.proto" api_annotations_protos)
external_googleapis_add_library("google/api/usage.proto" api_annotations_protos
                                api_visibility_protos)
external_googleapis_add_library(
    "google/api/service.proto"
    api_annotations_protos
    api_auth_protos
    api_backend_protos
    api_billing_protos
    api_client_protos
    api_context_protos
    api_control_protos
    api_documentation_protos
    api_endpoint_protos
    api_http_protos
    api_label_protos
    api_log_protos
    api_logging_protos
    api_metric_protos
    api_monitored_resource_protos
    api_monitoring_protos
    api_quota_protos
    api_resource_protos
    api_source_info_protos
    api_system_parameter_protos
    api_usage_protos)

external_googleapis_add_library("google/cloud/location/locations.proto"
                                api_annotations_protos api_client_protos)

external_googleapis_add_library("google/iam/v1/options.proto"
                                api_annotations_protos)
external_googleapis_add_library("google/iam/v1/policy.proto"
                                api_annotations_protos type_expr_protos)
external_googleapis_add_library("google/rpc/status.proto"
                                rpc_error_details_protos)

external_googleapis_add_library(
    "google/longrunning/operations.proto" api_annotations_protos
    api_client_protos rpc_status_protos)

external_googleapis_add_library(
    "google/iam/v1/iam_policy.proto"
    api_annotations_protos
    api_client_protos
    api_field_behavior_protos
    api_resource_protos
    iam_v1_options_protos
    iam_v1_policy_protos)

external_googleapis_add_library("google/iam/credentials/v1/common.proto"
                                api_field_behavior_protos api_resource_protos)

external_googleapis_add_library(
    "google/iam/credentials/v1/iamcredentials.proto" api_annotations_protos
    api_client_protos iam_credentials_v1_common_protos)

google_cloud_cpp_load_protolist(cloud_common_list "${GOOGLE_CLOUD_CPP_DIR}/external/googleapis/protolists/common.list")
google_cloud_cpp_load_protodeps(cloud_common_deps "${GOOGLE_CLOUD_CPP_DIR}/external/googleapis/protodeps/common.deps")
google_cloud_cpp_grpcpp_library(
    google_cloud_cpp_cloud_common_common_protos ${cloud_common_list}
    PROTO_PATH_DIRECTORIES "${EXTERNAL_GOOGLEAPIS_SOURCE}"
    "${PROTO_INCLUDE_DIR}")
external_googleapis_set_version_and_alias(cloud_common_common_protos)
target_link_libraries(google_cloud_cpp_cloud_common_common_protos
                      PUBLIC ${cloud_common_deps})
