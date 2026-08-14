# ~~~
# Copyright 2017, Google Inc.
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

# File copied from contrib/google-cloud-cpp/cmake/CompileProtos.cmake with minor modifications.

# Introduce a new TARGET property to associate proto files with a target.
#
# We use a function to define the property so it can be called multiple times
# without introducing the property over and over.
function (google_cloud_cpp_add_protos_property)
    set_property(
        TARGET
        PROPERTY PROTO_SOURCES BRIEF_DOCS
                 "The list of .proto files for a target." FULL_DOCS
                 "List of .proto files specified for a target.")
endfunction ()

# We use this call enough times to warrant factoring it out.
#
# Set `out_VAR` to the value of `in_VAR`, if `in_VAR` is set. Otherwise, set it
# to ${CMAKE_CURRENT_BINARY_DIR}.
function (google_cloud_cpp_set_out_directory in_VAR out_VAR)
    set(dir "${CMAKE_CURRENT_BINARY_DIR}")
    if (${in_VAR})
        set(dir "${${in_VAR}}")
    endif ()
    set(${out_VAR}
        "${dir}"
        PARENT_SCOPE)
endfunction ()

# Generate C++ for .proto files preserving the directory hierarchy
#
# Receives a list of `.proto` file names and (a) creates the runs to convert
# these files to `.pb.h` and `.pb.cc` output files, (b) returns the list of
# `.pb.cc` files and `.pb.h` files in @p HDRS, and (c) creates the list of files
# preserving the directory hierarchy, such that if a `.proto` file says:
#
# import "foo/bar/baz.proto"
#
# the resulting C++ code says:
#
# #include <foo/bar/baz.pb.h>
#
# Use the `PROTO_PATH` option to provide one or more directories to search for
# proto files in the import.
#
# @par Example
#
# google_cloud_cpp_generate_proto( MY_PB_FILES "foo/bar/baz.proto"
# "foo/bar/qux.proto" PROTO_PATH_DIRECTORIES "another/dir/with/protos")
#
# Note that `protoc` the protocol buffer compiler requires your protos to be
# somewhere in the search path defined by the `--proto_path` (aka -I) options.
# For example, if you want to generate the `.pb.{h,cc}` files for
# `foo/bar/baz.proto` then the directory containing `foo` must be in the search
# path.
function (google_cloud_cpp_generate_proto SRCS)
    cmake_parse_arguments(_opt "" "OUT_DIRECTORY" "PROTO_PATH_DIRECTORIES"
                          ${ARGN})
    google_cloud_cpp_set_out_directory(_opt_OUT_DIRECTORY OUT_DIR)
    if (NOT _opt_UNPARSED_ARGUMENTS)
        message(SEND_ERROR "Error: google_cloud_cpp_generate_proto() called"
                           " without any proto files")
        return()
    endif ()

    # Build the list of `--proto_path` options. Use the absolute path for each
    # option given, and do not include any path more than once.
    set(protobuf_include_path)
    foreach (dir IN LISTS _opt_PROTO_PATH_DIRECTORIES)
        get_filename_component(absolute_path ${dir} ABSOLUTE)
        list(FIND protobuf_include_path "${absolute_path}"
             already_in_search_path)
        if (${already_in_search_path} EQUAL -1)
            list(APPEND protobuf_include_path "--proto_path" "${absolute_path}")
        endif ()
    endforeach ()

    set(${SRCS})
    foreach (file_path IN LISTS _opt_UNPARSED_ARGUMENTS)
        get_filename_component(file_directory "${file_path}" DIRECTORY)
        get_filename_component(file_name "${file_path}" NAME)
        # This gets the file name without the ".proto" extension. We would like
        # to use get_filename_component with the option NAME_WLE, but that is
        # not available until CMake 3.14
        string(REPLACE ".proto" "" file_stem "${file_name}")

        # Strip all the prefixes in ${_opt_PROTO_PATH_DIRECTORIES} from the
        # source proto directory
        set(D "${file_directory}")
        if (DEFINED _opt_PROTO_PATH_DIRECTORIES)
            foreach (P IN LISTS _opt_PROTO_PATH_DIRECTORIES)
                string(REGEX REPLACE "^${P}" "" T "${D}")
                set(D ${T})
            endforeach ()
        endif ()
        set(pb_cc "${OUT_DIR}/${D}/${file_stem}.pb.cc")
        set(pb_h "${OUT_DIR}/${D}/${file_stem}.pb.h")
        list(APPEND ${SRCS} "${pb_cc}" "${pb_h}")

        if (NOT Protobuf_PROTOC_EXECUTABLE)
            set(Protobuf_PROTOC_EXECUTABLE $<TARGET_FILE:protobuf::protoc>)
        endif ()
        add_custom_command(
            OUTPUT "${pb_cc}" "${pb_h}"
            COMMAND ${Protobuf_PROTOC_EXECUTABLE} ARGS --cpp_out "${OUT_DIR}"
                    ${protobuf_include_path} "${file_path}"
            DEPENDS "${file_path}"
            COMMENT "Running C++ protocol buffer compiler on ${file_path}"
            VERBATIM)
    endforeach ()

    set_source_files_properties(${${SRCS}} PROPERTIES GENERATED TRUE)
    set(${SRCS}
        ${${SRCS}}
        PARENT_SCOPE)
endfunction ()

# Generate gRPC C++ files from .proto files preserving the directory hierarchy.
#
# Receives a list of `.proto` file names and (a) creates the runs to convert
# these files to `.grpc.pb.h` and `.grpc.pb.cc` output files, (b) returns the
# list of `.grpc.pb.cc` and `.pb.h` files in @p SRCS, and (c) creates the list
# of files preserving the directory hierarchy, such that if a `.proto` file says
#
# import "foo/bar/baz.proto"
#
# the resulting C++ code says:
#
# #include <foo/bar/baz.pb.h>
#
# Use the `PROTO_PATH` option to provide one or more directories to search for
# proto files in the import.
#
# @par Example
#
# google_cloud_cpp_generate_grpc( MY_GRPC_PB_FILES "foo/bar/baz.proto"
# "foo/bar/qux.proto" PROTO_PATH_DIRECTORIES "another/dir/with/protos")
#
# Note that `protoc` the protocol buffer compiler requires your protos to be
# somewhere in the search path defined by the `--proto_path` (aka -I) options.
# For example, if you want to generate the `.pb.{h,cc}` files for
# `foo/bar/baz.proto` then the directory containing `foo` must be in the search
# path.
function (google_cloud_cpp_generate_grpcpp SRCS)
    cmake_parse_arguments(_opt "" "OUT_DIRECTORY" "PROTO_PATH_DIRECTORIES"
                          ${ARGN})
    google_cloud_cpp_set_out_directory(_opt_OUT_DIRECTORY OUT_DIR)
    if (NOT _opt_UNPARSED_ARGUMENTS)
        message(
            SEND_ERROR "Error: google_cloud_cpp_generate_grpc() called without"
                       " any proto files")
        return()
    endif ()

    # Build the list of `--proto_path` options. Use the absolute path for each
    # option given, and do not include any path more than once.
    set(protobuf_include_path)
    foreach (dir ${_opt_PROTO_PATH_DIRECTORIES})
        get_filename_component(absolute_path ${dir} ABSOLUTE)
        list(FIND protobuf_include_path "${absolute_path}"
             already_in_search_path)
        if (${already_in_search_path} EQUAL -1)
            list(APPEND protobuf_include_path "--proto_path" "${absolute_path}")
        endif ()
    endforeach ()

    set(${SRCS})
    foreach (file_path ${_opt_UNPARSED_ARGUMENTS})
        get_filename_component(file_directory "${file_path}" DIRECTORY)
        get_filename_component(file_name "${file_path}" NAME)
        # This gets the file name without the ".proto" extension. We would like
        # to use get_filename_component with the option NAME_WLE, but that is
        # not available until CMake 3.14
        string(REPLACE ".proto" "" file_stem "${file_name}")

        # Strip all the prefixes in ${_opt_PROTO_PATH_DIRECTORIES} from the
        # source proto directory
        set(D "${file_directory}")
        if (DEFINED _opt_PROTO_PATH_DIRECTORIES)
            foreach (P ${_opt_PROTO_PATH_DIRECTORIES})
                string(REGEX REPLACE "^${P}" "" T "${D}")
                set(D ${T})
            endforeach ()
        endif ()
        set(grpc_pb_cc "${OUT_DIR}/${D}/${file_stem}.grpc.pb.cc")
        set(grpc_pb_h "${OUT_DIR}/${D}/${file_stem}.grpc.pb.h")
        list(APPEND ${SRCS} "${grpc_pb_cc}" "${grpc_pb_h}")
        if (NOT Protobuf_PROTOC_EXECUTABLE)
            set(Protobuf_PROTOC_EXECUTABLE $<TARGET_FILE:protobuf::protoc>)
        endif ()
        add_custom_command(
            OUTPUT "${grpc_pb_cc}" "${grpc_pb_h}"
            COMMAND
                ${Protobuf_PROTOC_EXECUTABLE} ARGS
                --plugin=protoc-gen-grpc=${GOOGLE_CLOUD_CPP_GRPC_PLUGIN_EXECUTABLE}
                "--grpc_out=${OUT_DIR}" "--cpp_out=${OUT_DIR}"
                ${protobuf_include_path} "${file_path}"
            DEPENDS "${file_path}"
            COMMENT "Running gRPC C++ protocol buffer compiler on ${file_path}"
            VERBATIM)
    endforeach ()

    set_source_files_properties(${${SRCS}} PROPERTIES GENERATED TRUE)
    set(${SRCS}
        ${${SRCS}}
        PARENT_SCOPE)
endfunction ()

# Generate a list of proto files from a `protolists/*.list` file.
#
# The proto libraries in googleapis/googleapis do not ship with support for
# CMake. We need to write our own CMake files. To ease the maintenance effort we
# use a script that queries the BUILD files in googleapis/googleapis, and
# extracts the list of `.proto` files for each proto library we are interested
# in. These files are extracted as Bazel rule names, for example:
#
# @com_google_googleapis//google/bigtable/v2:bigtable.proto
#
# We use naming conventions to convert these rules files to a path. Using the
# same example that becomes:
#
# ${EXTERNAL_GOOGLEAPIS_SOURCE}/google/bigtable/v2/bigtable.proto
#
function (google_cloud_cpp_load_protolist var file)
    cmake_parse_arguments(
        _opt
        # No boolean flags
        ""
        # If present PROTO_DIR overrides the default
        # "${EXTERNAL_GOOGLEAPIS_SOURCE}"
        "PROTO_DIR"
        # No multi-argument flags
        ""
        ${ARGN})
    if (NOT DEFINED _opt_PROTO_DIR)
        set(_opt_PROTO_DIR "${EXTERNAL_GOOGLEAPIS_SOURCE}")
    endif ()

    file(READ "${file}" contents)
    string(REGEX REPLACE "\n" ";" contents "${contents}")
    set(protos)
    foreach (line IN LISTS contents)
        string(REPLACE "@com_google_googleapis//" "" line "${line}")
        string(REPLACE ":" "/" line "${line}")
        if ("${line}" STREQUAL "")
            continue()
        endif ()
        list(APPEND protos "${_opt_PROTO_DIR}/${line}")
    endforeach ()
    set(${var}
        "${protos}"
        PARENT_SCOPE)
endfunction ()

# Generate a list of proto dependencies from a `protodeps/*.deps` file.
#
# The proto libraries in googleapis/googleapis do not ship with support for
# CMake. We need to write our own CMake files. To ease the maintenance effort we
# use a script that queries the BUILD files in googleapis/googleapis, and
# extracts the list of dependencies for each proto library we are interested in.
# These dependencies are extracted as Bazel rule names, for example:
#
# @com_google_googleapis//google/api:annotations_proto
#
# We use naming conventions to convert these rules files to a CMake target.
# Using the same example that becomes:
#
# google-cloud-cpp::api_annotations_proto
#
function (google_cloud_cpp_load_protodeps var file)
    file(READ "${file}" contents)
    string(REPLACE "\n" ";" contents "${contents}")
    set(deps)

    # Omit a target from deps.
    set(targets_to_omit
        "google-cloud-cpp::cloud_kms_v1_kms_protos"
        "google-cloud-cpp::cloud_orgpolicy_v1_orgpolicy_protos"
        "google-cloud-cpp::cloud_oslogin_common_common_protos"
        "google-cloud-cpp::cloud_recommender_v1_recommender_protos"
        "google-cloud-cpp::identity_accesscontextmanager_type_type_protos")
    # Replace "google-cloud-cpp::$1" with "google-cloud-cpp:$2" in deps. The
    # most common reason to need one of these is a dependency between the protos
    # in one library vs. the protos in a second library. The AIPs frown upon
    # such dependencies, but they do happen.
    set(target_substitutions
        "grafeas_v1_grafeas_protos\;grafeas_protos"
        "iam_v2_policy_protos\;iam_v2_protos"
        "logging_type_type_protos\;logging_type_protos"
        "identity_accesscontextmanager_v1_accesscontextmanager_protos\;accesscontextmanager_protos"
        "cloud_osconfig_v1_osconfig_protos\;osconfig_protos"
        "cloud_documentai_v1_documentai_protos\;documentai_protos")

    foreach (line IN LISTS contents)
        if ("${line}" STREQUAL "")
            continue()
        endif ()
        string(REPLACE ":" "_" line "${line}")
        string(REPLACE "_proto" "_protos" line "${line}")
        string(REPLACE "@com_google_googleapis//" "google-cloud-cpp::" line
                       "${line}")
        # Avoid duplicate `google`'s in the target name.
        string(REPLACE "google-cloud-cpp::google/" "google-cloud-cpp::" line
                       "${line}")
        string(REPLACE "/" "_" line "${line}")
        if ("${line}" IN_LIST targets_to_omit)
            continue()
        endif ()
        foreach (substitution IN LISTS target_substitutions)
            set(from_to "${substitution}")
            list(GET from_to 0 from)
            list(GET from_to 1 to)
            string(REPLACE "google-cloud-cpp::${from}"
                           "google-cloud-cpp::${to}" line "${line}")
        endforeach ()
        list(APPEND deps "${line}")
    endforeach ()
    set(${var}
        "${deps}"
        PARENT_SCOPE)
endfunction ()

function (google_cloud_cpp_proto_library libname)
    cmake_parse_arguments(_opt "" "OUT_DIRECTORY" "PROTO_PATH_DIRECTORIES"
                          ${ARGN})
    google_cloud_cpp_set_out_directory(_opt_OUT_DIRECTORY OUT_DIR)
    if (NOT _opt_UNPARSED_ARGUMENTS)
        message(SEND_ERROR "Error: google_cloud_cpp_proto_library() called"
                           " without any proto files")
        return()
    endif ()

    google_cloud_cpp_generate_proto(
        proto_sources ${_opt_UNPARSED_ARGUMENTS} PROTO_PATH_DIRECTORIES
        ${_opt_PROTO_PATH_DIRECTORIES} OUT_DIRECTORY ${_opt_OUT_DIRECTORY})

    add_library(${libname} ${proto_sources})
    set_property(TARGET ${libname} PROPERTY PROTO_SOURCES
                                            ${_opt_UNPARSED_ARGUMENTS})
    target_link_libraries(${libname} PUBLIC gRPC::grpc++ gRPC::grpc
                                            protobuf::libprotobuf)
    # We want to treat the generated code as "system" headers so they get
    # ignored by the more aggressive warnings.
    target_include_directories(
        ${libname} SYSTEM PUBLIC $<BUILD_INTERFACE:${OUT_DIR}>
                                 $<INSTALL_INTERFACE:include>)

    # In some configs we need to only generate the protocol definitions from
    # `*.proto` files. We achieve this by having this target depend on all proto
    # libraries. It has to be defined at the top level of the project.
    add_dependencies(google-cloud-cpp-protos ${libname})
endfunction ()

function (google_cloud_cpp_grpcpp_library libname)
    cmake_parse_arguments(_opt "" "OUT_DIRECTORY" "PROTO_PATH_DIRECTORIES"
                          ${ARGN})
    if (NOT _opt_UNPARSED_ARGUMENTS)
        message(SEND_ERROR "Error: google_cloud_cpp_proto_library() called"
                           " without any proto files")
        return()
    endif ()

    google_cloud_cpp_generate_grpcpp(
        grpcpp_sources ${_opt_UNPARSED_ARGUMENTS} PROTO_PATH_DIRECTORIES
        ${_opt_PROTO_PATH_DIRECTORIES} OUT_DIRECTORY ${_opt_OUT_DIRECTORY})
    google_cloud_cpp_proto_library(
        ${libname} ${_opt_UNPARSED_ARGUMENTS} PROTO_PATH_DIRECTORIES
        ${_opt_PROTO_PATH_DIRECTORIES} OUT_DIRECTORY ${_opt_OUT_DIRECTORY})
    target_sources(${libname} PRIVATE ${grpcpp_sources})
endfunction ()

macro (external_googleapis_install_pc_common target)
    string(REPLACE "google_cloud_cpp_" "" _short_name ${target})
    string(REPLACE "_protos" "" _short_name ${_short_name})
    set(GOOGLE_CLOUD_CPP_PC_NAME
        "The Google APIS C++ ${_short_name} Proto Library")
    set(GOOGLE_CLOUD_CPP_PC_DESCRIPTION "Compiled proto for C++.")
    # Examine the target LINK_LIBRARIES property, use that to pull the
    # dependencies between the google-cloud-cpp::* libraries.
    set(_target_pc_requires)
    get_target_property(_target_deps ${target} INTERFACE_LINK_LIBRARIES)
    foreach (dep ${_target_deps})
        if ("${dep}" MATCHES "^google-cloud-cpp::")
            string(REPLACE "google-cloud-cpp::" "google_cloud_cpp_" dep
                           "${dep}")
            list(APPEND _target_pc_requires " " "${dep}")
        endif ()
    endforeach ()
    # These dependencies are required for all the google-cloud-cpp::* libraries.
    list(
        APPEND
        _target_pc_requires
        "grpc++"
        "grpc"
        "openssl"
        "protobuf"
        "zlib"
        "libcares")
    string(JOIN " " GOOGLE_CLOUD_CPP_PC_REQUIRES ${_target_pc_requires})
    get_target_property(_target_type ${target} TYPE)
    if ("${_target_type}" STREQUAL "INTERFACE_LIBRARY")
        set(GOOGLE_CLOUD_CPP_PC_LIBS "")
    else ()
        set(GOOGLE_CLOUD_CPP_PC_LIBS "-l${target}")
    endif ()
endmacro ()

# Find the proto include directory
#
# Sometimes (this happens often with vcpkg) protobuf is installed in a non-
# standard directory. We need to find out where, and then add that directory to
# the search path for protos.
macro (google_cloud_cpp_find_proto_include_dir VAR)
    find_path(${VAR} google/protobuf/descriptor.proto)
    if (${VAR})
        list(INSERT PROTOBUF_IMPORT_DIRS 0 "${${VAR}}")
    endif ()
endmacro ()

# We used to offer the proto library by another name. Maintain backwards
# compatibility by providing an interface library with that name. See
# https://github.com/googleapis/google-cloud-cpp/issues/8022 for more details.
function (google_cloud_cpp_backwards_compat_protos_library old_name new_name)
    add_library(google_cloud_cpp_${old_name} INTERFACE)
    set_target_properties(google_cloud_cpp_${old_name}
                          PROPERTIES EXPORT_NAME google-cloud-cpp::${old_name})
    add_library(google-cloud-cpp::${old_name} ALIAS
                google_cloud_cpp_${old_name})
    target_link_libraries(
        google_cloud_cpp_${old_name}
        PUBLIC
        INTERFACE google-cloud-cpp::${new_name})

    google_cloud_cpp_add_pkgconfig(
        ${old_name} "The Google APIS C++ ${old_name} Proto Library"
        "Compiled proto for C++." "google_cloud_cpp_${new_name}")
endfunction ()
