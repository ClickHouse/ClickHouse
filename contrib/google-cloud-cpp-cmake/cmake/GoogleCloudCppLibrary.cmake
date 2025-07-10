# ~~~
# Copyright 2023 Google LLC
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

# File copied from contrib/google-cloud-cpp/cmake/GoogleCloudCppLibrary.cmake with minor modifications.

#
# A function to add proto libraries, as defined by their protolists and
# protodeps.
#
# * library: the short name of the associated client library, e.g. `kms`.
#
# The function also respects the following boolean keyword:
#
# * EXPORT_TARGET: Export the targets. This function will install a config file
#   named `google_cloud_cpp_${library}-targets.cmake`.
#
# Note that if this keyword is supplied, we will only create new targets for
# this proto **if they do not already exist**. This logic allows us to add
# common proto dependencies across multiple client libraries.
#
# For example, both `binaryauthorization` and `contentanalysis` depend on
# `grafeas`. So they use EXPORT_TARGET, to only add `grafeas_protos` if
# necessary.
function (google_cloud_cpp_add_library_protos library)
    cmake_parse_arguments(_opt "EXPORT_TARGET" "" "ADDITIONAL_PROTO_LISTS"
                          ${ARGN})

    set(protos_target "google_cloud_cpp_${library}_protos")
    # If this function is responsible for exporting the target, make sure the
    # target has not been defined before. This simplifies the logic to compile
    # protos shared across multiple client libraries.
    if (_opt_EXPORT_TARGET AND TARGET ${protos_target})
        return()
    endif ()

    include(CompileProtos)
    google_cloud_cpp_find_proto_include_dir(PROTO_INCLUDE_DIR)
    google_cloud_cpp_load_protolist(
        proto_list
        "${PROJECT_SOURCE_DIR}/external/googleapis/protolists/${library}.list")
    if (_opt_ADDITIONAL_PROTO_LISTS)
        list(APPEND proto_list "${_opt_ADDITIONAL_PROTO_LISTS}")
    endif ()
    google_cloud_cpp_load_protodeps(
        proto_deps
        "${PROJECT_SOURCE_DIR}/external/googleapis/protodeps/${library}.deps")
    if (_opt_EXPORT_TARGET)
        set(OUT_DIR "${PROJECT_BINARY_DIR}/external/googleapis")
    else ()
        set(OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}")
    endif ()
    google_cloud_cpp_grpcpp_library(
        ${protos_target}
        # cmake-format: sort
        ${proto_list}
        PROTO_PATH_DIRECTORIES
        "${EXTERNAL_GOOGLEAPIS_SOURCE}"
        "${PROTO_INCLUDE_DIR}"
        OUT_DIRECTORY
        ${OUT_DIR})
    external_googleapis_set_version_and_alias(${library}_protos)
    target_link_libraries(${protos_target} PUBLIC ${proto_deps})

    # google_cloud_cpp_install_proto_library_protos(
    #     "${protos_target}" "${EXTERNAL_GOOGLEAPIS_SOURCE}" OUT_DIRECTORY
    #     ${OUT_DIR})
    # google_cloud_cpp_install_proto_library_headers("${protos_target}"
    #                                                OUT_DIRECTORY ${OUT_DIR})

    # external_googleapis_install_pc("${protos_target}")

    if (NOT _opt_EXPORT_TARGET)
        return()
    endif ()

    set(library_target "google_cloud_cpp_${library}")

    # Export the CMake targets to make it easy to create configuration files.
    # install(
    #     EXPORT ${library_target}-targets
    #     DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${library_target}"
    #     COMPONENT google_cloud_cpp_development)

    # Install the libraries and headers in the locations determined by
    # GNUInstallDirs
    # install(
    #     TARGETS ${protos_target}
    #     EXPORT ${library_target}-targets
    #     RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    #             COMPONENT google_cloud_cpp_runtime
    #     LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    #             COMPONENT google_cloud_cpp_runtime
    #             NAMELINK_COMPONENT google_cloud_cpp_development
    #     ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    #             COMPONENT google_cloud_cpp_development)

    # # Create and install the CMake configuration files.
    # include(CMakePackageConfigHelpers)
    # set(GOOGLE_CLOUD_CPP_CONFIG_LIBRARY "${library_target}")
    # configure_file("${PROJECT_SOURCE_DIR}/cmake/templates/config.cmake.in"
    #                "${library_target}-config.cmake" @ONLY)
    # write_basic_package_version_file(
    #     "${library_target}-config-version.cmake"
    #     VERSION ${PROJECT_VERSION}
    #     COMPATIBILITY ExactVersion)

    # install(
    #     FILES
    #         "${CMAKE_CURRENT_BINARY_DIR}/${library_target}-config.cmake"
    #         "${CMAKE_CURRENT_BINARY_DIR}/${library_target}-config-version.cmake"
    #     DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${library_target}"
    #     COMPONENT google_cloud_cpp_development)
endfunction ()

#
# A function to add targets for GAPICS - libraries that use gRPC for transport.
#
# * library:      the short name of the library, e.g. `kms`.
# * display_name: the display name of the library, e.g. "Cloud Key Management
#   Service (KMS) API"
#
# The function respects the following keywords:
#
# * ADDITIONAL_PROTO_LISTS: a list of proto files that may be used indirectly.
#   `asset` sets this.
# * BACKWARDS_COMPAT_PROTO_TARGETS: a list of proto library names (e.g.
#   `cloud_speech_protos`) that must continue to exist. We add interface
#   libraries for these, which link to the desired proto library. See #8022 for
#   more details.
# * CROSS_LIB_DEPS: a list of client libraries which this library depends on.
# * SERVICE_DIRS: a list of service directories within the library. Use
#   "__EMPTY__" to represent the empty string in the list.
# * SHARED_PROTO_DEPS: a list of proto libraries which this library depends on,
#   e.g. `grafeas`. This function will define the proto library targets if they
#   do not already exist.
#
function (google_cloud_cpp_add_gapic_library library display_name)
    cmake_parse_arguments(
        _opt
        "EXPERIMENTAL;TRANSITION"
        ""
        "ADDITIONAL_PROTO_LISTS;BACKWARDS_COMPAT_PROTO_TARGETS;CROSS_LIB_DEPS;SERVICE_DIRS;SHARED_PROTO_DEPS"
        ${ARGN})
    if (_opt_EXPERIMENTAL AND _opt_TRANSITION)
        message(
            FATAL_ERROR
                "EXPERIMENTAL and TRANSITION keywords are mutually exclusive. Only supply one."
        )
    endif ()

    set(library_target "google_cloud_cpp_${library}")
    set(mocks_target "google_cloud_cpp_${library}_mocks")
    set(protos_target "google_cloud_cpp_${library}_protos")
    set(library_alias "google-cloud-cpp::${library}")
    set(experimental_alias "google-cloud-cpp::experimental-${library}")
    if (_opt_EXPERIMENTAL)
        set(library_alias "${experimental_alias}")
    endif ()

    include(GoogleapisConfig)
    set(DOXYGEN_PROJECT_NAME "${display_name} C++ Client")
    set(DOXYGEN_PROJECT_BRIEF "A C++ Client Library for the ${display_name}")
    set(DOXYGEN_PROJECT_NUMBER "${PROJECT_VERSION}")
    if (_opt_EXPERIMENTAL)
        set(DOXYGEN_PROJECT_NUMBER "${PROJECT_VERSION} (Experimental)")
    endif ()
    set(DOXYGEN_EXCLUDE_SYMBOLS "internal")
    set(DOXYGEN_EXAMPLE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/quickstart")
    set(GOOGLE_CLOUD_CPP_DOXYGEN_EXTRA_INCLUDES "${_opt_CROSS_LIB_DEPS}")
    list(TRANSFORM GOOGLE_CLOUD_CPP_DOXYGEN_EXTRA_INCLUDES
         PREPEND "${PROJECT_BINARY_DIR}/google/cloud/")

    unset(mocks_globs)
    unset(source_globs)
    foreach (dir IN LISTS _opt_SERVICE_DIRS)
        if ("${dir}" STREQUAL "__EMPTY__")
            set(dir "")
        endif ()
        string(REPLACE "/" "_" ns "${dir}")
        list(APPEND source_globs "${dir}*.h" "${dir}internal/*.h"
             "${dir}internal/*_sources.cc")
        list(APPEND mocks_globs "${dir}mocks/*.h")
        list(APPEND DOXYGEN_EXCLUDE_SYMBOLS "${library}_${ns}internal")
        if (IS_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/${dir}samples")
            list(APPEND DOXYGEN_EXAMPLE_PATH
                 "${CMAKE_CURRENT_SOURCE_DIR}/${dir}samples")
        endif ()
    endforeach ()

    include(GoogleCloudCppDoxygen)
    google_cloud_cpp_doxygen_targets("${library}" DEPENDS cloud-docs
                                     "google-cloud-cpp::${library}_protos")

    include(GoogleCloudCppCommon)

    include(CompileProtos)

    foreach (lib IN LISTS _opt_SHARED_PROTO_DEPS)
        google_cloud_cpp_add_library_protos("${lib}" EXPORT_TARGET)
    endforeach ()

    google_cloud_cpp_add_library_protos(${library} ADDITIONAL_PROTO_LISTS
                                        ${_opt_ADDITIONAL_PROTO_LISTS})

    set(shared_proto_dep_targets "${_opt_SHARED_PROTO_DEPS}")
    list(TRANSFORM shared_proto_dep_targets PREPEND "google_cloud_cpp_")
    list(TRANSFORM shared_proto_dep_targets APPEND "_protos")

    # We used to offer the proto library by another name. Maintain backwards
    # compatibility by providing an interface library with that name. Also make
    # sure we install it as part of google_cloud_cpp_${library}-targets.
    unset(backwards_compat_proto_targets)
    foreach (old_protos IN LISTS _opt_BACKWARDS_COMPAT_PROTO_TARGETS)
        google_cloud_cpp_backwards_compat_protos_library("${old_protos}"
                                                         "${library}_protos")
        list(APPEND backwards_compat_proto_targets
             "google_cloud_cpp_${old_protos}")
    endforeach ()

    file(
        GLOB source_files
        RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}"
        ${source_globs})
    list(SORT source_files)
    add_library(${library_target} ${source_files})
    target_include_directories(
        ${library_target}
        PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
               $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}>
               $<INSTALL_INTERFACE:include>)
    target_link_libraries(
        ${library_target}
        PUBLIC google-cloud-cpp::grpc_utils google-cloud-cpp::common
               google-cloud-cpp::${library}_protos ${shared_proto_dep_targets})
    google_cloud_cpp_add_common_options(${library_target})
    set_target_properties(
        ${library_target}
        PROPERTIES EXPORT_NAME ${library_alias}
                   VERSION "${PROJECT_VERSION}"
                   SOVERSION "${PROJECT_VERSION_MAJOR}")
    target_compile_options(${library_target}
                           PUBLIC ${GOOGLE_CLOUD_CPP_EXCEPTIONS_FLAG})

    add_library(${library_alias} ALIAS ${library_target})

    unset(transition_target)
    if (_opt_TRANSITION)
        # Define an interface library to allow for a smoother transition from
        # `experimental-foo` -> `foo`.
        set(transition_target "google_cloud_cpp_experimental_${library}")
        set(transition_alias "google-cloud-cpp::experimental-${library}")
        add_library(${transition_target} INTERFACE)
        set_target_properties(${transition_target}
                              PROPERTIES EXPORT_NAME ${transition_alias})
        target_link_libraries(
            ${transition_target}
            PUBLIC
            INTERFACE ${library_alias})
        add_library(${transition_alias} ALIAS ${transition_target})
    endif ()

    # # Get the destination directories based on the GNU recommendations.
    # include(GNUInstallDirs)

    # # Export the CMake targets to make it easy to create configuration files.
    # install(
    #     EXPORT ${library_target}-targets
    #     DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${library_target}"
    #     COMPONENT google_cloud_cpp_development)

    # # Install the libraries and headers in the locations determined by
    # # GNUInstallDirs
    # install(
    #     TARGETS ${library_target} ${protos_target}
    #             ${backwards_compat_proto_targets} ${transition_target}
    #     EXPORT ${library_target}-targets
    #     RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    #             COMPONENT google_cloud_cpp_runtime
    #     LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    #             COMPONENT google_cloud_cpp_runtime
    #             NAMELINK_COMPONENT google_cloud_cpp_development
    #     ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    #             COMPONENT google_cloud_cpp_development)

    # google_cloud_cpp_install_headers("${library_target}"
    #                                  "include/google/cloud/${library}")

    # google_cloud_cpp_add_pkgconfig(
    #     ${library}
    #     "The ${display_name} C++ Client Library"
    #     "Provides C++ APIs to use the ${display_name}"
    #     "google_cloud_cpp_grpc_utils"
    #     "${protos_target}"
    #     ${shared_proto_dep_targets})

    # # Create and install the CMake configuration files.
    # include(CMakePackageConfigHelpers)
    # set(GOOGLE_CLOUD_CPP_CONFIG_LIBRARY "${library_target}")
    # foreach (lib IN LISTS _opt_CROSS_LIB_DEPS _opt_SHARED_PROTO_DEPS)
    #     list(APPEND find_dependencies
    #          "find_dependency(google_cloud_cpp_${lib})")
    # endforeach ()
    # string(JOIN "\n" GOOGLE_CLOUD_CPP_ADDITIONAL_FIND_DEPENDENCIES
    #        ${find_dependencies})
    # configure_file("${PROJECT_SOURCE_DIR}/cmake/templates/config.cmake.in"
    #                "${library_target}-config.cmake" @ONLY)
    # write_basic_package_version_file(
    #     "${library_target}-config-version.cmake"
    #     VERSION ${PROJECT_VERSION}
    #     COMPATIBILITY ExactVersion)

    # install(
    #     FILES
    #         "${CMAKE_CURRENT_BINARY_DIR}/${library_target}-config.cmake"
    #         "${CMAKE_CURRENT_BINARY_DIR}/${library_target}-config-version.cmake"
    #     DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${library_target}"
    #     COMPONENT google_cloud_cpp_development)

    # if (GOOGLE_CLOUD_CPP_WITH_MOCKS)
    #     # Create a header-only library for the mocks. We use a CMake `INTERFACE`
    #     # library for these, a regular library would not work on macOS (where
    #     # the library needs at least one .o file). Unfortunately INTERFACE
    #     # libraries are a bit weird in that they need absolute paths for their
    #     # sources.
    #     file(
    #         GLOB relative_mock_files
    #         RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}"
    #         ${mocks_globs})
    #     list(SORT relative_mock_files)
    #     set(mock_files)
    #     foreach (file IN LISTS relative_mock_files)
    #         # We use a generator expression per the recommendation in:
    #         # https://stackoverflow.com/a/62465051
    #         list(APPEND mock_files
    #              "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/${file}>")
    #     endforeach ()
    #     add_library(${mocks_target} INTERFACE)
    #     target_sources(${mocks_target} INTERFACE ${mock_files})
    #     target_link_libraries(
    #         ${mocks_target} INTERFACE ${library_alias} GTest::gmock
    #                                   GTest::gtest)
    #     set_target_properties(${mocks_target} PROPERTIES EXPORT_NAME
    #                                                      ${library_alias}_mocks)
    #     target_include_directories(
    #         ${mocks_target}
    #         INTERFACE $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
    #                   $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}>
    #                   $<INSTALL_INTERFACE:include>)
    #     target_compile_options(${mocks_target}
    #                            INTERFACE ${GOOGLE_CLOUD_CPP_EXCEPTIONS_FLAG})
    #     google_cloud_cpp_install_mocks("${library}" "${display_name}")
    # endif ()

    # # ${library_alias} must be defined before we can add the samples.
    # if (BUILD_TESTING AND GOOGLE_CLOUD_CPP_ENABLE_CXX_EXCEPTIONS)
    #     foreach (dir IN LISTS _opt_SERVICE_DIRS)
    #         if ("${dir}" STREQUAL "__EMPTY__")
    #             set(dir "")
    #         endif ()
    #         google_cloud_cpp_add_samples_relative("${library}" "${dir}samples/")
    #     endforeach ()
    # endif ()
endfunction ()
