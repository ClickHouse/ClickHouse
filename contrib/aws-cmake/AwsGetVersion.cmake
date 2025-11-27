# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

function(aws_get_version var_version_major var_version_minor var_version_patch var_version_full var_git_hash)
    # Simple version is "MAJOR.MINOR.PATCH" from VERSION file
    file(READ "${AWS_CRT_DIR}/VERSION" version_simple)
    string(STRIP ${version_simple} version_simple)
    set(${var_version_simple} ${version_simple} PARENT_SCOPE)

    string(REPLACE "." ";" VERSION_LIST ${version_simple})
    list(GET VERSION_LIST 0 version_major)
    list(GET VERSION_LIST 1 version_minor)
    list(GET VERSION_LIST 2 version_patch)
    set(${var_version_major} ${version_major} PARENT_SCOPE)
    set(${var_version_minor} ${version_minor} PARENT_SCOPE)
    set(${var_version_patch} ${version_patch} PARENT_SCOPE)

    # By default, full version is same as simple version.
    # But we'll make it more specific later, if we determine that we're not at an exact tagged commit.
    set(${var_version_full} ${version_simple} PARENT_SCOPE)

    # Get git hash
    aws_git_try("rev-parse HEAD" git_hash git_success)
    if (git_success)
        set(${var_git_hash} ${git_hash} PARENT_SCOPE)

        # Determine if we're at the exact tagged commit
        set(is_exact_version FALSE)
        aws_git_try("tag --points-at HEAD" head_tags git_success)
        if (git_success)
            foreach(tag IN LISTS head_tags) # cmake 3.3+ could use IN_LIST instead
                if (tag STREQUAL "v${version_simple}")
                    set(is_exact_version TRUE)
                endif()
            endforeach()
        endif()

        # Full version should indicate when we're not at the exact tagged commit.
        # Be compliant with https://semver.org
        if (NOT is_exact_version)
            aws_git_try("rev-parse --short=8 HEAD" git_hash_short git_success)
            if (git_success)
                set(${var_version_full} "${version_simple}-dev+${git_hash_short}" PARENT_SCOPE)
            endif()
        endif()
    endif()
endfunction()

find_package(Git QUIET)

function(aws_git_try args var_output var_success)
    set(${var_success} FALSE PARENT_SCOPE)
    set(${var_output} "" PARENT_SCOPE)

    if (GIT_FOUND)
        separate_arguments(args UNIX_COMMAND ${args})
        execute_process(COMMAND ${GIT_EXECUTABLE} ${args}
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            RESULT_VARIABLE git_result
            OUTPUT_VARIABLE git_output
            OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_QUIET)
        if (git_result EQUAL 0)
            string(REPLACE "\n" ";" git_output "${git_output}")
            set(${var_success} TRUE PARENT_SCOPE)
            set(${var_output} ${git_output} PARENT_SCOPE)
        endif()
    endif()
endfunction()
