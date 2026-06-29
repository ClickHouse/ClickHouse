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

    # Don't include the hash of HEAD in a config file. It's just terrible for build caching and useless
    set(var_git_hash "" PARENT_SCOPE)
endfunction()
