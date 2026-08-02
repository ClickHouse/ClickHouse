# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

if (USE_CPU_EXTENSIONS)
    if (ENABLE_AVX2)
        set (AVX2_CFLAGS "-mavx -mavx2")
        set (HAVE_AVX2_INTRINSICS 1)
        set (HAVE_MM256_EXTRACT_EPI64 1)
    endif()
endif()

macro(simd_add_definition_if target definition)
    if(${definition})
        target_compile_definitions(${target} PRIVATE -D${definition})
    endif(${definition})
endmacro(simd_add_definition_if)

# Configure private preprocessor definitions for SIMD-related features
# Does not set any processor feature codegen flags
function(simd_add_definitions target)
    simd_add_definition_if(${target} HAVE_AVX2_INTRINSICS)
    simd_add_definition_if(${target} HAVE_MM256_EXTRACT_EPI64)
endfunction(simd_add_definitions)

# Adds source files only if AVX2 is supported. These files will be built with
# avx2 intrinsics enabled.
# Usage: simd_add_source_avx2(target file1.c file2.c ...)
function(simd_add_source_avx2 target)
    foreach(file ${ARGN})
        target_sources(${target} PRIVATE ${file})
        set_source_files_properties(${file} PROPERTIES COMPILE_FLAGS "${AVX2_CFLAGS}")
    endforeach()
endfunction(simd_add_source_avx2)
