# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

include(CheckCCompilerFlag)
include(CheckIncludeFile)

if (USE_CPU_EXTENSIONS)
    if (MSVC)
        check_c_compiler_flag("/arch:AVX2" HAVE_M_AVX2_FLAG)
        if (HAVE_M_AVX2_FLAG)
            set(AVX2_CFLAGS "/arch:AVX2")
        endif()
    else()
        check_c_compiler_flag(-mavx2 HAVE_M_AVX2_FLAG)
        if (HAVE_M_AVX2_FLAG)
            set(AVX2_CFLAGS "-mavx -mavx2")
        endif()
    endif()


    cmake_push_check_state()
    set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} ${AVX2_CFLAGS}")

    check_c_source_compiles("
        #include <immintrin.h>
        #include <emmintrin.h>
        #include <string.h>

        int main() {
            __m256i vec;
            memset(&vec, 0, sizeof(vec));

            _mm256_shuffle_epi8(vec, vec);
            _mm256_set_epi32(1,2,3,4,5,6,7,8);
            _mm256_permutevar8x32_epi32(vec, vec);

            return 0;
        }"  HAVE_AVX2_INTRINSICS)

    check_c_source_compiles("
        #include <immintrin.h>
        #include <string.h>

        int main() {
            __m256i vec;
            memset(&vec, 0, sizeof(vec));
            return (int)_mm256_extract_epi64(vec, 2);
        }" HAVE_MM256_EXTRACT_EPI64)

    cmake_pop_check_state()
endif() # USE_CPU_EXTENSIONS

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
