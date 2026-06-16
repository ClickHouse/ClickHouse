# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

option(USE_CPU_EXTENSIONS "Whenever possible, use functions optimized for CPUs with specific extensions (ex: SSE, AVX)." ON)

if (ARCH_AMD64)
    set (AWS_ARCH_INTEL 1)
elseif (ARCH_AARCH64)
    set (AWS_ARCH_ARM64 1)
endif ()

set (AWS_HAVE_GCC_INLINE_ASM 1)
set (AWS_HAVE_AUXV 1)
