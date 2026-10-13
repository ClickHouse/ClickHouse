/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
 * SPDX-License-Identifier: Apache-2.0
 */
#pragma once

#define CUDF_VERSION_MAJOR 26
#define CUDF_VERSION_MINOR 8
#define CUDF_VERSION_PATCH 1
#define CUDF_VERSION "26.8.1"
#ifndef CUDF_VERSION_GT
#define CUDF_VERSION_GT(maj, min, pat) \
  ((CUDF_VERSION_MAJOR > (maj)) || \
   ((CUDF_VERSION_MAJOR == (maj)) && \
    ((CUDF_VERSION_MINOR > (min)) || \
     ((CUDF_VERSION_MINOR == (min)) && (CUDF_VERSION_PATCH > (pat))))))
#endif

#ifndef CUDF_VERSION_LT
#define CUDF_VERSION_LT(maj, min, pat) \
  ((CUDF_VERSION_MAJOR < (maj)) || \
   ((CUDF_VERSION_MAJOR == (maj)) && \
    ((CUDF_VERSION_MINOR < (min)) || \
     ((CUDF_VERSION_MINOR == (min)) && (CUDF_VERSION_PATCH < (pat))))))
#endif

#ifndef CUDF_VERSION_EQ
#define CUDF_VERSION_EQ(maj, min, pat) \
  ((CUDF_VERSION_MAJOR == (maj)) && \
   (CUDF_VERSION_MINOR == (min)) && \
   (CUDF_VERSION_PATCH == (pat)))
#endif
