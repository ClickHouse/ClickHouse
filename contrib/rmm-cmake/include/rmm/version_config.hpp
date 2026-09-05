/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
 * SPDX-License-Identifier: Apache-2.0
 */
#pragma once

#define RMM_VERSION_MAJOR 26
#define RMM_VERSION_MINOR 8
#define RMM_VERSION_PATCH 0
#define RMM_VERSION "26.8.0"
#ifndef RMM_VERSION_GT
#define RMM_VERSION_GT(maj, min, pat) \
  ((RMM_VERSION_MAJOR > (maj)) || \
   ((RMM_VERSION_MAJOR == (maj)) && \
    ((RMM_VERSION_MINOR > (min)) || \
     ((RMM_VERSION_MINOR == (min)) && (RMM_VERSION_PATCH > (pat))))))
#endif

#ifndef RMM_VERSION_LT
#define RMM_VERSION_LT(maj, min, pat) \
  ((RMM_VERSION_MAJOR < (maj)) || \
   ((RMM_VERSION_MAJOR == (maj)) && \
    ((RMM_VERSION_MINOR < (min)) || \
     ((RMM_VERSION_MINOR == (min)) && (RMM_VERSION_PATCH < (pat))))))
#endif

#ifndef RMM_VERSION_EQ
#define RMM_VERSION_EQ(maj, min, pat) \
  ((RMM_VERSION_MAJOR == (maj)) && \
   (RMM_VERSION_MINOR == (min)) && \
   (RMM_VERSION_PATCH == (pat)))
#endif
