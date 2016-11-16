/**
 * Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#ifndef ERROR_PUBLIC_H_MODULE
#define ERROR_PUBLIC_H_MODULE

#if defined (__cplusplus)
extern "C" {
#endif

/*===== dependency =====*/
#include <stddef.h>   /* size_t */


/*-****************************************
*  error codes list
******************************************/
typedef enum {
  ZSTD_error_no_error,
  ZSTD_error_GENERIC,
  ZSTD_error_prefix_unknown,
  ZSTD_error_version_unsupported,
  ZSTD_error_parameter_unknown,
  ZSTD_error_frameParameter_unsupported,
  ZSTD_error_frameParameter_unsupportedBy32bits,
  ZSTD_error_compressionParameter_unsupported,
  ZSTD_error_init_missing,
  ZSTD_error_memory_allocation,
  ZSTD_error_stage_wrong,
  ZSTD_error_dstSize_tooSmall,
  ZSTD_error_srcSize_wrong,
  ZSTD_error_corruption_detected,
  ZSTD_error_checksum_wrong,
  ZSTD_error_tableLog_tooLarge,
  ZSTD_error_maxSymbolValue_tooLarge,
  ZSTD_error_maxSymbolValue_tooSmall,
  ZSTD_error_dictionary_corrupted,
  ZSTD_error_dictionary_wrong,
  ZSTD_error_maxCode
} ZSTD_ErrorCode;

/*! ZSTD_getErrorCode() :
    convert a `size_t` function result into a `ZSTD_ErrorCode` enum type,
    which can be used to compare directly with enum list published into "error_public.h" */
ZSTD_ErrorCode ZSTD_getErrorCode(size_t functionResult);
const char* ZSTD_getErrorString(ZSTD_ErrorCode code);


#if defined (__cplusplus)
}
#endif

#endif /* ERROR_PUBLIC_H_MODULE */
