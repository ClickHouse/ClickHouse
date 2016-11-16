/**
 * Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

/* Note : this module is expected to remain private, do not expose it */

#ifndef ERROR_H_MODULE
#define ERROR_H_MODULE

#if defined (__cplusplus)
extern "C" {
#endif


/* ****************************************
*  Dependencies
******************************************/
#include <stddef.h>        /* size_t */
#include "error_public.h"  /* enum list */


/* ****************************************
*  Compiler-specific
******************************************/
#if defined(__GNUC__)
#  define ERR_STATIC static __attribute__((unused))
#elif defined (__cplusplus) || (defined (__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L) /* C99 */)
#  define ERR_STATIC static inline
#elif defined(_MSC_VER)
#  define ERR_STATIC static __inline
#else
#  define ERR_STATIC static  /* this version may generate warnings for unused static functions; disable the relevant warning */
#endif


/*-****************************************
*  Customization (error_public.h)
******************************************/
typedef ZSTD_ErrorCode ERR_enum;
#define PREFIX(name) ZSTD_error_##name


/*-****************************************
*  Error codes handling
******************************************/
#ifdef ERROR
#  undef ERROR   /* reported already defined on VS 2015 (Rich Geldreich) */
#endif
#define ERROR(name) ((size_t)-PREFIX(name))

ERR_STATIC unsigned ERR_isError(size_t code) { return (code > ERROR(maxCode)); }

ERR_STATIC ERR_enum ERR_getErrorCode(size_t code) { if (!ERR_isError(code)) return (ERR_enum)0; return (ERR_enum) (0-code); }


/*-****************************************
*  Error Strings
******************************************/

ERR_STATIC const char* ERR_getErrorString(ERR_enum code)
{
    static const char* notErrorCode = "Unspecified error code";
    switch( code )
    {
    case PREFIX(no_error): return "No error detected";
    case PREFIX(GENERIC):  return "Error (generic)";
    case PREFIX(prefix_unknown): return "Unknown frame descriptor";
    case PREFIX(version_unsupported): return "Version not supported";
    case PREFIX(parameter_unknown): return "Unknown parameter type";
    case PREFIX(frameParameter_unsupported): return "Unsupported frame parameter";
    case PREFIX(frameParameter_unsupportedBy32bits): return "Frame parameter unsupported in 32-bits mode";
    case PREFIX(compressionParameter_unsupported): return "Compression parameter is out of bound";
    case PREFIX(init_missing): return "Context should be init first";
    case PREFIX(memory_allocation): return "Allocation error : not enough memory";
    case PREFIX(stage_wrong): return "Operation not authorized at current processing stage";
    case PREFIX(dstSize_tooSmall): return "Destination buffer is too small";
    case PREFIX(srcSize_wrong): return "Src size incorrect";
    case PREFIX(corruption_detected): return "Corrupted block detected";
    case PREFIX(checksum_wrong): return "Restored data doesn't match checksum";
    case PREFIX(tableLog_tooLarge): return "tableLog requires too much memory : unsupported";
    case PREFIX(maxSymbolValue_tooLarge): return "Unsupported max Symbol Value : too large";
    case PREFIX(maxSymbolValue_tooSmall): return "Specified maxSymbolValue is too small";
    case PREFIX(dictionary_corrupted): return "Dictionary is corrupted";
    case PREFIX(dictionary_wrong): return "Dictionary mismatch";
    case PREFIX(maxCode):
    default: return notErrorCode;
    }
}

ERR_STATIC const char* ERR_getErrorName(size_t code)
{
    return ERR_getErrorString(ERR_getErrorCode(code));
}

#if defined (__cplusplus)
}
#endif

#endif /* ERROR_H_MODULE */
