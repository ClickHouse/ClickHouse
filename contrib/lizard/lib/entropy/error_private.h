/* ******************************************************************
   Error codes and messages
   Copyright (C) 2013-2016, Yann Collet

   BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:

       * Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
       * Redistributions in binary form must reproduce the above
   copyright notice, this list of conditions and the following disclaimer
   in the documentation and/or other materials provided with the
   distribution.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

   You can contact the author at :
   - Homepage : http://www.zstd.net
****************************************************************** */
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
typedef FSE_ErrorCode ERR_enum;
#define PREFIX(name) FSE_error_##name


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
    case PREFIX(dstSize_tooSmall): return "Destination buffer is too small";
    case PREFIX(srcSize_wrong): return "Src size incorrect";
    case PREFIX(corruption_detected): return "Corrupted block detected";
    case PREFIX(tableLog_tooLarge): return "tableLog requires too much memory : unsupported";
    case PREFIX(maxSymbolValue_tooLarge): return "Unsupported max Symbol Value : too large";
    case PREFIX(maxSymbolValue_tooSmall): return "Specified maxSymbolValue is too small";
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
