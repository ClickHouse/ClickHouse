/* ******************************************************************
   Error codes list
   Copyright (C) 2016, Yann Collet

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
   - Source repository : https://github.com/Cyan4973/FiniteStateEntropy
   - Public forum : https://groups.google.com/forum/#!forum/lz4c
****************************************************************** */
#ifndef ERROR_PUBLIC_H_MODULE
#define ERROR_PUBLIC_H_MODULE

#if defined (__cplusplus)
extern "C" {
#endif


/* ****************************************
*  error codes list
******************************************/
typedef enum {
  FSE_error_no_error,
  FSE_error_GENERIC,
  FSE_error_dstSize_tooSmall,
  FSE_error_srcSize_wrong,
  FSE_error_corruption_detected,
  FSE_error_tableLog_tooLarge,
  FSE_error_maxSymbolValue_tooLarge,
  FSE_error_maxSymbolValue_tooSmall,
  FSE_error_maxCode
} FSE_ErrorCode;

/* note : compare with size_t function results using FSE_getError() */
        

#if defined (__cplusplus)
}
#endif

#endif /* ERROR_PUBLIC_H_MODULE */
