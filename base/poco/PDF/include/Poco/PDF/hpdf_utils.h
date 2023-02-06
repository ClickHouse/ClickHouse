/*
 * << Haru Free PDF Library >> -- fpdf_utils.h
 *
 * URL: http://libharu.org
 *
 * Copyright (c) 1999-2006 Takeshi Kanno <takeshi_kanno@est.hi-ho.ne.jp>
 * Copyright (c) 2007-2009 Antony Dovgal <tony@daylessday.org>
 *
 * Permission to use, copy, modify, distribute and sell this software
 * and its documentation for any purpose is hereby granted without fee,
 * provided that the above copyright notice appear in all copies and
 * that both that copyright notice and this permission notice appear
 * in supporting documentation.
 * It is provided "as is" without express or implied warranty.
 *
 */

#ifndef _HPDF_UTILS_H
#define _HPDF_UTILS_H

#include "hpdf_config.h"
#include "hpdf_types.h"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

HPDF_INT
HPDF_AToI  (const char*  s);


HPDF_DOUBLE
HPDF_AToF  (const char*  s);


char*
HPDF_IToA  (char*  s,
            HPDF_INT32  val,
            char*  eptr);


char*
HPDF_IToA2  (char    *s,
             HPDF_UINT32  val,
             HPDF_UINT    len);


char*
HPDF_FToA  (char  *s,
            HPDF_REAL  val,
            char  *eptr);


HPDF_BYTE*
HPDF_MemCpy  (HPDF_BYTE*        out,
              const HPDF_BYTE*  in,
              HPDF_UINT         n);


HPDF_BYTE*
HPDF_StrCpy  (char*        out,
              const char*  in,
              char*        eptr);


HPDF_INT
HPDF_MemCmp  (const HPDF_BYTE*  s1,
              const HPDF_BYTE*  s2,
              HPDF_UINT         n);


HPDF_INT
HPDF_StrCmp  (const char*  s1,
              const char*  s2);


const char*
HPDF_StrStr  (const char  *s1,
              const char  *s2,
              HPDF_UINT        maxlen);


void*
HPDF_MemSet  (void*      s,
              HPDF_BYTE  c,
              HPDF_UINT  n);


HPDF_UINT
HPDF_StrLen  (const char*  s,
              HPDF_INT          maxlen);


HPDF_Box
HPDF_ToBox  (HPDF_INT16  left,
             HPDF_INT16  bottom,
             HPDF_INT16  right,
             HPDF_INT16  top);


HPDF_Point
HPDF_ToPoint  (HPDF_INT16  x,
               HPDF_INT16  y);


HPDF_Rect
HPDF_ToRect  (HPDF_REAL  left,
              HPDF_REAL  bottom,
              HPDF_REAL  right,
              HPDF_REAL  top);


void
HPDF_UInt16Swap  (HPDF_UINT16  *value);


#ifdef __cplusplus
}
#endif /* __cplusplus */

#define HPDF_NEEDS_ESCAPE(c)    (c < 0x20 || \
                                 c > 0x7e || \
                                 c == '\\' || \
                                 c == '%' || \
                                 c == '#' || \
                                 c == '/' || \
                                 c == '(' || \
                                 c == ')' || \
                                 c == '<' || \
                                 c == '>' || \
                                 c == '[' || \
                                 c == ']' || \
                                 c == '{' || \
                                 c == '}' )  \

#define HPDF_IS_WHITE_SPACE(c)   (c == 0x00 || \
                                 c == 0x09 || \
                                 c == 0x0A || \
                                 c == 0x0C || \
                                 c == 0x0D || \
                                 c == 0x20 ) \

/*----------------------------------------------------------------------------*/
/*----- macros for debug -----------------------------------------------------*/

#ifdef LIBHPDF_DEBUG_TRACE
#ifndef HPDF_PTRACE_ON
#define HPDF_PTRACE_ON
#endif /* HPDF_PTRACE_ON */
#endif /* LIBHPDF_DEBUG_TRACE */

#ifdef HPDF_PTRACE_ON
#define HPDF_PTRACE(ARGS)  HPDF_PRINTF ARGS
#else
#define HPDF_PTRACE(ARGS)  /* do nothing */
#endif /* HPDF_PTRACE */

#ifdef LIBHPDF_DEBUG
#define HPDF_PRINT_BINARY(BUF, LEN, CAPTION) HPDF_PrintBinary(BUF, LEN, CAPTION)
#else
#define HPDF_PRINT_BINARY(BUF, LEN, CAPTION) /* do nothing */
#endif

#endif /* _HPDF_UTILS_H */

