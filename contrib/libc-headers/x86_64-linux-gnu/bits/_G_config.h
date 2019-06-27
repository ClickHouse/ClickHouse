/* This file is needed by libio to define various configuration parameters.
   These are always the same in the GNU C library.  */

#ifndef _BITS_G_CONFIG_H
#define _BITS_G_CONFIG_H 1

#if !defined _BITS_LIBIO_H && !defined _G_CONFIG_H
# error "Never include <bits/_G_config.h> directly; use <stdio.h> instead."
#endif

/* Define types for libio in terms of the standard internal type names.  */

#include <bits/types.h>
#define __need_size_t
#if defined _LIBC || defined _GLIBCPP_USE_WCHAR_T
# define __need_wchar_t
#endif
#define __need_NULL
#include <stddef.h>

#include <bits/types/__mbstate_t.h>
#if defined _LIBC || defined _GLIBCPP_USE_WCHAR_T
# include <bits/types/wint_t.h>
#endif

typedef struct
{
  __off_t __pos;
  __mbstate_t __state;
} _G_fpos_t;
typedef struct
{
  __off64_t __pos;
  __mbstate_t __state;
} _G_fpos64_t;
#if defined _LIBC || defined _GLIBCPP_USE_WCHAR_T
# include <gconv.h>
typedef union
{
  struct __gconv_info __cd;
  struct
  {
    struct __gconv_info __cd;
    struct __gconv_step_data __data;
  } __combined;
} _G_iconv_t;
#endif


/* These library features are always available in the GNU C library.  */
#define _G_va_list __gnuc_va_list

#define _G_HAVE_MMAP 1
#define _G_HAVE_MREMAP 1

#define _G_IO_IO_FILE_VERSION 0x20001

/* This is defined by <bits/stat.h> if `st_blksize' exists.  */
#define _G_HAVE_ST_BLKSIZE defined (_STATBUF_ST_BLKSIZE)

#define _G_BUFSIZ 8192

#endif	/* bits/_G_config.h */
