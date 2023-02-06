/*
 * << Haru Free PDF Library >> -- hpdf_image.h
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

#ifndef _HPDF_IMAGE_H
#define _HPDF_IMAGE_H

#include "hpdf_objects.h"

#ifdef __cplusplus
extern "C" {
#endif

HPDF_Image
HPDF_Image_Load1BitImageFromMem  (HPDF_MMgr  mmgr,
                          const HPDF_BYTE   *buf,
                          HPDF_Xref          xref,
                          HPDF_UINT          width,
                          HPDF_UINT          height,
                          HPDF_UINT          line_width,
                          HPDF_BOOL          top_is_first
                          );


#ifndef LIBHPDF_HAVE_NOPNGLIB

HPDF_Image
HPDF_Image_LoadPngImage  (HPDF_MMgr        mmgr,
                          HPDF_Stream      png_data,
                          HPDF_Xref        xref,
                          HPDF_BOOL        delayed_loading);

#endif

HPDF_Image
HPDF_Image_LoadJpegImage  (HPDF_MMgr        mmgr,
                           HPDF_Stream      jpeg_data,
                           HPDF_Xref        xref);

HPDF_Image
HPDF_Image_LoadJpegImageFromMem  (HPDF_MMgr        mmgr,
                            const HPDF_BYTE       *buf,
                                  HPDF_UINT        size,
                                  HPDF_Xref        xref);

HPDF_Image
HPDF_Image_LoadRawImage  (HPDF_MMgr          mmgr,
                          HPDF_Stream        stream,
                          HPDF_Xref          xref,
                          HPDF_UINT          width,
                          HPDF_UINT          height,
                          HPDF_ColorSpace    color_space);


HPDF_Image
HPDF_Image_LoadRawImageFromMem  (HPDF_MMgr          mmgr,
                                 const HPDF_BYTE   *buf,
                                 HPDF_Xref          xref,
                                 HPDF_UINT          width,
                                 HPDF_UINT          height,
                                 HPDF_ColorSpace    color_space,
                                 HPDF_UINT          bits_per_component);


HPDF_BOOL
HPDF_Image_Validate (HPDF_Image  image);


HPDF_STATUS
HPDF_Image_SetMask (HPDF_Image   image,
                    HPDF_BOOL    mask);

HPDF_STATUS
HPDF_Image_SetColorSpace  (HPDF_Image   image,
                           HPDF_Array   colorspace);

HPDF_STATUS
HPDF_Image_SetRenderingIntent  (HPDF_Image   image,
                                const char* intent);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _HPDF_XOBJECTS_H */

