/*
 * << Haru Free PDF Library >> -- hpdf_doc.h
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


#ifndef _HPDF_DOC_H
#define _HPDF_DOC_H

#define HPDF_SIG_BYTES 0x41504446L

#include "hpdf_catalog.h"
#include "hpdf_image.h"
#include "hpdf_pages.h"
#include "hpdf_outline.h"
#include "hpdf_ext_gstate.h"

#ifdef __cplusplus
extern "C" {
#endif

#define HPDF_VER_DEFAULT  HPDF_VER_12

typedef struct _HPDF_Doc_Rec {
    HPDF_UINT32     sig_bytes;
    HPDF_PDFVer     pdf_version;

    HPDF_MMgr         mmgr;
    HPDF_Catalog      catalog;
    HPDF_Outline      outlines;
    HPDF_Xref         xref;
    HPDF_Pages        root_pages;
    HPDF_Pages        cur_pages;
    HPDF_Page         cur_page;
    HPDF_List         page_list;
    HPDF_Error_Rec    error;
    HPDF_Dict         info;
    HPDF_Dict         trailer;

    HPDF_List         font_mgr;
    HPDF_BYTE         ttfont_tag[6];

    /* list for loaded fontdefs */
    HPDF_List         fontdef_list;

    /* list for loaded encodings */
    HPDF_List         encoder_list;

    HPDF_Encoder      cur_encoder;

    /* default compression mode */
    HPDF_BOOL         compression_mode;

    HPDF_BOOL         encrypt_on;
    HPDF_EncryptDict  encrypt_dict;

    HPDF_Encoder      def_encoder;

    HPDF_UINT         page_per_pages;
    HPDF_UINT         cur_page_num;

    /* buffer for saving into memory stream */
    HPDF_Stream       stream;
} HPDF_Doc_Rec;

typedef struct _HPDF_Doc_Rec  *HPDF_Doc;


HPDF_Encoder
HPDF_Doc_FindEncoder (HPDF_Doc         pdf,
                      const char  *encoding_name);


HPDF_FontDef
HPDF_Doc_FindFontDef (HPDF_Doc         pdf,
                      const char  *font_name);


HPDF_Font
HPDF_Doc_FindFont  (HPDF_Doc         pdf,
                    const char  *font_name,
                    const char  *encoding_name);


HPDF_BOOL
HPDF_Doc_Validate  (HPDF_Doc  pdf);


/*----- page handling -------------------------------------------------------*/

HPDF_Pages
HPDF_Doc_GetCurrentPages  (HPDF_Doc  pdf);


HPDF_Pages
HPDF_Doc_AddPagesTo  (HPDF_Doc     pdf,
                      HPDF_Pages   parent);


HPDF_STATUS
HPDF_Doc_SetCurrentPages  (HPDF_Doc    pdf,
                           HPDF_Pages  pages);


HPDF_STATUS
HPDF_Doc_SetCurrentPage  (HPDF_Doc   pdf,
                          HPDF_Page  page);




/*----- font handling -------------------------------------------------------*/

HPDF_FontDef
HPDF_GetFontDef (HPDF_Doc         pdf,
                 const char  *font_name);


HPDF_STATUS
HPDF_Doc_RegisterFontDef  (HPDF_Doc       pdf,
                           HPDF_FontDef   fontdef);


/*----- encoding handling ---------------------------------------------------*/

HPDF_STATUS
HPDF_Doc_RegisterEncoder  (HPDF_Doc       pdf,
                           HPDF_Encoder   encoder);



/*----- encryptio------------------------------------------------------------*/

HPDF_STATUS
HPDF_Doc_SetEncryptOn (HPDF_Doc  pdf);


HPDF_STATUS
HPDF_Doc_SetEncryptOff (HPDF_Doc  pdf);


HPDF_STATUS
HPDF_Doc_PrepareEncryption (HPDF_Doc  pdf);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _HPDF_DOC_H */

