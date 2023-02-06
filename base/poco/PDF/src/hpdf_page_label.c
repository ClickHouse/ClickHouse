/*
 * << Haru Free PDF Library >> -- hpdf_page_label.c
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

#include "hpdf_conf.h"
#include "hpdf_utils.h"
#include "hpdf_page_label.h"

HPDF_Dict
HPDF_PageLabel_New  (HPDF_Doc             pdf,
                     HPDF_PageNumStyle    style,
                     HPDF_INT             first_page,
                     const char     *prefix)
{
    HPDF_Dict obj = HPDF_Dict_New (pdf->mmgr);

    HPDF_PTRACE ((" HPDF_PageLabel_New\n"));

    if (!obj)
        return NULL;

    switch (style) {
        case HPDF_PAGE_NUM_STYLE_DECIMAL:
            if (HPDF_Dict_AddName (obj, "S", "D") != HPDF_OK)
                goto Fail;
            break;
        case HPDF_PAGE_NUM_STYLE_UPPER_ROMAN:
            if (HPDF_Dict_AddName (obj, "S", "R") != HPDF_OK)
                goto Fail;
            break;
        case HPDF_PAGE_NUM_STYLE_LOWER_ROMAN:
            if (HPDF_Dict_AddName (obj, "S", "r") != HPDF_OK)
                goto Fail;
            break;
        case HPDF_PAGE_NUM_STYLE_UPPER_LETTERS:
            if (HPDF_Dict_AddName (obj, "S", "A") != HPDF_OK)
                goto Fail;
            break;
        case HPDF_PAGE_NUM_STYLE_LOWER_LETTERS:
            if (HPDF_Dict_AddName (obj, "S", "a") != HPDF_OK)
                goto Fail;
            break;
        default:
            HPDF_SetError (&pdf->error, HPDF_PAGE_NUM_STYLE_OUT_OF_RANGE,
                    (HPDF_STATUS)style);
            goto Fail;
    }

    if (prefix && prefix[0] != 0)
        if (HPDF_Dict_Add (obj, "P", HPDF_String_New (pdf->mmgr, prefix,
                    pdf->def_encoder)) != HPDF_OK)
            goto Fail;

    if (first_page != 0)
        if (HPDF_Dict_AddNumber (obj, "St", first_page) != HPDF_OK)
            goto Fail;

    return obj;

Fail:
    HPDF_Dict_Free (obj);
    return NULL;
}

