/*
 * << Haru Free PDF Library >> -- hpdf_fontdef.c
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
#include "hpdf_fontdef.h"

void
HPDF_FontDef_Cleanup (HPDF_FontDef  fontdef)
{
    if (!fontdef)
        return;

    HPDF_PTRACE ((" HPDF_FontDef_Cleanup\n"));

    if (fontdef->clean_fn)
        fontdef->clean_fn (fontdef);

    fontdef->descriptor = NULL;
}

void
HPDF_FontDef_Free (HPDF_FontDef  fontdef)
{
    if (!fontdef)
        return;

    HPDF_PTRACE ((" HPDF_FontDef_Free\n"));

    if (fontdef->free_fn)
        fontdef->free_fn (fontdef);
    HPDF_FreeMem (fontdef->mmgr, fontdef);
}

HPDF_BOOL
HPDF_FontDef_Validate (HPDF_FontDef  fontdef)
{
    HPDF_PTRACE ((" HPDF_FontDef_Validate\n"));

    if (!fontdef || fontdef->sig_bytes != HPDF_FONTDEF_SIG_BYTES)
        return HPDF_FALSE;
    else
        return HPDF_TRUE;
}

