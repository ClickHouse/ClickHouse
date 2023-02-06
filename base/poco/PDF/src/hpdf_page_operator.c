/*
 * << Haru Free PDF Library >> -- hpdf_page_operator.c
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
#include "hpdf_pages.h"
#include "hpdf.h"

static const HPDF_Point INIT_POS = {0, 0};
static const HPDF_DashMode INIT_MODE = {{0, 0, 0, 0, 0, 0, 0, 0}, 0, 0};


static HPDF_STATUS
InternalWriteText  (HPDF_PageAttr    attr,
                    const char      *text);


static HPDF_STATUS
InternalArc  (HPDF_Page    page,
              HPDF_REAL    x,
              HPDF_REAL    y,
              HPDF_REAL    ray,
              HPDF_REAL    ang1,
              HPDF_REAL    ang2,
              HPDF_BOOL    cont_flg);


static HPDF_STATUS
InternalShowTextNextLine  (HPDF_Page    page,
                           const char  *text,
                           HPDF_UINT    len);



/*--- General graphics state ---------------------------------------------*/

/* w */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetLineWidth  (HPDF_Page  page,
                         HPDF_REAL  line_width)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_SetLineWidth\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (line_width < 0)
        return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE, 0);

    if (HPDF_Stream_WriteReal (attr->stream, line_width) != HPDF_OK)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteStr (attr->stream, " w\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->line_width = line_width;

    return ret;
}

/* J */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetLineCap  (HPDF_Page     page,
                       HPDF_LineCap  line_cap)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_SetLineCap\n"));

    if (ret != HPDF_OK)
        return ret;

    if (line_cap >= HPDF_LINECAP_EOF)
        return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE,
                (HPDF_STATUS)line_cap);

    attr = (HPDF_PageAttr)page->attr;

    if ((ret = HPDF_Stream_WriteInt (attr->stream,
                (HPDF_UINT)line_cap)) != HPDF_OK)
        return ret;

    if ((ret = HPDF_Stream_WriteStr (attr->stream,
                " J\012")) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->line_cap = line_cap;

    return ret;
}

/* j */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetLineJoin  (HPDF_Page      page,
                        HPDF_LineJoin  line_join)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_SetLineJoin\n"));

    if (ret != HPDF_OK)
        return ret;

    if (line_join >= HPDF_LINEJOIN_EOF)
        return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE,
                (HPDF_STATUS)line_join);

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteInt (attr->stream, (HPDF_UINT)line_join) != HPDF_OK)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteStr (attr->stream, " j\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->line_join = line_join;

    return ret;
}

/* M */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetMiterLimit  (HPDF_Page  page,
                          HPDF_REAL  miter_limit)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_SetMitterLimit\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (miter_limit < 1)
        return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE, 0);

    if (HPDF_Stream_WriteReal (attr->stream, miter_limit) != HPDF_OK)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteStr (attr->stream, " M\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->miter_limit = miter_limit;

    return ret;
}

/* d */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetDash  (HPDF_Page           page,
                    const HPDF_UINT16  *dash_ptn,
                    HPDF_UINT           num_param,
                    HPDF_UINT           phase)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_TEXT_OBJECT);
    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    const HPDF_UINT16 *pdash_ptn = dash_ptn;
    HPDF_PageAttr attr;
    HPDF_UINT i;

    HPDF_PTRACE ((" HPDF_Page_SetDash\n"));

    if (ret != HPDF_OK)
        return ret;

    if (num_param != 1 && (num_param / 2) * 2 != num_param)
        return HPDF_RaiseError (page->error, HPDF_PAGE_INVALID_PARAM_COUNT,
                num_param);

    if (num_param == 0 && phase > 0)
        return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE,
                phase);

    if (!dash_ptn && num_param > 0)
        return HPDF_RaiseError (page->error, HPDF_INVALID_PARAMETER,
                phase);

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);
    *pbuf++ = '[';

    for (i = 0; i < num_param; i++) {
        if (*pdash_ptn == 0 || *pdash_ptn > HPDF_MAX_DASH_PATTERN)
            return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE, 0);

        pbuf = HPDF_IToA (pbuf, *pdash_ptn, eptr);
        *pbuf++ = ' ';
        pdash_ptn++;
    }

    *pbuf++ = ']';
    *pbuf++ = ' ';

    pbuf = HPDF_IToA (pbuf, phase, eptr);
    HPDF_StrCpy (pbuf, " d\012", eptr);

    attr = (HPDF_PageAttr)page->attr;

    if ((ret = HPDF_Stream_WriteStr (attr->stream, buf)) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->dash_mode = INIT_MODE;
    attr->gstate->dash_mode.num_ptn = num_param;
    attr->gstate->dash_mode.phase = phase;

    pdash_ptn = dash_ptn;
    for (i = 0; i < num_param; i++) {
        attr->gstate->dash_mode.ptn[i] = *pdash_ptn;
        pdash_ptn++;
    }

    return ret;
}


/* ri --not implemented yet */

/* i */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetFlat  (HPDF_Page  page,
                    HPDF_REAL  flatness)
{
    HPDF_PageAttr attr;
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_TEXT_OBJECT);

    HPDF_PTRACE ((" HPDF_Page_SetFlat\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (flatness > 100 || flatness < 0)
        return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE, 0);

    if (HPDF_Stream_WriteReal (attr->stream, flatness) != HPDF_OK)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteStr (attr->stream, " i\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->flatness = flatness;

    return ret;
}

/* gs */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetExtGState  (HPDF_Page        page,
                         HPDF_ExtGState   ext_gstate)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION);
    HPDF_PageAttr attr;
    const char *local_name;

    HPDF_PTRACE ((" HPDF_Page_SetExtGState\n"));

    if (ret != HPDF_OK)
        return ret;

    if (!HPDF_ExtGState_Validate (ext_gstate))
        return HPDF_RaiseError (page->error, HPDF_INVALID_OBJECT, 0);

    if (page->mmgr != ext_gstate->mmgr)
        return HPDF_RaiseError (page->error, HPDF_INVALID_EXT_GSTATE, 0);

    attr = (HPDF_PageAttr)page->attr;
    local_name = HPDF_Page_GetExtGStateName (page, ext_gstate);

    if (!local_name)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteEscapeName (attr->stream, local_name) != HPDF_OK)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteStr (attr->stream, " gs\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    /* change objct class to read only. */
    ext_gstate->header.obj_class = (HPDF_OSUBCLASS_EXT_GSTATE_R | HPDF_OCLASS_DICT);

    return ret;
}


/*--- Special graphic state operator --------------------------------------*/

/* q */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_GSave  (HPDF_Page  page)
{
    HPDF_GState new_gstate;
    HPDF_PageAttr attr;
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION);

    HPDF_PTRACE ((" HPDF_Page_GSave\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    new_gstate = HPDF_GState_New (page->mmgr, attr->gstate);
    if (!new_gstate)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteStr (attr->stream, "q\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate = new_gstate;

    return ret;
}

/* Q */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_GRestore  (HPDF_Page  page)
{
    HPDF_GState new_gstate;
    HPDF_PageAttr attr;
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION);

    HPDF_PTRACE ((" HPDF_Page_GRestore\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (!attr->gstate->prev)
        return HPDF_RaiseError (page->error, HPDF_PAGE_CANNOT_RESTORE_GSTATE,
                0);

    new_gstate = HPDF_GState_Free (page->mmgr, attr->gstate);

    attr->gstate = new_gstate;

    if (HPDF_Stream_WriteStr (attr->stream, "Q\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    return ret;
}

/* cm */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_Concat  (HPDF_Page         page,
                   HPDF_REAL         a,
                   HPDF_REAL         b,
                   HPDF_REAL         c,
                   HPDF_REAL         d,
                   HPDF_REAL         x,
                   HPDF_REAL         y)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION);
    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    HPDF_PageAttr attr;
    HPDF_TransMatrix tm;

    HPDF_PTRACE ((" HPDF_Page_Concat\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);

    pbuf = HPDF_FToA (pbuf, a, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, b, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, c, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, d, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y, eptr);
    HPDF_StrCpy (pbuf, " cm\012", eptr);

    if (HPDF_Stream_WriteStr (attr->stream, buf) != HPDF_OK)
        return HPDF_CheckError (page->error);

    tm = attr->gstate->trans_matrix;

    attr->gstate->trans_matrix.a = tm.a * a + tm.b * c;
    attr->gstate->trans_matrix.b = tm.a * b + tm.b * d;
    attr->gstate->trans_matrix.c = tm.c * a + tm.d * c;
    attr->gstate->trans_matrix.d = tm.c * b + tm.d * d;
    attr->gstate->trans_matrix.x = tm.x + x * tm.a + y * tm.c;
    attr->gstate->trans_matrix.y = tm.y + x * tm.b + y * tm.d;

    return ret;
}

/*--- Path construction operator ------------------------------------------*/

/* m */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_MoveTo  (HPDF_Page  page,
                   HPDF_REAL  x,
                   HPDF_REAL  y)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_PATH_OBJECT);
    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_MoveTo\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);

    pbuf = HPDF_FToA (pbuf, x, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y, eptr);
    HPDF_StrCpy (pbuf, " m\012", eptr);

    if (HPDF_Stream_WriteStr (attr->stream, buf) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->cur_pos.x = x;
    attr->cur_pos.y = y;
    attr->str_pos = attr->cur_pos;
    attr->gmode = HPDF_GMODE_PATH_OBJECT;

    return ret;
}

/* l */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_LineTo  (HPDF_Page  page,
                   HPDF_REAL  x,
                   HPDF_REAL  y)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PATH_OBJECT);
    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_LineTo\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);

    pbuf = HPDF_FToA (pbuf, x, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y, eptr);
    HPDF_StrCpy (pbuf, " l\012", eptr);

    if (HPDF_Stream_WriteStr (attr->stream, buf) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->cur_pos.x = x;
    attr->cur_pos.y = y;

    return ret;
}

/* c */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_CurveTo  (HPDF_Page  page,
                    HPDF_REAL  x1,
                    HPDF_REAL  y1,
                    HPDF_REAL  x2,
                    HPDF_REAL  y2,
                    HPDF_REAL  x3,
                    HPDF_REAL  y3)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PATH_OBJECT);
    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_CurveTo\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);

    pbuf = HPDF_FToA (pbuf, x1, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y1, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x2, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y2, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x3, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y3, eptr);
    HPDF_StrCpy (pbuf, " c\012", eptr);

    if (HPDF_Stream_WriteStr (attr->stream, buf) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->cur_pos.x = x3;
    attr->cur_pos.y = y3;

    return ret;
}

/* v */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_CurveTo2  (HPDF_Page  page,
                     HPDF_REAL  x2,
                     HPDF_REAL  y2,
                     HPDF_REAL  x3,
                     HPDF_REAL  y3)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PATH_OBJECT);
    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_CurveTo2\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);

    pbuf = HPDF_FToA (pbuf, x2, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y2, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x3, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y3, eptr);
    HPDF_StrCpy (pbuf, " v\012", eptr);

    if (HPDF_Stream_WriteStr (attr->stream, buf) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->cur_pos.x = x3;
    attr->cur_pos.y = y3;

    return ret;
}

/* y */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_CurveTo3  (HPDF_Page  page,
                     HPDF_REAL  x1,
                     HPDF_REAL  y1,
                     HPDF_REAL  x3,
                     HPDF_REAL  y3)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PATH_OBJECT);
    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_CurveTo3\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);

    pbuf = HPDF_FToA (pbuf, x1, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y1, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x3, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y3, eptr);
    HPDF_StrCpy (pbuf, " y\012", eptr);

    if (HPDF_Stream_WriteStr (attr->stream, buf) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->cur_pos.x = x3;
    attr->cur_pos.y = y3;

    return ret;
}

/* h */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_ClosePath  (HPDF_Page  page)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PATH_OBJECT);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_ClosePath\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteStr (attr->stream, "h\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->cur_pos = attr->str_pos;

    return ret;
}

/* re */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_Rectangle  (HPDF_Page  page,
                      HPDF_REAL  x,
                      HPDF_REAL  y,
                      HPDF_REAL  width,
                      HPDF_REAL  height)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_PATH_OBJECT);
    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_Rectangle\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);

    pbuf = HPDF_FToA (pbuf, x, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, width, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, height, eptr);
    HPDF_StrCpy (pbuf, " re\012", eptr);

    if (HPDF_Stream_WriteStr (attr->stream, buf) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->cur_pos.x = x;
    attr->cur_pos.y = y;
    attr->str_pos = attr->cur_pos;
    attr->gmode = HPDF_GMODE_PATH_OBJECT;

    return ret;
}


/*--- Path painting operator ---------------------------------------------*/

/* S */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_Stroke  (HPDF_Page  page)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PATH_OBJECT |
        HPDF_GMODE_CLIPPING_PATH);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_Stroke\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteStr (attr->stream, "S\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gmode = HPDF_GMODE_PAGE_DESCRIPTION;
    attr->cur_pos = INIT_POS;

    return ret;
}

/* s */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_ClosePathStroke  (HPDF_Page  page)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PATH_OBJECT |
            HPDF_GMODE_CLIPPING_PATH);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_ClosePathStroke\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteStr (attr->stream, "s\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gmode = HPDF_GMODE_PAGE_DESCRIPTION;
    attr->cur_pos = INIT_POS;

    return ret;
}

/* f */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_Fill  (HPDF_Page  page)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PATH_OBJECT |
            HPDF_GMODE_CLIPPING_PATH);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_Fill\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteStr (attr->stream, "f\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gmode = HPDF_GMODE_PAGE_DESCRIPTION;
    attr->cur_pos = INIT_POS;

    return ret;
}

/* f* */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_Eofill  (HPDF_Page  page)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PATH_OBJECT |
            HPDF_GMODE_CLIPPING_PATH);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_Eofill\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteStr (attr->stream, "f*\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gmode = HPDF_GMODE_PAGE_DESCRIPTION;
    attr->cur_pos = INIT_POS;

    return ret;
}

/* B */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_FillStroke  (HPDF_Page  page)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PATH_OBJECT |
            HPDF_GMODE_CLIPPING_PATH);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_FillStroke\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteStr (attr->stream, "B\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gmode = HPDF_GMODE_PAGE_DESCRIPTION;
    attr->cur_pos = INIT_POS;

    return ret;
}

/* B* */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_EofillStroke  (HPDF_Page  page)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PATH_OBJECT |
            HPDF_GMODE_CLIPPING_PATH);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_EofillStroke\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteStr (attr->stream, "B*\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gmode = HPDF_GMODE_PAGE_DESCRIPTION;

    return ret;
}

/* b */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_ClosePathFillStroke  (HPDF_Page  page)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PATH_OBJECT |
            HPDF_GMODE_CLIPPING_PATH);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_ClosePathFillStroke\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteStr (attr->stream, "b\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gmode = HPDF_GMODE_PAGE_DESCRIPTION;
    attr->cur_pos = INIT_POS;

    return ret;
}

/* b* */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_ClosePathEofillStroke  (HPDF_Page  page)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PATH_OBJECT |
            HPDF_GMODE_CLIPPING_PATH);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_ClosePathEofillStroke\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteStr (attr->stream, "b*\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gmode = HPDF_GMODE_PAGE_DESCRIPTION;
    attr->cur_pos = INIT_POS;

    return ret;
}

/* n */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_EndPath  (HPDF_Page  page)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PATH_OBJECT |
            HPDF_GMODE_CLIPPING_PATH);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_PageEndPath\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteStr (attr->stream, "n\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gmode = HPDF_GMODE_PAGE_DESCRIPTION;
    attr->cur_pos = INIT_POS;

    return ret;
}


/*--- Clipping paths operator --------------------------------------------*/

/* W */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_Clip  (HPDF_Page  page)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PATH_OBJECT);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_Clip\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteStr (attr->stream, "W\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gmode = HPDF_GMODE_CLIPPING_PATH;

    return ret;
}

/* W* */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_Eoclip  (HPDF_Page  page)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PATH_OBJECT);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_Eoclip\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteStr (attr->stream, "W*\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gmode = HPDF_GMODE_CLIPPING_PATH;

    return ret;
}


/*--- Text object operator -----------------------------------------------*/

/* BT */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_BeginText  (HPDF_Page  page)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION);
    HPDF_PageAttr attr;
    const HPDF_TransMatrix INIT_MATRIX = {1, 0, 0, 1, 0, 0};

    HPDF_PTRACE ((" HPDF_Page_BeginText\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteStr (attr->stream, "BT\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gmode = HPDF_GMODE_TEXT_OBJECT;
    attr->text_pos = INIT_POS;
    attr->text_matrix = INIT_MATRIX;

    return ret;
}

/* ET */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_EndText  (HPDF_Page  page)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_EndText\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteStr (attr->stream, "ET\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->text_pos = INIT_POS;
    attr->gmode = HPDF_GMODE_PAGE_DESCRIPTION;

    return ret;
}

/*--- Text state ---------------------------------------------------------*/

/* Tc */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetCharSpace  (HPDF_Page  page,
                         HPDF_REAL  value)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_SetCharSpace\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (value < HPDF_MIN_CHARSPACE || value > HPDF_MAX_CHARSPACE)
        return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE, 0);

    if (HPDF_Stream_WriteReal (attr->stream, value) != HPDF_OK)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteStr (attr->stream, " Tc\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->char_space = value;

    return ret;
}

/* Tw */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetWordSpace  (HPDF_Page  page,
                         HPDF_REAL  value)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_SetWordSpace\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (value < HPDF_MIN_WORDSPACE || value > HPDF_MAX_WORDSPACE)
        return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE, 0);

    if (HPDF_Stream_WriteReal (attr->stream, value) != HPDF_OK)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteStr (attr->stream, " Tw\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->word_space = value;

    return ret;
}

/* Tz */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetHorizontalScalling  (HPDF_Page  page,
                                  HPDF_REAL  value)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_SetHorizontalScalling\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (value < HPDF_MIN_HORIZONTALSCALING ||
            value > HPDF_MAX_HORIZONTALSCALING)
        return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE, 0);

    if (HPDF_Stream_WriteReal (attr->stream, value) != HPDF_OK)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteStr (attr->stream, " Tz\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->h_scalling = value;

    return ret;
}

/* TL */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetTextLeading  (HPDF_Page  page,
                           HPDF_REAL  value)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_SetTextLeading\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteReal (attr->stream, value) != HPDF_OK)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteStr (attr->stream, " TL\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->text_leading = value;

    return ret;
}

/* Tf */

HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetFontAndSize  (HPDF_Page  page,
                           HPDF_Font  font,
                           HPDF_REAL  size)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_TEXT_OBJECT);
    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    const char *local_name;
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_SetFontAndSize\n"));

    if (ret != HPDF_OK)
        return ret;

    if (!HPDF_Font_Validate (font))
        return HPDF_RaiseError (page->error, HPDF_PAGE_INVALID_FONT, 0);

    if (size <= 0 || size > HPDF_MAX_FONTSIZE)
        return HPDF_RaiseError (page->error, HPDF_PAGE_INVALID_FONT_SIZE, size);

    if (page->mmgr != font->mmgr)
        return HPDF_RaiseError (page->error, HPDF_PAGE_INVALID_FONT, 0);

    attr = (HPDF_PageAttr)page->attr;
    local_name = HPDF_Page_GetLocalFontName (page, font);

    if (!local_name)
        return HPDF_RaiseError (page->error, HPDF_PAGE_INVALID_FONT, 0);

    if (HPDF_Stream_WriteEscapeName (attr->stream, local_name) != HPDF_OK)
        return HPDF_CheckError (page->error);

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, size, eptr);
    HPDF_StrCpy (pbuf, " Tf\012", eptr);

    if (HPDF_Stream_WriteStr (attr->stream, buf) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->font = font;
    attr->gstate->font_size = size;
    attr->gstate->writing_mode = ((HPDF_FontAttr)font->attr)->writing_mode;

    return ret;
}

/* Tr */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetTextRenderingMode  (HPDF_Page               page,
                                 HPDF_TextRenderingMode  mode)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_SetTextRenderingMode\n"));

    if (ret != HPDF_OK)
        return ret;

    if (mode >= HPDF_RENDERING_MODE_EOF)
        return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE,
                (HPDF_STATUS)mode);

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteInt (attr->stream, (HPDF_INT)mode) != HPDF_OK)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteStr (attr->stream, " Tr\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->rendering_mode = mode;

    return ret;
}

/* Ts */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetTextRaise  (HPDF_Page  page,
                         HPDF_REAL  value)
{
    return HPDF_Page_SetTextRise (page, value);
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetTextRise  (HPDF_Page  page,
                        HPDF_REAL  value)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_SetTextRaise\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteReal (attr->stream, value) != HPDF_OK)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteStr (attr->stream, " Ts\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->text_rise = value;

    return ret;
}

/*--- Text positioning ---------------------------------------------------*/

/* Td */

HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_MoveTextPos  (HPDF_Page  page,
                        HPDF_REAL  x,
                        HPDF_REAL  y)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_TEXT_OBJECT);
    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_MoveTextPos\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);

    pbuf = HPDF_FToA (pbuf, x, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y, eptr);
    HPDF_StrCpy (pbuf, " Td\012", eptr);

    if (HPDF_Stream_WriteStr (attr->stream, buf) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->text_matrix.x += x * attr->text_matrix.a + y * attr->text_matrix.c;
    attr->text_matrix.y += y * attr->text_matrix.d + x * attr->text_matrix.b;
    attr->text_pos.x = attr->text_matrix.x;
    attr->text_pos.y = attr->text_matrix.y;

    return ret;
}

/* TD */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_MoveTextPos2  (HPDF_Page  page,
                         HPDF_REAL  x,
                         HPDF_REAL  y)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_TEXT_OBJECT);
    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_MoveTextPos2\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);

    pbuf = HPDF_FToA (pbuf, x, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y, eptr);
    HPDF_StrCpy (pbuf, " TD\012", eptr);

    if (HPDF_Stream_WriteStr (attr->stream, buf) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->text_matrix.x += x * attr->text_matrix.a + y * attr->text_matrix.c;
    attr->text_matrix.y += y * attr->text_matrix.d + x * attr->text_matrix.b;
    attr->text_pos.x = attr->text_matrix.x;
    attr->text_pos.y = attr->text_matrix.y;
    attr->gstate->text_leading = -y;

    return ret;
}

/* Tm */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetTextMatrix  (HPDF_Page         page,
                          HPDF_REAL    a,
                          HPDF_REAL    b,
                          HPDF_REAL    c,
                          HPDF_REAL    d,
                          HPDF_REAL    x,
                          HPDF_REAL    y)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_TEXT_OBJECT);
    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_SetTextMatrix\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if ((a == 0 || d == 0) && (b == 0 || c == 0))
        return HPDF_RaiseError (page->error, HPDF_INVALID_PARAMETER, 0);

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);

    pbuf = HPDF_FToA (pbuf, a, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, b, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, c, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, d, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y, eptr);
    HPDF_StrCpy (pbuf, " Tm\012", eptr);

    if (HPDF_Stream_WriteStr (attr->stream, buf) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->text_matrix.a = a;
    attr->text_matrix.b = b;
    attr->text_matrix.c = c;
    attr->text_matrix.d = d;
    attr->text_matrix.x = x;
    attr->text_matrix.y = y;
    attr->text_pos.x = attr->text_matrix.x;
    attr->text_pos.y = attr->text_matrix.y;

    return ret;
}


/* T* */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_MoveToNextLine  (HPDF_Page  page)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_MoveToNextLine\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (HPDF_Stream_WriteStr (attr->stream, "T*\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    /* calculate the reference point of text */
    attr->text_matrix.x -= attr->gstate->text_leading * attr->text_matrix.c;
    attr->text_matrix.y -= attr->gstate->text_leading * attr->text_matrix.d;

    attr->text_pos.x = attr->text_matrix.x;
    attr->text_pos.y = attr->text_matrix.y;

    return ret;
}

/*--- Text showing -------------------------------------------------------*/

/* Tj */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_ShowText  (HPDF_Page    page,
                     const char  *text)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;
    HPDF_REAL tw;

    HPDF_PTRACE ((" HPDF_Page_ShowText\n"));

    if (ret != HPDF_OK || text == NULL || text[0] == 0)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    /* no font exists */
    if (!attr->gstate->font)
        return HPDF_RaiseError (page->error, HPDF_PAGE_FONT_NOT_FOUND, 0);

    tw = HPDF_Page_TextWidth (page, text);
    if (!tw)
        return ret;

    if (InternalWriteText (attr, text) != HPDF_OK)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteStr (attr->stream, " Tj\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    /* calculate the reference point of text */
    if (attr->gstate->writing_mode == HPDF_WMODE_HORIZONTAL) {
        attr->text_pos.x += tw * attr->text_matrix.a;
        attr->text_pos.y += tw * attr->text_matrix.b;
    } else {
        attr->text_pos.x -= tw * attr->text_matrix.b;
        attr->text_pos.y -= tw * attr->text_matrix.a;
    }

    return ret;
}

/* TJ */
/* ' */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_ShowTextNextLine  (HPDF_Page    page,
                             const char  *text)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;
    HPDF_REAL tw;

    HPDF_PTRACE ((" HPDF_Page_ShowTextNextLine\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    /* no font exists */
    if (!attr->gstate->font)
        return HPDF_RaiseError (page->error, HPDF_PAGE_FONT_NOT_FOUND, 0);

    if (text == NULL || text[0] == 0)
        return HPDF_Page_MoveToNextLine(page);

    if (InternalWriteText (attr, text) != HPDF_OK)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteStr (attr->stream, " \'\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    tw = HPDF_Page_TextWidth (page, text);

    /* calculate the reference point of text */
    attr->text_matrix.x -= attr->gstate->text_leading * attr->text_matrix.c;
    attr->text_matrix.y -= attr->gstate->text_leading * attr->text_matrix.d;

    attr->text_pos.x = attr->text_matrix.x;
    attr->text_pos.y = attr->text_matrix.y;

    if (attr->gstate->writing_mode == HPDF_WMODE_HORIZONTAL) {
        attr->text_pos.x += tw * attr->text_matrix.a;
        attr->text_pos.y += tw * attr->text_matrix.b;
    } else {
        attr->text_pos.x -= tw * attr->text_matrix.b;
        attr->text_pos.y -= tw * attr->text_matrix.a;
    }

    return ret;
}

/* " */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_ShowTextNextLineEx  (HPDF_Page    page,
                               HPDF_REAL    word_space,
                               HPDF_REAL    char_space,
                               const char  *text)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;
    HPDF_REAL tw;
    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;

    HPDF_PTRACE ((" HPDF_Page_ShowTextNextLineEX\n"));

    if (ret != HPDF_OK)
        return ret;

    if (word_space < HPDF_MIN_WORDSPACE || word_space > HPDF_MAX_WORDSPACE)
        return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE, 0);

    if (char_space < HPDF_MIN_CHARSPACE || char_space > HPDF_MAX_CHARSPACE)
        return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE, 0);

    attr = (HPDF_PageAttr)page->attr;

    /* no font exists */
    if (!attr->gstate->font)
        return HPDF_RaiseError (page->error, HPDF_PAGE_FONT_NOT_FOUND, 0);

    if (text == NULL || text[0] == 0)
        return HPDF_Page_MoveToNextLine(page);

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);
    pbuf = HPDF_FToA (pbuf, word_space, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, char_space, eptr);
    *pbuf = ' ';

    if (InternalWriteText (attr, buf) != HPDF_OK)
        return HPDF_CheckError (page->error);

    if (InternalWriteText (attr, text) != HPDF_OK)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteStr (attr->stream, " \"\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->word_space = word_space;
    attr->gstate->char_space = char_space;

    tw = HPDF_Page_TextWidth (page, text);

    /* calculate the reference point of text */
    attr->text_matrix.x += attr->gstate->text_leading * attr->text_matrix.b;
    attr->text_matrix.y -= attr->gstate->text_leading * attr->text_matrix.a;

    attr->text_pos.x = attr->text_matrix.x;
    attr->text_pos.y = attr->text_matrix.y;

    if (attr->gstate->writing_mode == HPDF_WMODE_HORIZONTAL) {
        attr->text_pos.x += tw * attr->text_matrix.a;
        attr->text_pos.y += tw * attr->text_matrix.b;
    } else {
        attr->text_pos.x -= tw * attr->text_matrix.b;
        attr->text_pos.y -= tw * attr->text_matrix.a;
    }

    return ret;
}


/*--- Color showing ------------------------------------------------------*/

/* cs --not implemented yet */
/* CS --not implemented yet */
/* sc --not implemented yet */
/* scn --not implemented yet */
/* SC --not implemented yet */
/* SCN --not implemented yet */

/* g */

HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetGrayFill  (HPDF_Page  page,
                        HPDF_REAL  gray)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_SetGrayFill\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (gray < 0 || gray > 1)
        return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE, 0);

    if (HPDF_Stream_WriteReal (attr->stream, gray) != HPDF_OK)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteStr (attr->stream, " g\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->gray_fill = gray;
    attr->gstate->cs_fill = HPDF_CS_DEVICE_GRAY;

    return ret;
}

/* G */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetGrayStroke  (HPDF_Page  page,
                          HPDF_REAL  gray)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_SetGrayStroke\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    if (gray < 0 || gray > 1)
        return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE, 0);

    if (HPDF_Stream_WriteReal (attr->stream, gray) != HPDF_OK)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteStr (attr->stream, " G\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->gray_stroke = gray;
    attr->gstate->cs_stroke = HPDF_CS_DEVICE_GRAY;

    return ret;
}

/* rg */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetRGBFill  (HPDF_Page  page,
                       HPDF_REAL  r,
                       HPDF_REAL  g,
                       HPDF_REAL  b)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_TEXT_OBJECT |
                    HPDF_GMODE_PAGE_DESCRIPTION);
    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_SetRGBFill\n"));

    if (ret != HPDF_OK)
        return ret;

    if (r < 0 || r > 1 || g < 0 || g > 1 || b < 0 || b > 1)
        return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE, 0);

    attr = (HPDF_PageAttr)page->attr;

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);

    pbuf = HPDF_FToA (pbuf, r, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, g, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, b, eptr);
    HPDF_StrCpy (pbuf, " rg\012", eptr);

    if (HPDF_Stream_WriteStr (attr->stream, buf) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->rgb_fill.r = r;
    attr->gstate->rgb_fill.g = g;
    attr->gstate->rgb_fill.b = b;
    attr->gstate->cs_fill = HPDF_CS_DEVICE_RGB;

    return ret;
}

/* RG */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetRGBStroke  (HPDF_Page  page,
                         HPDF_REAL  r,
                         HPDF_REAL  g,
                         HPDF_REAL  b)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_TEXT_OBJECT |
                    HPDF_GMODE_PAGE_DESCRIPTION);
    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_SetRGBStroke\n"));

    if (ret != HPDF_OK)
        return ret;

    if (r < 0 || r > 1 || g < 0 || g > 1 || b < 0 || b > 1)
        return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE, 0);

    attr = (HPDF_PageAttr)page->attr;

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);

    pbuf = HPDF_FToA (pbuf, r, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, g, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, b, eptr);
    HPDF_StrCpy (pbuf, " RG\012", eptr);

    if (HPDF_Stream_WriteStr (attr->stream, buf) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->rgb_stroke.r = r;
    attr->gstate->rgb_stroke.g = g;
    attr->gstate->rgb_stroke.b = b;
    attr->gstate->cs_stroke = HPDF_CS_DEVICE_RGB;

    return ret;
}

/* k */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetCMYKFill  (HPDF_Page  page,
                        HPDF_REAL  c,
                        HPDF_REAL  m,
                        HPDF_REAL  y,
                        HPDF_REAL  k)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_TEXT_OBJECT |
                    HPDF_GMODE_PAGE_DESCRIPTION);
    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_SetCMYKFill\n"));

    if (ret != HPDF_OK)
        return ret;

    if (c < 0 || c > 1 || m < 0 || m > 1 || y < 0 || y > 1 || k < 0 || k > 1)
        return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE, 0);

    attr = (HPDF_PageAttr)page->attr;

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);

    pbuf = HPDF_FToA (pbuf, c, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, m, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, k, eptr);
    HPDF_StrCpy (pbuf, " k\012", eptr);

    if (HPDF_Stream_WriteStr (attr->stream, buf) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->cmyk_fill.c = c;
    attr->gstate->cmyk_fill.m = m;
    attr->gstate->cmyk_fill.y = y;
    attr->gstate->cmyk_fill.k = k;
    attr->gstate->cs_fill = HPDF_CS_DEVICE_CMYK;

    return ret;
}

/* K */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetCMYKStroke  (HPDF_Page  page,
                          HPDF_REAL  c,
                          HPDF_REAL  m,
                          HPDF_REAL  y,
                          HPDF_REAL  k)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_TEXT_OBJECT |
                    HPDF_GMODE_PAGE_DESCRIPTION);
    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_SetCMYKStroke\n"));

    if (ret != HPDF_OK)
        return ret;

    if (c < 0 || c > 1 || m < 0 || m > 1 || y < 0 || y > 1 || k < 0 || k > 1)
        return HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE, 0);

    attr = (HPDF_PageAttr)page->attr;

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);

    pbuf = HPDF_FToA (pbuf, c, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, m, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, k, eptr);
    HPDF_StrCpy (pbuf, " K\012", eptr);

    if (HPDF_Stream_WriteStr (attr->stream, buf) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->gstate->cmyk_stroke.c = c;
    attr->gstate->cmyk_stroke.m = m;
    attr->gstate->cmyk_stroke.y = y;
    attr->gstate->cmyk_stroke.k = k;
    attr->gstate->cs_stroke = HPDF_CS_DEVICE_CMYK;

    return ret;
}

/*--- Shading patterns ---------------------------------------------------*/

/* sh --not implemented yet */

/*--- In-line images -----------------------------------------------------*/

/* BI --not implemented yet */
/* ID --not implemented yet */
/* EI --not implemented yet */

/*--- XObjects -----------------------------------------------------------*/

/* Do */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_ExecuteXObject  (HPDF_Page     page,
                           HPDF_XObject  obj)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION);
    HPDF_PageAttr attr;
    const char *local_name;

    HPDF_PTRACE ((" HPDF_Page_ExecuteXObject\n"));

    if (ret != HPDF_OK)
        return ret;

    if (!obj || obj->header.obj_class != (HPDF_OSUBCLASS_XOBJECT |
            HPDF_OCLASS_DICT))
        return HPDF_RaiseError (page->error, HPDF_INVALID_OBJECT, 0);

    if (page->mmgr != obj->mmgr)
        return HPDF_RaiseError (page->error, HPDF_PAGE_INVALID_XOBJECT, 0);

    attr = (HPDF_PageAttr)page->attr;
    local_name = HPDF_Page_GetXObjectName (page, obj);

    if (!local_name)
        return HPDF_RaiseError (page->error, HPDF_PAGE_INVALID_XOBJECT, 0);

    if (HPDF_Stream_WriteEscapeName (attr->stream, local_name) != HPDF_OK)
        return HPDF_CheckError (page->error);

    if (HPDF_Stream_WriteStr (attr->stream, " Do\012") != HPDF_OK)
        return HPDF_CheckError (page->error);

    return ret;
}

/*--- Marked content -----------------------------------------------------*/

/* BMC --not implemented yet */
/* BDC --not implemented yet */
/* EMC --not implemented yet */
/* MP --not implemented yet */
/* DP --not implemented yet */

/*--- Compatibility ------------------------------------------------------*/

/* BX --not implemented yet */
/* EX --not implemented yet */


/*--- combined function --------------------------------------------------*/

static const HPDF_REAL KAPPA = 0.552F;

static char*
QuarterCircleA  (char   *pbuf,
                 char   *eptr,
                 HPDF_REAL    x,
                 HPDF_REAL    y,
                 HPDF_REAL    ray)
{
    pbuf = HPDF_FToA (pbuf, x -ray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y + ray * KAPPA, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x -ray * KAPPA, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y + ray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y + ray, eptr);
    return (char *)HPDF_StrCpy (pbuf, " c\012", eptr);
}

static char*
QuarterCircleB  (char   *pbuf,
                 char   *eptr,
                 HPDF_REAL    x,
                 HPDF_REAL    y,
                 HPDF_REAL    ray)
{
    pbuf = HPDF_FToA (pbuf, x + ray * KAPPA, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y + ray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x + ray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y + ray * KAPPA, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x + ray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y, eptr);
    return (char *)HPDF_StrCpy (pbuf, " c\012", eptr);
}

static char*
QuarterCircleC  (char   *pbuf,
                 char   *eptr,
                 HPDF_REAL    x,
                 HPDF_REAL    y,
                 HPDF_REAL    ray)
{
    pbuf = HPDF_FToA (pbuf, x + ray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y - ray * KAPPA, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x + ray * KAPPA, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y - ray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y - ray, eptr);
    return (char *)HPDF_StrCpy (pbuf, " c\012", eptr);
}

static char*
QuarterCircleD  (char   *pbuf,
                 char   *eptr,
                 HPDF_REAL    x,
                 HPDF_REAL    y,
                 HPDF_REAL    ray)
{
    pbuf = HPDF_FToA (pbuf, x - ray * KAPPA, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y - ray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x - ray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y - ray * KAPPA, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x - ray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y, eptr);
    return (char *)HPDF_StrCpy (pbuf, " c\012", eptr);
}

HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_Circle  (HPDF_Page     page,
                   HPDF_REAL     x,
                   HPDF_REAL     y,
                   HPDF_REAL     ray)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
            HPDF_GMODE_PATH_OBJECT);
    char buf[HPDF_TMP_BUF_SIZ * 2];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_Circle\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);

    pbuf = HPDF_FToA (pbuf, x - ray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y, eptr);
    pbuf = (char *)HPDF_StrCpy (pbuf, " m\012", eptr);

    pbuf = QuarterCircleA (pbuf, eptr, x, y, ray);
    pbuf = QuarterCircleB (pbuf, eptr, x, y, ray);
    pbuf = QuarterCircleC (pbuf, eptr, x, y, ray);
    QuarterCircleD (pbuf, eptr, x, y, ray);

    if (HPDF_Stream_WriteStr (attr->stream, buf) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->cur_pos.x = x - ray;
    attr->cur_pos.y = y;
    attr->str_pos = attr->cur_pos;
    attr->gmode = HPDF_GMODE_PATH_OBJECT;

    return ret;
}


static char*
QuarterEllipseA  (char      *pbuf,
                  char      *eptr,
                  HPDF_REAL  x,
                  HPDF_REAL  y,
                  HPDF_REAL  xray,
                  HPDF_REAL  yray)
{
    pbuf = HPDF_FToA (pbuf, x - xray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y + yray * KAPPA, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x -xray * KAPPA, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y + yray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y + yray, eptr);
    return (char *)HPDF_StrCpy (pbuf, " c\012", eptr);
}

static char*
QuarterEllipseB  (char      *pbuf,
                  char      *eptr,
                  HPDF_REAL  x,
                  HPDF_REAL  y,
                  HPDF_REAL  xray,
                  HPDF_REAL  yray)
{
    pbuf = HPDF_FToA (pbuf, x + xray * KAPPA, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y + yray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x + xray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y + yray * KAPPA, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x + xray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y, eptr);
    return (char *)HPDF_StrCpy (pbuf, " c\012", eptr);
}

static char*
QuarterEllipseC  (char      *pbuf,
                  char      *eptr,
                  HPDF_REAL  x,
                  HPDF_REAL  y,
                  HPDF_REAL  xray,
                  HPDF_REAL  yray)
{
    pbuf = HPDF_FToA (pbuf, x + xray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y - yray * KAPPA, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x + xray * KAPPA, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y - yray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y - yray, eptr);
    return (char *)HPDF_StrCpy (pbuf, " c\012", eptr);
}

static char*
QuarterEllipseD  (char      *pbuf,
                  char      *eptr,
                  HPDF_REAL  x,
                  HPDF_REAL  y,
                  HPDF_REAL  xray,
                  HPDF_REAL  yray)
{
    pbuf = HPDF_FToA (pbuf, x - xray * KAPPA, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y - yray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x - xray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y - yray * KAPPA, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, x - xray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y, eptr);
    return (char *)HPDF_StrCpy (pbuf, " c\012", eptr);
}

HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_Ellipse  (HPDF_Page   page,
                    HPDF_REAL   x,
                    HPDF_REAL   y,
                    HPDF_REAL  xray,
                    HPDF_REAL  yray)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
            HPDF_GMODE_PATH_OBJECT);
    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_Ellipse\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);

    pbuf = HPDF_FToA (pbuf, x - xray, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, y, eptr);
    pbuf = (char *)HPDF_StrCpy (pbuf, " m\012", eptr);

    pbuf = QuarterEllipseA (pbuf, eptr, x, y, xray, yray);
    pbuf = QuarterEllipseB (pbuf, eptr, x, y, xray, yray);
    pbuf = QuarterEllipseC (pbuf, eptr, x, y, xray, yray);
    QuarterEllipseD (pbuf, eptr, x, y, xray, yray);

    if (HPDF_Stream_WriteStr (attr->stream, buf) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->cur_pos.x = x - xray;
    attr->cur_pos.y = y;
    attr->str_pos = attr->cur_pos;
    attr->gmode = HPDF_GMODE_PATH_OBJECT;

    return ret;
}


/*
 * this function is based on the code which is contributed by Riccardo Cohen.
 *
 * from http://www.tinaja.com/glib/bezarc1.pdf coming from
 * http://www.whizkidtech.redprince.net/bezier/circle/
 */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_Arc  (HPDF_Page    page,
                HPDF_REAL    x,
                HPDF_REAL    y,
                HPDF_REAL    ray,
                HPDF_REAL    ang1,
                HPDF_REAL    ang2)
{
    HPDF_BOOL cont_flg = HPDF_FALSE;

    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_PATH_OBJECT);

    HPDF_PTRACE ((" HPDF_Page_Arc\n"));

    if (fabs(ang2 - ang1) >= 360)
        HPDF_RaiseError (page->error, HPDF_PAGE_OUT_OF_RANGE, 0);

    if (ret != HPDF_OK)
        return ret;

    while (ang1 < 0 || ang2 < 0) {
        ang1 = ang1 + 360;
        ang2 = ang2 + 360;
    }


    for (;;) {
        if (fabs(ang2 - ang1) <= 90)
            return InternalArc (page, x, y, ray, ang1, ang2, cont_flg);
        else {
	    HPDF_REAL tmp_ang = (ang2 > ang1 ? ang1 + 90 : ang1 - 90);

            if ((ret = InternalArc (page, x, y, ray, ang1, tmp_ang, cont_flg))
                    != HPDF_OK)
                return ret;

            ang1 = tmp_ang;
        }

        if (fabs(ang1 - ang2) < 0.1)
            break;

        cont_flg = HPDF_TRUE;
    }

    return HPDF_OK;
}


static HPDF_STATUS
InternalArc  (HPDF_Page    page,
              HPDF_REAL    x,
              HPDF_REAL    y,
              HPDF_REAL    ray,
              HPDF_REAL    ang1,
              HPDF_REAL    ang2,
              HPDF_BOOL    cont_flg)
{
    const HPDF_REAL PIE = 3.14159F;

    char buf[HPDF_TMP_BUF_SIZ];
    char *pbuf = buf;
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;
    HPDF_PageAttr attr;
    HPDF_STATUS ret;

    HPDF_DOUBLE rx0, ry0, rx1, ry1, rx2, ry2, rx3, ry3;
    HPDF_DOUBLE x0, y0, x1, y1, x2, y2, x3, y3;
    HPDF_DOUBLE delta_angle;
    HPDF_DOUBLE new_angle;

    HPDF_PTRACE ((" HPDF_Page_InternalArc\n"));

    attr = (HPDF_PageAttr)page->attr;

    HPDF_MemSet (buf, 0, HPDF_TMP_BUF_SIZ);

    delta_angle = (90 - (HPDF_DOUBLE)(ang1 + ang2) / 2) / 180 * PIE;
    new_angle = (HPDF_DOUBLE)(ang2 - ang1) / 2 / 180 * PIE;

    rx0 = ray * HPDF_COS (new_angle);
    ry0 = ray * HPDF_SIN (new_angle);
    rx2 = (ray * 4.0 - rx0) / 3.0;
    ry2 = ((ray * 1.0 - rx0) * (rx0 - ray * 3.0)) / (3.0 * ry0);
    rx1 = rx2;
    ry1 = -ry2;
    rx3 = rx0;
    ry3 = -ry0;

    x0 = rx0 * HPDF_COS (delta_angle) - ry0 * HPDF_SIN (delta_angle) + x;
    y0 = rx0 * HPDF_SIN (delta_angle) + ry0 * HPDF_COS (delta_angle) + y;
    x1 = rx1 * HPDF_COS (delta_angle) - ry1 * HPDF_SIN (delta_angle) + x;
    y1 = rx1 * HPDF_SIN (delta_angle) + ry1 * HPDF_COS (delta_angle) + y;
    x2 = rx2 * HPDF_COS (delta_angle) - ry2 * HPDF_SIN (delta_angle) + x;
    y2 = rx2 * HPDF_SIN (delta_angle) + ry2 * HPDF_COS (delta_angle) + y;
    x3 = rx3 * HPDF_COS (delta_angle) - ry3 * HPDF_SIN (delta_angle) + x;
    y3 = rx3 * HPDF_SIN (delta_angle) + ry3 * HPDF_COS (delta_angle) + y;

    if (!cont_flg) {
        pbuf = HPDF_FToA (pbuf, (HPDF_REAL)x0, eptr);
        *pbuf++ = ' ';
        pbuf = HPDF_FToA (pbuf, (HPDF_REAL)y0, eptr);

	if (attr->gmode == HPDF_GMODE_PATH_OBJECT)
	  pbuf = (char *)HPDF_StrCpy (pbuf, " l\012", eptr);
	else
	  pbuf = (char *)HPDF_StrCpy (pbuf, " m\012", eptr);
    }

    pbuf = HPDF_FToA (pbuf, (HPDF_REAL)x1, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, (HPDF_REAL)y1, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, (HPDF_REAL)x2, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, (HPDF_REAL)y2, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, (HPDF_REAL)x3, eptr);
    *pbuf++ = ' ';
    pbuf = HPDF_FToA (pbuf, (HPDF_REAL)y3, eptr);
    HPDF_StrCpy (pbuf, " c\012", eptr);

    if ((ret = HPDF_Stream_WriteStr (attr->stream, buf)) != HPDF_OK)
        return HPDF_CheckError (page->error);

    attr->cur_pos.x = (HPDF_REAL)x3;
    attr->cur_pos.y = (HPDF_REAL)y3;
    attr->str_pos = attr->cur_pos;
    attr->gmode = HPDF_GMODE_PATH_OBJECT;

    return ret;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_DrawImage  (HPDF_Page    page,
                      HPDF_Image   image,
                      HPDF_REAL    x,
                      HPDF_REAL    y,
                      HPDF_REAL    width,
                      HPDF_REAL    height)
{
    HPDF_STATUS ret;

    if ((ret = HPDF_Page_GSave (page)) != HPDF_OK)
        return ret;

    if ((ret = HPDF_Page_Concat (page, width, 0, 0, height, x, y)) != HPDF_OK)
        return ret;

    if ((ret = HPDF_Page_ExecuteXObject (page, image)) != HPDF_OK)
        return ret;

    return HPDF_Page_GRestore (page);
}


static HPDF_STATUS
InternalWriteText  (HPDF_PageAttr      attr,
                    const char        *text)
{
    HPDF_FontAttr font_attr = (HPDF_FontAttr)attr->gstate->font->attr;
    HPDF_STATUS ret;

    HPDF_PTRACE ((" InternalWriteText\n"));

    if (font_attr->type == HPDF_FONT_TYPE0_TT ||
            font_attr->type == HPDF_FONT_TYPE0_CID) {
        HPDF_Encoder encoder;
	HPDF_UINT len;

        if ((ret = HPDF_Stream_WriteStr (attr->stream, "<")) != HPDF_OK)
            return ret;

        encoder = font_attr->encoder;
        len = HPDF_StrLen (text, HPDF_LIMIT_MAX_STRING_LEN);

        if (encoder->encode_text_fn == NULL) {
	    if ((ret = HPDF_Stream_WriteBinary (attr->stream, (HPDF_BYTE *)text,
						len, NULL))
		!= HPDF_OK)
	        return ret;
        } else {
	    char *encoded;
	    HPDF_UINT length;

	    encoded = (encoder->encode_text_fn)(encoder, text, len, &length);

	    ret = HPDF_Stream_WriteBinary (attr->stream, (HPDF_BYTE *)encoded,
					   length, NULL);

	    free(encoded);

	    if (ret != HPDF_OK)
                return ret;
        }

        return HPDF_Stream_WriteStr (attr->stream, ">");
    }

    return HPDF_Stream_WriteEscapeText (attr->stream, text);
}


/*
 * Convert a user space text position from absolute to relative coordinates.
 * Absolute values are passed in xAbs and yAbs, relative values are returned
 * to xRel and yRel. The latter two must not be NULL.
 */
static void
TextPos_AbsToRel (HPDF_TransMatrix text_matrix,
                  HPDF_REAL xAbs,
                  HPDF_REAL yAbs,
                  HPDF_REAL *xRel,
                  HPDF_REAL *yRel)
{
    if (text_matrix.a == 0) {
        *xRel = (yAbs - text_matrix.y - (xAbs - text_matrix.x) *
            text_matrix.d / text_matrix.c) / text_matrix.b;
        *yRel  = (xAbs - text_matrix.x) / text_matrix.c;
    } else {
        HPDF_REAL y = (yAbs - text_matrix.y - (xAbs - text_matrix.x) *
            text_matrix.b / text_matrix.a) / (text_matrix.d -
            text_matrix.c * text_matrix.b / text_matrix.a);
        *xRel = (xAbs - text_matrix.x - y * text_matrix.c) /
            text_matrix.a;
        *yRel = y;
    }
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_TextOut  (HPDF_Page    page,
                    HPDF_REAL    xpos,
                    HPDF_REAL    ypos,
                    const char  *text)
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_TEXT_OBJECT);
    HPDF_REAL x;
    HPDF_REAL y;
    HPDF_PageAttr attr;

    HPDF_PTRACE ((" HPDF_Page_TextOut\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr)page->attr;
    TextPos_AbsToRel (attr->text_matrix, xpos, ypos, &x, &y);
    if ((ret = HPDF_Page_MoveTextPos (page, x, y)) != HPDF_OK)
        return ret;

    return  HPDF_Page_ShowText (page, text);
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_TextRect  (HPDF_Page            page,
                     HPDF_REAL            left,
                     HPDF_REAL            top,
                     HPDF_REAL            right,
                     HPDF_REAL            bottom,
                     const char          *text,
                     HPDF_TextAlignment   align,
                     HPDF_UINT           *len
                     )
{
    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;
    const char *ptr = text;
    HPDF_BOOL pos_initialized = HPDF_FALSE;
    HPDF_REAL save_char_space = 0;
    HPDF_BOOL is_insufficient_space = HPDF_FALSE;
    HPDF_UINT num_rest;
    HPDF_Box bbox;
    HPDF_BOOL char_space_changed = HPDF_FALSE;

    HPDF_PTRACE ((" HPDF_Page_TextRect\n"));

    if (ret != HPDF_OK)
        return ret;

    attr = (HPDF_PageAttr )page->attr;

    /* no font exists */
    if (!attr->gstate->font) {
        return HPDF_RaiseError (page->error, HPDF_PAGE_FONT_NOT_FOUND, 0);
    }

    bbox = HPDF_Font_GetBBox (attr->gstate->font);

    if (len)
        *len = 0;
    num_rest = HPDF_StrLen (text, HPDF_LIMIT_MAX_STRING_LEN + 1);

    if (num_rest > HPDF_LIMIT_MAX_STRING_LEN) {
        return HPDF_RaiseError (page->error, HPDF_STRING_OUT_OF_RANGE, 0);
    } else if (!num_rest)
        return HPDF_OK;

    if (attr->gstate->text_leading == 0)
        HPDF_Page_SetTextLeading (page, (bbox.top - bbox.bottom) / 1000 *
                attr->gstate->font_size);

    top = top - bbox.top / 1000 * attr->gstate->font_size +
                attr->gstate->text_leading;
    bottom = bottom - bbox.bottom / 1000 * attr->gstate->font_size;

    if (align == HPDF_TALIGN_JUSTIFY) {
        save_char_space = attr->gstate->char_space;
        attr->gstate->char_space = 0;
    }

    for (;;) {
        HPDF_REAL x, y;
        HPDF_UINT line_len, tmp_len;
        HPDF_REAL rw;
        HPDF_BOOL LineBreak;

        attr->gstate->char_space = 0;
        line_len = tmp_len = HPDF_Page_MeasureText (page, ptr, right - left, HPDF_TRUE, &rw);
        if (line_len == 0) {
            is_insufficient_space = HPDF_TRUE;
            break;
        }

        if (len)
            *len += line_len;
        num_rest -= line_len;

        /* Shorten tmp_len by trailing whitespace and control characters. */
        LineBreak = HPDF_FALSE;
        while (tmp_len > 0 && HPDF_IS_WHITE_SPACE(ptr[tmp_len - 1])) {
            tmp_len--;
            if (ptr[tmp_len] == 0x0A || ptr[tmp_len] == 0x0D) {
                LineBreak = HPDF_TRUE;
            }
        }

        switch (align) {

            case HPDF_TALIGN_RIGHT:
                TextPos_AbsToRel (attr->text_matrix, right - rw, top, &x, &y);
                if (!pos_initialized) {
                    pos_initialized = HPDF_TRUE;
                } else {
                    y = 0;
                }
                if ((ret = HPDF_Page_MoveTextPos (page, x, y)) != HPDF_OK)
                    return ret;
                break;

            case HPDF_TALIGN_CENTER:
                TextPos_AbsToRel (attr->text_matrix, left + (right - left - rw) / 2, top, &x, &y);
                if (!pos_initialized) {
                    pos_initialized = HPDF_TRUE;
                } else {
                    y = 0;
                }
                if ((ret = HPDF_Page_MoveTextPos (page, x, y)) != HPDF_OK)
                    return ret;
                break;

            case HPDF_TALIGN_JUSTIFY:
                if (!pos_initialized) {
                    pos_initialized = HPDF_TRUE;
                    TextPos_AbsToRel (attr->text_matrix, left, top, &x, &y);
                    if ((ret = HPDF_Page_MoveTextPos (page, x, y)) != HPDF_OK)
                        return ret;
                }

                /* Do not justify last line of paragraph or text. */
                if (LineBreak || num_rest <= 0) {
                    if ((ret = HPDF_Page_SetCharSpace (page, save_char_space))
                                    != HPDF_OK)
                        return ret;
                    char_space_changed = HPDF_FALSE;
                } else {
                    HPDF_REAL x_adjust;
                    HPDF_ParseText_Rec state;
                    HPDF_UINT i = 0;
                    HPDF_UINT num_char = 0;
                    HPDF_Encoder encoder = ((HPDF_FontAttr)attr->gstate->font->attr)->encoder;
                    const char *tmp_ptr = ptr;
                    HPDF_Encoder_SetParseText (encoder, &state, (HPDF_BYTE *)tmp_ptr, tmp_len);
                    while (*tmp_ptr) {
                        HPDF_ByteType btype = HPDF_Encoder_ByteType (encoder, &state);
                        if (btype != HPDF_BYTE_TYPE_TRIAL)
                            num_char++;
                        i++;
                        if (i >= tmp_len)
                            break;
                        tmp_ptr++;
                    }

                    x_adjust = num_char == 0 ? 0 : (right - left - rw) / (num_char - 1);
                    if ((ret = HPDF_Page_SetCharSpace (page, x_adjust)) != HPDF_OK)
                        return ret;
                    char_space_changed = HPDF_TRUE;
                }
                break;

            default:
                if (!pos_initialized) {
                    pos_initialized = HPDF_TRUE;
                    TextPos_AbsToRel (attr->text_matrix, left, top, &x, &y);
                    if ((ret = HPDF_Page_MoveTextPos (page, x, y)) != HPDF_OK)
                        return ret;
                }
        }

        if (InternalShowTextNextLine (page, ptr, tmp_len) != HPDF_OK)
            return HPDF_CheckError (page->error);

        if (num_rest <= 0)
            break;

        if (attr->text_pos.y - attr->gstate->text_leading < bottom) {
            is_insufficient_space = HPDF_TRUE;
            break;
        }

        ptr += line_len;
    }

    if (char_space_changed && save_char_space != attr->gstate->char_space) {
        if ((ret = HPDF_Page_SetCharSpace (page, save_char_space)) != HPDF_OK)
            return ret;
    }

    if (is_insufficient_space)
        return HPDF_PAGE_INSUFFICIENT_SPACE;
    else
        return HPDF_OK;
}


static HPDF_STATUS
InternalShowTextNextLine  (HPDF_Page    page,
                           const char  *text,
                           HPDF_UINT    len)
{
    HPDF_STATUS ret;
    HPDF_PageAttr attr;
    HPDF_REAL tw;
    HPDF_FontAttr font_attr;

    HPDF_PTRACE ((" ShowTextNextLine\n"));

    attr = (HPDF_PageAttr)page->attr;
    font_attr = (HPDF_FontAttr)attr->gstate->font->attr;

    if (font_attr->type == HPDF_FONT_TYPE0_TT ||
            font_attr->type == HPDF_FONT_TYPE0_CID) {
        HPDF_Encoder encoder = font_attr->encoder;

        if ((ret = HPDF_Stream_WriteStr (attr->stream, "<")) != HPDF_OK)
            return ret;

        if (encoder->encode_text_fn == NULL) {
	    if ((ret = HPDF_Stream_WriteBinary (attr->stream, (HPDF_BYTE *)text,
						len, NULL))
		!= HPDF_OK)
	        return ret;
        } else {
	    char *encoded;
	    HPDF_UINT length;

	    encoded = (encoder->encode_text_fn)(encoder, text, len, &length);
	    ret = HPDF_Stream_WriteBinary (attr->stream, (HPDF_BYTE *)encoded,
					   length, NULL);
	    free(encoded);

	    if (ret != HPDF_OK)
	        return ret;
        }

        if ((ret = HPDF_Stream_WriteStr (attr->stream, ">")) != HPDF_OK)
            return ret;
    } else  if ((ret = HPDF_Stream_WriteEscapeText2 (attr->stream, text,
                len)) != HPDF_OK)
        return ret;

    if ((ret = HPDF_Stream_WriteStr (attr->stream, " \'\012")) != HPDF_OK)
        return ret;

    tw = HPDF_Page_TextWidth (page, text);

    /* calculate the reference point of text */
    attr->text_matrix.x -= attr->gstate->text_leading * attr->text_matrix.c;
    attr->text_matrix.y -= attr->gstate->text_leading * attr->text_matrix.d;

    attr->text_pos.x = attr->text_matrix.x;
    attr->text_pos.y = attr->text_matrix.y;

    if (attr->gstate->writing_mode == HPDF_WMODE_HORIZONTAL) {
        attr->text_pos.x += tw * attr->text_matrix.a;
        attr->text_pos.y += tw * attr->text_matrix.b;
    } else {
        attr->text_pos.x -= tw * attr->text_matrix.b;
        attr->text_pos.y -= tw * attr->text_matrix.a;
    }

    return ret;
}


/*
 *  This function is contributed by Adrian Nelson (adenelson).
 */
HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_SetSlideShow  (HPDF_Page            page,
                         HPDF_TransitionStyle   type,
                         HPDF_REAL            disp_time,
                         HPDF_REAL            trans_time)
    {
    HPDF_STATUS ret = HPDF_OK;
    HPDF_Dict dict;

    HPDF_PTRACE((" HPDF_Page_SetSlideShow\n"));

    if (!HPDF_Page_Validate (page))
        return HPDF_INVALID_PAGE;

    if (disp_time < 0)
        return HPDF_RaiseError (page->error, HPDF_PAGE_INVALID_DISPLAY_TIME,
                    (HPDF_STATUS)disp_time);

    if (trans_time < 0)
        return HPDF_RaiseError (page->error, HPDF_PAGE_INVALID_TRANSITION_TIME,
                    (HPDF_STATUS)trans_time);

    dict = HPDF_Dict_New(page->mmgr);

    if (!dict)
        return HPDF_Error_GetCode (page->error);

    if (HPDF_Dict_AddName (dict, "Type", "Trans") != HPDF_OK)
        goto Fail;

    if (HPDF_Dict_AddReal (dict, "D", trans_time) != HPDF_OK)
        goto Fail;

    switch (type) {
        case HPDF_TS_WIPE_RIGHT:
            ret += HPDF_Dict_AddName (dict, "S", "Wipe");
            ret += HPDF_Dict_AddNumber (dict, "Di", 0);
            break;
        case HPDF_TS_WIPE_UP:
            ret += HPDF_Dict_AddName (dict, "S", "Wipe");
            ret += HPDF_Dict_AddNumber (dict, "Di", 90);
            break;
        case HPDF_TS_WIPE_LEFT:
            ret += HPDF_Dict_AddName (dict, "S", "Wipe");
            ret += HPDF_Dict_AddNumber (dict, "Di", 180);
            break;
        case HPDF_TS_WIPE_DOWN:
            ret += HPDF_Dict_AddName (dict, "S", "Wipe");
            ret += HPDF_Dict_AddNumber    (dict, "Di", 270);
            break;
        case HPDF_TS_BARN_DOORS_HORIZONTAL_OUT:
            ret += HPDF_Dict_AddName (dict, "S", "Split");
            ret += HPDF_Dict_AddName (dict, "Dm", "H");
            ret += HPDF_Dict_AddName (dict, "M", "O");
            break;
        case HPDF_TS_BARN_DOORS_HORIZONTAL_IN:
            ret += HPDF_Dict_AddName (dict, "S", "Split");
            ret += HPDF_Dict_AddName (dict, "Dm", "H");
            ret += HPDF_Dict_AddName (dict, "M", "I");
            break;
        case HPDF_TS_BARN_DOORS_VERTICAL_OUT:
            ret += HPDF_Dict_AddName (dict, "S", "Split");
            ret += HPDF_Dict_AddName (dict, "Dm", "V");
            ret += HPDF_Dict_AddName (dict, "M", "O");
            break;
        case HPDF_TS_BARN_DOORS_VERTICAL_IN:
            ret += HPDF_Dict_AddName (dict, "S", "Split");
            ret += HPDF_Dict_AddName (dict, "Dm", "V");
            ret += HPDF_Dict_AddName (dict, "M", "I");
            break;
        case HPDF_TS_BOX_OUT:
            ret += HPDF_Dict_AddName (dict, "S", "Box");
            ret += HPDF_Dict_AddName (dict, "M", "O");
            break;
        case HPDF_TS_BOX_IN:
            ret += HPDF_Dict_AddName (dict, "S", "Box");
            ret += HPDF_Dict_AddName (dict, "M", "I");
            break;
        case HPDF_TS_BLINDS_HORIZONTAL:
            ret += HPDF_Dict_AddName (dict, "S", "Blinds");
            ret += HPDF_Dict_AddName (dict, "Dm", "H");
            break;
        case HPDF_TS_BLINDS_VERTICAL:
            ret += HPDF_Dict_AddName (dict, "S", "Blinds");
            ret += HPDF_Dict_AddName (dict, "Dm", "V");
            break;
        case HPDF_TS_DISSOLVE:
            ret += HPDF_Dict_AddName (dict, "S", "Dissolve");
            break;
        case HPDF_TS_GLITTER_RIGHT:
            ret += HPDF_Dict_AddName (dict, "S", "Glitter");
            ret += HPDF_Dict_AddNumber (dict, "Di", 0);
            break;
        case HPDF_TS_GLITTER_DOWN:
            ret += HPDF_Dict_AddName (dict, "S", "Glitter");
            ret += HPDF_Dict_AddNumber (dict, "Di", 270);
            break;
        case HPDF_TS_GLITTER_TOP_LEFT_TO_BOTTOM_RIGHT:
            ret += HPDF_Dict_AddName (dict, "S", "Glitter");
            ret += HPDF_Dict_AddNumber (dict, "Di", 315);
            break;
        case HPDF_TS_REPLACE:
            ret += HPDF_Dict_AddName  (dict, "S", "R");
            break;
        default:
            ret += HPDF_SetError(page->error, HPDF_INVALID_PAGE_SLIDESHOW_TYPE, 0);
    }

    if (ret != HPDF_OK)
        goto Fail;

    if (HPDF_Dict_AddReal (page, "Dur", disp_time) != HPDF_OK)
        goto Fail;

    if ((ret = HPDF_Dict_Add (page, "Trans", dict)) != HPDF_OK)
        return ret;

    return HPDF_OK;

Fail:
    HPDF_Dict_Free (dict);
    return HPDF_Error_GetCode (page->error);
}


/*
 *  This function is contributed by Finn Arildsen.
 */

HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_New_Content_Stream  (HPDF_Page page,
                               HPDF_Dict* new_stream)
{
    /* Call this function to start a new content stream on a page. The
       handle is returned to new_stream.
       new_stream can later be used on other pages as a shared content stream;
       insert using HPDF_Page_Insert_Shared_Content_Stream */

    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_TEXT_OBJECT);
    HPDF_PageAttr attr;
    HPDF_UINT filter;
    HPDF_Array contents_array;

    HPDF_PTRACE((" HPDF_Page_New_Content_Stream\n"));

    attr = (HPDF_PageAttr)page->attr;
    filter = attr->contents->filter;

    /* check if there is already an array of contents */
    contents_array = (HPDF_Array) HPDF_Dict_GetItem(page,"Contents", HPDF_OCLASS_ARRAY);
    if (!contents_array) {	
        HPDF_Error_Reset (page->error);
        /* no contents_array already -- create one
           and replace current single contents item */
        contents_array = HPDF_Array_New(page->mmgr);
        if (!contents_array)
            return HPDF_Error_GetCode (page->error);
        ret += HPDF_Array_Add(contents_array,attr->contents);
        ret += HPDF_Dict_Add (page, "Contents", contents_array);
    }

    /* create new contents stream and add it to the page's contents array */
    attr->contents = HPDF_DictStream_New (page->mmgr, attr->xref);
    attr->contents->filter = filter;
    attr->stream = attr->contents->stream;

    if (!attr->contents)
        return HPDF_Error_GetCode (page->error);

    ret += HPDF_Array_Add (contents_array,attr->contents);

    /* return the value of the new stream, so that 
       the application can use it as a shared contents stream */
    if (ret == HPDF_OK && new_stream != NULL)
        *new_stream = attr->contents;

    return ret;
}


/*
 *  This function is contributed by Finn Arildsen.
 */

HPDF_EXPORT(HPDF_STATUS)
HPDF_Page_Insert_Shared_Content_Stream  (HPDF_Page page,
                               HPDF_Dict shared_stream)
{
    /* Call this function to insert a previously (with HPDF_New_Content_Stream) created content stream
       as a shared content stream on this page */

    HPDF_STATUS ret = HPDF_Page_CheckState (page, HPDF_GMODE_PAGE_DESCRIPTION |
                    HPDF_GMODE_TEXT_OBJECT);
    HPDF_Array contents_array;

    HPDF_PTRACE((" HPDF_Page_Insert_Shared_Content_Stream\n"));

    /* check if there is already an array of contents */
    contents_array = (HPDF_Array) HPDF_Dict_GetItem(page,"Contents", HPDF_OCLASS_ARRAY);
    if (!contents_array) {	
        HPDF_PageAttr attr;
        HPDF_Error_Reset (page->error);
        /* no contents_array already -- create one
           and replace current single contents item */
        contents_array = HPDF_Array_New(page->mmgr);
        if (!contents_array)
            return HPDF_Error_GetCode (page->error);
        attr = (HPDF_PageAttr)page->attr;
        ret += HPDF_Array_Add(contents_array,attr->contents);
        ret += HPDF_Dict_Add (page, "Contents", contents_array);
    }

    ret += HPDF_Array_Add (contents_array,shared_stream);

    /* Continue with a new stream */
    ret += HPDF_Page_New_Content_Stream (page, NULL);

    return ret;
}
