/*
 * << Haru Free PDF Library >> -- hpdf_font_type1.c
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
#include "hpdf_font.h"

static HPDF_STATUS
Type1Font_OnWrite  (HPDF_Dict    obj,
          HPDF_Stream  stream);


static void
Type1Font_OnFree  (HPDF_Dict  obj);


static HPDF_TextWidth
Type1Font_TextWidth  (HPDF_Font        font,
                      const HPDF_BYTE  *text,
                      HPDF_UINT        len);


static HPDF_UINT
Type1Font_MeasureText  (HPDF_Font          font,
                        const HPDF_BYTE   *text,
                        HPDF_UINT          len,
                        HPDF_REAL          width,
                        HPDF_REAL          font_size,
                        HPDF_REAL          char_space,
                        HPDF_REAL          word_space,
                        HPDF_BOOL          wordwrap,
                        HPDF_REAL         *real_width);


static HPDF_STATUS
Type1Font_CreateDescriptor  (HPDF_MMgr  mmgr,
                             HPDF_Font  font,
                             HPDF_Xref  xref);


HPDF_Font
HPDF_Type1Font_New  (HPDF_MMgr        mmgr,
                     HPDF_FontDef     fontdef,
                     HPDF_Encoder     encoder,
                     HPDF_Xref        xref)
{
    HPDF_Dict font;
    HPDF_FontAttr attr;
    HPDF_Type1FontDefAttr fontdef_attr;
    HPDF_BasicEncoderAttr encoder_attr;
    HPDF_STATUS ret = 0;
    HPDF_UINT i;

    HPDF_PTRACE ((" HPDF_Type1Font_New\n"));

    /* check whether the fontdef object and the encoder object is valid. */
    if (encoder->type != HPDF_ENCODER_TYPE_SINGLE_BYTE) {
        HPDF_SetError(mmgr->error, HPDF_INVALID_ENCODER_TYPE, 0);
        return NULL;
    }

    if (fontdef->type != HPDF_FONTDEF_TYPE_TYPE1) {
        HPDF_SetError(mmgr->error, HPDF_INVALID_FONTDEF_TYPE, 0);
        return NULL;
    }

    font = HPDF_Dict_New (mmgr);
    if (!font)
        return NULL;

    font->header.obj_class |= HPDF_OSUBCLASS_FONT;

    attr = HPDF_GetMem (mmgr, sizeof(HPDF_FontAttr_Rec));
    if (!attr) {
        HPDF_Dict_Free (font);
        return NULL;
    }

    font->header.obj_class |= HPDF_OSUBCLASS_FONT;
    font->write_fn = Type1Font_OnWrite;
    font->free_fn = Type1Font_OnFree;

    HPDF_MemSet (attr, 0, sizeof(HPDF_FontAttr_Rec));

    font->attr = attr;
    attr->type = HPDF_FONT_TYPE1;
    attr->writing_mode = HPDF_WMODE_HORIZONTAL;
    attr->text_width_fn = Type1Font_TextWidth;
    attr->measure_text_fn = Type1Font_MeasureText;
    attr->fontdef = fontdef;
    attr->encoder = encoder;
    attr->xref = xref;

    /* singlebyte-font has a widths-array which is an array of 256 signed
     * short integer.
     */
    attr->widths = HPDF_GetMem (mmgr, sizeof(HPDF_INT16) * 256);
    if (!attr->widths) {
        HPDF_Dict_Free (font);
        return NULL;
    }

    encoder_attr = (HPDF_BasicEncoderAttr)encoder->attr;

    HPDF_MemSet (attr->widths, 0, sizeof(HPDF_INT16) * 256);
    for (i = encoder_attr->first_char; i <= encoder_attr->last_char; i++) {
        HPDF_UNICODE u = encoder_attr->unicode_map[i];

        HPDF_UINT16 w = HPDF_Type1FontDef_GetWidth (fontdef, u);
        attr->widths[i] = w;
    }

    fontdef_attr = (HPDF_Type1FontDefAttr)fontdef->attr;

    ret += HPDF_Dict_AddName (font, "Type", "Font");
    ret += HPDF_Dict_AddName (font, "BaseFont", fontdef->base_font);
    ret += HPDF_Dict_AddName (font, "Subtype", "Type1");

    if (!fontdef_attr->is_base14font) {
        if (fontdef->missing_width != 0)
            ret += HPDF_Dict_AddNumber (font, "MissingWidth",
                    fontdef->missing_width);

        ret += Type1Font_CreateDescriptor (mmgr, font, xref);
    }

    if (ret != HPDF_OK) {
        HPDF_Dict_Free (font);
        return NULL;
    }

    if (HPDF_Xref_Add (xref, font) != HPDF_OK)
        return NULL;

    return font;
}


static HPDF_STATUS
Type1Font_CreateDescriptor  (HPDF_MMgr  mmgr,
                             HPDF_Font  font,
                             HPDF_Xref  xref)
{
    HPDF_FontAttr font_attr = (HPDF_FontAttr)font->attr;
    HPDF_FontDef def = font_attr->fontdef;
    HPDF_Type1FontDefAttr def_attr = (HPDF_Type1FontDefAttr)def->attr;

    HPDF_PTRACE ((" HPDF_Type1Font_CreateDescriptor\n"));

    if (!font_attr->fontdef->descriptor) {
        HPDF_Dict descriptor = HPDF_Dict_New (mmgr);
        HPDF_STATUS ret = 0;
        HPDF_Array array;

        if (!descriptor)
            return HPDF_Error_GetCode (font->error);

        ret += HPDF_Xref_Add (xref, descriptor);
        ret += HPDF_Dict_AddName (descriptor, "Type", "FontDescriptor");
        ret += HPDF_Dict_AddNumber (descriptor, "Ascent", def->ascent);
        ret += HPDF_Dict_AddNumber (descriptor, "Descent", def->descent);
        ret += HPDF_Dict_AddNumber (descriptor, "Flags", def->flags);

        array = HPDF_Box_Array_New (mmgr, def->font_bbox);
        ret += HPDF_Dict_Add (descriptor, "FontBBox", array);

        ret += HPDF_Dict_AddName (descriptor, "FontName",
                font_attr->fontdef->base_font);
        ret += HPDF_Dict_AddNumber (descriptor, "ItalicAngle",
                def->italic_angle);
        ret += HPDF_Dict_AddNumber (descriptor, "StemV", def->stemv);
        ret += HPDF_Dict_AddNumber (descriptor, "XHeight", def->x_height);

        if (def_attr->char_set)
            ret += HPDF_Dict_AddName (descriptor, "CharSet",
                        def_attr->char_set);

        if (ret != HPDF_OK)
            return HPDF_Error_GetCode (font->error);

        if (def_attr->font_data) {
            HPDF_Dict font_data = HPDF_DictStream_New (mmgr, xref);

            if (!font_data)
                return HPDF_Error_GetCode (font->error);

            if (HPDF_Stream_WriteToStream (def_attr->font_data,
                font_data->stream, HPDF_STREAM_FILTER_NONE, NULL) != HPDF_OK)
                return HPDF_Error_GetCode (font->error);

            ret += HPDF_Dict_Add (descriptor, "FontFile", font_data);
            ret += HPDF_Dict_AddNumber (font_data, "Length1",
                    def_attr->length1);
            ret += HPDF_Dict_AddNumber (font_data, "Length2",
                    def_attr->length2);
            ret += HPDF_Dict_AddNumber (font_data, "Length3",
                    def_attr->length3);

            font_data->filter = font->filter;
        }

        if (ret != HPDF_OK)
            return HPDF_Error_GetCode (font->error);

        font_attr->fontdef->descriptor = descriptor;
    }

    return HPDF_Dict_Add (font, "FontDescriptor",
            font_attr->fontdef->descriptor);
}


static HPDF_TextWidth
Type1Font_TextWidth  (HPDF_Font        font,
                      const HPDF_BYTE  *text,
                      HPDF_UINT        len)
{
    HPDF_FontAttr attr = (HPDF_FontAttr)font->attr;
    HPDF_TextWidth ret = {0, 0, 0, 0};
    HPDF_UINT i;
    HPDF_BYTE b = 0;

    HPDF_PTRACE ((" HPDF_Type1Font_TextWidth\n"));

    if (attr->widths) {
        for (i = 0; i < len; i++) {
            b = text[i];
            ret.numchars++;
            ret.width += attr->widths[b];

            if (HPDF_IS_WHITE_SPACE(b)) {
                ret.numspace++;
                ret.numwords++;
            }
        }
    } else
        HPDF_SetError (font->error, HPDF_FONT_INVALID_WIDTHS_TABLE, 0);

    /* 2006.08.19 add. */
    if (HPDF_IS_WHITE_SPACE(b))
        ; /* do nothing. */
    else
        ret.numwords++;

    return ret;
}


static HPDF_UINT
Type1Font_MeasureText (HPDF_Font          font,
                       const HPDF_BYTE   *text,
                       HPDF_UINT          len,
                       HPDF_REAL          width,
                       HPDF_REAL          font_size,
                       HPDF_REAL          char_space,
                       HPDF_REAL          word_space,
                       HPDF_BOOL          wordwrap,
                       HPDF_REAL         *real_width)
{
    HPDF_REAL w = 0;
    HPDF_UINT tmp_len = 0;
    HPDF_UINT i;
    HPDF_FontAttr attr = (HPDF_FontAttr)font->attr;

    HPDF_PTRACE ((" HPDF_Type1Font_MeasureText\n"));

    for (i = 0; i < len; i++) {
        HPDF_BYTE b = text[i];

        if (HPDF_IS_WHITE_SPACE(b)) {
            tmp_len = i + 1;

            if (real_width)
                *real_width = w;

            w += word_space;
        } else if (!wordwrap) {
            tmp_len = i;

            if (real_width)
                *real_width = w;
        }

        w += attr->widths[b] * font_size / 1000;

        /* 2006.08.04 break when it encountered  line feed */
        if (w > width || b == 0x0A)
            return tmp_len;

        if (i > 0)
            w += char_space;
    }

    /* all of text can be put in the specified width */
    if (real_width)
        *real_width = w;

    return len;
}


static HPDF_STATUS
Type1Font_OnWrite  (HPDF_Dict    obj,
          HPDF_Stream  stream)
{
    HPDF_FontAttr attr = (HPDF_FontAttr)obj->attr;
    HPDF_Type1FontDefAttr fontdef_attr =
                    (HPDF_Type1FontDefAttr)attr->fontdef->attr;
    HPDF_BasicEncoderAttr encoder_attr =
                    (HPDF_BasicEncoderAttr)attr->encoder->attr;
    HPDF_UINT i;
    HPDF_STATUS ret;
    char buf[HPDF_TMP_BUF_SIZ];
    char *eptr = buf + HPDF_TMP_BUF_SIZ - 1;

    HPDF_PTRACE ((" HPDF_Font_Type1Font_OnWrite\n"));

    /* if font is base14-font these entries is not required */
    if (!fontdef_attr->is_base14font || encoder_attr->has_differences) {
        char *pbuf;

        pbuf = (char *)HPDF_StrCpy (buf, "/FirstChar ", eptr);
        pbuf = HPDF_IToA (pbuf, encoder_attr->first_char, eptr);
        HPDF_StrCpy (pbuf, "\012", eptr);
        if ((ret = HPDF_Stream_WriteStr (stream, buf)) != HPDF_OK)
            return ret;

        pbuf = (char *)HPDF_StrCpy (buf, "/LastChar ", eptr);
        pbuf = HPDF_IToA (pbuf, encoder_attr->last_char, eptr);
        HPDF_StrCpy (pbuf, "\012", eptr);
        if ((ret = HPDF_Stream_WriteStr (stream, buf)) != HPDF_OK)
            return ret;

        /* Widths entry */
        if ((ret = HPDF_Stream_WriteEscapeName (stream, "Widths")) != HPDF_OK)
            return ret;

        if ((ret = HPDF_Stream_WriteStr (stream, " [\012")) != HPDF_OK)
            return ret;

        pbuf = buf;
        for (i = encoder_attr->first_char; i <= encoder_attr->last_char; i++) {

            pbuf = HPDF_IToA (pbuf, attr->widths[i], eptr);
            *pbuf++ = ' ';

            if ((i + 1) % 16 == 0) {
                HPDF_StrCpy(pbuf, "\012", eptr);
                if ((ret = HPDF_Stream_WriteStr (stream, buf)) != HPDF_OK)
                    return ret;
                pbuf = buf;
            }
        }

        HPDF_StrCpy (pbuf, "]\012", eptr);

        if ((ret = HPDF_Stream_WriteStr (stream, buf)) != HPDF_OK)
            return ret;
    }

    return attr->encoder->write_fn (attr->encoder, stream);
}


static void
Type1Font_OnFree  (HPDF_Dict  obj)
{
    HPDF_FontAttr attr = (HPDF_FontAttr)obj->attr;

    HPDF_PTRACE ((" HPDF_Type1Font_OnFree\n"));

    if (attr) {
        if (attr->widths) {
            HPDF_FreeMem (obj->mmgr, attr->widths);
        }
        HPDF_FreeMem (obj->mmgr, attr);
    }
}


