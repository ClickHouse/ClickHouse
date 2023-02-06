/*
 * << Haru Free PDF Library >> -- hpdf_font.c
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
#include "hpdf.h"


HPDF_EXPORT(HPDF_TextWidth)
HPDF_Font_TextWidth  (HPDF_Font        font,
                      const HPDF_BYTE  *text,
                      HPDF_UINT        len)
{
    HPDF_TextWidth tw = {0, 0, 0, 0};
    HPDF_FontAttr attr;

    HPDF_PTRACE ((" HPDF_Font_TextWidth\n"));

    if (!HPDF_Font_Validate(font))
        return tw;

    if (len > HPDF_LIMIT_MAX_STRING_LEN) {
        HPDF_RaiseError (font->error, HPDF_STRING_OUT_OF_RANGE, 0);
        return tw;
    }

    attr = (HPDF_FontAttr)font->attr;

    if (!attr->text_width_fn) {
        HPDF_SetError (font->error, HPDF_INVALID_OBJECT, 0);
        return tw;
    }

    tw = attr->text_width_fn (font, text, len);

    return tw;
}


HPDF_EXPORT(HPDF_UINT)
HPDF_Font_MeasureText (HPDF_Font          font,
                       const HPDF_BYTE   *text,
                       HPDF_UINT          len,
                       HPDF_REAL          width,
                       HPDF_REAL          font_size,
                       HPDF_REAL          char_space,
                       HPDF_REAL          word_space,
                       HPDF_BOOL          wordwrap,
                       HPDF_REAL         *real_width)
{
    HPDF_FontAttr attr;

    HPDF_PTRACE ((" HPDF_Font_MeasureText\n"));

    if (!HPDF_Font_Validate(font))
        return 0;

    if (len > HPDF_LIMIT_MAX_STRING_LEN) {
        HPDF_RaiseError (font->error, HPDF_STRING_OUT_OF_RANGE, 0);
        return 0;
    }

    attr = (HPDF_FontAttr)font->attr;

    if (!attr->measure_text_fn) {
        HPDF_RaiseError (font->error, HPDF_INVALID_OBJECT, 0);
        return 0;
    }

    return attr->measure_text_fn (font, text, len, width, font_size,
                            char_space, word_space, wordwrap, real_width);
}


HPDF_EXPORT(const char*)
HPDF_Font_GetFontName  (HPDF_Font font)
{
    HPDF_FontAttr attr;

    HPDF_PTRACE((" HPDF_Font_GetFontName\n"));

    if (!HPDF_Font_Validate(font))
        return NULL;

    attr = (HPDF_FontAttr)font->attr;

    return attr->fontdef->base_font;
}


HPDF_EXPORT(const char*)
HPDF_Font_GetEncodingName  (HPDF_Font font)
{
    HPDF_FontAttr attr;

    HPDF_PTRACE((" HPDF_Font_GetEncodingName\n"));

    if (!HPDF_Font_Validate(font))
        return NULL;

    attr = (HPDF_FontAttr)font->attr;

    return attr->encoder->name;
}


HPDF_EXPORT(HPDF_INT)
HPDF_Font_GetUnicodeWidth  (HPDF_Font       font,
                            HPDF_UNICODE    code)
{
    HPDF_FontAttr attr;
    HPDF_FontDef fontdef;

    HPDF_PTRACE((" HPDF_Font_GetUnicodeWidth\n"));

    if (!HPDF_Font_Validate(font))
        return 0;

    attr = (HPDF_FontAttr)font->attr;
    fontdef = attr->fontdef;

    if (fontdef->type == HPDF_FONTDEF_TYPE_TYPE1) {
        return HPDF_Type1FontDef_GetWidth (fontdef, code);
    } else if (fontdef->type == HPDF_FONTDEF_TYPE_TRUETYPE) {
        return HPDF_TTFontDef_GetCharWidth (fontdef, code);
    } else if (fontdef->type == HPDF_FONTDEF_TYPE_CID) {
        HPDF_CMapEncoderAttr encoder_attr =
            (HPDF_CMapEncoderAttr)attr->encoder->attr;
        HPDF_UINT l, h;

        for (l = 0; l <= 255; l++) {
            for (h = 0; h < 255; h++) {
                if (code == encoder_attr->unicode_map[l][h]) {
                    HPDF_UINT16 cid = encoder_attr->cid_map[l][h];

                    return HPDF_CIDFontDef_GetCIDWidth (fontdef, cid);
                }
            }
        }
    }

    HPDF_PTRACE((" HPDF_Font_GetUnicodeWidth not found (0x%04X)\n", code));

    return 0;
}


HPDF_EXPORT(HPDF_Box)
HPDF_Font_GetBBox  (HPDF_Font  font)
{
    HPDF_Box bbox = {0, 0, 0, 0};

    HPDF_PTRACE((" HPDF_Font_GetBBox\n"));
    if (HPDF_Font_Validate(font))
        return ((HPDF_FontAttr)font->attr)->fontdef->font_bbox;

    return bbox;
}

HPDF_EXPORT(HPDF_INT)
HPDF_Font_GetAscent  (HPDF_Font  font)
{
    HPDF_PTRACE((" HPDF_Font_GetAscent\n"));

    if (HPDF_Font_Validate(font))
        return ((HPDF_FontAttr)font->attr)->fontdef->ascent;

    return 0;
}

HPDF_EXPORT(HPDF_INT)
HPDF_Font_GetDescent  (HPDF_Font  font)
{
    HPDF_PTRACE((" HPDF_Font_GetDescent\n"));

    if (HPDF_Font_Validate(font))
        return ((HPDF_FontAttr)font->attr)->fontdef->descent;

    return 0;
}

HPDF_EXPORT(HPDF_UINT)
HPDF_Font_GetXHeight  (HPDF_Font  font)
{
    HPDF_PTRACE((" HPDF_Font_GetXHeight\n"));

    if (HPDF_Font_Validate(font))
        return ((HPDF_FontAttr)font->attr)->fontdef->x_height;

    return 0;
}

HPDF_EXPORT(HPDF_UINT)
HPDF_Font_GetCapHeight  (HPDF_Font  font)
{
    HPDF_PTRACE((" HPDF_Font_GetCapHeight\n"));

    if (HPDF_Font_Validate(font))
        return ((HPDF_FontAttr)font->attr)->fontdef->cap_height;

    return 0;
}


HPDF_BOOL
HPDF_Font_Validate  (HPDF_Font font)
{
    HPDF_PTRACE((" HPDF_Font_Validate\n"));

    if (!font || !font->attr || font->header.obj_class !=
            (HPDF_OSUBCLASS_FONT | HPDF_OCLASS_DICT))
        return HPDF_FALSE;

    return HPDF_TRUE;
}


