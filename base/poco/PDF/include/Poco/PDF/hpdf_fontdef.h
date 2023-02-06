/*
 * << Haru Free PDF Library >> -- hpdf_fontdef.h
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

#ifndef _HPDF_FONTDEF_H
#define _HPDF_FONTDEF_H

#include "hpdf_objects.h"

#ifdef __cplusplus
extern "C" {
#endif


#define HPDF_FONTDEF_SIG_BYTES 0x464F4E54L

/*------ collection of flags for defining characteristics. ---*/

#define HPDF_FONT_FIXED_WIDTH    1
#define HPDF_FONT_SERIF          2
#define HPDF_FONT_SYMBOLIC       4
#define HPDF_FONT_SCRIPT         8
  /* Reserved                    16 */
#define HPDF_FONT_STD_CHARSET    32
#define HPDF_FONT_ITALIC         64
  /* Reserved                    128
     Reserved                    256
     Reserved                    512
     Reserved                    1024
     Reserved                    2048
     Reserved                    4096
     Reserved                    8192
     Reserved                    16384
     Reserved                    32768 */
#define HPDF_FONT_ALL_CAP        65536
#define HPDF_FONT_SMALL_CAP      131072
#define HPDF_FONT_FOURCE_BOLD    262144

#define HPDF_CID_W_TYPE_FROM_TO     0
#define HPDF_CID_W_TYPE_FROM_ARRAY  1

/*-- HPDF_FontDef ---------------------------------------*/

typedef struct _HPDF_CharData {
    HPDF_INT16     char_cd;
    HPDF_UNICODE   unicode;
    HPDF_INT16     width;
} HPDF_CharData;

typedef enum  _HPDF_FontDefType {
    HPDF_FONTDEF_TYPE_TYPE1,
    HPDF_FONTDEF_TYPE_TRUETYPE,
    HPDF_FONTDEF_TYPE_CID,
    HPDF_FONTDEF_TYPE_UNINITIALIZED,
    HPDF_FONTDEF_TYPE_EOF
} HPDF_FontDefType;

typedef struct _HPDF_CID_Width {
    HPDF_UINT16   cid;
    HPDF_INT16    width;
}  HPDF_CID_Width;

/*----------------------------------------------------------------------------*/
/*----- HPDF_FontDef ---------------------------------------------------------*/

typedef struct _HPDF_FontDef_Rec  *HPDF_FontDef;

typedef void  (*HPDF_FontDef_FreeFunc)  (HPDF_FontDef  fontdef);

typedef void  (*HPDF_FontDef_CleanFunc)  (HPDF_FontDef  fontdef);

typedef HPDF_STATUS  (*HPDF_FontDef_InitFunc)  (HPDF_FontDef  fontdef);

typedef struct _HPDF_FontDef_Rec {
    HPDF_UINT32              sig_bytes;
    char                base_font[HPDF_LIMIT_MAX_NAME_LEN + 1];
    HPDF_MMgr                mmgr;
    HPDF_Error               error;
    HPDF_FontDefType         type;
    HPDF_FontDef_CleanFunc   clean_fn;
    HPDF_FontDef_FreeFunc    free_fn;
    HPDF_FontDef_InitFunc    init_fn;

    HPDF_INT16    ascent;
    HPDF_INT16    descent;
    HPDF_UINT     flags;
    HPDF_Box      font_bbox;
    HPDF_INT16    italic_angle;
    HPDF_UINT16   stemv;
    HPDF_INT16    avg_width;
    HPDF_INT16    max_width;
    HPDF_INT16    missing_width;
    HPDF_UINT16   stemh;
    HPDF_UINT16   x_height;
    HPDF_UINT16   cap_height;

    /*  the initial value of descriptor entry is NULL.
     *  when first font-object besed on the fontdef object is created,
     *  the font-descriptor object is created and descriptor entry is set.
     */
    HPDF_Dict                descriptor;
    HPDF_Stream              data;

    HPDF_BOOL                valid;
    void                    *attr;
} HPDF_FontDef_Rec;


void
HPDF_FontDef_Free  (HPDF_FontDef  fontdef);


void
HPDF_FontDef_Cleanup  (HPDF_FontDef  fontdef);


HPDF_BOOL
HPDF_FontDef_Validate  (HPDF_FontDef  fontdef);


/*----------------------------------------------------------------------------*/
/*----- HPDF_Type1FontDef  ---------------------------------------------------*/

typedef struct _HPDF_Type1FontDefAttrRec   *HPDF_Type1FontDefAttr;

typedef struct _HPDF_Type1FontDefAttrRec {
    HPDF_BYTE       first_char;                               /* Required */
    HPDF_BYTE       last_char;                                /* Required */
    HPDF_CharData  *widths;                                   /* Required */
    HPDF_UINT       widths_count;

    HPDF_INT16      leading;
    char      *char_set;
    char       encoding_scheme[HPDF_LIMIT_MAX_NAME_LEN + 1];
    HPDF_UINT       length1;
    HPDF_UINT       length2;
    HPDF_UINT       length3;
    HPDF_BOOL       is_base14font;
    HPDF_BOOL       is_fixed_pitch;

    HPDF_Stream     font_data;
} HPDF_Type1FontDefAttr_Rec;



HPDF_FontDef
HPDF_Type1FontDef_New  (HPDF_MMgr  mmgr);


HPDF_FontDef
HPDF_Type1FontDef_Load  (HPDF_MMgr         mmgr,
                         HPDF_Stream       afm,
                         HPDF_Stream       font_data);


HPDF_FontDef
HPDF_Type1FontDef_Duplicate  (HPDF_MMgr     mmgr,
                              HPDF_FontDef  src);


HPDF_STATUS
HPDF_Type1FontDef_SetWidths  (HPDF_FontDef         fontdef,
                              const HPDF_CharData  *widths);


HPDF_INT16
HPDF_Type1FontDef_GetWidthByName  (HPDF_FontDef     fontdef,
                                   const char  *gryph_name);


HPDF_INT16
HPDF_Type1FontDef_GetWidth  (HPDF_FontDef  fontdef,
                             HPDF_UNICODE  unicode);


HPDF_FontDef
HPDF_Base14FontDef_New  (HPDF_MMgr        mmgr,
                         const char  *font_name);



/*----------------------------------------------------------------------------*/
/*----- HPDF_TTFontDef  ------------------------------------------------------*/

#define HPDF_TTF_FONT_TAG_LEN  6

typedef struct _HPDF_TTF_Table {
        char     tag[4];
        HPDF_UINT32   check_sum;
        HPDF_UINT32   offset;
        HPDF_UINT32   length;
} HPDF_TTFTable;


typedef struct _HPDF_TTF_OffsetTbl {
        HPDF_UINT32     sfnt_version;
        HPDF_UINT16     num_tables;
        HPDF_UINT16     search_range;
        HPDF_UINT16     entry_selector;
        HPDF_UINT16     range_shift;
        HPDF_TTFTable  *table;
} HPDF_TTF_OffsetTbl;


typedef struct _HPDF_TTF_CmapRange {
        HPDF_UINT16   format;
        HPDF_UINT16   length;
        HPDF_UINT16   language;
        HPDF_UINT16   seg_count_x2;
        HPDF_UINT16   search_range;
        HPDF_UINT16   entry_selector;
        HPDF_UINT16   range_shift;
        HPDF_UINT16  *end_count;
        HPDF_UINT16   reserved_pad;
        HPDF_UINT16  *start_count;
        HPDF_INT16   *id_delta;
        HPDF_UINT16  *id_range_offset;
        HPDF_UINT16  *glyph_id_array;
        HPDF_UINT     glyph_id_array_count;
} HPDF_TTF_CmapRange;


typedef struct _HPDF_TTF_GryphOffsets {
        HPDF_UINT32   base_offset;
        HPDF_UINT32  *offsets;
        HPDF_BYTE    *flgs;   /* 0: unused, 1: used */
} HPDF_TTF_GryphOffsets;


typedef struct _HPDF_TTF_LongHorMetric {
        HPDF_UINT16  advance_width;
        HPDF_INT16   lsb;
} HPDF_TTF_LongHorMetric;


typedef struct _HPDF_TTF_FontHeader {
    HPDF_BYTE     version_number[4];
    HPDF_UINT32   font_revision;
    HPDF_UINT32   check_sum_adjustment;
    HPDF_UINT32   magic_number;
    HPDF_UINT16   flags;
    HPDF_UINT16   units_per_em;
    HPDF_BYTE     created[8];
    HPDF_BYTE     modified[8];
    HPDF_INT16    x_min;
    HPDF_INT16    y_min;
    HPDF_INT16    x_max;
    HPDF_INT16    y_max;
    HPDF_UINT16   mac_style;
    HPDF_UINT16   lowest_rec_ppem;
    HPDF_INT16    font_direction_hint;
    HPDF_INT16    index_to_loc_format;
    HPDF_INT16    glyph_data_format;
} HPDF_TTF_FontHeader;


typedef struct _HPDF_TTF_NameRecord {
    HPDF_UINT16   platform_id;
    HPDF_UINT16   encoding_id;
    HPDF_UINT16   language_id;
    HPDF_UINT16   name_id;
    HPDF_UINT16   length;
    HPDF_UINT16   offset;
}  HPDF_TTF_NameRecord;


typedef struct _HPDF_TTF_NamingTable {
    HPDF_UINT16           format;
    HPDF_UINT16           count;
    HPDF_UINT16           string_offset;
    HPDF_TTF_NameRecord  *name_records;
}  HPDF_TTF_NamingTable;


typedef struct _HPDF_TTFontDefAttr_Rec   *HPDF_TTFontDefAttr;

typedef struct _HPDF_TTFontDefAttr_Rec {
    char                base_font[HPDF_LIMIT_MAX_NAME_LEN + 1];
    HPDF_BYTE                first_char;
    HPDF_BYTE                last_char;
    char               *char_set;
    char                tag_name[HPDF_TTF_FONT_TAG_LEN + 1];
    char                tag_name2[(HPDF_TTF_FONT_TAG_LEN + 1) * 2];
    HPDF_TTF_FontHeader      header;
    HPDF_TTF_GryphOffsets    glyph_tbl;
    HPDF_UINT16              num_glyphs;
    HPDF_TTF_NamingTable     name_tbl;
    HPDF_TTF_LongHorMetric  *h_metric;
    HPDF_UINT16              num_h_metric;
    HPDF_TTF_OffsetTbl       offset_tbl;
    HPDF_TTF_CmapRange       cmap;
    HPDF_UINT16              fs_type;
    HPDF_BYTE                sfamilyclass[2];
    HPDF_BYTE                panose[10];
    HPDF_UINT32              code_page_range1;
    HPDF_UINT32              code_page_range2;

    HPDF_UINT                length1;

    HPDF_BOOL                embedding;
    HPDF_BOOL                is_cidfont;

    HPDF_Stream              stream;
} HPDF_TTFontDefAttr_Rec;



HPDF_FontDef
HPDF_TTFontDef_New (HPDF_MMgr   mmgr);


HPDF_FontDef
HPDF_TTFontDef_Load  (HPDF_MMgr     mmgr,
                      HPDF_Stream   stream,
                      HPDF_BOOL     embedding);


HPDF_FontDef
HPDF_TTFontDef_Load2  (HPDF_MMgr     mmgr,
                       HPDF_Stream   stream,
                       HPDF_UINT     index,
                       HPDF_BOOL     embedding);


HPDF_UINT16
HPDF_TTFontDef_GetGlyphid  (HPDF_FontDef   fontdef,
                            HPDF_UINT16    unicode);


HPDF_INT16
HPDF_TTFontDef_GetCharWidth  (HPDF_FontDef   fontdef,
                              HPDF_UINT16    unicode);


HPDF_INT16
HPDF_TTFontDef_GetGidWidth  (HPDF_FontDef   fontdef,
                             HPDF_UINT16    gid);


HPDF_STATUS
HPDF_TTFontDef_SaveFontData  (HPDF_FontDef   fontdef,
                              HPDF_Stream    stream);


HPDF_Box
HPDF_TTFontDef_GetCharBBox  (HPDF_FontDef   fontdef,
                             HPDF_UINT16    unicode);


void
HPDF_TTFontDef_SetTagName  (HPDF_FontDef   fontdef,
                            char     *tag);


/*----------------------------------------------------------------------------*/
/*----- HPDF_CIDFontDef  -----------------------------------------------------*/

typedef struct _HPDF_CIDFontDefAttrRec   *HPDF_CIDFontDefAttr;

typedef struct _HPDF_CIDFontDefAttrRec {
    HPDF_List     widths;
    HPDF_INT16    DW;
    HPDF_INT16    DW2[2];
} HPDF_CIDFontDefAttr_Rec;


HPDF_FontDef
HPDF_CIDFontDef_New  (HPDF_MMgr               mmgr,
                      char              *name,
                      HPDF_FontDef_InitFunc   init_fn);


HPDF_STATUS
HPDF_CIDFontDef_AddWidth  (HPDF_FontDef            fontdef,
                           const HPDF_CID_Width   *widths);


HPDF_INT16
HPDF_CIDFontDef_GetCIDWidth  (HPDF_FontDef  fontdef,
                              HPDF_UINT16   cid);



HPDF_STATUS
HPDF_CIDFontDef_ChangeStyle   (HPDF_FontDef    fontdef,
                               HPDF_BOOL       bold,
                               HPDF_BOOL       italic);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _HPDF_FONTDEF_H */
