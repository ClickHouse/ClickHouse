/*
 * << Haru Free PDF Library >> -- hpdf_streams.h
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
 * 2005.12.20 Created.
 *
 */

#ifndef _HPDF_STREAMS_H
#define _HPDF_STREAMS_H

#include "hpdf_list.h"
#include "hpdf_encrypt.h"

#ifdef __cplusplus
extern "C" {
#endif


#define HPDF_STREAM_SIG_BYTES  0x5354524DL

typedef enum _HPDF_StreamType {
    HPDF_STREAM_UNKNOWN = 0,
    HPDF_STREAM_CALLBACK,
    HPDF_STREAM_FILE,
    HPDF_STREAM_MEMORY
} HPDF_StreamType;

#define HPDF_STREAM_FILTER_NONE          0x0000
#define HPDF_STREAM_FILTER_ASCIIHEX      0x0100
#define HPDF_STREAM_FILTER_ASCII85       0x0200
#define HPDF_STREAM_FILTER_FLATE_DECODE  0x0400
#define HPDF_STREAM_FILTER_DCT_DECODE    0x0800
#define HPDF_STREAM_FILTER_CCITT_DECODE  0x1000

typedef enum _HPDF_WhenceMode {
    HPDF_SEEK_SET = 0,
    HPDF_SEEK_CUR,
    HPDF_SEEK_END
} HPDF_WhenceMode;

typedef struct _HPDF_Stream_Rec  *HPDF_Stream;

typedef HPDF_STATUS
(*HPDF_Stream_Write_Func)  (HPDF_Stream      stream,
                            const HPDF_BYTE  *ptr,
                            HPDF_UINT        siz);


typedef HPDF_STATUS
(*HPDF_Stream_Read_Func)  (HPDF_Stream  stream,
                           HPDF_BYTE    *ptr,
                           HPDF_UINT    *siz);


typedef HPDF_STATUS
(*HPDF_Stream_Seek_Func)  (HPDF_Stream      stream,
                           HPDF_INT         pos,
                           HPDF_WhenceMode  mode);


typedef HPDF_INT32
(*HPDF_Stream_Tell_Func)  (HPDF_Stream      stream);


typedef void
(*HPDF_Stream_Free_Func)  (HPDF_Stream  stream);


typedef HPDF_UINT32
(*HPDF_Stream_Size_Func)  (HPDF_Stream  stream);


typedef struct _HPDF_MemStreamAttr_Rec  *HPDF_MemStreamAttr;


typedef struct _HPDF_MemStreamAttr_Rec {
    HPDF_List  buf;
    HPDF_UINT  buf_siz;
    HPDF_UINT  w_pos;
    HPDF_BYTE  *w_ptr;
    HPDF_UINT  r_ptr_idx;
    HPDF_UINT  r_pos;
    HPDF_BYTE  *r_ptr;
} HPDF_MemStreamAttr_Rec;


typedef struct _HPDF_Stream_Rec {
    HPDF_UINT32               sig_bytes;
    HPDF_StreamType           type;
    HPDF_MMgr                 mmgr;
    HPDF_Error                error;
    HPDF_UINT                 size;
    HPDF_Stream_Write_Func    write_fn;
    HPDF_Stream_Read_Func     read_fn;
    HPDF_Stream_Seek_Func     seek_fn;
    HPDF_Stream_Free_Func     free_fn;
    HPDF_Stream_Tell_Func     tell_fn;
    HPDF_Stream_Size_Func     size_fn;
    void*                     attr;
} HPDF_Stream_Rec;



HPDF_Stream
HPDF_MemStream_New  (HPDF_MMgr  mmgr,
                     HPDF_UINT  buf_siz);


HPDF_BYTE*
HPDF_MemStream_GetBufPtr  (HPDF_Stream  stream,
                           HPDF_UINT    index,
                           HPDF_UINT    *length);


HPDF_UINT
HPDF_MemStream_GetBufSize  (HPDF_Stream  stream);


HPDF_UINT
HPDF_MemStream_GetBufCount  (HPDF_Stream  stream);


HPDF_STATUS
HPDF_MemStream_Rewrite  (HPDF_Stream  stream,
                         HPDF_BYTE    *buf,
                         HPDF_UINT    size);


void
HPDF_MemStream_FreeData  (HPDF_Stream  stream);


HPDF_STATUS
HPDF_Stream_WriteToStream  (HPDF_Stream   src,
                            HPDF_Stream   dst,
                            HPDF_UINT     filter,
                            HPDF_Encrypt  e);


HPDF_Stream
HPDF_FileReader_New  (HPDF_MMgr   mmgr,
                      const char  *fname);


HPDF_Stream
HPDF_FileWriter_New  (HPDF_MMgr        mmgr,
                      const char  *fname);


HPDF_Stream
HPDF_CallbackReader_New  (HPDF_MMgr              mmgr,
                          HPDF_Stream_Read_Func  read_fn,
                          HPDF_Stream_Seek_Func  seek_fn,
                          HPDF_Stream_Tell_Func  tell_fn,
                          HPDF_Stream_Size_Func  size_fn,
                          void*                  data);


HPDF_Stream
HPDF_CallbackWriter_New (HPDF_MMgr               mmgr,
                         HPDF_Stream_Write_Func  write_fn,
                         void*                   data);


void
HPDF_Stream_Free  (HPDF_Stream  stream);


HPDF_STATUS
HPDF_Stream_WriteChar  (HPDF_Stream  stream,
                        char    value);


HPDF_STATUS
HPDF_Stream_WriteStr  (HPDF_Stream      stream,
                       const char  *value);


HPDF_STATUS
HPDF_Stream_WriteUChar  (HPDF_Stream  stream,
                         HPDF_BYTE    value);


HPDF_STATUS
HPDF_Stream_WriteInt  (HPDF_Stream  stream,
                       HPDF_INT     value);


HPDF_STATUS
HPDF_Stream_WriteUInt  (HPDF_Stream  stream,
                        HPDF_UINT    value);


HPDF_STATUS
HPDF_Stream_WriteReal  (HPDF_Stream  stream,
                        HPDF_REAL    value);


HPDF_STATUS
HPDF_Stream_Write  (HPDF_Stream      stream,
                    const HPDF_BYTE  *ptr,
                    HPDF_UINT        size);


HPDF_STATUS
HPDF_Stream_Read  (HPDF_Stream  stream,
                   HPDF_BYTE    *ptr,
                   HPDF_UINT    *size);

HPDF_STATUS
HPDF_Stream_ReadLn  (HPDF_Stream  stream,
                     char    *s,
                     HPDF_UINT    *size);


HPDF_INT32
HPDF_Stream_Tell  (HPDF_Stream  stream);


HPDF_STATUS
HPDF_Stream_Seek  (HPDF_Stream      stream,
                   HPDF_INT         pos,
                   HPDF_WhenceMode  mode);


HPDF_BOOL
HPDF_Stream_EOF  (HPDF_Stream  stream);


HPDF_UINT32
HPDF_Stream_Size  (HPDF_Stream  stream);

HPDF_STATUS
HPDF_Stream_Flush  (HPDF_Stream  stream);


HPDF_STATUS
HPDF_Stream_WriteEscapeName  (HPDF_Stream      stream,
                              const char  *value);


HPDF_STATUS
HPDF_Stream_WriteEscapeText2  (HPDF_Stream    stream,
                               const char    *text,
                               HPDF_UINT      len);


HPDF_STATUS
HPDF_Stream_WriteEscapeText  (HPDF_Stream      stream,
                              const char  *text);


HPDF_STATUS
HPDF_Stream_WriteBinary  (HPDF_Stream      stream,
                          const HPDF_BYTE  *data,
                          HPDF_UINT        len,
                          HPDF_Encrypt     e);


HPDF_STATUS
HPDF_Stream_Validate  (HPDF_Stream  stream);


#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _HPDF_STREAMS_H */
