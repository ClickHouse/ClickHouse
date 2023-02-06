/*
 * << Haru Free PDF Library >> -- hpdf_image.c
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
#include <memory.h>
#include <assert.h>

#define	G3CODES
#include "t4.h"

typedef unsigned int uint32;
typedef int int32;
typedef unsigned short uint16;
typedef int32 tsize_t;          /* i/o size in bytes */
/*
 * Typedefs for ``method pointers'' used internally.
 */
typedef	unsigned char tidataval_t;	/* internal image data value type */
typedef	tidataval_t* tidata_t;		/* reference to internal image data */

/*
 * Compression+decompression state blocks are
 * derived from this ``base state'' block.
 */
typedef struct {
        /* int     rw_mode;                */ /* O_RDONLY for decode, else encode */
	int	mode;			/* operating mode */
	uint32	rowbytes;		/* bytes in a decoded scanline */
	uint32	rowpixels;		/* pixels in a scanline */

	uint16	cleanfaxdata;		/* CleanFaxData tag */
	uint32	badfaxrun;		/* BadFaxRun tag */
	uint32	badfaxlines;		/* BadFaxLines tag */
	uint32	groupoptions;		/* Group 3/4 options tag */
	uint32	recvparams;		/* encoded Class 2 session params */
	char*	subaddress;		/* subaddress string */
	uint32	recvtime;		/* time spent receiving (secs) */
	char*	faxdcs;			/* Table 2/T.30 encoded session params */
} HPDF_Fax3BaseState;

typedef struct {
	HPDF_Fax3BaseState b;

	/* Decoder state info */
	const unsigned char* bitmap;	/* bit reversal table */
	uint32	data;			/* current i/o byte/word */
	int	bit;			/* current i/o bit in byte */
	int	EOLcnt;			/* count of EOL codes recognized */
	/* TIFFFaxFillFunc fill;*/		/* fill routine */
	uint32*	runs;			/* b&w runs for current/previous row */
	uint32*	refruns;		/* runs for reference line */
	uint32*	curruns;		/* runs for current line */

	/* Encoder state info */
	/* Ttag    tag;		*/	/* encoding state */
	unsigned char*	refline;	/* reference line for 2d decoding */
	int	k;			/* #rows left that can be 2d encoded */
	int	maxk;			/* max #rows that can be 2d encoded */

	int line;
} HPDF_Fax3CodecState;

#define	Fax3State(tif)		(&(tif)->tif_data->b)
#define	EncoderState(tif)	((tif)->tif_data)
#define	isAligned(p,t)	((((unsigned long)(p)) & (sizeof (t)-1)) == 0)

/* NB: the uint32 casts are to silence certain ANSI-C compilers */
#define TIFFhowmany(x, y) ((((uint32)(x))+(((uint32)(y))-1))/((uint32)(y)))
#define TIFFhowmany8(x) (((x)&0x07)?((uint32)(x)>>3)+1:(uint32)(x)>>3)
#define	TIFFroundup(x, y) (TIFFhowmany(x,y)*(y))


/*
struct _HPDF_CCITT_Encoder {
} HPDF_CCITT_Encoder;
*/

struct _HPDF_CCITT_Data {
	HPDF_Fax3CodecState *tif_data;

	HPDF_Stream  dst;

	tsize_t		tif_rawdatasize;/* # of bytes in raw data buffer */
	tsize_t		tif_rawcc;	/* bytes unread from raw buffer */
	tidata_t	tif_rawcp;	/* current spot in raw buffer */
	tidata_t	tif_rawdata;	/* raw data buffer */	

} HPDF_CCITT_Data;

static HPDF_STATUS HPDF_InitCCITTFax3(struct _HPDF_CCITT_Data *pData)
{
	HPDF_Fax3BaseState* sp;
	HPDF_Fax3CodecState* esp;

	/*
	 * Allocate state block so tag methods have storage to record values.
	 */
	pData->tif_data = (HPDF_Fax3CodecState *)
		malloc(sizeof (HPDF_Fax3CodecState));

	if (pData->tif_data == NULL) {
		return 1;
	}

	sp = Fax3State(pData);
    /* sp->rw_mode = pData->tif_mode; */

	/*
	 * Override parent get/set field methods.
	 */
	sp->groupoptions = 0;	
	sp->recvparams = 0;
	sp->subaddress = NULL;
	sp->faxdcs = NULL;

	esp = EncoderState(pData);
	esp->refline = NULL;
	esp->runs = NULL;

	return HPDF_OK;
}

static HPDF_STATUS HPDF_FreeCCITTFax3(struct _HPDF_CCITT_Data *pData)
{
	if(pData->tif_data!=NULL) {
		HPDF_Fax3CodecState* esp=pData->tif_data;
		if(esp->refline!=NULL) {
			free(esp->refline);
			esp->refline=NULL;
		}
		if(esp->runs!=NULL) {
			free(esp->runs);
			esp->runs=NULL;
		}
		free(pData->tif_data);
		pData->tif_data=NULL;
	}
	if(pData->tif_rawdata!=NULL) {
		free(pData->tif_rawdata);
		pData->tif_rawdata=NULL;
	}
	return HPDF_OK;
}


/*
 * Setup G3/G4-related compression/decompression state
 * before data is processed.  This routine is called once
 * per image -- it sets up different state based on whether
 * or not decoding or encoding is being done and whether
 * 1D- or 2D-encoded data is involved.
 */
static int
HPDF_Fax3SetupState(struct _HPDF_CCITT_Data *pData, HPDF_UINT          width,
							HPDF_UINT          height,
							HPDF_UINT          line_width)
{
	HPDF_Fax3BaseState* sp = Fax3State(pData);
	HPDF_Fax3CodecState* esp = EncoderState(pData);
	uint32 rowbytes, rowpixels, nruns;

	HPDF_UNUSED (height);

	rowbytes = line_width;
	rowpixels = width;

	sp->rowbytes = (uint32) rowbytes;
	sp->rowpixels = (uint32) rowpixels;

	nruns = 2*TIFFroundup(rowpixels,32);
	nruns += 3;
	esp->runs = (uint32*) malloc(2*nruns * sizeof (uint32));
	if (esp->runs == NULL)
		return 1;
	esp->curruns = esp->runs;
	esp->refruns = esp->runs + nruns;

	/*
	 * 2d encoding requires a scanline
	 * buffer for the ``reference line''; the
	 * scanline against which delta encoding
	 * is referenced.  The reference line must
	 * be initialized to be ``white'' (done elsewhere).
	 */
	esp->refline = (unsigned char*) malloc(rowbytes);
	if (esp->refline == NULL) {
		return 1;
	}

	return HPDF_OK;
}

/*
 * Reset encoding state at the start of a strip.
 */
static HPDF_STATUS 
HPDF_Fax3PreEncode(struct _HPDF_CCITT_Data *pData/*, tsample_t s*/)
{
	HPDF_Fax3CodecState* sp = EncoderState(pData);

	/* assert(sp != NULL); */
	sp->bit = 8;
	sp->data = 0;
	/* sp->tag = G3_1D; */
	/*
	 * This is necessary for Group 4; otherwise it isn't
	 * needed because the first scanline of each strip ends
	 * up being copied into the refline.
	 */
	if (sp->refline)
		memset(sp->refline, 0x00, sp->b.rowbytes);
	sp->k = sp->maxk = 0;
	sp->line = 0;
	return HPDF_OK;
}

static HPDF_STATUS 
HPDF_CCITT_AppendToStream(HPDF_Stream  dst, 	
						  tidata_t	tif_rawdata,
						  tsize_t	tif_rawcc)
{
	if(HPDF_Stream_Write(dst, tif_rawdata, tif_rawcc)!=HPDF_OK)
		return 1;
	return HPDF_OK;
}

/*
 * Internal version of TIFFFlushData that can be
 * called by ``encodestrip routines'' w/o concern
 * for infinite recursion.
 */
static HPDF_STATUS 
HPDF_CCITT_FlushData(struct _HPDF_CCITT_Data *pData)
{
	if (pData->tif_rawcc > 0) {
		/*if (!isFillOrder(tif, tif->tif_dir.td_fillorder) &&
		    (tif->tif_flags & TIFF_NOBITREV) == 0)
			TIFFReverseBits((unsigned char *pData->tif_rawdata,
			    pData->tif_rawcc);*/
		if (HPDF_CCITT_AppendToStream(pData->dst,
		    pData->tif_rawdata, pData->tif_rawcc)!=HPDF_OK)
			return 1;
		pData->tif_rawcc = 0;
		pData->tif_rawcp = pData->tif_rawdata;
	}
	return HPDF_OK;
}

#define	HPDF_Fax3FlushBits(tif, sp) {				\
	if ((tif)->tif_rawcc >= (tif)->tif_rawdatasize)		\
		(void) HPDF_CCITT_FlushData(tif);			\
	*(tif)->tif_rawcp++ = (tidataval_t) (sp)->data;		\
	(tif)->tif_rawcc++;					\
	(sp)->data = 0, (sp)->bit = 8;				\
}
#define	_FlushBits(tif) {					\
	if ((tif)->tif_rawcc >= (tif)->tif_rawdatasize)		\
		(void) HPDF_CCITT_FlushData(tif);			\
	*(tif)->tif_rawcp++ = (tidataval_t) data;		\
	(tif)->tif_rawcc++;					\
	data = 0, bit = 8;					\
}
static const int _msbmask[9] =
    { 0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f, 0xff };
#define	_PutBits(tif, bits, length) {				\
	while (length > bit) {					\
		data |= bits >> (length - bit);			\
		length -= bit;					\
		_FlushBits(tif);				\
	}							\
	data |= (bits & _msbmask[length]) << (bit - length);	\
	bit -= length;						\
	if (bit == 0)						\
		_FlushBits(tif);				\
}

/*
 * Write a variable-length bit-value to
 * the output stream.  Values are
 * assumed to be at most 16 bits.
 */
static void
HPDF_Fax3PutBits(struct _HPDF_CCITT_Data *pData, unsigned int bits, unsigned int length)
{
	HPDF_Fax3CodecState* sp = EncoderState(pData);
	unsigned int bit = sp->bit;
	int data = sp->data;

	_PutBits(pData, bits, length);

	sp->data = data;
	sp->bit = bit;
}

/*
 * Write a code to the output stream.
 */
#define putcode(tif, te)	HPDF_Fax3PutBits(tif, (te)->code, (te)->length)


/*
 * Write the sequence of codes that describes
 * the specified span of zero's or one's.  The
 * appropriate table that holds the make-up and
 * terminating codes is supplied.
 */
static void
putspan(struct _HPDF_CCITT_Data *pData, int32 span, const tableentry* tab)
{
	HPDF_Fax3CodecState* sp = EncoderState(pData);
	unsigned int bit = sp->bit;
	int data = sp->data;
	unsigned int code, length;

	while (span >= 2624) {
		const tableentry* te = &tab[63 + (2560>>6)];
		code = te->code, length = te->length;
#ifdef FAX3_DEBUG
		DEBUG_PRINT("MakeUp", te->runlen);
#endif
		_PutBits(pData, code, length);
		span -= te->runlen;
	}
	if (span >= 64) {
		const tableentry* te = &tab[63 + (span>>6)];
		assert(te->runlen == 64*(span>>6));
		code = te->code, length = te->length;
#ifdef FAX3_DEBUG
		DEBUG_PRINT("MakeUp", te->runlen);
#endif
		_PutBits(pData, code, length);
		span -= te->runlen;
	}
	code = tab[span].code, length = tab[span].length;
#ifdef FAX3_DEBUG
	DEBUG_PRINT("  Term", tab[span].runlen);
#endif
	_PutBits(pData, code, length);

	sp->data = data;
	sp->bit = bit;
}

static const unsigned char zeroruns[256] = {
    8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4,	/* 0x00 - 0x0f */
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,	/* 0x10 - 0x1f */
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,	/* 0x20 - 0x2f */
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,	/* 0x30 - 0x3f */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,	/* 0x40 - 0x4f */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,	/* 0x50 - 0x5f */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,	/* 0x60 - 0x6f */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,	/* 0x70 - 0x7f */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,	/* 0x80 - 0x8f */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,	/* 0x90 - 0x9f */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,	/* 0xa0 - 0xaf */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,	/* 0xb0 - 0xbf */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,	/* 0xc0 - 0xcf */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,	/* 0xd0 - 0xdf */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,	/* 0xe0 - 0xef */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,	/* 0xf0 - 0xff */
};
static const unsigned char oneruns[256] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,	/* 0x00 - 0x0f */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,	/* 0x10 - 0x1f */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,	/* 0x20 - 0x2f */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,	/* 0x30 - 0x3f */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,	/* 0x40 - 0x4f */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,	/* 0x50 - 0x5f */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,	/* 0x60 - 0x6f */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,	/* 0x70 - 0x7f */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,	/* 0x80 - 0x8f */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,	/* 0x90 - 0x9f */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,	/* 0xa0 - 0xaf */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,	/* 0xb0 - 0xbf */
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,	/* 0xc0 - 0xcf */
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,	/* 0xd0 - 0xdf */
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,	/* 0xe0 - 0xef */
    4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 7, 8,	/* 0xf0 - 0xff */
};

/*
 * Find a span of ones or zeros using the supplied
 * table.  The ``base'' of the bit string is supplied
 * along with the start+end bit indices.
 */
static /*inline*/ int32 find0span(unsigned char* bp, int32 bs, int32 be)
{
	int32 bits = be - bs;
	int32 n, span;

	bp += bs>>3;
	/*
	 * Check partial byte on lhs.
	 */
	if (bits > 0 && (n = (bs & 7))) {
		span = zeroruns[(*bp << n) & 0xff];
		if (span > 8-n)		/* table value too generous */
			span = 8-n;
		if (span > bits)	/* constrain span to bit range */
			span = bits;
		if (n+span < 8)		/* doesn't extend to edge of byte */
			return (span);
		bits -= span;
		bp++;
	} else
		span = 0;
	if (bits >= (int32)(2 * 8 * sizeof(long))) {
		long* lp;
		/*
		 * Align to longword boundary and check longwords.
		 */
		while (!isAligned(bp, long)) {
			if (*bp != 0x00)
				return (span + zeroruns[*bp]);
			span += 8, bits -= 8;
			bp++;
		}
		lp = (long*) bp;
		while ((bits >= (int32)(8 * sizeof(long))) && (0 == *lp)) {
			span += 8*sizeof (long), bits -= 8*sizeof (long);
			lp++;
		}
		bp = (unsigned char*) lp;
	}
	/*
	 * Scan full bytes for all 0's.
	 */
	while (bits >= 8) {
		if (*bp != 0x00)	/* end of run */
			return (span + zeroruns[*bp]);
		span += 8, bits -= 8;
		bp++;
	}
	/*
	 * Check partial byte on rhs.
	 */
	if (bits > 0) {
		n = zeroruns[*bp];
		span += (n > bits ? bits : n);
	}
	return (span);
}

static /*inline*/ int32
find1span(unsigned char* bp, int32 bs, int32 be)
{
	int32 bits = be - bs;
	int32 n, span;

	bp += bs>>3;
	/*
	 * Check partial byte on lhs.
	 */
	if (bits > 0 && (n = (bs & 7))) {
		span = oneruns[(*bp << n) & 0xff];
		if (span > 8-n)		/* table value too generous */
			span = 8-n;
		if (span > bits)	/* constrain span to bit range */
			span = bits;
		if (n+span < 8)		/* doesn't extend to edge of byte */
			return (span);
		bits -= span;
		bp++;
	} else
		span = 0;
	if (bits >= (int32)(2 * 8 * sizeof(long))) {
		long* lp;
		/*
		 * Align to longword boundary and check longwords.
		 */
		while (!isAligned(bp, long)) {
			if (*bp != 0xff)
				return (span + oneruns[*bp]);
			span += 8, bits -= 8;
			bp++;
		}
		lp = (long*) bp;
		while ((bits >= (int32)(8 * sizeof(long))) && (~0 == *lp)) {
			span += 8*sizeof (long), bits -= 8*sizeof (long);
			lp++;
		}
		bp = (unsigned char*) lp;
	}
	/*
	 * Scan full bytes for all 1's.
	 */
	while (bits >= 8) {
		if (*bp != 0xff)	/* end of run */
			return (span + oneruns[*bp]);
		span += 8, bits -= 8;
		bp++;
	}
	/*
	 * Check partial byte on rhs.
	 */
	if (bits > 0) {
		n = oneruns[*bp];
		span += (n > bits ? bits : n);
	}
	return (span);
}

/*
 * Return the offset of the next bit in the range
 * [bs..be] that is different from the specified
 * color.  The end, be, is returned if no such bit
 * exists.
 */
#define	finddiff(_cp, _bs, _be, _color)	\
	(_bs + (_color ? find1span(_cp,_bs,_be) : find0span(_cp,_bs,_be)))
/*
 * Like finddiff, but also check the starting bit
 * against the end in case start > end.
 */
#define	finddiff2(_cp, _bs, _be, _color) \
	(_bs < _be ? finddiff(_cp,_bs,_be,_color) : _be)


/*
void 
HPDF_Fax3PostEncode(struct _HPDF_CCITT_Data *pData)
{
	HPDF_Fax3CodecState* sp = EncoderState(pData);

	if (sp->bit != 8)
		HPDF_Fax3FlushBits(pData, sp);
}
*/

static const tableentry horizcode =
    { 3, 0x1, 0 };	/* 001 */
static const tableentry passcode =
    { 4, 0x1, 0 };	/* 0001 */
static const tableentry vcodes[7] = {
    { 7, 0x03, 0 },	/* 0000 011 */
    { 6, 0x03, 0 },	/* 0000 11 */
    { 3, 0x03, 0 },	/* 011 */
    { 1, 0x1, 0 },	/* 1 */
    { 3, 0x2, 0 },	/* 010 */
    { 6, 0x02, 0 },	/* 0000 10 */
    { 7, 0x02, 0 }	/* 0000 010 */
};

/*
 * 2d-encode a row of pixels.  Consult the CCITT
 * documentation for the algorithm.
 */
static HPDF_STATUS 
HPDF_Fax3Encode2DRow(struct _HPDF_CCITT_Data *pData, unsigned char* bp, unsigned char* rp, uint32 bits)
{
#define	PIXEL(buf,ix)	((((buf)[(ix)>>3]) >> (7-((ix)&7))) & 1)
        uint32 a0 = 0;
	uint32 a1 = (PIXEL(bp, 0) != 0 ? 0 : finddiff(bp, 0, bits, 0));
	uint32 b1 = (PIXEL(rp, 0) != 0 ? 0 : finddiff(rp, 0, bits, 0));
	uint32 a2, b2;

	for (;;) {
		b2 = finddiff2(rp, b1, bits, PIXEL(rp,b1));
		if (b2 >= a1) {
			int32 d = b1 - a1;
			if (!(-3 <= d && d <= 3)) {	/* horizontal mode */
				a2 = finddiff2(bp, a1, bits, PIXEL(bp,a1));
				putcode(pData, &horizcode);
				if (a0+a1 == 0 || PIXEL(bp, a0) == 0) {
					putspan(pData, a1-a0, TIFFFaxWhiteCodes);
					putspan(pData, a2-a1, TIFFFaxBlackCodes);
				} else {
					putspan(pData, a1-a0, TIFFFaxBlackCodes);
					putspan(pData, a2-a1, TIFFFaxWhiteCodes);
				}
				a0 = a2;
			} else {			/* vertical mode */
				putcode(pData, &vcodes[d+3]);
				a0 = a1;
			}
		} else {				/* pass mode */
			putcode(pData, &passcode);
			a0 = b2;
		}
		if (a0 >= bits)
			break;
		a1 = finddiff(bp, a0, bits, PIXEL(bp,a0));
		b1 = finddiff(rp, a0, bits, !PIXEL(bp,a0));
		b1 = finddiff(rp, b1, bits, PIXEL(bp,a0));
	}
	return HPDF_OK;
#undef PIXEL
}

/*
 * Encode the requested amount of data.
 */
static HPDF_STATUS 
HPDF_Fax4Encode(struct _HPDF_CCITT_Data *pData, tidata_t bp, tsize_t cc/*, tsample_t s*/)
{
	HPDF_Fax3CodecState *sp = EncoderState(pData);

	/* (void) s; */
	while ((long)cc > 0) {
		if (HPDF_Fax3Encode2DRow(pData, bp, sp->refline, sp->b.rowpixels)!=HPDF_OK)
			return 1;
		memcpy(sp->refline, bp, sp->b.rowbytes);
		bp += sp->b.rowbytes;
		cc -= sp->b.rowbytes;
	}
	return HPDF_OK;
}

static void
HPDF_Fax4PostEncode(struct _HPDF_CCITT_Data *pData)
{
	/* HPDF_Fax3CodecState *sp = EncoderState(pData); */

	/* terminate strip w/ EOFB */
	HPDF_Fax3PutBits(pData, EOL, 12);
	HPDF_Fax3PutBits(pData, EOL, 12);
	/*if (sp->bit != 8)
		HPDF_Fax3FlushBits(pData, sp);	
		*/
	HPDF_CCITT_FlushData(pData);
}



HPDF_STATUS 
HPDF_Stream_CcittToStream( const HPDF_BYTE   *buf,
                            HPDF_Stream  dst,
							HPDF_Encrypt  e,
							HPDF_UINT          width,
							HPDF_UINT          height,
							HPDF_UINT          line_width,
							HPDF_BOOL		   top_is_first)
{
	const HPDF_BYTE   *pBufPos;
	const HPDF_BYTE   *pBufEnd; /* end marker */
	int lineIncrement;
	struct _HPDF_CCITT_Data data;

	HPDF_UNUSED (e);

	if(height==0) return 1;
	if(top_is_first) {
		pBufPos = buf;
		pBufEnd=buf+(line_width*height);
		lineIncrement = line_width;
	} else {
		pBufPos = buf+(line_width*(height-1));
		pBufEnd= buf-line_width;
		lineIncrement = -((int)line_width);
	}	

	memset(&data, 0, sizeof(struct _HPDF_CCITT_Data));
	data.dst = dst;
	data.tif_rawdata = (tidata_t) malloc( 16384 ); /*  16 kb buffer */
	data.tif_rawdatasize = 16384;
	data.tif_rawcc = 0;
	data.tif_rawcp = data.tif_rawdata;

	if(HPDF_InitCCITTFax3(&data)!=HPDF_OK)
		return 1;

	if(HPDF_Fax3SetupState(&data, width, height, line_width)!=HPDF_OK)
	{
		HPDF_FreeCCITTFax3(&data);
		return 1;
	}

	if(HPDF_Fax3PreEncode(&data)!=HPDF_OK)
	{
		HPDF_FreeCCITTFax3(&data);
		return 1;
	}

	/*  encode data */
	while(pBufEnd!=pBufPos)
	{
		HPDF_Fax4Encode(&data, (tidata_t)pBufPos, line_width);
		pBufPos+=lineIncrement;
	}

	HPDF_Fax4PostEncode(&data);

	HPDF_FreeCCITTFax3(&data);

	return HPDF_OK;
}

HPDF_Image
HPDF_Image_Load1BitImageFromMem  (HPDF_MMgr        mmgr,
                          const HPDF_BYTE   *buf,
                          HPDF_Xref        xref,
                          HPDF_UINT          width,
                          HPDF_UINT          height,
						  HPDF_UINT          line_width,
						  HPDF_BOOL			 top_is_first
                          )
{
    HPDF_Dict image;
    HPDF_STATUS ret = HPDF_OK;
    /* HPDF_UINT size; */

    HPDF_PTRACE ((" HPDF_Image_Load1BitImage\n"));

    image = HPDF_DictStream_New (mmgr, xref);
    if (!image)
        return NULL;

    image->header.obj_class |= HPDF_OSUBCLASS_XOBJECT;
    ret += HPDF_Dict_AddName (image, "Type", "XObject");
    ret += HPDF_Dict_AddName (image, "Subtype", "Image");
    if (ret != HPDF_OK)
        return NULL;

    /* size = width * height; */
    ret = HPDF_Dict_AddName (image, "ColorSpace", "DeviceGray");
    if (ret != HPDF_OK)
        return NULL;

    if (HPDF_Dict_AddNumber (image, "Width", width) != HPDF_OK)
        return NULL;

    if (HPDF_Dict_AddNumber (image, "Height", height) != HPDF_OK)
        return NULL;

    if (HPDF_Dict_AddNumber (image, "BitsPerComponent", 1) != HPDF_OK)
        return NULL;

    if (HPDF_Stream_CcittToStream (buf, image->stream, NULL, width, height, line_width, top_is_first) != HPDF_OK)
        return NULL;

    return image;
}

/*
 * Load image from buffer
 * line_width - width of the line in bytes
 * top_is_first - image orientation: 
 *      TRUE if image is oriented TOP-BOTTOM;
 *      FALSE if image is oriented BOTTOM-TOP
 */
HPDF_EXPORT(HPDF_Image)
HPDF_Image_LoadRaw1BitImageFromMem  (HPDF_Doc           pdf,
                           const HPDF_BYTE   *buf,
                          HPDF_UINT          width,
                          HPDF_UINT          height,
						  HPDF_UINT          line_width,
						  HPDF_BOOL          black_is1,
						  HPDF_BOOL			 top_is_first)
{
    HPDF_Image image;

    HPDF_PTRACE ((" HPDF_Image_Load1BitImageFromMem\n"));

    if (!HPDF_HasDoc (pdf))
        return NULL;

    image = HPDF_Image_Load1BitImageFromMem(pdf->mmgr, buf, pdf->xref, width,
                height, line_width, top_is_first);

    if (!image)
        HPDF_CheckError (&pdf->error);

    if (pdf->compression_mode & HPDF_COMP_IMAGE)
	{
		image->filter = HPDF_STREAM_FILTER_CCITT_DECODE;
		image->filterParams = HPDF_Dict_New(pdf->mmgr);
		if(image->filterParams==NULL) {
			return NULL;
		}
		
		/* pure 2D encoding, default is 0 */
		HPDF_Dict_AddNumber (image->filterParams, "K", -1);
		/* default is 1728 */
		HPDF_Dict_AddNumber (image->filterParams, "Columns", width);
		/* default is 0 */
		HPDF_Dict_AddNumber (image->filterParams, "Rows", height);
		HPDF_Dict_AddBoolean (image->filterParams, "BlackIs1", black_is1);
	}

    return image;
}
