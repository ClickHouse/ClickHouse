/*
  [0_MMMM_LLL] - 16-bit offset, 4-bit match length (4-15+), 3-bit literal length (0-7+)
  [1_MMMM_LLL] -   last offset, 4-bit match length (0-15+), 3-bit literal length (0-7+)
  flag 31      - 24-bit offset,        match length (47+),    no literal length
  flag 0-30    - 24-bit offset,  31 match lengths (16-46),    no literal length
*/

/*! Lizard_decompress_LIZv1() :
 *  This generic decompression function cover all use cases.
 *  It shall be instantiated several times, using different sets of directives
 *  Note that it is important this generic function is really inlined,
 *  in order to remove useless branches during compilation optimization.
 */
FORCE_INLINE int Lizard_decompress_LIZv1(
                 Lizard_dstream_t* ctx,
                 BYTE* const dest,
                 int outputSize,         /* this value is the max size of Output Buffer. */

                 int partialDecoding,    /* full, partial */
                 int targetOutputSize,   /* only used if partialDecoding==partial */
                 int dict,               /* noDict, withPrefix64k, usingExtDict */
                 const BYTE* const lowPrefix,  /* == dest if dict == noDict */
                 const BYTE* const dictStart,  /* only if dict==usingExtDict */
                 const size_t dictSize,         /* note : = 0 if noDict */
                 int compressionLevel
                 )
{
    /* Local Variables */
    int inputSize = (int)(ctx->flagsEnd - ctx->flagsPtr);
    const BYTE* const blockBase = ctx->flagsPtr;
    const BYTE* const iend = ctx->literalsEnd;

    BYTE* op = dest;
    BYTE* const oend = op + outputSize;
    BYTE* cpy = NULL;
    BYTE* oexit = op + targetOutputSize;
    const BYTE* const lowLimit = lowPrefix - dictSize;
    const BYTE* const dictEnd = (const BYTE*)dictStart + dictSize;

    const int checkOffset = (dictSize < (int)(LIZARD_DICT_SIZE));

    intptr_t last_off = ctx->last_off;
    intptr_t length = 0;
    (void)compressionLevel;

    /* Special cases */
    if (unlikely(outputSize==0)) return ((inputSize==1) && (*ctx->flagsPtr==0)) ? 0 : -1;  /* Empty output buffer */

    /* Main Loop : decode sequences */
    while (ctx->flagsPtr < ctx->flagsEnd) {
        unsigned token;
        const BYTE* match;
    //    intptr_t litLength;

        if ((partialDecoding) && (op >= oexit)) return (int) (op-dest); 

        /* get literal length */
        token = *ctx->flagsPtr++;

        if (token >= 32)
        {
            if ((length=(token & MAX_SHORT_LITLEN)) == MAX_SHORT_LITLEN) {
                if (unlikely(ctx->literalsPtr > iend - 1)) { LIZARD_LOG_DECOMPRESS_LIZv1("1"); goto _output_error; } 
                length = *ctx->literalsPtr;
                if unlikely(length >= 254) {
                    if (length == 254) {
                        length = MEM_readLE16(ctx->literalsPtr+1);
                        ctx->literalsPtr += 2;
                    } else {
                        length = MEM_readLE24(ctx->literalsPtr+1);
                        ctx->literalsPtr += 3;
                    }
                }
                length += MAX_SHORT_LITLEN;
                ctx->literalsPtr++;
                if (unlikely((size_t)(op+length)<(size_t)(op))) { LIZARD_LOG_DECOMPRESS_LIZv1("2"); goto _output_error; }  /* overflow detection */
                if (unlikely((size_t)(ctx->literalsPtr+length)<(size_t)(ctx->literalsPtr))) { LIZARD_LOG_DECOMPRESS_LIZv1("3"); goto _output_error; }   /* overflow detection */
            }

            /* copy literals */
            cpy = op + length;
            if (unlikely(cpy > oend - WILDCOPYLENGTH || ctx->literalsPtr > iend - WILDCOPYLENGTH)) { LIZARD_LOG_DECOMPRESS_LIZv1("offset outside buffers\n"); goto _output_error; }   /* Error : offset outside buffers */
    #if 1
            Lizard_wildCopy16(op, ctx->literalsPtr, cpy);
            op = cpy;
            ctx->literalsPtr += length; 
    #else
            Lizard_copy8(op, ctx->literalsPtr);
            Lizard_copy8(op+8, ctx->literalsPtr+8);
            if (length > 16)
                Lizard_wildCopy16(op + 16, ctx->literalsPtr + 16, cpy);
            op = cpy;
            ctx->literalsPtr += length; 
    #endif

            /* get offset */
            if (unlikely(ctx->offset16Ptr > ctx->offset16End)) { LIZARD_LOG_DECOMPRESS_LIZv1("(ctx->offset16Ptr > ctx->offset16End\n"); goto _output_error; } 
#if 1
            { /* branchless */
                intptr_t new_off = MEM_readLE16(ctx->offset16Ptr);
                uintptr_t not_repCode = (uintptr_t)(token >> ML_RUN_BITS) - 1;
                last_off ^= not_repCode & (last_off ^ -new_off);
                ctx->offset16Ptr = (BYTE*)((uintptr_t)ctx->offset16Ptr + (not_repCode & 2));
            }
#else
            if ((token >> ML_RUN_BITS) == 0)
            {
                last_off = -(intptr_t)MEM_readLE16(ctx->offset16Ptr); 
                ctx->offset16Ptr += 2;
            }
#endif

            /* get matchlength */
            length = (token >> RUN_BITS_LIZv1) & MAX_SHORT_MATCHLEN;
            if (length == MAX_SHORT_MATCHLEN) {
                if (unlikely(ctx->literalsPtr > iend - 1)) { LIZARD_LOG_DECOMPRESS_LIZv1("6"); goto _output_error; } 
                length = *ctx->literalsPtr;
                if unlikely(length >= 254) {
                    if (length == 254) {
                        length = MEM_readLE16(ctx->literalsPtr+1);
                        ctx->literalsPtr += 2;
                    } else {
                        length = MEM_readLE24(ctx->literalsPtr+1);
                        ctx->literalsPtr += 3;
                    }
                }
                length += MAX_SHORT_MATCHLEN;
                ctx->literalsPtr++;
                if (unlikely((size_t)(op+length)<(size_t)(op))) { LIZARD_LOG_DECOMPRESS_LIZv1("7"); goto _output_error; }  /* overflow detection */
            }

            DECOMPLOG_CODEWORDS_LIZv1("T32+ literal=%u match=%u offset=%d ipos=%d opos=%d\n", (U32)litLength, (U32)length, (int)-last_off, (U32)(ctx->flagsPtr-blockBase), (U32)(op-dest));
        }
        else
        if (token < LIZARD_LAST_LONG_OFF)
        {
            if (unlikely(ctx->offset24Ptr > ctx->offset24End - 3)) { LIZARD_LOG_DECOMPRESS_LIZv1("8"); goto _output_error; } 
            length = token + MM_LONGOFF;
            last_off = -(intptr_t)MEM_readLE24(ctx->offset24Ptr); 
            ctx->offset24Ptr += 3;
            DECOMPLOG_CODEWORDS_LIZv1("T0-30 literal=%u match=%u offset=%d\n", 0, (U32)length, (int)-last_off);
        }
        else 
        { 
            if (unlikely(ctx->literalsPtr > iend - 1)) { LIZARD_LOG_DECOMPRESS_LIZv1("9"); goto _output_error; } 
            length = *ctx->literalsPtr;
            if unlikely(length >= 254) {
                if (length == 254) {
                    length = MEM_readLE16(ctx->literalsPtr+1);
                    ctx->literalsPtr += 2;
                } else {
                    length = MEM_readLE24(ctx->literalsPtr+1);
                    ctx->literalsPtr += 3;
                }
            }
            ctx->literalsPtr++;
            length += LIZARD_LAST_LONG_OFF + MM_LONGOFF;

            if (unlikely(ctx->offset24Ptr > ctx->offset24End - 3)) { LIZARD_LOG_DECOMPRESS_LIZv1("10"); goto _output_error; } 
            last_off = -(intptr_t)MEM_readLE24(ctx->offset24Ptr); 
            ctx->offset24Ptr += 3;
        }


        match = op + last_off;
        if ((checkOffset) && ((unlikely((uintptr_t)(-last_off) > (uintptr_t)op) || (match < lowLimit)))) { LIZARD_LOG_DECOMPRESS_LIZv1("lowPrefix[%p]-dictSize[%d]=lowLimit[%p] match[%p]=op[%p]-last_off[%d]\n", lowPrefix, (int)dictSize, lowLimit, match, op, (int)last_off); goto _output_error; }  /* Error : offset outside buffers */

        /* check external dictionary */
        if ((dict==usingExtDict) && (match < lowPrefix)) {
            if (unlikely(op + length > oend - WILDCOPYLENGTH)) { LIZARD_LOG_DECOMPRESS_LIZv1("12"); goto _output_error; }  /* doesn't respect parsing restriction */

            if (length <= (intptr_t)(lowPrefix - match)) {
                /* match can be copied as a single segment from external dictionary */
                memmove(op, dictEnd - (lowPrefix-match), length);
                op += length;
            } else {
                /* match encompass external dictionary and current block */
                size_t const copySize = (size_t)(lowPrefix-match);
                size_t const restSize = length - copySize;
                memcpy(op, dictEnd - copySize, copySize);
                op += copySize;
                if (restSize > (size_t)(op-lowPrefix)) {  /* overlap copy */
                    BYTE* const endOfMatch = op + restSize;
                    const BYTE* copyFrom = lowPrefix;
                    while (op < endOfMatch) *op++ = *copyFrom++;
                } else {
                    memcpy(op, lowPrefix, restSize);
                    op += restSize;
            }   }
            continue;
        }

        /* copy match within block */
        cpy = op + length;
        if (unlikely(cpy > oend - WILDCOPYLENGTH)) { LIZARD_LOG_DECOMPRESS_LIZv1("13match=%p lowLimit=%p\n", match, lowLimit); goto _output_error; }   /* Error : offset outside buffers */
        Lizard_copy8(op, match);
        Lizard_copy8(op+8, match+8);
        if (length > 16)
            Lizard_wildCopy16(op + 16, match + 16, cpy);
        op = cpy;
    }

    /* last literals */
    length = ctx->literalsEnd - ctx->literalsPtr;
    cpy = op + length;
    if ((length < 0) || (ctx->literalsPtr+length != iend) || (cpy > oend)) { LIZARD_LOG_DECOMPRESS_LIZv1("14"); goto _output_error; }   /* Error : input must be consumed */
    memcpy(op, ctx->literalsPtr, length);
    ctx->literalsPtr += length;
    op += length;

    /* end of decoding */
    ctx->last_off = last_off;
    return (int) (op-dest);     /* Nb of output bytes decoded */

    /* Overflow error detected */
_output_error:
    LIZARD_LOG_DECOMPRESS_LIZv1("_output_error=%d ctx->flagsPtr=%p blockBase=%p\n", (int) (-(ctx->flagsPtr-blockBase))-1, ctx->flagsPtr, blockBase);
    LIZARD_LOG_DECOMPRESS_LIZv1("cpy=%p oend=%p ctx->literalsPtr+length[%d]=%p iend=%p\n", cpy, oend, (int)length, ctx->literalsPtr+length, iend);
    return (int) (-(ctx->flagsPtr-blockBase))-1;
}
