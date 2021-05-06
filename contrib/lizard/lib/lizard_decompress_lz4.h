/*! Lizard_decompress_LZ4() :
 *  This generic decompression function cover all use cases.
 *  It shall be instantiated several times, using different sets of directives
 *  Note that it is important this generic function is really inlined,
 *  in order to remove useless branches during compilation optimization.
 */
FORCE_INLINE int Lizard_decompress_LZ4(
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

    intptr_t length = 0;
    (void)compressionLevel;

    /* Special cases */
    if (unlikely(outputSize==0)) return ((inputSize==1) && (*ctx->flagsPtr==0)) ? 0 : -1;  /* Empty output buffer */

    /* Main Loop : decode sequences */
    while (ctx->flagsPtr < ctx->flagsEnd) {
        unsigned token;
        const BYTE* match;
        size_t offset;

        /* get literal length */
        token = *ctx->flagsPtr++;
        if ((length=(token & RUN_MASK_LZ4)) == RUN_MASK_LZ4) {
            if (unlikely(ctx->literalsPtr > iend - 5)) { LIZARD_LOG_DECOMPRESS_LZ4("0"); goto _output_error; } 
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
            length += RUN_MASK_LZ4;
            ctx->literalsPtr++;
            if (unlikely((size_t)(op+length)<(size_t)(op))) { LIZARD_LOG_DECOMPRESS_LZ4("1"); goto _output_error; }  /* overflow detection */
            if (unlikely((size_t)(ctx->literalsPtr+length)<(size_t)(ctx->literalsPtr))) { LIZARD_LOG_DECOMPRESS_LZ4("2"); goto _output_error; }   /* overflow detection */
        }

        /* copy literals */
        cpy = op + length;
        if (unlikely(cpy > oend - WILDCOPYLENGTH || ctx->literalsPtr + length > iend - (2 + WILDCOPYLENGTH))) { LIZARD_LOG_DECOMPRESS_LZ4("offset outside buffers\n"); goto _output_error; }   /* Error : offset outside buffers */

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
        if ((partialDecoding) && (op >= oexit)) return (int) (op-dest);

        /* get offset */
        offset = MEM_readLE16(ctx->literalsPtr); 
        ctx->literalsPtr += 2;

        match = op - offset;
        if ((checkOffset) && (unlikely(match < lowLimit))) { LIZARD_LOG_DECOMPRESS_LZ4("lowPrefix[%p]-dictSize[%d]=lowLimit[%p] match[%p]=op[%p]-offset[%d]\n", lowPrefix, (int)dictSize, lowLimit, match, op, (int)offset); goto _output_error; }  /* Error : offset outside buffers */

        /* get matchlength */
        length = token >> RUN_BITS_LZ4;
        if (length == ML_MASK_LZ4) {
            if (unlikely(ctx->literalsPtr > iend - 5)) { LIZARD_LOG_DECOMPRESS_LZ4("4"); goto _output_error; } 
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
            length += ML_MASK_LZ4;
            ctx->literalsPtr++;
            if (unlikely((size_t)(op+length)<(size_t)(op))) { LIZARD_LOG_DECOMPRESS_LZ4("5"); goto _output_error; }  /* overflow detection */
        }
        length += MINMATCH;

        /* check external dictionary */
        if ((dict==usingExtDict) && (match < lowPrefix)) {
            if (unlikely(op + length > oend - WILDCOPYLENGTH)) { LIZARD_LOG_DECOMPRESS_LZ4("6"); goto _output_error; }  /* doesn't respect parsing restriction */

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
        if (unlikely(cpy > oend - WILDCOPYLENGTH)) { LIZARD_LOG_DECOMPRESS_LZ4("1match=%p lowLimit=%p\n", match, lowLimit); goto _output_error; }   /* Error : offset outside buffers */
        Lizard_copy8(op, match);
        Lizard_copy8(op+8, match+8);
        if (length > 16)
            Lizard_wildCopy16(op + 16, match + 16, cpy);
        op = cpy;
        if ((partialDecoding) && (op >= oexit)) return (int) (op-dest);
    }

    /* last literals */
    length = ctx->literalsEnd - ctx->literalsPtr;
    cpy = op + length;
    if ((length < 0) || (ctx->literalsPtr+length != iend) || (cpy > oend)) { LIZARD_LOG_DECOMPRESS_LZ4("9"); goto _output_error; }   /* Error : input must be consumed */
    memcpy(op, ctx->literalsPtr, length);
    ctx->literalsPtr += length;
    op += length;

    /* end of decoding */
    return (int) (op-dest);     /* Nb of output bytes decoded */

    /* Overflow error detected */
_output_error:
    LIZARD_LOG_DECOMPRESS_LZ4("_output_error=%d ctx->flagsPtr=%p blockBase=%p\n", (int) (-(ctx->flagsPtr-blockBase))-1, ctx->flagsPtr, blockBase);
    LIZARD_LOG_DECOMPRESS_LZ4("cpy=%p oend=%p ctx->literalsPtr+length[%d]=%p iend=%p\n", cpy, oend, (int)length, ctx->literalsPtr+length, iend);
    return (int) (-(ctx->flagsPtr-blockBase))-1;
}
