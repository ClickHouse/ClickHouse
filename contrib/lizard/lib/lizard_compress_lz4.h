#define LIZARD_LENGTH_SIZE_LZ4(len) ((len >= (1<<16)+RUN_MASK_LZ4) ? 5 : ((len >= 254+RUN_MASK_LZ4) ? 3 : ((len >= RUN_MASK_LZ4) ? 1 : 0)))

FORCE_INLINE int Lizard_encodeSequence_LZ4 (
    Lizard_stream_t* ctx,
    const BYTE** ip,
    const BYTE** anchor,
    size_t matchLength,
    const BYTE* const match)
{
    size_t length = (size_t)(*ip - *anchor);
    BYTE* token = (ctx->flagsPtr)++;
    (void) ctx;

    COMPLOG_CODEWORDS_LZ4("literal : %u  --  match : %u  --  offset : %u\n", (U32)(*ip - *anchor), (U32)matchLength, (U32)(*ip-match));
  
    /* Encode Literal length */
 //   if (ctx->literalsPtr > ctx->literalsEnd - length - LIZARD_LENGTH_SIZE_LZ4(length) - 2 - WILDCOPYLENGTH) { LIZARD_LOG_COMPRESS_LZ4("encodeSequence overflow1\n"); return 1; }   /* Check output limit */
    if (length >= RUN_MASK_LZ4) 
    {   size_t len = length - RUN_MASK_LZ4;
        *token = RUN_MASK_LZ4; 
        if (len >= (1<<16)) { *(ctx->literalsPtr) = 255;  MEM_writeLE24(ctx->literalsPtr+1, (U32)(len));  ctx->literalsPtr += 4; }
        else if (len >= 254) { *(ctx->literalsPtr) = 254;  MEM_writeLE16(ctx->literalsPtr+1, (U16)(len));  ctx->literalsPtr += 3; }
        else *(ctx->literalsPtr)++ = (BYTE)len;
    }
    else *token = (BYTE)length;

    /* Copy Literals */
    if (length > 0) {
        Lizard_wildCopy(ctx->literalsPtr, *anchor, (ctx->literalsPtr) + length);
#if 0 //def LIZARD_USE_HUFFMAN
        ctx->litSum += (U32)length;
        ctx->litPriceSum += (U32)(length * ctx->log2LitSum);
        {   U32 u;
            for (u=0; u < length; u++) {
                ctx->litPriceSum -= Lizard_highbit32(ctx->litFreq[ctx->literalsPtr[u]]+1);
                ctx->litFreq[ctx->literalsPtr[u]]++;
        }   }
#endif
        ctx->literalsPtr += length;
    }

    /* Encode Offset */
    MEM_writeLE16(ctx->literalsPtr, (U16)(*ip-match));
    ctx->literalsPtr+=2;

    /* Encode MatchLength */
    length = matchLength - MINMATCH;
  //  if (ctx->literalsPtr > ctx->literalsEnd - 5 /*LIZARD_LENGTH_SIZE_LZ4(length)*/) { LIZARD_LOG_COMPRESS_LZ4("encodeSequence overflow2\n"); return 1; }   /* Check output limit */
    if (length >= ML_MASK_LZ4) {
        *token += (BYTE)(ML_MASK_LZ4<<RUN_BITS_LZ4);
        length -= ML_MASK_LZ4;
        if (length >= (1<<16)) { *(ctx->literalsPtr) = 255;  MEM_writeLE24(ctx->literalsPtr+1, (U32)(length));  ctx->literalsPtr += 4; }
        else if (length >= 254) { *(ctx->literalsPtr) = 254;  MEM_writeLE16(ctx->literalsPtr+1, (U16)(length));  ctx->literalsPtr += 3; }
        else *(ctx->literalsPtr)++ = (BYTE)length;
    }
    else *token += (BYTE)(length<<RUN_BITS_LZ4);

#ifndef LIZARD_NO_HUFFMAN
    if (ctx->huffType) { 
        ctx->flagFreq[*token]++;
        ctx->flagSum++;
        Lizard_setLog2Prices(ctx);
    }
#endif

    /* Prepare next loop */
    *ip += matchLength;
    *anchor = *ip;

    return 0;
}


FORCE_INLINE int Lizard_encodeLastLiterals_LZ4 (
    Lizard_stream_t* ctx,
    const BYTE** ip,
    const BYTE** anchor)
{
    size_t length = (int)(*ip - *anchor);

    (void)ctx;

    memcpy(ctx->literalsPtr, *anchor, length);
    ctx->literalsPtr += length;
    return 0;
}


#define LIZARD_GET_TOKEN_PRICE_LZ4(token)  (ctx->log2FlagSum - Lizard_highbit32(ctx->flagFreq[token]+1))

FORCE_INLINE size_t Lizard_get_price_LZ4(Lizard_stream_t* const ctx, const BYTE *ip, const size_t litLength, U32 offset, size_t matchLength) 
{
    size_t price = 0;
    BYTE token = 0;
#if 0 //def LIZARD_USE_HUFFMAN
    const BYTE* literals = ip - litLength;
    U32 u;

    if (ctx->cachedLiterals == literals && litLength >= ctx->cachedLitLength) {
        size_t const additional = litLength - ctx->cachedLitLength;
        const BYTE* literals2 = ctx->cachedLiterals + ctx->cachedLitLength;
        price = ctx->cachedPrice + additional * ctx->log2LitSum;
        for (u=0; u < additional; u++)
            price -= Lizard_highbit32(ctx->litFreq[literals2[u]]+1);
        ctx->cachedPrice = (U32)price;
        ctx->cachedLitLength = (U32)litLength;
    } else {
        price = litLength * ctx->log2LitSum;
        for (u=0; u < litLength; u++)
            price -= Lizard_highbit32(ctx->litFreq[literals[u]]+1);

        if (litLength >= 12) {
            ctx->cachedLiterals = literals;
            ctx->cachedPrice = (U32)price;
            ctx->cachedLitLength = (U32)litLength;
        }
    }
#else
    price += 8*litLength;  /* Copy Literals */
    (void)ip;
    (void)ctx;
#endif

    /* Encode Literal length */
    if (litLength >= RUN_MASK_LZ4) {
        size_t len = litLength - RUN_MASK_LZ4;
        token = RUN_MASK_LZ4; 
        if (len >= (1<<16)) price += 32;
        else if (len >= 254) price += 24;
        else price += 8;
    }
    else token = (BYTE)litLength;


    /* Encode MatchLength */
    if (offset) {
        size_t length;
        price += 16; /* Encode Offset */

        if (offset < 8) return LIZARD_MAX_PRICE; // error
        if (matchLength < MINMATCH) return LIZARD_MAX_PRICE; // error
            
        length = matchLength - MINMATCH;
        if (length >= ML_MASK_LZ4) {
            token += (BYTE)(ML_MASK_LZ4<<RUN_BITS_LZ4);
            length -= ML_MASK_LZ4;
            if (length >= (1<<16)) price += 32;
            else if (length >= 254) price += 24;
            else price += 8;
        }
        else token += (BYTE)(length<<RUN_BITS_LZ4);
    }

    if (ctx->huffType) { 
        if (offset > 0 || matchLength > 0) price += 2;
        price += LIZARD_GET_TOKEN_PRICE_LZ4(token);
    } else {
        price += 8; // token
    }

    return price;
}
