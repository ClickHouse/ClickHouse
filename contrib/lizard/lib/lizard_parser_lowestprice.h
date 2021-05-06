#define LIZARD_LOWESTPRICE_MIN_OFFSET 8


FORCE_INLINE size_t Lizard_more_profitable(Lizard_stream_t* const ctx, const BYTE *best_ip, size_t best_off, size_t best_common, const BYTE *ip, size_t off, size_t common, size_t literals, int last_off)
{
    size_t sum;

    if (literals > 0)
        sum = MAX(common + literals, best_common);
    else
        sum = MAX(common, best_common - literals);

    if ((int)off == last_off) off = 0; // rep code
    if ((int)best_off == last_off) best_off = 0;

    return Lizard_get_price_LIZv1(ctx, last_off, ip, ctx->off24pos, sum - common, (U32)off, common) <= Lizard_get_price_LIZv1(ctx, last_off, best_ip, ctx->off24pos, sum - best_common, (U32)best_off, best_common);
} 


FORCE_INLINE size_t Lizard_better_price(Lizard_stream_t* const ctx, const BYTE *best_ip, size_t best_off, size_t best_common, const BYTE *ip, size_t off, size_t common, int last_off)
{
    if ((int)off == last_off) off = 0; // rep code
    if ((int)best_off == last_off) best_off = 0;

    return Lizard_get_price_LIZv1(ctx, last_off, ip, ctx->off24pos, 0, (U32)off, common) < Lizard_get_price_LIZv1(ctx, last_off, best_ip, ctx->off24pos, common - best_common, (U32)best_off, best_common);
}


FORCE_INLINE int Lizard_FindMatchLowestPrice (Lizard_stream_t* ctx,   /* Index table will be updated */
                                               const BYTE* ip, const BYTE* const iLimit,
                                               const BYTE** matchpos)
{
    U32* const chainTable = ctx->chainTable;
    U32* const HashTable = ctx->hashTable;
    const BYTE* const base = ctx->base;
    const BYTE* const dictBase = ctx->dictBase;
    const intptr_t dictLimit = ctx->dictLimit;
    const BYTE* const dictEnd = dictBase + dictLimit;
    const intptr_t maxDistance = (1 << ctx->params.windowLog) - 1;
    const intptr_t current = (ip - base);
    const intptr_t lowLimit = ((intptr_t)ctx->lowLimit + maxDistance >= current) ? (intptr_t)ctx->lowLimit : current - maxDistance;
    const BYTE* const lowPrefixPtr = base + dictLimit;
    const U32 contentMask = (1 << ctx->params.contentLog) - 1;
    const size_t minMatchLongOff = ctx->params.minMatchLongOff;
    intptr_t matchIndex;
    const BYTE* match, *matchDict;
    int nbAttempts=ctx->params.searchNum;
    size_t ml=0, mlt;

    matchIndex = HashTable[Lizard_hashPtr(ip, ctx->params.hashLog, ctx->params.searchLength)];

    if (ctx->last_off >= LIZARD_LOWESTPRICE_MIN_OFFSET) {
        intptr_t matchIndexLO = (ip - ctx->last_off) - base;
        if (matchIndexLO >= lowLimit) {
            if (matchIndexLO >= dictLimit) {
                match = base + matchIndexLO;
                mlt = Lizard_count(ip, match, iLimit);// + MINMATCH;
          //      if ((mlt >= minMatchLongOff) || (ctx->last_off < LIZARD_MAX_16BIT_OFFSET))
                if (mlt > REPMINMATCH) {
                    *matchpos = match;
                    return (int)mlt;
                }
            } else {
                match = dictBase + matchIndexLO;
                if ((U32)((dictLimit-1) - matchIndexLO) >= 3) {  /* intentional overflow */
                    mlt = Lizard_count_2segments(ip, match, iLimit, dictEnd, lowPrefixPtr);
                 //   if ((mlt >= minMatchLongOff) || (ctx->last_off < LIZARD_MAX_16BIT_OFFSET)) 
                    if (mlt > REPMINMATCH) {
                        *matchpos = base + matchIndexLO;  /* virtual matchpos */
                        return (int)mlt;
                    }
               }
            }
        }
    }


#if MINMATCH == 3
    {
        U32 matchIndex3 = ctx->hashTable3[Lizard_hash3Ptr(ip, ctx->params.hashLog3)];
        if (matchIndex3 < current && matchIndex3 >= lowLimit)
        {
            size_t offset = (size_t)current - matchIndex3;
            if (offset < LIZARD_MAX_8BIT_OFFSET)
            {
                match = ip - offset;
                if (match > base && MEM_readMINMATCH(ip) == MEM_readMINMATCH(match))
                {
                    ml = 3;//Lizard_count(ip+MINMATCH, match+MINMATCH, iLimit) + MINMATCH;
                    *matchpos = match;
                }
            }
        }
    }
#endif
    while ((matchIndex < current) && (matchIndex >= lowLimit) && (nbAttempts)) {
        nbAttempts--;
        match = base + matchIndex;
        if ((U32)(ip - match) >= LIZARD_LOWESTPRICE_MIN_OFFSET) {
            if (matchIndex >= dictLimit) {
                if (*(match+ml) == *(ip+ml) && (MEM_read32(match) == MEM_read32(ip))) {
                    mlt = Lizard_count(ip+MINMATCH, match+MINMATCH, iLimit) + MINMATCH;
                    if ((mlt >= minMatchLongOff) || ((U32)(ip - match) < LIZARD_MAX_16BIT_OFFSET))
                    if (!ml || (mlt > ml && Lizard_better_price(ctx, ip, (ip - *matchpos), ml, ip, (ip - match), mlt, ctx->last_off)))
                    { ml = mlt; *matchpos = match; }
                }
            } else {
                matchDict = dictBase + matchIndex;
                if ((U32)((dictLimit-1) - matchIndex) >= 3)  /* intentional overflow */
                if (MEM_read32(matchDict) == MEM_read32(ip)) {
                    mlt = Lizard_count_2segments(ip+MINMATCH, matchDict+MINMATCH, iLimit, dictEnd, lowPrefixPtr) + MINMATCH;
                    if ((mlt >= minMatchLongOff) || ((U32)(ip - match) < LIZARD_MAX_16BIT_OFFSET))
                    if (!ml || (mlt > ml && Lizard_better_price(ctx, ip, (ip - *matchpos), ml, ip, (U32)(ip - match), mlt, ctx->last_off)))
                    { ml = mlt; *matchpos = match; }   /* virtual matchpos */
                }
            }
        }
        matchIndex -= chainTable[matchIndex & contentMask];
    }

    return (int)ml;
}


FORCE_INLINE size_t Lizard_GetWiderMatch (
    Lizard_stream_t* ctx,
    const BYTE* const ip,
    const BYTE* const iLowLimit,
    const BYTE* const iHighLimit,
    size_t longest,
    const BYTE** matchpos,
    const BYTE** startpos)
{
    U32* const chainTable = ctx->chainTable;
    U32* const HashTable = ctx->hashTable;
    const BYTE* const base = ctx->base;
    const BYTE* const dictBase = ctx->dictBase;
    const intptr_t dictLimit = ctx->dictLimit;
    const BYTE* const dictEnd = dictBase + dictLimit;
    const intptr_t maxDistance = (1 << ctx->params.windowLog) - 1;
    const intptr_t current = (ip - base);
    const intptr_t lowLimit = ((intptr_t)ctx->lowLimit + maxDistance >= current) ? (intptr_t)ctx->lowLimit : current - maxDistance;
    const BYTE* const lowPrefixPtr = base + dictLimit;
    const U32 contentMask = (1 << ctx->params.contentLog) - 1;
    const BYTE* match, *matchDict;
    const size_t minMatchLongOff = ctx->params.minMatchLongOff;
    intptr_t matchIndex;
    int nbAttempts = ctx->params.searchNum;
    size_t mlt;

    /* First Match */
    matchIndex = HashTable[Lizard_hashPtr(ip, ctx->params.hashLog, ctx->params.searchLength)];

    if (ctx->last_off >= LIZARD_LOWESTPRICE_MIN_OFFSET) {
        intptr_t matchIndexLO = (ip - ctx->last_off) - base;
        if (matchIndexLO >= lowLimit) {
            if (matchIndexLO >= dictLimit) {
                match = base + matchIndexLO;
                if (MEM_readMINMATCH(match) == MEM_readMINMATCH(ip)) {
                    int back = 0;
                    mlt = Lizard_count(ip+MINMATCH, match+MINMATCH, iHighLimit) + MINMATCH;
                    while ((ip+back > iLowLimit) && (match+back > lowPrefixPtr) && (ip[back-1] == match[back-1])) back--;
                    mlt -= back;

                    if (mlt > longest)
                    if ((mlt >= minMatchLongOff) || (ctx->last_off < LIZARD_MAX_16BIT_OFFSET)) {
                        *matchpos = match+back;
                        *startpos = ip+back;
                        longest = mlt;
                    }
                }
            } else {
                match = dictBase + matchIndexLO;
                if ((U32)((dictLimit-1) - matchIndexLO) >= 3)  /* intentional overflow */
                if (MEM_readMINMATCH(match) == MEM_readMINMATCH(ip)) {
                    int back=0;
                    mlt = Lizard_count_2segments(ip+MINMATCH, match+MINMATCH, iHighLimit, dictEnd, lowPrefixPtr) + MINMATCH;
                    while ((ip+back > iLowLimit) && (matchIndexLO+back > lowLimit) && (ip[back-1] == match[back-1])) back--;
                    mlt -= back;

                    if (mlt > longest)
                    if ((mlt >= minMatchLongOff) || (ctx->last_off < LIZARD_MAX_16BIT_OFFSET)) {
                        *matchpos = base + matchIndexLO + back;  /* virtual matchpos */
                        *startpos = ip+back;
                        longest = mlt;
                    }
                }
            }
        }
    }

#if MINMATCH == 3
    {
        U32 matchIndex3 = ctx->hashTable3[Lizard_hash3Ptr(ip, ctx->params.hashLog3)];
        if (matchIndex3 < current && matchIndex3 >= lowLimit) {
            size_t offset = (size_t)current - matchIndex3;
            if (offset < LIZARD_MAX_8BIT_OFFSET) {
                match = ip - offset;
                if (match > base && MEM_readMINMATCH(ip) == MEM_readMINMATCH(match)) {
                    mlt = Lizard_count(ip + MINMATCH, match + MINMATCH, iHighLimit) + MINMATCH;

                    int back = 0;
                    while ((ip + back > iLowLimit) && (match + back > lowPrefixPtr) && (ip[back - 1] == match[back - 1])) back--;
                    mlt -= back;

                    if (!longest || (mlt > longest && Lizard_better_price(ctx, *startpos, (*startpos - *matchpos), longest, ip, (ip - match), mlt, ctx->last_off))) {
                        *matchpos = match + back;
                        *startpos = ip + back;
                        longest = mlt;
                    }
                }
            }
        }
    }
#endif

    while ((matchIndex < current) && (matchIndex >= lowLimit) && (nbAttempts)) {
        nbAttempts--;
        match = base + matchIndex;
        if ((U32)(ip - match) >= LIZARD_LOWESTPRICE_MIN_OFFSET) {
            if (matchIndex >= dictLimit) {
                if (MEM_read32(match) == MEM_read32(ip)) {
                    int back = 0;
                    mlt = Lizard_count(ip+MINMATCH, match+MINMATCH, iHighLimit) + MINMATCH;
                    while ((ip+back > iLowLimit) && (match+back > lowPrefixPtr) && (ip[back-1] == match[back-1])) back--;
                    mlt -= back;

                    if ((mlt >= minMatchLongOff) || ((U32)(ip - match) < LIZARD_MAX_16BIT_OFFSET))
                    if (!longest || (mlt > longest && Lizard_better_price(ctx, *startpos, (*startpos - *matchpos), longest, ip, (ip - match), mlt, ctx->last_off)))
                    { longest = mlt; *startpos = ip+back; *matchpos = match+back; }
                }
            } else {
                matchDict = dictBase + matchIndex;
                if ((U32)((dictLimit-1) - matchIndex) >= 3)  /* intentional overflow */
                if (MEM_read32(matchDict) == MEM_read32(ip)) {
                    int back=0;
                    mlt = Lizard_count_2segments(ip+MINMATCH, matchDict+MINMATCH, iHighLimit, dictEnd, lowPrefixPtr) + MINMATCH;
                    while ((ip+back > iLowLimit) && (matchIndex+back > lowLimit) && (ip[back-1] == matchDict[back-1])) back--;
                    mlt -= back;

                    if ((mlt >= minMatchLongOff) || ((U32)(ip - match) < LIZARD_MAX_16BIT_OFFSET))
                    if (!longest || (mlt > longest && Lizard_better_price(ctx, *startpos, (*startpos - *matchpos), longest, ip, (U32)(ip - match), mlt, ctx->last_off)))
                    { longest = mlt; *startpos = ip+back;  *matchpos = match+back; }   /* virtual matchpos */
                }
            }
        }
        matchIndex -= chainTable[matchIndex & contentMask];
    }

    return longest;
}




FORCE_INLINE int Lizard_compress_lowestPrice(
        Lizard_stream_t* const ctx,
        const BYTE* ip,
        const BYTE* const iend)
{
    const BYTE* anchor = ip;
    const BYTE* const mflimit = iend - MFLIMIT;
    const BYTE* const matchlimit = (iend - LASTLITERALS);

    size_t   ml, ml2, ml0;
    const BYTE* ref=NULL;
    const BYTE* start2=NULL;
    const BYTE* ref2=NULL;
    const BYTE* start0;
    const BYTE* ref0;
    const BYTE* lowPrefixPtr = ctx->base + ctx->dictLimit;
    const size_t minMatchLongOff = ctx->params.minMatchLongOff;
    const size_t sufficient_len = ctx->params.sufficientLength;

    /* Main Loop */
    while (ip < mflimit)
    {
        Lizard_Insert(ctx, ip);
        ml = Lizard_FindMatchLowestPrice (ctx, ip, matchlimit, (&ref));
        if (!ml) { ip++; continue; }

        {
            int back = 0;
            while ((ip + back > anchor) && (ref + back > lowPrefixPtr) && (ip[back - 1] == ref[back - 1])) back--;
            ml -= back;
            ip += back;
            ref += back;
        }

        /* saved, in case we would skip too much */
        start0 = ip;
        ref0 = ref;
        ml0 = ml;
    //    goto _Encode;

_Search:
        if (ip+ml >= mflimit) { goto _Encode; }
        if (ml >= sufficient_len) { goto _Encode; }

        Lizard_Insert(ctx, ip);
        ml2 = (int)Lizard_GetWiderMatch(ctx, ip + ml - 2, anchor, matchlimit, 0, &ref2, &start2);
        if (!ml2) goto _Encode;

        {
        U64 price, best_price;
        int off0=0, off1=0;
        const BYTE *pos, *best_pos;

    //	find the lowest price for encoding ml bytes
        best_pos = ip;
        best_price = LIZARD_MAX_PRICE;
        off0 = (int)(ip - ref);
        off1 = (int)(start2 - ref2);

        for (pos = ip + ml; pos >= start2; pos--)
        {
            int common0 = (int)(pos - ip);
            if (common0 >= MINMATCH) {
                price = (int)Lizard_get_price_LIZv1(ctx, ctx->last_off, ip, ctx->off24pos, ip - anchor, (off0 == ctx->last_off) ? 0 : off0, common0);
                
                {
                    int common1 = (int)(start2 + ml2 - pos);
                    if (common1 >= MINMATCH)
                        price += Lizard_get_price_LIZv1(ctx, ctx->last_off, pos, ctx->off24pos, 0, (off1 == off0) ? 0 : (off1), common1);
                    else
                        price += Lizard_get_price_LIZv1(ctx, ctx->last_off, pos, ctx->off24pos, common1, 0, 0);
                }

                if (price < best_price) {
                    best_price = price;
                    best_pos = pos;
                }
            } else {
                price = Lizard_get_price_LIZv1(ctx, ctx->last_off, ip, ctx->off24pos, start2 - anchor, (off1 == ctx->last_off) ? 0 : off1, ml2);

                if (price < best_price)
                    best_pos = pos;
                break;
            }
        }
        ml = (int)(best_pos - ip);
        }


        if ((ml < MINMATCH) || ((ml < minMatchLongOff) && ((U32)(ip-ref) >= LIZARD_MAX_16BIT_OFFSET)))
        {
            ip = start2;
            ref = ref2;
            ml = ml2;
            goto _Search;
        }

_Encode:
        if (start0 < ip)
        {
            if (Lizard_more_profitable(ctx, ip, (ip - ref), ml, start0, (start0 - ref0), ml0, (ref0 - ref), ctx->last_off))
            {
                ip = start0;
                ref = ref0;
                ml = ml0;
            }
        }

        if (Lizard_encodeSequence_LIZv1(ctx, &ip, &anchor, ml, ((ip - ref == ctx->last_off) ? ip : ref))) return 0;
    }

    /* Encode Last Literals */
    ip = iend;
    if (Lizard_encodeLastLiterals_LIZv1(ctx, &ip, &anchor)) goto _output_error;

    /* End */
    return 1;
_output_error:
    return 0;
}

