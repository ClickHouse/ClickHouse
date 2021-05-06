/**************************************
*  Hash Functions
**************************************/
static size_t Lizard_hashPositionSmall(const void* p) 
{
    if (MEM_64bits())
        return Lizard_hash5Ptr(p, LIZARD_HASHLOG_LZ4SM);
    return Lizard_hash4Ptr(p, LIZARD_HASHLOG_LZ4SM);
}

static void Lizard_putPositionOnHashSmall(const BYTE* p, size_t h, U32* hashTable, const BYTE* srcBase)
{
    hashTable[h] = (U32)(p-srcBase);
}

static void Lizard_putPositionSmall(const BYTE* p, U32* hashTable, const BYTE* srcBase)
{
    size_t const h = Lizard_hashPositionSmall(p);
    Lizard_putPositionOnHashSmall(p, h, hashTable, srcBase);
}

static U32 Lizard_getPositionOnHashSmall(size_t h, U32* hashTable)
{
    return hashTable[h];
}

static U32 Lizard_getPositionSmall(const BYTE* p, U32* hashTable)
{
    size_t const h = Lizard_hashPositionSmall(p);
    return Lizard_getPositionOnHashSmall(h, hashTable);
}


FORCE_INLINE int Lizard_compress_fastSmall(
        Lizard_stream_t* const ctx,
        const BYTE* ip,
        const BYTE* const iend)
{
    const U32 acceleration = 1;
    const BYTE* base = ctx->base;
    const U32 dictLimit = ctx->dictLimit;
    const BYTE* const lowPrefixPtr = base + dictLimit;
    const BYTE* const dictBase = ctx->dictBase;
    const BYTE* const dictEnd = dictBase + dictLimit;
    const BYTE* const mflimit = iend - MFLIMIT;
    const BYTE* const matchlimit = iend - LASTLITERALS;
    const BYTE* anchor = ip;

    size_t forwardH, matchIndex;
    const U32 maxDistance = (1 << ctx->params.windowLog) - 1;
    const U32 lowLimit = (ctx->lowLimit + maxDistance >= (U32)(ip - base)) ? ctx->lowLimit : (U32)(ip - base) - maxDistance;

    /* Init conditions */
    if ((U32)(iend-ip) > (U32)LIZARD_MAX_INPUT_SIZE) goto _output_error;   /* Unsupported inputSize, too large (or negative) */

    if ((U32)(iend-ip) < Lizard_minLength) goto _last_literals;                  /* Input too small, no compression (all literals) */

    /* First Byte */
    Lizard_putPositionSmall(ip, ctx->hashTable, base);
    ip++; forwardH = Lizard_hashPositionSmall(ip);

    /* Main Loop */
    for ( ; ; ) {
        const BYTE* match;
        size_t matchLength;

        /* Find a match */
        {   const BYTE* forwardIp = ip;
            unsigned step = 1;
            unsigned searchMatchNb = acceleration << Lizard_skipTrigger;
            while (1) {
                size_t const h = forwardH;
                ip = forwardIp;
                forwardIp += step;
                step = (searchMatchNb++ >> Lizard_skipTrigger);

                if (unlikely(forwardIp > mflimit)) goto _last_literals;

                matchIndex = Lizard_getPositionOnHashSmall(h, ctx->hashTable);
                forwardH = Lizard_hashPositionSmall(forwardIp);
                Lizard_putPositionOnHashSmall(ip, h, ctx->hashTable, base);

                if ((matchIndex < lowLimit) || (matchIndex >= (U32)(ip - base)) || (base + matchIndex + maxDistance < ip)) continue;

                if (matchIndex >= dictLimit) {
                    match = base + matchIndex;
#if LIZARD_FAST_MIN_OFFSET > 0
                    if ((U32)(ip - match) >= LIZARD_FAST_MIN_OFFSET)
#endif
                    if (MEM_read32(match) == MEM_read32(ip))
                    {
                        int back = 0;
                        matchLength = Lizard_count(ip+MINMATCH, match+MINMATCH, matchlimit);

                        while ((ip+back > anchor) && (match+back > lowPrefixPtr) && (ip[back-1] == match[back-1])) back--;
                        matchLength -= back;
#if LIZARD_FAST_LONGOFF_MM > 0
                        if ((matchLength >= LIZARD_FAST_LONGOFF_MM) || ((U32)(ip - match) < LIZARD_MAX_16BIT_OFFSET))
#endif
                        {
                            ip += back;
                            match += back;
                            break;
                        }
                    }
                } else {
                    match = dictBase + matchIndex;
#if LIZARD_FAST_MIN_OFFSET > 0
                    if ((U32)(ip - (base + matchIndex)) >= LIZARD_FAST_MIN_OFFSET)
#endif
                    if ((U32)((dictLimit-1) - matchIndex) >= 3)  /* intentional overflow */
                    if (MEM_read32(match) == MEM_read32(ip)) {
                        const U32 newLowLimit = (lowLimit + maxDistance >= (U32)(ip-base)) ? lowLimit : (U32)(ip - base) - maxDistance;
                        int back = 0;
                        matchLength = Lizard_count_2segments(ip+MINMATCH, match+MINMATCH, matchlimit, dictEnd, lowPrefixPtr);

                        while ((ip+back > anchor) && (matchIndex+back > newLowLimit) && (ip[back-1] == match[back-1])) back--;
                        matchLength -= back;
                        match = base + matchIndex + back;
#if LIZARD_FAST_LONGOFF_MM > 0
                        if ((matchLength >= LIZARD_FAST_LONGOFF_MM) || ((U32)(ip - match) < LIZARD_MAX_16BIT_OFFSET))
#endif
                        {
                            ip += back;
                            break;
                        }
                    }
                }
            } // while (1)
        }

_next_match:
        if (Lizard_encodeSequence_LZ4(ctx, &ip, &anchor, matchLength+MINMATCH, match)) goto _output_error;
        
        /* Test end of chunk */
        if (ip > mflimit) break;

        /* Fill table */
        Lizard_putPositionSmall(ip-2, ctx->hashTable, base);

        /* Test next position */
        matchIndex = Lizard_getPositionSmall(ip, ctx->hashTable);
        Lizard_putPositionSmall(ip, ctx->hashTable, base);
        if ((matchIndex >= lowLimit) && (matchIndex < (U32)(ip - base)) && (base + matchIndex + maxDistance >= ip))
        {
            if (matchIndex >= dictLimit) {
                match = base + matchIndex;
#if LIZARD_FAST_MIN_OFFSET > 0
                if ((U32)(ip - match) >= LIZARD_FAST_MIN_OFFSET)
#endif
                if (MEM_read32(match) == MEM_read32(ip))
                {
                    matchLength = Lizard_count(ip+MINMATCH, match+MINMATCH, matchlimit);
#if LIZARD_FAST_LONGOFF_MM > 0
                    if ((matchLength >= LIZARD_FAST_LONGOFF_MM) || ((U32)(ip - match) < LIZARD_MAX_16BIT_OFFSET))
#endif
                        goto _next_match;
                }
            } else {
                match = dictBase + matchIndex;
#if LIZARD_FAST_MIN_OFFSET > 0
                if ((U32)(ip - (base + matchIndex)) >= LIZARD_FAST_MIN_OFFSET)
#endif
                if ((U32)((dictLimit-1) - matchIndex) >= 3)  /* intentional overflow */
                if (MEM_read32(match) == MEM_read32(ip)) {
                    matchLength = Lizard_count_2segments(ip+MINMATCH, match+MINMATCH, matchlimit, dictEnd, lowPrefixPtr);
                    match = base + matchIndex;
#if LIZARD_FAST_LONGOFF_MM > 0
                    if ((matchLength >= LIZARD_FAST_LONGOFF_MM) || ((U32)(ip - match) < LIZARD_MAX_16BIT_OFFSET))
#endif
                        goto _next_match;
                }
            }
        }

        /* Prepare next loop */
        forwardH = Lizard_hashPositionSmall(++ip);
    }

_last_literals:
    /* Encode Last Literals */
    ip = iend;
    if (Lizard_encodeLastLiterals_LZ4(ctx, &ip, &anchor)) goto _output_error;

    /* End */
    return 1;
_output_error:
    return 0;
}
