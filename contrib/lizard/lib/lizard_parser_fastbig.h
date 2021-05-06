#define LIZARD_FASTBIG_LONGOFF_MM MM_LONGOFF

/**************************************
*  Hash Functions
**************************************/
static size_t Lizard_hashPositionHLog(const void* p, int hashLog) 
{
    if (MEM_64bits())
        return Lizard_hash5Ptr(p, hashLog);
    return Lizard_hash4Ptr(p, hashLog);
}

static void Lizard_putPositionOnHashHLog(const BYTE* p, size_t h, U32* hashTable, const BYTE* srcBase)
{
    hashTable[h] = (U32)(p-srcBase);
}

static void Lizard_putPositionHLog(const BYTE* p, U32* hashTable, const BYTE* srcBase, int hashLog)
{
    size_t const h = Lizard_hashPositionHLog(p, hashLog);
    Lizard_putPositionOnHashHLog(p, h, hashTable, srcBase);
}

static U32 Lizard_getPositionOnHashHLog(size_t h, U32* hashTable)
{
    return hashTable[h];
}

static U32 Lizard_getPositionHLog(const BYTE* p, U32* hashTable, int hashLog)
{
    size_t const h = Lizard_hashPositionHLog(p, hashLog);
    return Lizard_getPositionOnHashHLog(h, hashTable);
}

FORCE_INLINE int Lizard_compress_fastBig(
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
    const int hashLog = ctx->params.hashLog;
    const U32 maxDistance = (1 << ctx->params.windowLog) - 1;
    const U32 lowLimit = (ctx->lowLimit + maxDistance >= (U32)(ip - base)) ? ctx->lowLimit : (U32)(ip - base) - maxDistance;

    /* Init conditions */
    if ((U32)(iend-ip) > (U32)LIZARD_MAX_INPUT_SIZE) goto _output_error;   /* Unsupported inputSize, too large (or negative) */

    if ((U32)(iend-ip) < Lizard_minLength) goto _last_literals;                  /* Input too small, no compression (all literals) */

    /* First Byte */
    Lizard_putPositionHLog(ip, ctx->hashTable, base, hashLog);
    ip++; forwardH = Lizard_hashPositionHLog(ip, hashLog);

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

                matchIndex = Lizard_getPositionOnHashHLog(h, ctx->hashTable);
                forwardH = Lizard_hashPositionHLog(forwardIp, hashLog);
                Lizard_putPositionOnHashHLog(ip, h, ctx->hashTable, base);

                if ((matchIndex < lowLimit) || (matchIndex >= (U32)(ip - base)) || (base + matchIndex + maxDistance < ip)) continue;

                if (matchIndex >= dictLimit) {
                    match = base + matchIndex;
                    if ((U32)(ip - match) >= LIZARD_FAST_MIN_OFFSET)
                    if (MEM_read32(match) == MEM_read32(ip))
                    {
                        int back = 0;
                        matchLength = Lizard_count(ip+MINMATCH, match+MINMATCH, matchlimit);

                        while ((ip+back > anchor) && (match+back > lowPrefixPtr) && (ip[back-1] == match[back-1])) back--;
                        matchLength -= back;
                        if ((matchLength >= LIZARD_FASTBIG_LONGOFF_MM) || ((U32)(ip - match) < LIZARD_MAX_16BIT_OFFSET))
                        {
                            ip += back;
                            match += back;
                            break;
                        }
                    }
                } else {
                    match = dictBase + matchIndex;
                    if ((U32)(ip - (base + matchIndex)) >= LIZARD_FAST_MIN_OFFSET)
                    if ((U32)((dictLimit-1) - matchIndex) >= 3)  /* intentional overflow */
                    if (MEM_read32(match) == MEM_read32(ip)) {
                        const U32 newLowLimit = (lowLimit + maxDistance >= (U32)(ip-base)) ? lowLimit : (U32)(ip - base) - maxDistance;
                        int back = 0;
                        matchLength = Lizard_count_2segments(ip+MINMATCH, match+MINMATCH, matchlimit, dictEnd, lowPrefixPtr);

                        while ((ip+back > anchor) && (matchIndex+back > newLowLimit) && (ip[back-1] == match[back-1])) back--;
                        matchLength -= back;
                        match = base + matchIndex + back;
                        if ((matchLength >= LIZARD_FASTBIG_LONGOFF_MM) || ((U32)(ip - match) < LIZARD_MAX_16BIT_OFFSET))
                        {
                            ip += back;
                            break;
                        }
                    }
                }
            } // while (1)
        }

_next_match:
        if (Lizard_encodeSequence_LIZv1(ctx, &ip, &anchor, matchLength+MINMATCH, match)) goto _output_error;
        
        /* Test end of chunk */
        if (ip > mflimit) break;

        /* Fill table */
        Lizard_putPositionHLog(ip-2, ctx->hashTable, base, hashLog);

        /* Test next position */
        matchIndex = Lizard_getPositionHLog(ip, ctx->hashTable, hashLog);
        Lizard_putPositionHLog(ip, ctx->hashTable, base, hashLog);
        if ((matchIndex >= lowLimit) && (matchIndex < (U32)(ip - base)) && (base + matchIndex + maxDistance >= ip))
        {
            if (matchIndex >= dictLimit) {
                match = base + matchIndex;
                if ((U32)(ip - match) >= LIZARD_FAST_MIN_OFFSET)
                if (MEM_read32(match) == MEM_read32(ip))
                {
                    matchLength = Lizard_count(ip+MINMATCH, match+MINMATCH, matchlimit);
                    if ((matchLength >= LIZARD_FASTBIG_LONGOFF_MM) || ((U32)(ip - match) < LIZARD_MAX_16BIT_OFFSET))
                        goto _next_match;
                }
            } else {
                match = dictBase + matchIndex;
                if ((U32)(ip - (base + matchIndex)) >= LIZARD_FAST_MIN_OFFSET)
                if ((U32)((dictLimit-1) - matchIndex) >= 3)  /* intentional overflow */
                if (MEM_read32(match) == MEM_read32(ip)) {
                    matchLength = Lizard_count_2segments(ip+MINMATCH, match+MINMATCH, matchlimit, dictEnd, lowPrefixPtr);
                    match = base + matchIndex;
                    if ((matchLength >= LIZARD_FASTBIG_LONGOFF_MM) || ((U32)(ip - match) < LIZARD_MAX_16BIT_OFFSET))
                        goto _next_match;
                }
            }
        }

        /* Prepare next loop */
        forwardH = Lizard_hashPositionHLog(++ip, hashLog);
    }

_last_literals:
    /* Encode Last Literals */
    ip = iend;
    if (Lizard_encodeLastLiterals_LIZv1(ctx, &ip, &anchor)) goto _output_error;

    /* End */
    return 1;
_output_error:
    return 0;
}
