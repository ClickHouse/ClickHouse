#define LIZARD_HC_MIN_OFFSET 8
#define LIZARD_HC_LONGOFF_MM 0 /* not used with offsets > 1<<16 */
#define OPTIMAL_ML (int)((ML_MASK_LZ4-1)+MINMATCH)
#define GET_MINMATCH(offset) (MINMATCH)

#if 1
    #define LIZARD_HC_HASH_FUNCTION(ip, hashLog) Lizard_hashPtr(ip, hashLog, ctx->params.searchLength)
#else
    #define LIZARD_HC_HASH_FUNCTION(ip, hashLog) Lizard_hash5Ptr(ip, hashLog)
#endif

/* Update chains up to ip (excluded) */
FORCE_INLINE void Lizard_Insert (Lizard_stream_t* ctx, const BYTE* ip)
{
    U32* const chainTable = ctx->chainTable;
    U32* const hashTable  = ctx->hashTable;
#if MINMATCH == 3
    U32* HashTable3  = ctx->hashTable3;
#endif 
    const BYTE* const base = ctx->base;
    U32 const target = (U32)(ip - base);
    U32 idx = ctx->nextToUpdate;
    const int hashLog = ctx->params.hashLog;
    const U32 contentMask = (1 << ctx->params.contentLog) - 1;
    const U32 maxDistance = (1 << ctx->params.windowLog) - 1;

    while (idx < target) {
        size_t const h = Lizard_hashPtr(base+idx, hashLog, ctx->params.searchLength);
        size_t delta = idx - hashTable[h];
        if (delta>maxDistance) delta = maxDistance;
        DELTANEXT(idx) = (U32)delta;
        if ((hashTable[h] >= idx) || (idx >= hashTable[h] + LIZARD_HC_MIN_OFFSET))
            hashTable[h] = idx;
#if MINMATCH == 3
        HashTable3[Lizard_hash3Ptr(base+idx, ctx->params.hashLog3)] = idx;
#endif 
        idx++;
    }

    ctx->nextToUpdate = target;
}



FORCE_INLINE int Lizard_InsertAndFindBestMatch (Lizard_stream_t* ctx,   /* Index table will be updated */
                                               const BYTE* ip, const BYTE* const iLimit,
                                               const BYTE** matchpos)
{
    U32* const chainTable = ctx->chainTable;
    U32* const HashTable = ctx->hashTable;
    const BYTE* const base = ctx->base;
    const BYTE* const dictBase = ctx->dictBase;
    const U32 dictLimit = ctx->dictLimit;
    const BYTE* const lowPrefixPtr = base + dictLimit;
    const BYTE* const dictEnd = dictBase + dictLimit;
    U32 matchIndex, delta;
    const BYTE* match;
    int nbAttempts=ctx->params.searchNum;
    size_t ml=0;
    const int hashLog = ctx->params.hashLog;
    const U32 contentMask = (1 << ctx->params.contentLog) - 1;
    const U32 maxDistance = (1 << ctx->params.windowLog) - 1;
    const U32 current = (U32)(ip - base);
    const U32 lowLimit = (ctx->lowLimit + maxDistance >= current) ? ctx->lowLimit : current - maxDistance;

    /* HC4 match finder */
    Lizard_Insert(ctx, ip);
    matchIndex = HashTable[LIZARD_HC_HASH_FUNCTION(ip, hashLog)];

    while ((matchIndex < current) && (matchIndex >= lowLimit) && (nbAttempts)) {
        nbAttempts--;
        if (matchIndex >= dictLimit) {
            match = base + matchIndex;
#if LIZARD_HC_MIN_OFFSET > 0
            if ((U32)(ip - match) >= LIZARD_HC_MIN_OFFSET)
#endif
            if (*(match+ml) == *(ip+ml)
                && (MEM_read32(match) == MEM_read32(ip)))
            {
                size_t const mlt = Lizard_count(ip+MINMATCH, match+MINMATCH, iLimit) + MINMATCH;
#if LIZARD_HC_LONGOFF_MM > 0
                if ((mlt >= LIZARD_HC_LONGOFF_MM) || ((U32)(ip - match) < LIZARD_MAX_16BIT_OFFSET))
#endif
                if (mlt > ml) { ml = mlt; *matchpos = match; }
            }
        } else {
            match = dictBase + matchIndex;
#if LIZARD_HC_MIN_OFFSET > 0
            if ((U32)(ip - (base + matchIndex)) >= LIZARD_HC_MIN_OFFSET)
#endif
            if ((U32)((dictLimit-1) - matchIndex) >= 3)  /* intentional overflow */
            if (MEM_read32(match) == MEM_read32(ip)) {
                size_t mlt = Lizard_count_2segments(ip+MINMATCH, match+MINMATCH, iLimit, dictEnd, lowPrefixPtr) + MINMATCH;
#if LIZARD_HC_LONGOFF_MM > 0
                if ((mlt >= LIZARD_HC_LONGOFF_MM) || ((U32)(ip - (base + matchIndex)) < LIZARD_MAX_16BIT_OFFSET))
#endif
                if (mlt > ml) { ml = mlt; *matchpos = base + matchIndex; }   /* virtual matchpos */
            }
        }
        delta = DELTANEXT(matchIndex);
        if (delta > matchIndex) break;
        matchIndex -= delta;
    }

    return (int)ml;
}


FORCE_INLINE int Lizard_InsertAndGetWiderMatch (
    Lizard_stream_t* ctx,
    const BYTE* const ip,
    const BYTE* const iLowLimit,
    const BYTE* const iHighLimit,
    int longest,
    const BYTE** matchpos,
    const BYTE** startpos)
{
    U32* const chainTable = ctx->chainTable;
    U32* const HashTable = ctx->hashTable;
    const BYTE* const base = ctx->base;
    const U32 dictLimit = ctx->dictLimit;
    const BYTE* const lowPrefixPtr = base + dictLimit;
    const BYTE* const dictBase = ctx->dictBase;
    const BYTE* const dictEnd = dictBase + dictLimit;
    U32   matchIndex, delta;
    int nbAttempts = ctx->params.searchNum;
    int LLdelta = (int)(ip-iLowLimit);
    const int hashLog = ctx->params.hashLog;
    const U32 contentMask = (1 << ctx->params.contentLog) - 1;
    const U32 maxDistance = (1 << ctx->params.windowLog) - 1;
    const U32 current = (U32)(ip - base);
    const U32 lowLimit = (ctx->lowLimit + maxDistance >= current) ? ctx->lowLimit : current - maxDistance;

    /* First Match */
    Lizard_Insert(ctx, ip);
    matchIndex = HashTable[LIZARD_HC_HASH_FUNCTION(ip, hashLog)];

    while ((matchIndex < current) && (matchIndex >= lowLimit) && (nbAttempts)) {
        nbAttempts--;
        if (matchIndex >= dictLimit) {
            const BYTE* match = base + matchIndex;
#if LIZARD_HC_MIN_OFFSET > 0
            if ((U32)(ip - match) >= LIZARD_HC_MIN_OFFSET)
#endif
            if (*(iLowLimit + longest) == *(match - LLdelta + longest)) {
                if (MEM_read32(match) == MEM_read32(ip)) {
                    int mlt = MINMATCH + Lizard_count(ip+MINMATCH, match+MINMATCH, iHighLimit);
                    int back = 0;
                    while ((ip+back > iLowLimit) && (match+back > lowPrefixPtr) && (ip[back-1] == match[back-1])) back--;
                    mlt -= back;

#if LIZARD_HC_LONGOFF_MM > 0
                    if ((mlt >= LIZARD_HC_LONGOFF_MM) || ((U32)(ip - match) < LIZARD_MAX_16BIT_OFFSET))
#endif
                    if (mlt > longest) {
                        longest = (int)mlt;
                        *matchpos = match+back;
                        *startpos = ip+back;
                    }
                }
            }
        } else {
            const BYTE* match = dictBase + matchIndex;
#if LIZARD_HC_MIN_OFFSET > 0
            if ((U32)(ip - (base + matchIndex)) >= LIZARD_HC_MIN_OFFSET)
#endif
            if ((U32)((dictLimit-1) - matchIndex) >= 3)  /* intentional overflow */
            if (MEM_read32(match) == MEM_read32(ip)) {
                int back=0;
                size_t mlt = Lizard_count_2segments(ip+MINMATCH, match+MINMATCH, iHighLimit, dictEnd, lowPrefixPtr) + MINMATCH;
                while ((ip+back > iLowLimit) && (matchIndex+back > lowLimit) && (ip[back-1] == match[back-1])) back--;
                mlt -= back;
#if LIZARD_HC_LONGOFF_MM > 0
                if ((mlt >= LIZARD_HC_LONGOFF_MM) || ((U32)(ip - (base + matchIndex)) < LIZARD_MAX_16BIT_OFFSET))
#endif
                if ((int)mlt > longest) { longest = (int)mlt; *matchpos = base + matchIndex + back; *startpos = ip+back; }
            }
        }
        delta = DELTANEXT(matchIndex);
        if (delta > matchIndex) break;
        matchIndex -= delta;
    }

    return longest;
}


FORCE_INLINE int Lizard_compress_hashChain (
        Lizard_stream_t* const ctx,
        const BYTE* ip,
        const BYTE* const iend)
{
    const BYTE* anchor = ip;
    const BYTE* const mflimit = iend - MFLIMIT;
    const BYTE* const matchlimit = (iend - LASTLITERALS);

    int   ml, ml2, ml3, ml0;
    const BYTE* ref = NULL;
    const BYTE* start2 = NULL;
    const BYTE* ref2 = NULL;
    const BYTE* start3 = NULL;
    const BYTE* ref3 = NULL;
    const BYTE* start0;
    const BYTE* ref0;

    /* init */
    ip++;

    /* Main Loop */
    while (ip < mflimit) {
        ml = Lizard_InsertAndFindBestMatch (ctx, ip, matchlimit, (&ref));
        if (!ml) { ip++; continue; }

        /* saved, in case we would skip too much */
        start0 = ip;
        ref0 = ref;
        ml0 = ml;

_Search2:
        if (ip+ml < mflimit)
            ml2 = Lizard_InsertAndGetWiderMatch(ctx, ip + ml - 2, ip + 1, matchlimit, ml, &ref2, &start2);
        else ml2 = ml;

        if (ml2 == ml) { /* No better match */
            if (Lizard_encodeSequence_LZ4(ctx, &ip, &anchor, ml, ref)) return 0;
            continue;
        }

        if (start0 < ip) {
            if (start2 < ip + ml0) {  /* empirical */
                ip = start0;
                ref = ref0;
                ml = ml0;
            }
        }

        /* Here, start0==ip */
        if ((start2 - ip) < 3) {  /* First Match too small : removed */
            ml = ml2;
            ip = start2;
            ref =ref2;
            goto _Search2;
        }

_Search3:
        /*
        * Currently we have :
        * ml2 > ml1, and
        * ip1+3 <= ip2 (usually < ip1+ml1)
        */
        if ((start2 - ip) < OPTIMAL_ML) {
            int correction;
            int new_ml = ml;
            if (new_ml > OPTIMAL_ML) new_ml = OPTIMAL_ML;
            if (ip+new_ml > start2 + ml2 - GET_MINMATCH((U32)(start2 - ref2))) {
                new_ml = (int)(start2 - ip) + ml2 - GET_MINMATCH((U32)(start2 - ref2));
                if (new_ml < GET_MINMATCH((U32)(ip - ref))) { // match2 doesn't fit
                    if (Lizard_encodeSequence_LZ4(ctx, &ip, &anchor, ml, ref)) return 0;
                    continue;
                }
            }
            correction = new_ml - (int)(start2 - ip);
            if (correction > 0) {
                start2 += correction;
                ref2 += correction;
                ml2 -= correction;
            }
        }
        /* Now, we have start2 = ip+new_ml, with new_ml = min(ml, OPTIMAL_ML=18) */

        if (start2 + ml2 < mflimit)
            ml3 = Lizard_InsertAndGetWiderMatch(ctx, start2 + ml2 - 3, start2, matchlimit, ml2, &ref3, &start3);
        else ml3 = ml2;

        if (ml3 == ml2) {  /* No better match : 2 sequences to encode */
            /* ip & ref are known; Now for ml */
            if (start2 < ip+ml)  ml = (int)(start2 - ip);
            /* Now, encode 2 sequences */
            if (Lizard_encodeSequence_LZ4(ctx, &ip, &anchor, ml, ref)) return 0;
            ip = start2;
            if (Lizard_encodeSequence_LZ4(ctx, &ip, &anchor, ml2, ref2)) return 0;
            continue;
        }

        if (start3 < ip+ml+3) {  /* Not enough space for match 2 : remove it */
            if (start3 >= (ip+ml)) {  /* can write Seq1 immediately ==> Seq2 is removed, so Seq3 becomes Seq1 */
                if (start2 < ip+ml) {
                    int correction = (int)(ip+ml - start2);
                    start2 += correction;
                    ref2 += correction;
                    ml2 -= correction;
                    if (ml2 < GET_MINMATCH((U32)(start2 - ref2))) {
                        start2 = start3;
                        ref2 = ref3;
                        ml2 = ml3;
                    }
                }

                if (Lizard_encodeSequence_LZ4(ctx, &ip, &anchor, ml, ref)) return 0;
                ip  = start3;
                ref = ref3;
                ml  = ml3;

                start0 = start2;
                ref0 = ref2;
                ml0 = ml2;
                goto _Search2;
            }

            start2 = start3;
            ref2 = ref3;
            ml2 = ml3;
            goto _Search3;
        }

        /*
        * OK, now we have 3 ascending matches; let's write at least the first one
        * ip & ref are known; Now for ml
        */
        if (start2 < ip+ml) {
            if ((start2 - ip) < (int)ML_MASK_LZ4) {
                int correction;
                if (ml > OPTIMAL_ML) ml = OPTIMAL_ML;
                if (ip + ml > start2 + ml2 - GET_MINMATCH((U32)(start2 - ref2))) {
                    ml = (int)(start2 - ip) + ml2 - GET_MINMATCH((U32)(start2 - ref2));
                    if (ml < GET_MINMATCH((U32)(ip - ref))) { // match2 doesn't fit, remove it
                        if (Lizard_encodeSequence_LZ4(ctx, &ip, &anchor, ml, ref)) return 0;
                        ip  = start3;
                        ref = ref3;
                        ml  = ml3;

                        start0 = start2;
                        ref0 = ref2;
                        ml0 = ml2;
                        goto _Search2;
                    }
                }
                correction = ml - (int)(start2 - ip);
                if (correction > 0) {
                    start2 += correction;
                    ref2 += correction;
                    ml2 -= correction;
                }
            } else {
                ml = (int)(start2 - ip);
            }
        }
        if (Lizard_encodeSequence_LZ4(ctx, &ip, &anchor, ml, ref)) return 0;

        ip = start2;
        ref = ref2;
        ml = ml2;

        start2 = start3;
        ref2 = ref3;
        ml2 = ml3;

        goto _Search3;
    }

    /* Encode Last Literals */
    ip = iend;
    if (Lizard_encodeLastLiterals_LZ4(ctx, &ip, &anchor)) goto _output_error;

    /* End */
    return 1;
_output_error:
    return 0;
}
