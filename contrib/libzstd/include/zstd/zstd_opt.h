/*
    ZSTD Optimal mode
    Copyright (C) 2016, Przemyslaw Skibinski, Yann Collet.

    BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are
    met:

    * Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
    copyright notice, this list of conditions and the following disclaimer
    in the documentation and/or other materials provided with the
    distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
    A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
    OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
    DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
    THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

    You can contact the author at :
       - Zstd source repository : https://www.zstd.net
*/

/* Note : this file is intended to be included within zstd_compress.c */


#define ZSTD_FREQ_DIV   5

/*-*************************************
*  Price functions for optimal parser
***************************************/
FORCE_INLINE void ZSTD_setLog2Prices(seqStore_t* ssPtr)
{
    ssPtr->log2matchLengthSum = ZSTD_highbit(ssPtr->matchLengthSum+1);
    ssPtr->log2litLengthSum = ZSTD_highbit(ssPtr->litLengthSum+1);
    ssPtr->log2litSum = ZSTD_highbit(ssPtr->litSum+1);
    ssPtr->log2offCodeSum = ZSTD_highbit(ssPtr->offCodeSum+1);
    ssPtr->factor = 1 + ((ssPtr->litSum>>5) / ssPtr->litLengthSum) + ((ssPtr->litSum<<1) / (ssPtr->litSum + ssPtr->matchSum));
}


MEM_STATIC void ZSTD_rescaleFreqs(seqStore_t* ssPtr)
{
    unsigned u;

    ssPtr->cachedLiterals = NULL;
    ssPtr->cachedPrice = ssPtr->cachedLitLength = 0;

    if (ssPtr->litLengthSum == 0) {
        ssPtr->litSum = (2<<Litbits);
        ssPtr->litLengthSum = MaxLL+1;
        ssPtr->matchLengthSum = MaxML+1;
        ssPtr->offCodeSum = (MaxOff+1);
        ssPtr->matchSum = (2<<Litbits);

        for (u=0; u<=MaxLit; u++)
            ssPtr->litFreq[u] = 2;
        for (u=0; u<=MaxLL; u++)
            ssPtr->litLengthFreq[u] = 1;
        for (u=0; u<=MaxML; u++)
            ssPtr->matchLengthFreq[u] = 1;
        for (u=0; u<=MaxOff; u++)
            ssPtr->offCodeFreq[u] = 1;
    } else {
        ssPtr->matchLengthSum = 0;
        ssPtr->litLengthSum = 0;
        ssPtr->offCodeSum = 0;
        ssPtr->matchSum = 0;
        ssPtr->litSum = 0;

        for (u=0; u<=MaxLit; u++) {
            ssPtr->litFreq[u] = 1 + (ssPtr->litFreq[u]>>ZSTD_FREQ_DIV);
            ssPtr->litSum += ssPtr->litFreq[u];
        }
        for (u=0; u<=MaxLL; u++) {
            ssPtr->litLengthFreq[u] = 1 + (ssPtr->litLengthFreq[u]>>ZSTD_FREQ_DIV);
            ssPtr->litLengthSum += ssPtr->litLengthFreq[u];
        }
        for (u=0; u<=MaxML; u++) {
            ssPtr->matchLengthFreq[u] = 1 + (ssPtr->matchLengthFreq[u]>>ZSTD_FREQ_DIV);
            ssPtr->matchLengthSum += ssPtr->matchLengthFreq[u];
            ssPtr->matchSum += ssPtr->matchLengthFreq[u] * (u + 3);
        }
        for (u=0; u<=MaxOff; u++) {
            ssPtr->offCodeFreq[u] = 1 + (ssPtr->offCodeFreq[u]>>ZSTD_FREQ_DIV);
            ssPtr->offCodeSum += ssPtr->offCodeFreq[u];
        }
    }

    ZSTD_setLog2Prices(ssPtr);
}


FORCE_INLINE U32 ZSTD_getLiteralPrice(seqStore_t* ssPtr, U32 litLength, const BYTE* literals)
{
    U32 price, u;

    if (litLength == 0)
        return ssPtr->log2litLengthSum - ZSTD_highbit(ssPtr->litLengthFreq[0]+1);

    /* literals */
    if (ssPtr->cachedLiterals == literals) {
        U32 additional = litLength - ssPtr->cachedLitLength;
        const BYTE* literals2 = ssPtr->cachedLiterals + ssPtr->cachedLitLength;
        price = ssPtr->cachedPrice + additional * ssPtr->log2litSum;
        for (u=0; u < additional; u++)
            price -= ZSTD_highbit(ssPtr->litFreq[literals2[u]]+1);
        ssPtr->cachedPrice = price;
        ssPtr->cachedLitLength = litLength;
    } else {
        price = litLength * ssPtr->log2litSum;
        for (u=0; u < litLength; u++)
            price -= ZSTD_highbit(ssPtr->litFreq[literals[u]]+1);

        if (litLength >= 12) {
            ssPtr->cachedLiterals = literals;
            ssPtr->cachedPrice = price;
            ssPtr->cachedLitLength = litLength;
        }
    }

    /* literal Length */
    {   static const BYTE LL_Code[64] = {  0,  1,  2,  3,  4,  5,  6,  7,
                                           8,  9, 10, 11, 12, 13, 14, 15,
                                          16, 16, 17, 17, 18, 18, 19, 19,
                                          20, 20, 20, 20, 21, 21, 21, 21,
                                          22, 22, 22, 22, 22, 22, 22, 22,
                                          23, 23, 23, 23, 23, 23, 23, 23,
                                          24, 24, 24, 24, 24, 24, 24, 24,
                                          24, 24, 24, 24, 24, 24, 24, 24 };
        const BYTE LL_deltaCode = 19;
        const BYTE llCode = (litLength>63) ? (BYTE)ZSTD_highbit(litLength) + LL_deltaCode : LL_Code[litLength];
        price += LL_bits[llCode] + ssPtr->log2litLengthSum - ZSTD_highbit(ssPtr->litLengthFreq[llCode]+1);
    }

    return price;
}


FORCE_INLINE U32 ZSTD_getPrice(seqStore_t* seqStorePtr, U32 litLength, const BYTE* literals, U32 offset, U32 matchLength)
{
    /* offset */
    BYTE offCode = (BYTE)ZSTD_highbit(offset+1);
    U32 price = offCode + seqStorePtr->log2offCodeSum - ZSTD_highbit(seqStorePtr->offCodeFreq[offCode]+1);

    /* match Length */
    {   static const BYTE ML_Code[128] = { 0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15,
                                          16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
                                          32, 32, 33, 33, 34, 34, 35, 35, 36, 36, 36, 36, 37, 37, 37, 37,
                                          38, 38, 38, 38, 38, 38, 38, 38, 39, 39, 39, 39, 39, 39, 39, 39,
                                          40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
                                          41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
                                          42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
                                          42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42 };
        const BYTE ML_deltaCode = 36;
        const BYTE mlCode = (matchLength>127) ? (BYTE)ZSTD_highbit(matchLength) + ML_deltaCode : ML_Code[matchLength];
        price += ML_bits[mlCode] + seqStorePtr->log2matchLengthSum - ZSTD_highbit(seqStorePtr->matchLengthFreq[mlCode]+1);
    }

    return price + ZSTD_getLiteralPrice(seqStorePtr, litLength, literals) + seqStorePtr->factor;
}


MEM_STATIC void ZSTD_updatePrice(seqStore_t* seqStorePtr, U32 litLength, const BYTE* literals, U32 offset, U32 matchLength)
{
    U32 u;

    /* literals */
    seqStorePtr->litSum += litLength;
    for (u=0; u < litLength; u++)
        seqStorePtr->litFreq[literals[u]]++;

    /* literal Length */
    {   static const BYTE LL_Code[64] = {  0,  1,  2,  3,  4,  5,  6,  7,
                                           8,  9, 10, 11, 12, 13, 14, 15,
                                          16, 16, 17, 17, 18, 18, 19, 19,
                                          20, 20, 20, 20, 21, 21, 21, 21,
                                          22, 22, 22, 22, 22, 22, 22, 22,
                                          23, 23, 23, 23, 23, 23, 23, 23,
                                          24, 24, 24, 24, 24, 24, 24, 24,
                                          24, 24, 24, 24, 24, 24, 24, 24 };
        const BYTE LL_deltaCode = 19;
        const BYTE llCode = (litLength>63) ? (BYTE)ZSTD_highbit(litLength) + LL_deltaCode : LL_Code[litLength];
        seqStorePtr->litLengthFreq[llCode]++;
        seqStorePtr->litLengthSum++;
    }

    /* match offset */
	{   BYTE offCode = (BYTE)ZSTD_highbit(offset+1);
		seqStorePtr->offCodeSum++;
		seqStorePtr->offCodeFreq[offCode]++;
	}

    /* match Length */
    {   static const BYTE ML_Code[128] = { 0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15,
                                          16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
                                          32, 32, 33, 33, 34, 34, 35, 35, 36, 36, 36, 36, 37, 37, 37, 37,
                                          38, 38, 38, 38, 38, 38, 38, 38, 39, 39, 39, 39, 39, 39, 39, 39,
                                          40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
                                          41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
                                          42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
                                          42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42 };
        const BYTE ML_deltaCode = 36;
        const BYTE mlCode = (matchLength>127) ? (BYTE)ZSTD_highbit(matchLength) + ML_deltaCode : ML_Code[matchLength];
        seqStorePtr->matchLengthFreq[mlCode]++;
        seqStorePtr->matchLengthSum++;
    }

    ZSTD_setLog2Prices(seqStorePtr);
}


#define SET_PRICE(pos, mlen_, offset_, litlen_, price_)   \
    {                                                 \
        while (last_pos < pos)  { opt[last_pos+1].price = 1<<30; last_pos++; } \
        opt[pos].mlen = mlen_;                         \
        opt[pos].off = offset_;                        \
        opt[pos].litlen = litlen_;                     \
        opt[pos].price = price_;                       \
        ZSTD_LOG_PARSER("%d: SET price[%d/%d]=%d litlen=%d len=%d off=%d\n", (int)(inr-base), (int)pos, (int)last_pos, opt[pos].price, opt[pos].litlen, opt[pos].mlen, opt[pos].off); \
    }




/* Update hashTable3 up to ip (excluded)
   Assumption : always within prefix (ie. not within extDict) */
FORCE_INLINE
U32 ZSTD_insertAndFindFirstIndexHash3 (ZSTD_CCtx* zc, const BYTE* ip)
{
    U32* const hashTable3  = zc->hashTable3;
    U32 const hashLog3  = zc->hashLog3;
    const BYTE* const base = zc->base;
    U32 idx = zc->nextToUpdate3;
    const U32 target = zc->nextToUpdate3 = (U32)(ip - base);
    const size_t hash3 = ZSTD_hash3Ptr(ip, hashLog3);

    while(idx < target) {
        hashTable3[ZSTD_hash3Ptr(base+idx, hashLog3)] = idx;
        idx++;
    }

    return hashTable3[hash3];
}


/*-*************************************
*  Binary Tree search
***************************************/
static U32 ZSTD_insertBtAndGetAllMatches (
                        ZSTD_CCtx* zc,
                        const BYTE* const ip, const BYTE* const iLimit,
                        U32 nbCompares, const U32 mls,
                        U32 extDict, ZSTD_match_t* matches, const U32 minMatchLen)
{
    const BYTE* const base = zc->base;
    const U32 current = (U32)(ip-base);
    const U32 hashLog = zc->params.cParams.hashLog;
    const size_t h  = ZSTD_hashPtr(ip, hashLog, mls);
    U32* const hashTable = zc->hashTable;
    U32 matchIndex  = hashTable[h];
    U32* const bt   = zc->chainTable;
    const U32 btLog = zc->params.cParams.chainLog - 1;
    const U32 btMask= (1U << btLog) - 1;
    size_t commonLengthSmaller=0, commonLengthLarger=0;
    const BYTE* const dictBase = zc->dictBase;
    const U32 dictLimit = zc->dictLimit;
    const BYTE* const dictEnd = dictBase + dictLimit;
    const BYTE* const prefixStart = base + dictLimit;
    const U32 btLow = btMask >= current ? 0 : current - btMask;
    const U32 windowLow = zc->lowLimit;
    U32* smallerPtr = bt + 2*(current&btMask);
    U32* largerPtr  = bt + 2*(current&btMask) + 1;
    U32 matchEndIdx = current+8;
    U32 dummy32;   /* to be nullified at the end */
    U32 mnum = 0;

    const U32 minMatch = (mls == 3) ? 3 : 4;
    size_t bestLength = minMatchLen-1;

    if (minMatch == 3) { /* HC3 match finder */
        U32 const matchIndex3 = ZSTD_insertAndFindFirstIndexHash3 (zc, ip);
        if (matchIndex3>windowLow && (current - matchIndex3 < (1<<18))) {
            const BYTE* match;
            size_t currentMl=0;
            if ((!extDict) || matchIndex3 >= dictLimit) {
                match = base + matchIndex3;
                if (match[bestLength] == ip[bestLength]) currentMl = ZSTD_count(ip, match, iLimit);
            } else {
                match = dictBase + matchIndex3;
                if (MEM_readMINMATCH(match, MINMATCH) == MEM_readMINMATCH(ip, MINMATCH))    /* assumption : matchIndex3 <= dictLimit-4 (by table construction) */
                    currentMl = ZSTD_count_2segments(ip+MINMATCH, match+MINMATCH, iLimit, dictEnd, prefixStart) + MINMATCH;
            }

            /* save best solution */
            if (currentMl > bestLength) {
                bestLength = currentMl;
                matches[mnum].off = ZSTD_REP_MOVE + current - matchIndex3;
                matches[mnum].len = (U32)currentMl;
                mnum++;
                if (currentMl > ZSTD_OPT_NUM) goto update;
                if (ip+currentMl == iLimit) goto update; /* best possible, and avoid read overflow*/
            }
        }
    }

    hashTable[h] = current;   /* Update Hash Table */

    while (nbCompares-- && (matchIndex > windowLow)) {
        U32* nextPtr = bt + 2*(matchIndex & btMask);
        size_t matchLength = MIN(commonLengthSmaller, commonLengthLarger);   /* guaranteed minimum nb of common bytes */
        const BYTE* match;

        if ((!extDict) || (matchIndex+matchLength >= dictLimit)) {
            match = base + matchIndex;
            if (match[matchLength] == ip[matchLength]) {
#if ZSTD_OPT_DEBUG >= 5
            size_t ml;
            if (matchIndex < dictLimit)
                ml = ZSTD_count_2segments(ip, dictBase + matchIndex, iLimit, dictEnd, prefixStart);
            else
                ml = ZSTD_count(ip, match, ip+matchLength);
            if (ml < matchLength)
                printf("%d: ERROR_NOEXT: offset=%d matchLength=%d matchIndex=%d dictLimit=%d ml=%d\n", current, (int)(current - matchIndex), (int)matchLength, (int)matchIndex, (int)dictLimit, (int)ml), exit(0);
#endif
                matchLength += ZSTD_count(ip+matchLength+1, match+matchLength+1, iLimit) +1;
            }
        } else {
            match = dictBase + matchIndex;
#if ZSTD_OPT_DEBUG >= 5
            if (memcmp(match, ip, matchLength) != 0)
                 printf("%d: ERROR_EXT: matchLength=%d ZSTD_count=%d\n", current, (int)matchLength, (int)ZSTD_count_2segments(ip+matchLength, match+matchLength, iLimit, dictEnd, prefixStart)), exit(0);
#endif
            matchLength += ZSTD_count_2segments(ip+matchLength, match+matchLength, iLimit, dictEnd, prefixStart);
            ZSTD_LOG_PARSER("%d: ZSTD_INSERTBTANDGETALLMATCHES=%d offset=%d dictBase=%p dictEnd=%p prefixStart=%p ip=%p match=%p\n", (int)current, (int)matchLength, (int)(current - matchIndex), dictBase, dictEnd, prefixStart, ip, match);
            if (matchIndex+matchLength >= dictLimit)
                match = base + matchIndex;   /* to prepare for next usage of match[matchLength] */
        }

        if (matchLength > bestLength) {
            if (matchLength > matchEndIdx - matchIndex) matchEndIdx = matchIndex + (U32)matchLength;
            bestLength = matchLength;
            matches[mnum].off = ZSTD_REP_MOVE + current - matchIndex;
            matches[mnum].len = (U32)matchLength;
            mnum++;
            if (matchLength > ZSTD_OPT_NUM) break;
            if (ip+matchLength == iLimit)   /* equal : no way to know if inf or sup */
                break;   /* drop, to guarantee consistency (miss a little bit of compression) */
        }

        if (match[matchLength] < ip[matchLength]) {
            /* match is smaller than current */
            *smallerPtr = matchIndex;             /* update smaller idx */
            commonLengthSmaller = matchLength;    /* all smaller will now have at least this guaranteed common length */
            if (matchIndex <= btLow) { smallerPtr=&dummy32; break; }   /* beyond tree size, stop the search */
            smallerPtr = nextPtr+1;               /* new "smaller" => larger of match */
            matchIndex = nextPtr[1];              /* new matchIndex larger than previous (closer to current) */
        } else {
            /* match is larger than current */
            *largerPtr = matchIndex;
            commonLengthLarger = matchLength;
            if (matchIndex <= btLow) { largerPtr=&dummy32; break; }   /* beyond tree size, stop the search */
            largerPtr = nextPtr;
            matchIndex = nextPtr[0];
    }   }

    *smallerPtr = *largerPtr = 0;

update:
    zc->nextToUpdate = (matchEndIdx > current + 8) ? matchEndIdx - 8 : current+1;
    return mnum;
}


/** Tree updater, providing best match */
static U32 ZSTD_BtGetAllMatches (
                        ZSTD_CCtx* zc,
                        const BYTE* const ip, const BYTE* const iLimit,
                        const U32 maxNbAttempts, const U32 mls, ZSTD_match_t* matches, const U32 minMatchLen)
{
    if (ip < zc->base + zc->nextToUpdate) return 0;   /* skipped area */
    ZSTD_updateTree(zc, ip, iLimit, maxNbAttempts, mls);
    return ZSTD_insertBtAndGetAllMatches(zc, ip, iLimit, maxNbAttempts, mls, 0, matches, minMatchLen);
}


static U32 ZSTD_BtGetAllMatches_selectMLS (
                        ZSTD_CCtx* zc,   /* Index table will be updated */
                        const BYTE* ip, const BYTE* const iHighLimit,
                        const U32 maxNbAttempts, const U32 matchLengthSearch, ZSTD_match_t* matches, const U32 minMatchLen)
{
    switch(matchLengthSearch)
    {
    case 3 : return ZSTD_BtGetAllMatches(zc, ip, iHighLimit, maxNbAttempts, 3, matches, minMatchLen);
    default :
    case 4 : return ZSTD_BtGetAllMatches(zc, ip, iHighLimit, maxNbAttempts, 4, matches, minMatchLen);
    case 5 : return ZSTD_BtGetAllMatches(zc, ip, iHighLimit, maxNbAttempts, 5, matches, minMatchLen);
    case 6 : return ZSTD_BtGetAllMatches(zc, ip, iHighLimit, maxNbAttempts, 6, matches, minMatchLen);
    }
}

/** Tree updater, providing best match */
static U32 ZSTD_BtGetAllMatches_extDict (
                        ZSTD_CCtx* zc,
                        const BYTE* const ip, const BYTE* const iLimit,
                        const U32 maxNbAttempts, const U32 mls, ZSTD_match_t* matches, const U32 minMatchLen)
{
    if (ip < zc->base + zc->nextToUpdate) return 0;   /* skipped area */
    ZSTD_updateTree_extDict(zc, ip, iLimit, maxNbAttempts, mls);
    return ZSTD_insertBtAndGetAllMatches(zc, ip, iLimit, maxNbAttempts, mls, 1, matches, minMatchLen);
}


static U32 ZSTD_BtGetAllMatches_selectMLS_extDict (
                        ZSTD_CCtx* zc,   /* Index table will be updated */
                        const BYTE* ip, const BYTE* const iHighLimit,
                        const U32 maxNbAttempts, const U32 matchLengthSearch, ZSTD_match_t* matches, const U32 minMatchLen)
{
    switch(matchLengthSearch)
    {
    case 3 : return ZSTD_BtGetAllMatches_extDict(zc, ip, iHighLimit, maxNbAttempts, 3, matches, minMatchLen);
    default :
    case 4 : return ZSTD_BtGetAllMatches_extDict(zc, ip, iHighLimit, maxNbAttempts, 4, matches, minMatchLen);
    case 5 : return ZSTD_BtGetAllMatches_extDict(zc, ip, iHighLimit, maxNbAttempts, 5, matches, minMatchLen);
    case 6 : return ZSTD_BtGetAllMatches_extDict(zc, ip, iHighLimit, maxNbAttempts, 6, matches, minMatchLen);
    }
}


/*-*******************************
*  Optimal parser
*********************************/
FORCE_INLINE
void ZSTD_compressBlock_opt_generic(ZSTD_CCtx* ctx,
                                    const void* src, size_t srcSize)
{
    seqStore_t* seqStorePtr = &(ctx->seqStore);
    const BYTE* const istart = (const BYTE*)src;
    const BYTE* ip = istart;
    const BYTE* anchor = istart;
    const BYTE* const iend = istart + srcSize;
    const BYTE* const ilimit = iend - 8;
    const BYTE* const base = ctx->base;
    const BYTE* const prefixStart = base + ctx->dictLimit;

    const U32 maxSearches = 1U << ctx->params.cParams.searchLog;
    const U32 sufficient_len = ctx->params.cParams.targetLength;
    const U32 mls = ctx->params.cParams.searchLength;
    const U32 minMatch = (ctx->params.cParams.searchLength == 3) ? 3 : 4;

    ZSTD_optimal_t* opt = seqStorePtr->priceTable;
    ZSTD_match_t* matches = seqStorePtr->matchTable;
    const BYTE* inr;

    /* init */
    U32 offset, rep[ZSTD_REP_INIT];
    { U32 i; for (i=0; i<ZSTD_REP_INIT; i++) rep[i]=REPCODE_STARTVALUE; }

    ctx->nextToUpdate3 = ctx->nextToUpdate;
    ZSTD_resetSeqStore(seqStorePtr);
    ZSTD_rescaleFreqs(seqStorePtr);
    if ((ip-prefixStart) < REPCODE_STARTVALUE) ip = prefixStart + REPCODE_STARTVALUE;

    ZSTD_LOG_BLOCK("%d: COMPBLOCK_OPT_GENERIC srcSz=%d maxSrch=%d mls=%d sufLen=%d\n", (int)(ip-base), (int)srcSize, maxSearches, mls, sufficient_len);

    /* Match Loop */
    while (ip < ilimit) {
        U32 cur, match_num, last_pos, litlen, price;
        U32 u, mlen, best_mlen, best_off, litLength;
        memset(opt, 0, sizeof(ZSTD_optimal_t));
        last_pos = 0;
        litlen = (U32)(ip - anchor);

        /* check repCode */
        { U32 i; for (i=0; i<ZSTD_REP_NUM; i++)
        if (MEM_readMINMATCH(ip, minMatch) == MEM_readMINMATCH(ip - rep[i], minMatch)) {
            /* repcode : we take it */
            mlen = (U32)ZSTD_count(ip+minMatch, ip+minMatch-rep[i], iend) + minMatch;
            ZSTD_LOG_PARSER("%d: start try REP rep[%d]=%d mlen=%d\n", (int)(ip-base), i, (int)rep[i], (int)mlen);
            if (mlen > sufficient_len || mlen >= ZSTD_OPT_NUM) {
                best_mlen = mlen; best_off = i; cur = 0; last_pos = 1;
                goto _storeSequence;
            }
            best_off = (i<=1 && ip == anchor) ? 1-i : i;
            do {
                price = ZSTD_getPrice(seqStorePtr, litlen, anchor, best_off, mlen - MINMATCH);
                if (mlen > last_pos || price < opt[mlen].price)
                    SET_PRICE(mlen, mlen, i, litlen, price);   /* note : macro modifies last_pos */
                mlen--;
            } while (mlen >= minMatch);
        } }

        match_num = ZSTD_BtGetAllMatches_selectMLS(ctx, ip, iend, maxSearches, mls, matches, minMatch);

        ZSTD_LOG_PARSER("%d: match_num=%d last_pos=%d\n", (int)(ip-base), match_num, last_pos);
        if (!last_pos && !match_num) { ip++; continue; }

        if (match_num && (matches[match_num-1].len > sufficient_len || matches[match_num-1].len >= ZSTD_OPT_NUM)) {
            best_mlen = matches[match_num-1].len;
            best_off = matches[match_num-1].off;
            cur = 0;
            last_pos = 1;
            goto _storeSequence;
        }

        /* set prices using matches at position = 0 */
        best_mlen = (last_pos) ? last_pos : minMatch;
        for (u = 0; u < match_num; u++) {
           mlen = (u>0) ? matches[u-1].len+1 : best_mlen;
           best_mlen = matches[u].len;
           ZSTD_LOG_PARSER("%d: start Found mlen=%d off=%d best_mlen=%d last_pos=%d\n", (int)(ip-base), matches[u].len, matches[u].off, (int)best_mlen, (int)last_pos);
           while (mlen <= best_mlen) {
                price = ZSTD_getPrice(seqStorePtr, litlen, anchor, matches[u].off, mlen - MINMATCH);
                if (mlen > last_pos || price < opt[mlen].price)
                    SET_PRICE(mlen, mlen, matches[u].off, litlen, price);   /* note : macro modifies last_pos */
                mlen++;
        }  }

        if (last_pos < minMatch) { ip++; continue; }

        /* initialize opt[0] */
        { U32 i ; for (i=0; i<ZSTD_REP_INIT; i++) opt[0].rep[i] = rep[i]; }
        opt[0].mlen = 1;
        opt[0].litlen = litlen;

         /* check further positions */
        for (cur = 1; cur <= last_pos; cur++) {
           inr = ip + cur;

           if (opt[cur-1].mlen == 1) {
                litlen = opt[cur-1].litlen + 1;
                if (cur > litlen) {
                    price = opt[cur - litlen].price + ZSTD_getLiteralPrice(seqStorePtr, litlen, inr-litlen);
                } else
                    price = ZSTD_getLiteralPrice(seqStorePtr, litlen, anchor);
           } else {
                litlen = 1;
                price = opt[cur - 1].price + ZSTD_getLiteralPrice(seqStorePtr, litlen, inr-1);
           }

           if (cur > last_pos || price <= opt[cur].price) // || ((price == opt[cur].price) && (opt[cur-1].mlen == 1) && (cur != litlen)))
                SET_PRICE(cur, 1, 0, litlen, price);

           if (cur == last_pos) break;

           if (inr > ilimit)  /* last match must start at a minimum distance of 8 from oend */
               continue;

           mlen = opt[cur].mlen;
           if (opt[cur].off >= ZSTD_REP_NUM) {
                opt[cur].rep[2] = opt[cur-mlen].rep[1];
                opt[cur].rep[1] = opt[cur-mlen].rep[0];
                opt[cur].rep[0] = opt[cur].off - ZSTD_REP_MOVE;
                ZSTD_LOG_ENCODE("%d: COPYREP_OFF cur=%d mlen=%d rep[0]=%d rep[1]=%d\n", (int)(inr-base), cur, mlen, opt[cur].rep[0], opt[cur].rep[1]);
           } else {
                opt[cur].rep[2] = (opt[cur].off > 1) ? opt[cur-mlen].rep[1] : opt[cur-mlen].rep[2];
                opt[cur].rep[1] = (opt[cur].off > 0) ? opt[cur-mlen].rep[0] : opt[cur-mlen].rep[1];
                opt[cur].rep[0] = opt[cur-mlen].rep[opt[cur].off];
                ZSTD_LOG_ENCODE("%d: COPYREP_NOR cur=%d mlen=%d rep[0]=%d rep[1]=%d\n", (int)(inr-base), cur, mlen, opt[cur].rep[0], opt[cur].rep[1]);
           }

           ZSTD_LOG_PARSER("%d: CURRENT_NoExt price[%d/%d]=%d off=%d mlen=%d litlen=%d rep[0]=%d rep[1]=%d\n", (int)(inr-base), cur, last_pos, opt[cur].price, opt[cur].off, opt[cur].mlen, opt[cur].litlen, opt[cur].rep[0], opt[cur].rep[1]);

           best_mlen = minMatch;
           { U32 i; for (i=0; i<ZSTD_REP_NUM; i++)
           if (MEM_readMINMATCH(inr, minMatch) == MEM_readMINMATCH(inr - opt[cur].rep[i], minMatch)) {  /* check rep */
               mlen = (U32)ZSTD_count(inr+minMatch, inr+minMatch - opt[cur].rep[i], iend) + minMatch;
               ZSTD_LOG_PARSER("%d: Found REP %d/%d mlen=%d off=%d rep=%d opt[%d].off=%d\n", (int)(inr-base), i, ZSTD_REP_NUM, mlen, i, opt[cur].rep[i], cur, opt[cur].off);

               if (mlen > sufficient_len || cur + mlen >= ZSTD_OPT_NUM) {
                    ZSTD_LOG_PARSER("%d: REP sufficient_len=%d best_mlen=%d best_off=%d last_pos=%d\n", (int)(inr-base), sufficient_len, best_mlen, best_off, last_pos);
                    best_mlen = mlen; best_off = i; last_pos = cur + 1;
                    goto _storeSequence;
               }

               best_off = (i<=1 && opt[cur].mlen != 1) ? 1-i : i;
               if (opt[cur].mlen == 1) {
                    litlen = opt[cur].litlen;
                    if (cur > litlen) {
                        price = opt[cur - litlen].price + ZSTD_getPrice(seqStorePtr, litlen, inr-litlen, best_off, mlen - MINMATCH);
                    } else
                        price = ZSTD_getPrice(seqStorePtr, litlen, anchor, best_off, mlen - MINMATCH);
                } else {
                    litlen = 0;
                    price = opt[cur].price + ZSTD_getPrice(seqStorePtr, 0, NULL, best_off, mlen - MINMATCH);
                }

                if (mlen > best_mlen) best_mlen = mlen;
                ZSTD_LOG_PARSER("%d: Found REP mlen=%d off=%d price=%d litlen=%d\n", (int)(inr-base), mlen, best_off, price, litlen);

                do {
                    if (cur + mlen > last_pos || price <= opt[cur + mlen].price)
                        SET_PRICE(cur + mlen, mlen, i, litlen, price);
                    mlen--;
                } while (mlen >= minMatch);
            } }

            match_num = ZSTD_BtGetAllMatches_selectMLS(ctx, inr, iend, maxSearches, mls, matches, best_mlen);
            ZSTD_LOG_PARSER("%d: ZSTD_GetAllMatches match_num=%d\n", (int)(inr-base), match_num);

            if (match_num > 0 && (matches[match_num-1].len > sufficient_len || cur + matches[match_num-1].len >= ZSTD_OPT_NUM)) {
                best_mlen = matches[match_num-1].len;
                best_off = matches[match_num-1].off;
                last_pos = cur + 1;
                goto _storeSequence;
            }

            /* set prices using matches at position = cur */
            for (u = 0; u < match_num; u++) {
                mlen = (u>0) ? matches[u-1].len+1 : best_mlen;
                best_mlen = matches[u].len;

              //  ZSTD_LOG_PARSER("%d: Found1 cur=%d mlen=%d off=%d best_mlen=%d last_pos=%d\n", (int)(inr-base), cur, matches[u].len, matches[u].off, best_mlen, last_pos);
                while (mlen <= best_mlen) {
                    if (opt[cur].mlen == 1) {
                        litlen = opt[cur].litlen;
                        if (cur > litlen)
                            price = opt[cur - litlen].price + ZSTD_getPrice(seqStorePtr, litlen, ip+cur-litlen, matches[u].off, mlen - MINMATCH);
                        else
                            price = ZSTD_getPrice(seqStorePtr, litlen, anchor, matches[u].off, mlen - MINMATCH);
                    } else {
                        litlen = 0;
                        price = opt[cur].price + ZSTD_getPrice(seqStorePtr, 0, NULL, matches[u].off, mlen - MINMATCH);
                    }

                  //  ZSTD_LOG_PARSER("%d: Found2 mlen=%d best_mlen=%d off=%d price=%d litlen=%d\n", (int)(inr-base), mlen, best_mlen, matches[u].off, price, litlen);
                    if (cur + mlen > last_pos || (price < opt[cur + mlen].price))
                        SET_PRICE(cur + mlen, mlen, matches[u].off, litlen, price);

                    mlen++;
        }   }   }   //  for (cur = 1; cur <= last_pos; cur++)

        best_mlen = opt[last_pos].mlen;
        best_off = opt[last_pos].off;
        cur = last_pos - best_mlen;

        /* store sequence */
_storeSequence:   /* cur, last_pos, best_mlen, best_off have to be set */
        for (u = 1; u <= last_pos; u++)
            ZSTD_LOG_PARSER("%d: price[%d/%d]=%d off=%d mlen=%d litlen=%d rep[0]=%d rep[1]=%d\n", (int)(ip-base+u), u, last_pos, opt[u].price, opt[u].off, opt[u].mlen, opt[u].litlen, opt[u].rep[0], opt[u].rep[1]);
        ZSTD_LOG_PARSER("%d: cur=%d/%d best_mlen=%d best_off=%d rep[0]=%d\n", (int)(ip-base+cur), (int)cur, (int)last_pos, (int)best_mlen, (int)best_off, opt[cur].rep[0]);

        opt[0].mlen = 1;

        while (1) {
            mlen = opt[cur].mlen;
            offset = opt[cur].off;
            opt[cur].mlen = best_mlen;
            opt[cur].off = best_off;
            best_mlen = mlen;
            best_off = offset;
            if (mlen > cur) break;
            cur -= mlen;
        }

        for (u = 0; u <= last_pos;) {
            ZSTD_LOG_PARSER("%d: price2[%d/%d]=%d off=%d mlen=%d litlen=%d rep[0]=%d rep[1]=%d\n", (int)(ip-base+u), u, last_pos, opt[u].price, opt[u].off, opt[u].mlen, opt[u].litlen, opt[u].rep[0], opt[u].rep[1]);
            u += opt[u].mlen;
        }

        for (cur=0; cur < last_pos; ) {
            ZSTD_LOG_PARSER("%d: price3[%d/%d]=%d off=%d mlen=%d litlen=%d rep[0]=%d rep[1]=%d\n", (int)(ip-base+cur), cur, last_pos, opt[cur].price, opt[cur].off, opt[cur].mlen, opt[cur].litlen, opt[cur].rep[0], opt[cur].rep[1]);
            mlen = opt[cur].mlen;
            if (mlen == 1) { ip++; cur++; continue; }
            offset = opt[cur].off;
            cur += mlen;
            litLength = (U32)(ip - anchor);
           // ZSTD_LOG_ENCODE("%d/%d: ENCODE literals=%d mlen=%d off=%d rep[0]=%d rep[1]=%d\n", (int)(ip-base), (int)(iend-base), (int)(litLength), (int)mlen, (int)(offset), (int)rep[0], (int)rep[1]);

            if (offset >= ZSTD_REP_NUM) {
                rep[2] = rep[1];
                rep[1] = rep[0];
                rep[0] = offset - ZSTD_REP_MOVE;
            } else {
                if (offset != 0) {
                    best_off = rep[offset];
                    if (offset != 1) rep[2] = rep[1];
                    rep[1] = rep[0];
                    rep[0] = best_off;
                }
                if (litLength == 0 && offset<=1) offset = 1-offset;
            }

            ZSTD_LOG_ENCODE("%d/%d: ENCODE literals=%d mlen=%d off=%d rep[0]=%d rep[1]=%d\n", (int)(ip-base), (int)(iend-base), (int)(litLength), (int)mlen, (int)(offset), (int)rep[0], (int)rep[1]);

#if ZSTD_OPT_DEBUG >= 5
            U32 ml2;
            if (offset >= ZSTD_REP_NUM)
                ml2 = (U32)ZSTD_count(ip, ip-(offset-ZSTD_REP_MOVE), iend);
            else
                ml2 = (U32)ZSTD_count(ip, ip-rep[0], iend);
            if ((offset >= 8) && (ml2 < mlen || ml2 < minMatch)) {
                printf("%d: ERROR_NoExt iend=%d mlen=%d offset=%d ml2=%d\n", (int)(ip - base), (int)(iend - ip), (int)mlen, (int)offset, (int)ml2); exit(0); }
            if (ip < anchor) {
                printf("%d: ERROR_NoExt ip < anchor iend=%d mlen=%d offset=%d\n", (int)(ip - base), (int)(iend - ip), (int)mlen, (int)offset); exit(0); }
            if (ip + mlen > iend) {
                printf("%d: ERROR_NoExt ip + mlen >= iend iend=%d mlen=%d offset=%d\n", (int)(ip - base), (int)(iend - ip), (int)mlen, (int)offset); exit(0); }
#endif

            ZSTD_updatePrice(seqStorePtr, litLength, anchor, offset, mlen-MINMATCH);
            ZSTD_storeSeq(seqStorePtr, litLength, anchor, offset, mlen-MINMATCH);
            anchor = ip = ip + mlen;
    }    }   /* for (cur=0; cur < last_pos; ) */

    {   /* Last Literals */
        size_t lastLLSize = iend - anchor;
        ZSTD_LOG_ENCODE("%d: lastLLSize literals=%u\n", (int)(ip-base), (U32)lastLLSize);
        memcpy(seqStorePtr->lit, anchor, lastLLSize);
        seqStorePtr->lit += lastLLSize;
    }
}


FORCE_INLINE
void ZSTD_compressBlock_opt_extDict_generic(ZSTD_CCtx* ctx,
                                     const void* src, size_t srcSize)
{
    seqStore_t* seqStorePtr = &(ctx->seqStore);
    const BYTE* const istart = (const BYTE*)src;
    const BYTE* ip = istart;
    const BYTE* anchor = istart;
    const BYTE* const iend = istart + srcSize;
    const BYTE* const ilimit = iend - 8;
    const BYTE* const base = ctx->base;
    const U32 dictLimit = ctx->dictLimit;
    const BYTE* const prefixStart = base + dictLimit;
    const BYTE* const dictBase = ctx->dictBase;
    const BYTE* const dictEnd  = dictBase + dictLimit;

    const U32 maxSearches = 1U << ctx->params.cParams.searchLog;
    const U32 sufficient_len = ctx->params.cParams.targetLength;
    const U32 mls = ctx->params.cParams.searchLength;
    const U32 minMatch = (ctx->params.cParams.searchLength == 3) ? 3 : 4;

    ZSTD_optimal_t* opt = seqStorePtr->priceTable;
    ZSTD_match_t* matches = seqStorePtr->matchTable;
    const BYTE* inr;

    /* init */
    U32 offset, rep[ZSTD_REP_INIT];
    { U32 i; for (i=0; i<ZSTD_REP_INIT; i++) rep[i]=REPCODE_STARTVALUE; }

    ctx->nextToUpdate3 = ctx->nextToUpdate;
    ZSTD_resetSeqStore(seqStorePtr);
    ZSTD_rescaleFreqs(seqStorePtr);
    if ((ip - prefixStart) < REPCODE_STARTVALUE) ip += REPCODE_STARTVALUE;

    ZSTD_LOG_BLOCK("%d: COMPBLOCK_OPT_EXTDICT srcSz=%d maxSrch=%d mls=%d sufLen=%d\n", (int)(ip-base), (int)srcSize, maxSearches, mls, sufficient_len);

    /* Match Loop */
    while (ip < ilimit) {
        U32 cur, match_num, last_pos, litlen, price;
        U32 u, mlen, best_mlen, best_off, litLength;
        U32 current = (U32)(ip-base);
        memset(opt, 0, sizeof(ZSTD_optimal_t));
        last_pos = 0;
        inr = ip;
        opt[0].litlen = (U32)(ip - anchor);

        /* check repCode */
        { U32 i; for (i=0; i<ZSTD_REP_NUM; i++) {
            const U32 repIndex = (U32)(current - rep[i]);
            const BYTE* const repBase = repIndex < dictLimit ? dictBase : base;
            const BYTE* const repMatch = repBase + repIndex;
            if ( ((U32)((dictLimit-1) - repIndex) >= 3)   /* intentional overflow */
               && (MEM_readMINMATCH(ip, minMatch) == MEM_readMINMATCH(repMatch, minMatch)) ) {
                /* repcode detected we should take it */
                const BYTE* const repEnd = repIndex < dictLimit ? dictEnd : iend;
                mlen = (U32)ZSTD_count_2segments(ip+minMatch, repMatch+minMatch, iend, repEnd, prefixStart) + minMatch;

                ZSTD_LOG_PARSER("%d: start try REP rep[%d]=%d mlen=%d\n", (int)(ip-base), i, (int)rep[i], (int)mlen);
                if (mlen > sufficient_len || mlen >= ZSTD_OPT_NUM) {
                    best_mlen = mlen; best_off = i; cur = 0; last_pos = 1;
                    goto _storeSequence;
                }

                best_off = (i<=1 && ip == anchor) ? 1-i : i;
                litlen = opt[0].litlen;
                do {
                    price = ZSTD_getPrice(seqStorePtr, litlen, anchor, best_off, mlen - MINMATCH);
                    if (mlen > last_pos || price < opt[mlen].price)
                        SET_PRICE(mlen, mlen, i, litlen, price);   /* note : macro modifies last_pos */
                    mlen--;
                } while (mlen >= minMatch);
        }   } }

        match_num = ZSTD_BtGetAllMatches_selectMLS_extDict(ctx, ip, iend, maxSearches, mls, matches, minMatch);  /* first search (depth 0) */

        ZSTD_LOG_PARSER("%d: match_num=%d last_pos=%d\n", (int)(ip-base), match_num, last_pos);
        if (!last_pos && !match_num) { ip++; continue; }

        { U32 i; for (i=0; i<ZSTD_REP_INIT; i++) opt[0].rep[i] = rep[i]; }
        opt[0].mlen = 1;

        if (match_num && (matches[match_num-1].len > sufficient_len || matches[match_num-1].len >= ZSTD_OPT_NUM)) {
            best_mlen = matches[match_num-1].len;
            best_off = matches[match_num-1].off;
            cur = 0;
            last_pos = 1;
            goto _storeSequence;
        }

        best_mlen = (last_pos) ? last_pos : minMatch;

        // set prices using matches at position = 0
        for (u = 0; u < match_num; u++) {
            mlen = (u>0) ? matches[u-1].len+1 : best_mlen;
            best_mlen = matches[u].len;
            ZSTD_LOG_PARSER("%d: start Found mlen=%d off=%d best_mlen=%d last_pos=%d\n", (int)(ip-base), matches[u].len, matches[u].off, (int)best_mlen, (int)last_pos);
            litlen = opt[0].litlen;
            while (mlen <= best_mlen) {
                price = ZSTD_getPrice(seqStorePtr, litlen, anchor, matches[u].off, mlen - MINMATCH);
                if (mlen > last_pos || price < opt[mlen].price)
                    SET_PRICE(mlen, mlen, matches[u].off, litlen, price);
                mlen++;
        }   }

        if (last_pos < minMatch) {
            // ip += ((ip-anchor) >> g_searchStrength) + 1;   /* jump faster over incompressible sections */
            ip++; continue;
        }

        /* check further positions */
        for (cur = 1; cur <= last_pos; cur++) {
            inr = ip + cur;

            if (opt[cur-1].mlen == 1) {
                litlen = opt[cur-1].litlen + 1;
                if (cur > litlen) {
                    price = opt[cur - litlen].price + ZSTD_getLiteralPrice(seqStorePtr, litlen, inr-litlen);
                } else
                    price = ZSTD_getLiteralPrice(seqStorePtr, litlen, anchor);
            } else {
                litlen = 1;
                price = opt[cur - 1].price + ZSTD_getLiteralPrice(seqStorePtr, litlen, inr-1);
            }

            if (cur > last_pos || price <= opt[cur].price) // || ((price == opt[cur].price) && (opt[cur-1].mlen == 1) && (cur != litlen)))
                SET_PRICE(cur, 1, 0, litlen, price);

            if (cur == last_pos) break;

            if (inr > ilimit)  /* last match must start at a minimum distance of 8 from oend */
                continue;

            mlen = opt[cur].mlen;
            if (opt[cur].off >= ZSTD_REP_NUM) {
                opt[cur].rep[2] = opt[cur-mlen].rep[1];
                opt[cur].rep[1] = opt[cur-mlen].rep[0];
                opt[cur].rep[0] = opt[cur].off - ZSTD_REP_MOVE;
                ZSTD_LOG_ENCODE("%d: COPYREP_OFF cur=%d mlen=%d rep[0]=%d rep[1]=%d\n", (int)(inr-base), cur, mlen, opt[cur].rep[0], opt[cur].rep[1]);
            } else {
                opt[cur].rep[2] = (opt[cur].off > 1) ? opt[cur-mlen].rep[1] : opt[cur-mlen].rep[2];
                opt[cur].rep[1] = (opt[cur].off > 0) ? opt[cur-mlen].rep[0] : opt[cur-mlen].rep[1];
                opt[cur].rep[0] = opt[cur-mlen].rep[opt[cur].off];
                ZSTD_LOG_ENCODE("%d: COPYREP_NOR cur=%d mlen=%d rep[0]=%d rep[1]=%d\n", (int)(inr-base), cur, mlen, opt[cur].rep[0], opt[cur].rep[1]);
            }

            ZSTD_LOG_PARSER("%d: CURRENT_Ext price[%d/%d]=%d off=%d mlen=%d litlen=%d rep[0]=%d rep[1]=%d\n", (int)(inr-base), cur, last_pos, opt[cur].price, opt[cur].off, opt[cur].mlen, opt[cur].litlen, opt[cur].rep[0], opt[cur].rep[1]);
            best_mlen = 0;

            { U32 i; for (i=0; i<ZSTD_REP_NUM; i++) {
                const U32 repIndex = (U32)(current+cur - opt[cur].rep[i]);
                const BYTE* const repBase = repIndex < dictLimit ? dictBase : base;
                const BYTE* const repMatch = repBase + repIndex;
                if ( ((U32)((dictLimit-1) - repIndex) >= 3)   /* intentional overflow */
                  && (MEM_readMINMATCH(inr, minMatch) == MEM_readMINMATCH(repMatch, minMatch)) ) {
                    /* repcode detected */
                    const BYTE* const repEnd = repIndex < dictLimit ? dictEnd : iend;
                    mlen = (U32)ZSTD_count_2segments(inr+minMatch, repMatch+minMatch, iend, repEnd, prefixStart) + minMatch;
                    ZSTD_LOG_PARSER("%d: Found REP %d/%d mlen=%d off=%d rep=%d opt[%d].off=%d\n", (int)(inr-base), i, ZSTD_REP_NUM, mlen, i, opt[cur].rep[i], cur, opt[cur].off);

                    if (mlen > sufficient_len || cur + mlen >= ZSTD_OPT_NUM) {
                        ZSTD_LOG_PARSER("%d: REP sufficient_len=%d best_mlen=%d best_off=%d last_pos=%d\n", (int)(inr-base), sufficient_len, best_mlen, best_off, last_pos);
                        best_mlen = mlen; best_off = i; last_pos = cur + 1;
                        goto _storeSequence;
                    }

                    best_off = (i<=1 && opt[cur].mlen != 1) ? 1-i : i;
                    if (opt[cur].mlen == 1) {
                        litlen = opt[cur].litlen;
                        if (cur > litlen) {
                            price = opt[cur - litlen].price + ZSTD_getPrice(seqStorePtr, litlen, inr-litlen, best_off, mlen - MINMATCH);
                        } else
                            price = ZSTD_getPrice(seqStorePtr, litlen, anchor, best_off, mlen - MINMATCH);
                    } else {
                        litlen = 0;
                        price = opt[cur].price + ZSTD_getPrice(seqStorePtr, 0, NULL, best_off, mlen - MINMATCH);
                    }

                    best_mlen = mlen;
                    ZSTD_LOG_PARSER("%d: Found REP mlen=%d off=%d price=%d litlen=%d\n", (int)(inr-base), mlen, best_off, price, litlen);

                    do {
                        if (cur + mlen > last_pos || price <= opt[cur + mlen].price)
                            SET_PRICE(cur + mlen, mlen, i, litlen, price);
                        mlen--;
                    } while (mlen >= minMatch);
            }   } }

            match_num = ZSTD_BtGetAllMatches_selectMLS_extDict(ctx, inr, iend, maxSearches, mls, matches, minMatch);
            ZSTD_LOG_PARSER("%d: ZSTD_GetAllMatches match_num=%d\n", (int)(inr-base), match_num);

            if (match_num > 0 && matches[match_num-1].len > sufficient_len) {
                best_mlen = matches[match_num-1].len;
                best_off = matches[match_num-1].off;
                last_pos = cur + 1;
                goto _storeSequence;
            }

            best_mlen = (best_mlen > minMatch) ? best_mlen : minMatch;

            /* set prices using matches at position = cur */
            for (u = 0; u < match_num; u++) {
                mlen = (u>0) ? matches[u-1].len+1 : best_mlen;
                best_mlen = (cur + matches[u].len < ZSTD_OPT_NUM) ? matches[u].len : ZSTD_OPT_NUM - cur;

            //    ZSTD_LOG_PARSER("%d: Found1 cur=%d mlen=%d off=%d best_mlen=%d last_pos=%d\n", (int)(inr-base), cur, matches[u].len, matches[u].off, best_mlen, last_pos);
                while (mlen <= best_mlen) {
                    if (opt[cur].mlen == 1) {
                        litlen = opt[cur].litlen;
                        if (cur > litlen)
                            price = opt[cur - litlen].price + ZSTD_getPrice(seqStorePtr, litlen, ip+cur-litlen, matches[u].off, mlen - MINMATCH);
                        else
                            price = ZSTD_getPrice(seqStorePtr, litlen, anchor, matches[u].off, mlen - MINMATCH);
                    } else {
                        litlen = 0;
                        price = opt[cur].price + ZSTD_getPrice(seqStorePtr, 0, NULL, matches[u].off, mlen - MINMATCH);
                    }

                //    ZSTD_LOG_PARSER("%d: Found2 mlen=%d best_mlen=%d off=%d price=%d litlen=%d\n", (int)(inr-base), mlen, best_mlen, matches[u].off, price, litlen);
                    if (cur + mlen > last_pos || (price < opt[cur + mlen].price))
                        SET_PRICE(cur + mlen, mlen, matches[u].off, litlen, price);

                    mlen++;
        }   }   }   /* for (cur = 1; cur <= last_pos; cur++) */

        best_mlen = opt[last_pos].mlen;
        best_off = opt[last_pos].off;
        cur = last_pos - best_mlen;

        /* store sequence */
_storeSequence:   /* cur, last_pos, best_mlen, best_off have to be set */
        for (u = 1; u <= last_pos; u++)
            ZSTD_LOG_PARSER("%d: price[%u/%d]=%d off=%d mlen=%d litlen=%d rep[0]=%d rep[1]=%d\n", (int)(ip-base+u), u, last_pos, opt[u].price, opt[u].off, opt[u].mlen, opt[u].litlen, opt[u].rep[0], opt[u].rep[1]);
        ZSTD_LOG_PARSER("%d: cur=%d/%d best_mlen=%d best_off=%d rep[0]=%d\n", (int)(ip-base+cur), (int)cur, (int)last_pos, (int)best_mlen, (int)best_off, opt[cur].rep[0]);

        opt[0].mlen = 1;

        while (1) {
            mlen = opt[cur].mlen;
            offset = opt[cur].off;
            opt[cur].mlen = best_mlen;
            opt[cur].off = best_off;
            best_mlen = mlen;
            best_off = offset;
            if (mlen > cur) break;
            cur -= mlen;
        }

        for (u = 0; u <= last_pos; ) {
            ZSTD_LOG_PARSER("%d: price2[%d/%d]=%d off=%d mlen=%d litlen=%d rep[0]=%d rep[1]=%d\n", (int)(ip-base+u), u, last_pos, opt[u].price, opt[u].off, opt[u].mlen, opt[u].litlen, opt[u].rep[0], opt[u].rep[1]);
            u += opt[u].mlen;
        }

        for (cur=0; cur < last_pos; ) {
            ZSTD_LOG_PARSER("%d: price3[%d/%d]=%d off=%d mlen=%d litlen=%d rep[0]=%d rep[1]=%d\n", (int)(ip-base+cur), cur, last_pos, opt[cur].price, opt[cur].off, opt[cur].mlen, opt[cur].litlen, opt[cur].rep[0], opt[cur].rep[1]);
            mlen = opt[cur].mlen;
            if (mlen == 1) { ip++; cur++; continue; }
            offset = opt[cur].off;
            cur += mlen;
            litLength = (U32)(ip - anchor);
         //   ZSTD_LOG_ENCODE("%d/%d: ENCODE1 literals=%d mlen=%d off=%d rep[0]=%d rep[1]=%d\n", (int)(ip-base), (int)(iend-base), (int)(litLength), (int)mlen, (int)(offset), (int)rep[0], (int)rep[1]);

            if (offset >= ZSTD_REP_NUM) {
                rep[2] = rep[1];
                rep[1] = rep[0];
                rep[0] = offset - ZSTD_REP_MOVE;
            } else {
                if (offset != 0) {
                    best_off = rep[offset];
                    if (offset != 1) rep[2] = rep[1];
                    rep[1] = rep[0];
                    rep[0] = best_off;
                 }
                 if (litLength == 0 && offset<=1) offset = 1-offset;
            }

            ZSTD_LOG_ENCODE("%d/%d: ENCODE literals=%d mlen=%d off=%d rep[0]=%d rep[1]=%d\n", (int)(ip-base), (int)(iend-base), (int)(litLength), (int)mlen, (int)(offset), (int)rep[0], (int)rep[1]);

#if ZSTD_OPT_DEBUG >= 5
            U32 ml2;
            if (offset >= ZSTD_REP_NUM) {
                best_off = offset - ZSTD_REP_MOVE;
                if (best_off > (size_t)(ip - prefixStart))  {
                    const BYTE* match = dictEnd - (best_off - (ip - prefixStart));
                    ml2 = ZSTD_count_2segments(ip, match, iend, dictEnd, prefixStart);
                    ZSTD_LOG_PARSER("%d: ZSTD_count_2segments=%d offset=%d dictBase=%p dictEnd=%p prefixStart=%p ip=%p match=%p\n", (int)current, (int)ml2, (int)best_off, dictBase, dictEnd, prefixStart, ip, match);
                }
                else ml2 = (U32)ZSTD_count(ip, ip-offset, iend);
            }
            else ml2 = (U32)ZSTD_count(ip, ip-rep[0], iend);
            if ((offset >= 8) && (ml2 < mlen || ml2 < minMatch)) {
                printf("%d: ERROR_Ext iend=%d mlen=%d offset=%d ml2=%d\n", (int)(ip - base), (int)(iend - ip), (int)mlen, (int)offset, (int)ml2); exit(0); }
            if (ip < anchor) {
                printf("%d: ERROR_Ext ip < anchor iend=%d mlen=%d offset=%d\n", (int)(ip - base), (int)(iend - ip), (int)mlen, (int)offset); exit(0); }
            if (ip + mlen > iend) {
                printf("%d: ERROR_Ext ip + mlen >= iend iend=%d mlen=%d offset=%d\n", (int)(ip - base), (int)(iend - ip), (int)mlen, (int)offset); exit(0); }
#endif

            ZSTD_updatePrice(seqStorePtr, litLength, anchor, offset, mlen-MINMATCH);
            ZSTD_storeSeq(seqStorePtr, litLength, anchor, offset, mlen-MINMATCH);
            anchor = ip = ip + mlen;
    }    }   /* for (cur=0; cur < last_pos; ) */

    {   /* Last Literals */
        size_t lastLLSize = iend - anchor;
        ZSTD_LOG_ENCODE("%d: lastLLSize literals=%u\n", (int)(ip-base), (U32)(lastLLSize));
        memcpy(seqStorePtr->lit, anchor, lastLLSize);
        seqStorePtr->lit += lastLLSize;
    }
}
