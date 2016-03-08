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

/*-  Dependencies  -*/
#include <stdio.h>  /* for debug */


/*-  Local types  -*/
typedef struct {
    U32 off;
    U32 len;
    U32 back;
} ZSTD_match_t;

typedef struct {
    U32 price;
    U32 off;
    U32 mlen;
    U32 litlen;
    U32 rep;
    U32 rep2;
} ZSTD_optimal_t;


/*-  Constants  -*/
#define ZSTD_OPT_NUM   (1<<12)
#define ZSTD_FREQ_THRESHOLD (256)

/*-  Debug  -*/
#define ZSTD_OPT_DEBUG 0     // 1 = tableID=0;    5 = check encoded sequences

#if defined(ZSTD_OPT_DEBUG) && ZSTD_OPT_DEBUG>=1
    #define ZSTD_LOG_PARSER(...) printf(__VA_ARGS__)
    #define ZSTD_LOG_ENCODE(...) printf(__VA_ARGS__)
    #define ZSTD_LOG_TRY_PRICE(...) printf(__VA_ARGS__)
#else
    #define ZSTD_LOG_PARSER(...)
    #define ZSTD_LOG_ENCODE(...)
    #define ZSTD_LOG_TRY_PRICE(...)
#endif


FORCE_INLINE U32 ZSTD_getLiteralPriceReal(seqStore_t* seqStorePtr, U32 litLength, const BYTE* literals)
{
    U32 price, freq, u;

    if (!litLength) return 1;   /* special case */

    /* literals */
    price = litLength * ZSTD_highbit(seqStorePtr->litSum);
    for (u=0; u < litLength; u++)
        price -= ZSTD_highbit(seqStorePtr->litFreq[literals[u]]);

    /* literal Length */
    price += ((litLength >= MaxLL)*8) + ((litLength >= 255+MaxLL)*16) + ((litLength>=(1<<15))*8);
    if (litLength >= MaxLL) litLength = MaxLL;
    freq = seqStorePtr->litLengthFreq[litLength];
    price += ZSTD_highbit(seqStorePtr->litLengthSum) - ZSTD_highbit(freq);

    return price;
}


FORCE_INLINE U32 ZSTD_getLiteralPrice(seqStore_t* seqStorePtr, U32 litLength, const BYTE* literals)
{
    if (seqStorePtr->litSum > ZSTD_FREQ_THRESHOLD)
        return ZSTD_getLiteralPriceReal(seqStorePtr, litLength, literals);
    /* backup eval */
    return 1 + (litLength<<3);
}


FORCE_INLINE U32 ZSTD_getMatchPriceReal(seqStore_t* seqStorePtr, U32 offset, U32 matchLength)
{
    /* offset */
    BYTE offCode = offset ? (BYTE)ZSTD_highbit(offset) + 1 : 0;
    U32 price = ZSTD_highbit(seqStorePtr->offCodeSum) - ZSTD_highbit(seqStorePtr->offCodeFreq[offCode]);
    price += offCode;

    /* match Length */
    price += ((matchLength >= MaxML)*8) + ((matchLength >= 255+MaxML)*16) + ((matchLength>=(1<<15))*8);
    if (matchLength >= MaxML) matchLength = MaxML;
    price += ZSTD_highbit(seqStorePtr->matchLengthSum) - ZSTD_highbit(seqStorePtr->matchLengthFreq[matchLength]);

    return price;
}


FORCE_INLINE U32 ZSTD_getPrice(seqStore_t* seqStorePtr, U32 litLength, const BYTE* literals, U32 offset, U32 matchLength)
{
    if (seqStorePtr->litSum > ZSTD_FREQ_THRESHOLD)
        return ZSTD_getLiteralPriceReal(seqStorePtr, litLength, literals) + ZSTD_getMatchPriceReal(seqStorePtr, offset, matchLength);
    /* backup eval */
    return (litLength<<3) + ZSTD_highbit((U32)matchLength+1) + Offbits + ZSTD_highbit((U32)offset+1);
}


MEM_STATIC void ZSTD_updatePrice(seqStore_t* seqStorePtr, U32 litLength, const BYTE* literals, U32 offset, U32 matchLength)
{
    U32 u;

    /* literals */
    seqStorePtr->litSum += litLength;
    for (u=0; u < litLength; u++)
        seqStorePtr->litFreq[literals[u]]++;

    /* literal Length */
    seqStorePtr->litLengthSum++;
    if (litLength >= MaxLL)
        seqStorePtr->litLengthFreq[MaxLL]++;
    else
        seqStorePtr->litLengthFreq[litLength]++;

    /* match offset */
    seqStorePtr->offCodeSum++;
    BYTE offCode = (BYTE)ZSTD_highbit(offset) + 1;
    if (offset==0) offCode=0;
    seqStorePtr->offCodeFreq[offCode]++;

    /* match Length */
    seqStorePtr->matchLengthSum++;
    if (matchLength >= MaxML)
        seqStorePtr->matchLengthFreq[MaxML]++;
    else
        seqStorePtr->matchLengthFreq[matchLength]++;
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



/*-*************************************
*  Binary Tree search
***************************************/
static U32 ZSTD_insertBtAndGetAllMatches (
                        ZSTD_CCtx* zc,
                        const BYTE* const ip, const BYTE* const iend,
                        U32 nbCompares, const U32 mls,
                        U32 extDict, ZSTD_match_t* matches, size_t bestLength)
{
    const BYTE* const base = zc->base;
    const U32 current = (U32)(ip-base);
    const U32 hashLog = zc->params.hashLog;
    const size_t h  = ZSTD_hashPtr(ip, hashLog, mls);
    U32* const hashTable = zc->hashTable;
    U32 matchIndex  = hashTable[h];
    U32* const bt   = zc->contentTable;
    const U32 btLog = zc->params.contentLog - 1;
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

    bestLength = MINMATCH-1;
    hashTable[h] = current;   /* Update Hash Table */

    while (nbCompares-- && (matchIndex > windowLow)) {
        U32* nextPtr = bt + 2*(matchIndex & btMask);
        size_t matchLength = MIN(commonLengthSmaller, commonLengthLarger);   /* guaranteed minimum nb of common bytes */
        const BYTE* match;

        if ((!extDict) || (matchIndex+matchLength >= dictLimit)) {
            match = base + matchIndex;
            if (match[matchLength] == ip[matchLength])
                matchLength += ZSTD_count(ip+matchLength+1, match+matchLength+1, iend) +1;
        } else {
            match = dictBase + matchIndex;
            matchLength += ZSTD_count_2segments(ip+matchLength, match+matchLength, iend, dictEnd, prefixStart);
            if (matchIndex+matchLength >= dictLimit)
                match = base + matchIndex;   /* to prepare for next usage of match[matchLength] */
        }

        if (matchLength > bestLength) {
            if (matchLength > matchEndIdx - matchIndex) matchEndIdx = matchIndex + (U32)matchLength;
            bestLength = matchLength;
            matches[mnum].off = current - matchIndex;
            matches[mnum].len = (U32)matchLength;
            matches[mnum].back = 0;
            mnum++;
            if (matchLength > ZSTD_OPT_NUM) break;
            if (ip+matchLength == iend)   /* equal : no way to know if inf or sup */
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

    zc->nextToUpdate = (matchEndIdx > current + 8) ? matchEndIdx - 8 : current+1;
    return mnum;
}


/** Tree updater, providing best match */
static U32 ZSTD_BtGetAllMatches (
                        ZSTD_CCtx* zc,
                        const BYTE* const ip, const BYTE* const iLimit,
                        const U32 maxNbAttempts, const U32 mls, ZSTD_match_t* matches, U32 minml)
{
    if (ip < zc->base + zc->nextToUpdate) return 0;   /* skipped area */
    ZSTD_updateTree(zc, ip, iLimit, maxNbAttempts, mls);
    return ZSTD_insertBtAndGetAllMatches(zc, ip, iLimit, maxNbAttempts, mls, 0, matches, minml);
}


static U32 ZSTD_BtGetAllMatches_selectMLS (
                        ZSTD_CCtx* zc,   /* Index table will be updated */
                        const BYTE* ip, const BYTE* const iLowLimit, const BYTE* const iHighLimit,
                        const U32 maxNbAttempts, const U32 matchLengthSearch, ZSTD_match_t* matches, U32 minml)
{
    (void)iLowLimit;  /* unused */
    switch(matchLengthSearch)
    {
    default :
    case 4 : return ZSTD_BtGetAllMatches(zc, ip, iHighLimit, maxNbAttempts, 4, matches, minml);
    case 5 : return ZSTD_BtGetAllMatches(zc, ip, iHighLimit, maxNbAttempts, 5, matches, minml);
    case 6 : return ZSTD_BtGetAllMatches(zc, ip, iHighLimit, maxNbAttempts, 6, matches, minml);
    }
}

/** Tree updater, providing best match */
static U32 ZSTD_BtGetAllMatches_extDict (
                        ZSTD_CCtx* zc,
                        const BYTE* const ip, const BYTE* const iLimit,
                        const U32 maxNbAttempts, const U32 mls, ZSTD_match_t* matches, U32 minml)
{
    if (ip < zc->base + zc->nextToUpdate) return 0;   /* skipped area */
    ZSTD_updateTree_extDict(zc, ip, iLimit, maxNbAttempts, mls);
    return ZSTD_insertBtAndGetAllMatches(zc, ip, iLimit, maxNbAttempts, mls, 1, matches, minml);
}


static U32 ZSTD_BtGetAllMatches_selectMLS_extDict (
                        ZSTD_CCtx* zc,   /* Index table will be updated */
                        const BYTE* ip, const BYTE* const iLowLimit, const BYTE* const iHighLimit,
                        const U32 maxNbAttempts, const U32 matchLengthSearch, ZSTD_match_t* matches, U32 minml)
{
    (void)iLowLimit;
    switch(matchLengthSearch)
    {
    default :
    case 4 : return ZSTD_BtGetAllMatches_extDict(zc, ip, iHighLimit, maxNbAttempts, 4, matches, minml);
    case 5 : return ZSTD_BtGetAllMatches_extDict(zc, ip, iHighLimit, maxNbAttempts, 5, matches, minml);
    case 6 : return ZSTD_BtGetAllMatches_extDict(zc, ip, iHighLimit, maxNbAttempts, 6, matches, minml);
    }
}


/* ***********************
*  Hash Chain
*************************/
FORCE_INLINE /* inlining is important to hardwire a hot branch (template emulation) */
U32 ZSTD_HcGetAllMatches_generic (
                        ZSTD_CCtx* zc,   /* Index table will be updated */
                        const BYTE* const ip, const BYTE* const iLowLimit, const BYTE* const iHighLimit,
                        const U32 maxNbAttempts, const U32 mls, const U32 extDict, ZSTD_match_t* matches, size_t minml)
{
    U32* const chainTable = zc->contentTable;
    const U32 chainSize = (1U << zc->params.contentLog);
    const U32 chainMask = chainSize-1;
    const BYTE* const base = zc->base;
    const BYTE* const dictBase = zc->dictBase;
    const U32 dictLimit = zc->dictLimit;
    const BYTE* const prefixStart = base + dictLimit;
    const BYTE* const dictEnd = dictBase + dictLimit;
    const BYTE* const dictStart  = dictBase + zc->lowLimit;
    const U32 lowLimit = zc->lowLimit;
    const U32 current = (U32)(ip-base);
    const U32 minChain = current > chainSize ? current - chainSize : 0;
    U32 matchIndex;
    U32 mnum = 0;
    const BYTE* match;
    U32 nbAttempts=maxNbAttempts;
    minml=MINMATCH-1;

    /* HC4 match finder */
    matchIndex = ZSTD_insertAndFindFirstIndex (zc, ip, mls);

    while ((matchIndex>lowLimit) && (nbAttempts)) {
        size_t currentMl=0;
        int back = 0;
        nbAttempts--;
        if ((!extDict) || matchIndex >= dictLimit) {
            match = base + matchIndex;
            if (match[minml] == ip[minml]) currentMl = ZSTD_count(ip, match, iHighLimit); if (currentMl>0) {   // faster
            //if (MEM_read32(match) == MEM_read32(ip)) { currentMl = ZSTD_count(ip+MINMATCH, match+MINMATCH, iHighLimit)+MINMATCH;  // stronger
                while ((match-back > prefixStart) && (ip-back > iLowLimit) && (ip[-back-1] == match[-back-1])) back++;
                currentMl += back;
            }
        } else {
            match = dictBase + matchIndex;
            if (MEM_read32(match) == MEM_read32(ip)) {   /* assumption : matchIndex <= dictLimit-4 (by table construction) */
                currentMl = ZSTD_count_2segments(ip+MINMATCH, match+MINMATCH, iHighLimit, dictEnd, prefixStart) + MINMATCH;
                while ((match-back > dictStart) && (ip-back > iLowLimit) && (ip[-back-1] == match[-back-1])) back++;   /* backward match extension */
                currentMl += back;
        }   }

        /* save best solution */
        if (currentMl > minml) {
            minml = currentMl;
            matches[mnum].off = current - matchIndex;
            matches[mnum].len = (U32)currentMl;
            matches[mnum].back = back;
            mnum++;
            if (currentMl > ZSTD_OPT_NUM) break;
            if (ip+currentMl == iHighLimit) break; /* best possible, and avoid read overflow*/
        }

        if (matchIndex <= minChain) break;
        matchIndex = NEXT_IN_CHAIN(matchIndex, chainMask);
    }

    return mnum;
}


static U32 ZSTD_HcGetAllMatches_selectMLS (
                        ZSTD_CCtx* zc,
                        const BYTE* ip, const BYTE* const iLowLimit, const BYTE* const iHighLimit,
                        const U32 maxNbAttempts, const U32 matchLengthSearch, ZSTD_match_t* matches, U32 minml)
{
    switch(matchLengthSearch)
    {
    default :
    case 4 : return ZSTD_HcGetAllMatches_generic(zc, ip, iLowLimit, iHighLimit, maxNbAttempts, 4, 0, matches, minml);
    case 5 : return ZSTD_HcGetAllMatches_generic(zc, ip, iLowLimit, iHighLimit, maxNbAttempts, 5, 0, matches, minml);
    case 6 : return ZSTD_HcGetAllMatches_generic(zc, ip, iLowLimit, iHighLimit, maxNbAttempts, 6, 0, matches, minml);
    }
}

static U32 ZSTD_HcGetAllMatches_selectMLS_extDict (
                        ZSTD_CCtx* zc,
                        const BYTE* ip, const BYTE* const iLowLimit, const BYTE* const iHighLimit,
                        const U32 maxNbAttempts, const U32 matchLengthSearch, ZSTD_match_t* matches, U32 minml)
{
    switch(matchLengthSearch)
    {
    default :
    case 4 : return ZSTD_HcGetAllMatches_generic(zc, ip, iLowLimit, iHighLimit, maxNbAttempts, 4, 1, matches, minml);
    case 5 : return ZSTD_HcGetAllMatches_generic(zc, ip, iLowLimit, iHighLimit, maxNbAttempts, 5, 1, matches, minml);
    case 6 : return ZSTD_HcGetAllMatches_generic(zc, ip, iLowLimit, iHighLimit, maxNbAttempts, 6, 1, matches, minml);
    }
}


/*-*******************************
*  Optimal parser
*********************************/
FORCE_INLINE
void ZSTD_compressBlock_opt_generic(ZSTD_CCtx* ctx,
                                    const void* src, size_t srcSize,
                                    const U32 searchMethod, const U32 depth)
{
    seqStore_t* seqStorePtr = &(ctx->seqStore);
    const BYTE* const istart = (const BYTE*)src;
    const BYTE* ip = istart;
    const BYTE* anchor = istart;
    const BYTE* const iend = istart + srcSize;
    const BYTE* const ilimit = iend - 8;
    const BYTE* const base = ctx->base + ctx->dictLimit;

    U32 rep_2=REPCODE_STARTVALUE, rep_1=REPCODE_STARTVALUE;
    const U32 maxSearches = 1U << ctx->params.searchLog;
    const U32 mls = ctx->params.searchLength;

    typedef U32 (*getAllMatches_f)(ZSTD_CCtx* zc, const BYTE* ip, const BYTE* iLowLimit, const BYTE* iHighLimit,
                        U32 maxNbAttempts, U32 matchLengthSearch, ZSTD_match_t* matches, U32 minml);
    getAllMatches_f getAllMatches = searchMethod ? ZSTD_BtGetAllMatches_selectMLS : ZSTD_HcGetAllMatches_selectMLS;

    ZSTD_optimal_t opt[ZSTD_OPT_NUM+4];
    ZSTD_match_t matches[ZSTD_OPT_NUM+1];
    const BYTE* inr;
    U32 skip_num, cur, cur2, match_num, last_pos, litlen, price;

    const U32 sufficient_len = ctx->params.targetLength;
    const U32 faster_get_matches = (ctx->params.strategy == ZSTD_opt);


    /* init */
    ZSTD_resetSeqStore(seqStorePtr);
    ZSTD_resetFreqs(seqStorePtr);
    if ((ip-base) < REPCODE_STARTVALUE) ip = base + REPCODE_STARTVALUE;

    /* Match Loop */
    while (ip < ilimit) {
        U32 u;
        U32 mlen=0;
        U32 best_mlen=0;
        U32 best_off=0;
        memset(opt, 0, sizeof(ZSTD_optimal_t));
        last_pos = 0;
        inr = ip;
        opt[0].litlen = (U32)(ip - anchor);

        /* check repCode */
        if (MEM_read32(ip+1) == MEM_read32(ip+1 - rep_1)) {
            /* repcode : we take it */
            mlen = (U32)ZSTD_count(ip+1+MINMATCH, ip+1+MINMATCH-rep_1, iend) + MINMATCH;

            ZSTD_LOG_PARSER("%d: start try REP rep=%d mlen=%d\n", (int)(ip-base), (int)rep_1, (int)mlen);
            if (depth==0 || mlen > sufficient_len || mlen >= ZSTD_OPT_NUM) {
                ip+=1; best_mlen = mlen; best_off = 0; cur = 0; last_pos = 1;
                goto _storeSequence;
            }

            litlen = opt[0].litlen + 1;
            do {
                price = ZSTD_getPrice(seqStorePtr, litlen, anchor, 0, mlen - MINMATCH);
                if (mlen + 1 > last_pos || price < opt[mlen + 1].price)
                    SET_PRICE(mlen + 1, mlen, 0, litlen, price);   /* note : macro modifies last_pos */
                mlen--;
            } while (mlen >= MINMATCH);
        }

        best_mlen = (last_pos) ? last_pos : MINMATCH;

        if (faster_get_matches && last_pos)
           match_num = 0;
        else
           match_num = getAllMatches(ctx, ip, ip, iend, maxSearches, mls, matches, best_mlen); /* first search (depth 0) */

        ZSTD_LOG_PARSER("%d: match_num=%d last_pos=%d\n", (int)(ip-base), match_num, last_pos);
        if (!last_pos && !match_num) { ip++; continue; }

        opt[0].rep = rep_1;
        opt[0].rep2 = rep_2;
        opt[0].mlen = 1;

        if (match_num && matches[match_num-1].len > sufficient_len) {
            best_mlen = matches[match_num-1].len;
            best_off = matches[match_num-1].off;
            cur = 0;
            last_pos = 1;
            goto _storeSequence;
        }

       // set prices using matches at position = 0
       for (u = 0; u < match_num; u++) {
           mlen = (u>0) ? matches[u-1].len+1 : best_mlen;
           best_mlen = (matches[u].len < ZSTD_OPT_NUM) ? matches[u].len : ZSTD_OPT_NUM;
           ZSTD_LOG_PARSER("%d: start Found mlen=%d off=%d best_mlen=%d last_pos=%d\n", (int)(ip-base), matches[u].len, matches[u].off, (int)best_mlen, (int)last_pos);
           litlen = opt[0].litlen;
           while (mlen <= best_mlen) {
                price = ZSTD_getPrice(seqStorePtr, litlen, anchor, matches[u].off, mlen - MINMATCH);
                if (mlen > last_pos || price < opt[mlen].price)
                    SET_PRICE(mlen, mlen, matches[u].off, litlen, price);
                mlen++;
        }  }

        if (last_pos < MINMATCH) { ip++; continue; }

         /* check further positions */
        for (skip_num = 0, cur = 1; cur <= last_pos; cur++) {
           size_t cur_rep;
           inr = ip + cur;

           if (opt[cur-1].mlen == 1) {
                litlen = opt[cur-1].litlen + 1;
                if (cur > litlen) {
                    price = opt[cur - litlen].price + ZSTD_getLiteralPrice(seqStorePtr, litlen, inr-litlen);
                    ZSTD_LOG_TRY_PRICE("%d: TRY1 opt[%d].price=%d price=%d cur=%d litlen=%d\n", (int)(inr-base), cur - litlen, opt[cur - litlen].price, price, cur, litlen);
                } else
                    price = ZSTD_getLiteralPrice(seqStorePtr, litlen, anchor);
           } else {
                litlen = 1;
                price = opt[cur - 1].price + ZSTD_getLiteralPrice(seqStorePtr, litlen, inr-1);
                ZSTD_LOG_TRY_PRICE("%d: TRY3 price=%d cur=%d litlen=%d litonly=%d\n", (int)(inr-base), price, cur, litlen, (int)ZSTD_getLiteralPrice(seqStorePtr, litlen, inr-1));
           }

           ZSTD_LOG_TRY_PRICE("%d: TRY4 price=%d opt[%d].price=%d\n", (int)(inr-base), price, cur, opt[cur].price);

           if (cur > last_pos || price <= opt[cur].price) // || ((price == opt[cur].price) && (opt[cur-1].mlen == 1) && (cur != litlen)))
                SET_PRICE(cur, 1, 0, litlen, price);

           if (cur == last_pos) break;

           if (inr > ilimit)  /* last match must start at a minimum distance of 8 from oend */
               continue;

            mlen = opt[cur].mlen;

            if (opt[cur-mlen].off) {
                opt[cur].rep2 = opt[cur-mlen].rep;
                opt[cur].rep = opt[cur-mlen].off;
                ZSTD_LOG_PARSER("%d: COPYREP1 cur=%d mlen=%d rep=%d rep2=%d\n", (int)(inr-base), cur, mlen, opt[cur].rep, opt[cur].rep2);
            } else {
                if (cur!=mlen && opt[cur-mlen].litlen == 0) {
                    opt[cur].rep2 = opt[cur-mlen].rep;
                    opt[cur].rep = opt[cur-mlen].rep2;
                    ZSTD_LOG_PARSER("%d: COPYREP2 cur=%d mlen=%d rep=%d rep2=%d\n", (int)(inr-base), cur, mlen, opt[cur].rep, opt[cur].rep2);
                } else {
                    opt[cur].rep2 = opt[cur-mlen].rep2;
                    opt[cur].rep = opt[cur-mlen].rep;
                    ZSTD_LOG_PARSER("%d: COPYREP3 cur=%d mlen=%d rep=%d rep2=%d\n", (int)(inr-base), cur, mlen, opt[cur].rep, opt[cur].rep2);
            }   }

           ZSTD_LOG_PARSER("%d: CURRENT price[%d/%d]=%d off=%d mlen=%d litlen=%d rep=%d rep2=%d\n", (int)(inr-base), cur, last_pos, opt[cur].price, opt[cur].off, opt[cur].mlen, opt[cur].litlen, opt[cur].rep, opt[cur].rep2);

           best_mlen = 0;

           if (!opt[cur].off && opt[cur].mlen != 1) {
               cur_rep = opt[cur].rep2;
               ZSTD_LOG_PARSER("%d: try REP2 rep2=%u mlen=%u\n", (int)(inr-base), (U32)cur_rep, mlen);
           } else {
               cur_rep = opt[cur].rep;
               ZSTD_LOG_PARSER("%d: try REP1 rep=%u mlen=%u\n", (int)(inr-base), (U32)cur_rep, mlen);
           }

           if (MEM_read32(inr) == MEM_read32(inr - cur_rep)) {  // check rep
               mlen = (U32)ZSTD_count(inr+MINMATCH, inr+MINMATCH - cur_rep, iend) + MINMATCH;
               ZSTD_LOG_PARSER("%d: Found REP mlen=%d off=%d rep=%d opt[%d].off=%d\n", (int)(inr-base), mlen, 0, opt[cur].rep, cur, opt[cur].off);

               if (mlen > sufficient_len || cur + mlen >= ZSTD_OPT_NUM) {
                    best_mlen = mlen;
                    best_off = 0;
                    ZSTD_LOG_PARSER("%d: REP sufficient_len=%d best_mlen=%d best_off=%d last_pos=%d\n", (int)(inr-base), sufficient_len, best_mlen, best_off, last_pos);
                    last_pos = cur + 1;
                    goto _storeSequence;
               }

               if (opt[cur].mlen == 1) {
                    litlen = opt[cur].litlen;
                    if (cur > litlen) {
                        price = opt[cur - litlen].price + ZSTD_getPrice(seqStorePtr, litlen, inr-litlen, 0, mlen - MINMATCH);
                        ZSTD_LOG_TRY_PRICE("%d: TRY5 opt[%d].price=%d price=%d cur=%d litlen=%d\n", (int)(inr-base), cur - litlen, opt[cur - litlen].price, price, cur, litlen);
                    } else
                        price = ZSTD_getPrice(seqStorePtr, litlen, anchor, 0, mlen - MINMATCH);
                } else {
                    litlen = 0;
                    price = opt[cur].price + ZSTD_getPrice(seqStorePtr, 0, NULL, 0, mlen - MINMATCH);
                    ZSTD_LOG_TRY_PRICE("%d: TRY7 price=%d cur=%d litlen=0 getprice=%d\n", (int)(inr-base), price, cur, (int)ZSTD_getPrice(seqStorePtr, 0, NULL, 0, mlen - MINMATCH));
                }

                best_mlen = mlen;
                if (faster_get_matches) skip_num = best_mlen;
                ZSTD_LOG_PARSER("%d: Found REP mlen=%d off=%d price=%d litlen=%d price[%d]=%d\n", (int)(inr-base), mlen, 0, price, litlen, cur - litlen, opt[cur - litlen].price);

                do {
                    if (cur + mlen > last_pos || price <= opt[cur + mlen].price)
                        SET_PRICE(cur + mlen, mlen, 0, litlen, price);
                    mlen--;
                } while (mlen >= MINMATCH);
            }

            if (faster_get_matches && skip_num > 0) { skip_num--; continue; }

            best_mlen = (best_mlen > MINMATCH) ? best_mlen : MINMATCH;

            match_num = getAllMatches(ctx, inr, ip, iend, maxSearches, mls, matches, best_mlen);
            ZSTD_LOG_PARSER("%d: ZSTD_GetAllMatches match_num=%d\n", (int)(inr-base), match_num);

            if (match_num > 0 && matches[match_num-1].len > sufficient_len) {
                cur -= matches[match_num-1].back;
                best_mlen = matches[match_num-1].len;
                best_off = matches[match_num-1].off;
                last_pos = cur + 1;
                goto _storeSequence;
            }

            /* set prices using matches at position = cur */
            for (u = 0; u < match_num; u++) {
                mlen = (u>0) ? matches[u-1].len+1 : best_mlen;
                cur2 = cur - matches[u].back;
                best_mlen = (cur2 + matches[u].len < ZSTD_OPT_NUM) ? matches[u].len : ZSTD_OPT_NUM - cur2;

                ZSTD_LOG_PARSER("%d: Found1 cur=%d cur2=%d mlen=%d off=%d best_mlen=%d last_pos=%d\n", (int)(inr-base), cur, cur2, matches[u].len, matches[u].off, best_mlen, last_pos);
                if (mlen < matches[u].back + 1)
                    mlen = matches[u].back + 1;

                while (mlen <= best_mlen) {
                    if (opt[cur2].mlen == 1) {
                        litlen = opt[cur2].litlen;
                        if (cur2 > litlen)
                            price = opt[cur2 - litlen].price + ZSTD_getPrice(seqStorePtr, litlen, ip+cur2-litlen, matches[u].off, mlen - MINMATCH);
                        else
                            price = ZSTD_getPrice(seqStorePtr, litlen, anchor, matches[u].off, mlen - MINMATCH);
                    } else {
                        litlen = 0;
                        price = opt[cur2].price + ZSTD_getPrice(seqStorePtr, 0, NULL, matches[u].off, mlen - MINMATCH);
                    }

                    ZSTD_LOG_PARSER("%d: Found2 pred=%d mlen=%d best_mlen=%d off=%d price=%d litlen=%d price[%d]=%d\n", (int)(inr-base), matches[u].back, mlen, best_mlen, matches[u].off, price, litlen, cur - litlen, opt[cur - litlen].price);
                    ZSTD_LOG_TRY_PRICE("%d: TRY8 price=%d opt[%d].price=%d\n", (int)(inr-base), price, cur2 + mlen, opt[cur2 + mlen].price);

                    if (cur2 + mlen > last_pos || (price < opt[cur2 + mlen].price))
                        SET_PRICE(cur2 + mlen, mlen, matches[u].off, litlen, price);

                    mlen++;
        }   }   }   //  for (skip_num = 0, cur = 1; cur <= last_pos; cur++)

        best_mlen = opt[last_pos].mlen;
        best_off = opt[last_pos].off;
        cur = last_pos - best_mlen;
        // printf("%d: start=%d best_mlen=%d best_off=%d cur=%d\n", (int)(ip - base), (int)(start - ip), (int)best_mlen, (int)best_off, cur);

        /* store sequence */
_storeSequence:   /* cur, last_pos, best_mlen, best_off have to be set */
        for (u = 1; u <= last_pos; u++)
            ZSTD_LOG_PARSER("%d: price[%d/%d]=%d off=%d mlen=%d litlen=%d rep=%d rep2=%d\n", (int)(ip-base+u), u, last_pos, opt[u].price, opt[u].off, opt[u].mlen, opt[u].litlen, opt[u].rep, opt[u].rep2);
        ZSTD_LOG_PARSER("%d: cur=%d/%d best_mlen=%d best_off=%d rep=%d\n", (int)(ip-base+cur), (int)cur, (int)last_pos, (int)best_mlen, (int)best_off, opt[cur].rep);

        opt[0].mlen = 1;
        U32 offset;

        while (1) {
            mlen = opt[cur].mlen;
            ZSTD_LOG_PARSER("%d: cur=%d mlen=%d\n", (int)(ip-base), cur, mlen);
            offset = opt[cur].off;
            opt[cur].mlen = best_mlen;
            opt[cur].off = best_off;
            best_mlen = mlen;
            best_off = offset;
            if (mlen > cur) break;
            cur -= mlen;
        }

        for (u = 0; u <= last_pos;) {
            ZSTD_LOG_PARSER("%d: price2[%d/%d]=%d off=%d mlen=%d litlen=%d rep=%d rep2=%d\n", (int)(ip-base+u), u, last_pos, opt[u].price, opt[u].off, opt[u].mlen, opt[u].litlen, opt[u].rep, opt[u].rep2);
            u += opt[u].mlen;
        }

        for (cur=0; cur < last_pos; ) {
            ZSTD_LOG_PARSER("%d: price3[%d/%d]=%d off=%d mlen=%d litlen=%d rep=%d rep2=%d\n", (int)(ip-base+cur), cur, last_pos, opt[cur].price, opt[cur].off, opt[cur].mlen, opt[cur].litlen, opt[cur].rep, opt[cur].rep2);
            mlen = opt[cur].mlen;
            if (mlen == 1) { ip++; cur++; continue; }
            offset = opt[cur].off;
            cur += mlen;

            U32 litLength = (U32)(ip - anchor);
            ZSTD_LOG_ENCODE("%d/%d: ENCODE1 literals=%d mlen=%d off=%d rep1=%d rep2=%d\n", (int)(ip-base), (int)(iend-base), (int)(litLength), (int)mlen, (int)(offset), (int)rep_1, (int)rep_2);

            if (offset) {
                rep_2 = rep_1;
                rep_1 = offset;
            } else {
                if (litLength == 0) {
                    best_off = rep_2;
                    rep_2 = rep_1;
                    rep_1 = best_off;
            }   }

            ZSTD_LOG_ENCODE("%d/%d: ENCODE2 literals=%d mlen=%d off=%d rep1=%d rep2=%d\n", (int)(ip-base), (int)(iend-base), (int)(litLength), (int)mlen, (int)(offset), (int)rep_1, (int)rep_2);

#if ZSTD_OPT_DEBUG >= 5
            int ml2;
            if (offset)
                ml2 = ZSTD_count(ip, ip-offset, iend);
            else
                ml2 = ZSTD_count(ip, ip-rep_1, iend);
            if (ml2 < mlen && ml2 < MINMATCH) {
                printf("%d: ERROR iend=%d mlen=%d offset=%d ml2=%d\n", (int)(ip - base), (int)(iend - ip), (int)mlen, (int)offset, (int)ml2); exit(0); }
            if (ip < anchor) {
                printf("%d: ERROR ip < anchor iend=%d mlen=%d offset=%d\n", (int)(ip - base), (int)(iend - ip), (int)mlen, (int)offset); exit(0); }
            if (ip - offset < ctx->base) {
                printf("%d: ERROR ip - offset < base iend=%d mlen=%d offset=%d\n", (int)(ip - base), (int)(iend - ip), (int)mlen, (int)offset); exit(0); }
            if ((int)offset >= (1 << ctx->params.windowLog)) {
                printf("%d: offset >= (1 << params.windowLog) iend=%d mlen=%d offset=%d\n", (int)(ip - base), (int)(iend - ip), (int)mlen, (int)offset); exit(0); }
            if (mlen < MINMATCH) {
                printf("%d: ERROR mlen < MINMATCH iend=%d mlen=%d offset=%d\n", (int)(ip - base), (int)(iend - ip), (int)mlen, (int)offset); exit(0); }
            if (ip + mlen > iend) {
                printf("%d: ERROR ip + mlen >= iend iend=%d mlen=%d offset=%d\n", (int)(ip - base), (int)(iend - ip), (int)mlen, (int)offset); exit(0); }
#endif

            ZSTD_updatePrice(seqStorePtr, litLength, anchor, offset, mlen-MINMATCH);
            ZSTD_storeSeq(seqStorePtr, litLength, anchor, offset, mlen-MINMATCH);
            anchor = ip = ip + mlen;
        }   /* for (cur=0; cur < last_pos; ) */

        /* check immediate repcode */
        while ( (anchor <= ilimit)
             && (MEM_read32(anchor) == MEM_read32(anchor - rep_2)) ) {
            /* store sequence */
            best_mlen = (U32)ZSTD_count(anchor+MINMATCH, anchor+MINMATCH-rep_2, iend);
            best_off = rep_2;
            rep_2 = rep_1;
            rep_1 = best_off;
            ZSTD_LOG_ENCODE("%d/%d: ENCODE REP literals=%d mlen=%d off=%d rep1=%d rep2=%d\n", (int)(anchor-base), (int)(iend-base), (int)(0), (int)best_mlen, (int)(0), (int)rep_1, (int)rep_2);
            ZSTD_updatePrice(seqStorePtr, 0, anchor, 0, best_mlen);
            ZSTD_storeSeq(seqStorePtr, 0, anchor, 0, best_mlen);
            anchor += best_mlen+MINMATCH;
            continue;   /* faster when present ... (?) */
        }
        if (anchor > ip) ip = anchor;
    }

    {   /* Last Literals */
        size_t lastLLSize = iend - anchor;
        ZSTD_LOG_ENCODE("%d: lastLLSize literals=%u\n", (int)(ip-base), (U32)lastLLSize);
        memcpy(seqStorePtr->lit, anchor, lastLLSize);
        seqStorePtr->lit += lastLLSize;
    }
}


FORCE_INLINE
void ZSTD_compressBlock_opt_extDict_generic(ZSTD_CCtx* ctx,
                                     const void* src, size_t srcSize,
                                     const U32 searchMethod, const U32 depth)
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

    U32 rep_2=REPCODE_STARTVALUE, rep_1=REPCODE_STARTVALUE;
    const U32 maxSearches = 1U << ctx->params.searchLog;
    const U32 mls = ctx->params.searchLength;

    typedef U32 (*getAllMatches_f)(ZSTD_CCtx* zc, const BYTE* ip, const BYTE* iLowLimit, const BYTE* iHighLimit,
                        U32 maxNbAttempts, U32 matchLengthSearch, ZSTD_match_t* matches, U32 minml);
    getAllMatches_f getAllMatches = searchMethod ? ZSTD_BtGetAllMatches_selectMLS_extDict : ZSTD_HcGetAllMatches_selectMLS_extDict;

    ZSTD_optimal_t opt[ZSTD_OPT_NUM+4];
    ZSTD_match_t matches[ZSTD_OPT_NUM+1];
    const BYTE* inr;
    U32 skip_num, cur, cur2, match_num, last_pos, litlen, price;

    const U32 sufficient_len = ctx->params.targetLength;
    const U32 faster_get_matches = (ctx->params.strategy == ZSTD_opt);

    /* init */
    ZSTD_resetSeqStore(seqStorePtr);
    ZSTD_resetFreqs(seqStorePtr);
    if ((ip - prefixStart) < REPCODE_STARTVALUE) ip += REPCODE_STARTVALUE;

    /* Match Loop */
    while (ip < ilimit) {
        U32 u, offset, best_off=0;
        U32 mlen=0, best_mlen=0;
        U32 current = (U32)(ip-base);
        memset(opt, 0, sizeof(ZSTD_optimal_t));
        last_pos = 0;
        inr = ip;
        opt[0].litlen = (U32)(ip - anchor);

        /* check repCode */
        {
            const U32 repIndex = (U32)(current+1 - rep_1);
            const BYTE* const repBase = repIndex < dictLimit ? dictBase : base;
            const BYTE* const repMatch = repBase + repIndex;
            if ( ((U32)((dictLimit-1) - repIndex) >= 3)   /* intentional overflow */
               && (MEM_read32(ip+1) == MEM_read32(repMatch)) ) {
                /* repcode detected we should take it */
                const BYTE* const repEnd = repIndex < dictLimit ? dictEnd : iend;
                mlen = (U32)ZSTD_count_2segments(ip+1+MINMATCH, repMatch+MINMATCH, iend, repEnd, prefixStart) + MINMATCH;

                ZSTD_LOG_PARSER("%d: start try REP rep=%d mlen=%d\n", (int)(ip-base), (int)rep_1, (int)mlen);
                if (depth==0 || mlen > sufficient_len || mlen >= ZSTD_OPT_NUM) {
                    ip+=1; best_mlen = mlen; best_off = 0; cur = 0; last_pos = 1;
                    goto _storeSequence;
                }

                litlen = opt[0].litlen + 1;
                do {
                    price = ZSTD_getPrice(seqStorePtr, litlen, anchor, 0, mlen - MINMATCH);
                    if (mlen + 1 > last_pos || price < opt[mlen + 1].price)
                        SET_PRICE(mlen + 1, mlen, 0, litlen, price);
                    mlen--;
                } while (mlen >= MINMATCH);
        }   }

       best_mlen = (last_pos) ? last_pos : MINMATCH;

       if (faster_get_matches && last_pos)
           match_num = 0;
       else
           match_num = getAllMatches(ctx, ip, ip, iend, maxSearches, mls, matches, best_mlen);  /* first search (depth 0) */

       ZSTD_LOG_PARSER("%d: match_num=%d last_pos=%d\n", (int)(ip-base), match_num, last_pos);
       if (!last_pos && !match_num) { ip++; continue; }

       opt[0].rep = rep_1;
       opt[0].rep2 = rep_2;
       opt[0].mlen = 1;

       if (match_num && matches[match_num-1].len > sufficient_len) {
            best_mlen = matches[match_num-1].len;
            best_off = matches[match_num-1].off;
            cur = 0;
            last_pos = 1;
            goto _storeSequence;
       }

        // set prices using matches at position = 0
        for (u = 0; u < match_num; u++) {
            mlen = (u>0) ? matches[u-1].len+1 : best_mlen;
            best_mlen = (matches[u].len < ZSTD_OPT_NUM) ? matches[u].len : ZSTD_OPT_NUM;
            ZSTD_LOG_PARSER("%d: start Found mlen=%d off=%d best_mlen=%d last_pos=%d\n", (int)(ip-base), matches[u].len, matches[u].off, (int)best_mlen, (int)last_pos);
            litlen = opt[0].litlen;
            while (mlen <= best_mlen) {
                price = ZSTD_getPrice(seqStorePtr, litlen, anchor, matches[u].off, mlen - MINMATCH);
                if (mlen > last_pos || price < opt[mlen].price)
                    SET_PRICE(mlen, mlen, matches[u].off, litlen, price);
                mlen++;
        }   }

        if (last_pos < MINMATCH) {
            // ip += ((ip-anchor) >> g_searchStrength) + 1;   /* jump faster over incompressible sections */
            ip++; continue;
        }

        /* check further positions */
        for (skip_num = 0, cur = 1; cur <= last_pos; cur++) {
           size_t cur_rep;
           inr = ip + cur;

           if (opt[cur-1].mlen == 1) {
                litlen = opt[cur-1].litlen + 1;
                if (cur > litlen) {
                    price = opt[cur - litlen].price + ZSTD_getLiteralPrice(seqStorePtr, litlen, inr-litlen);
                    ZSTD_LOG_TRY_PRICE("%d: TRY1 opt[%d].price=%d price=%d cur=%d litlen=%d\n", (int)(inr-base), cur - litlen, opt[cur - litlen].price, price, cur, litlen);
                } else
                    price = ZSTD_getLiteralPrice(seqStorePtr, litlen, anchor);
           } else {
                litlen = 1;
                price = opt[cur - 1].price + ZSTD_getLiteralPrice(seqStorePtr, litlen, inr-1);
                ZSTD_LOG_TRY_PRICE("%d: TRY3 price=%d cur=%d litlen=%d litonly=%d\n", (int)(inr-base), price, cur, litlen, (int)ZSTD_getLiteralPrice(seqStorePtr, litlen, inr-1));
           }

           ZSTD_LOG_TRY_PRICE("%d: TRY4 price=%d opt[%d].price=%d\n", (int)(inr-base), price, cur, opt[cur].price);

           if (cur > last_pos || price <= opt[cur].price) // || ((price == opt[cur].price) && (opt[cur-1].mlen == 1) && (cur != litlen)))
                SET_PRICE(cur, 1, 0, litlen, price);

           if (cur == last_pos) break;

           if (inr > ilimit) // last match must start at a minimum distance of 8 from oend
               continue;

            mlen = opt[cur].mlen;

            if (opt[cur-mlen].off) {
                opt[cur].rep2 = opt[cur-mlen].rep;
                opt[cur].rep = opt[cur-mlen].off;
                ZSTD_LOG_PARSER("%d: COPYREP1 cur=%d mlen=%d rep=%d rep2=%d\n", (int)(inr-base), cur, mlen, opt[cur].rep, opt[cur].rep2);
            } else {
                if (cur!=mlen && opt[cur-mlen].litlen == 0) {
                    opt[cur].rep2 = opt[cur-mlen].rep;
                    opt[cur].rep = opt[cur-mlen].rep2;
                    ZSTD_LOG_PARSER("%d: COPYREP2 cur=%d mlen=%d rep=%d rep2=%d\n", (int)(inr-base), cur, mlen, opt[cur].rep, opt[cur].rep2);
                } else {
                    opt[cur].rep2 = opt[cur-mlen].rep2;
                    opt[cur].rep = opt[cur-mlen].rep;
                    ZSTD_LOG_PARSER("%d: COPYREP3 cur=%d mlen=%d rep=%d rep2=%d\n", (int)(inr-base), cur, mlen, opt[cur].rep, opt[cur].rep2);
            }   }

           ZSTD_LOG_PARSER("%d: CURRENT price[%d/%d]=%d off=%d mlen=%d litlen=%d rep=%d rep2=%d\n", (int)(inr-base), cur, last_pos, opt[cur].price, opt[cur].off, opt[cur].mlen, opt[cur].litlen, opt[cur].rep, opt[cur].rep2);

           best_mlen = 0;

           if (!opt[cur].off && opt[cur].mlen != 1) {
               cur_rep = opt[cur].rep2;
               ZSTD_LOG_PARSER("%d: try REP2 rep2=%u mlen=%u\n", (int)(inr-base), (U32)cur_rep, mlen);
           } else {
               cur_rep = opt[cur].rep;
               ZSTD_LOG_PARSER("%d: try REP1 rep=%u mlen=%u\n", (int)(inr-base), (U32)cur_rep, mlen);
           }

           const U32 repIndex = (U32)(current+cur - cur_rep);
           const BYTE* const repBase = repIndex < dictLimit ? dictBase : base;
           const BYTE* const repMatch = repBase + repIndex;
           if ( ((U32)((dictLimit-1) - repIndex) >= 3)   /* intentional overflow */
              &&(MEM_read32(inr) == MEM_read32(repMatch)) ) {
                /* repcode detected */
                const BYTE* const repEnd = repIndex < dictLimit ? dictEnd : iend;
                mlen = (U32)ZSTD_count_2segments(inr+MINMATCH, repMatch+MINMATCH, iend, repEnd, prefixStart) + MINMATCH;
                ZSTD_LOG_PARSER("%d: Found REP mlen=%d off=%d rep=%d opt[%d].off=%d\n", (int)(inr-base), mlen, 0, opt[cur].rep, cur, opt[cur].off);

                if (mlen > sufficient_len || cur + mlen >= ZSTD_OPT_NUM) {
                    best_mlen = mlen;
                    best_off = 0;
                    ZSTD_LOG_PARSER("%d: REP sufficient_len=%d best_mlen=%d best_off=%d last_pos=%d\n", (int)(inr-base), sufficient_len, best_mlen, best_off, last_pos);
                    last_pos = cur + 1;
                    goto _storeSequence;
                }

                if (opt[cur].mlen == 1) {
                    litlen = opt[cur].litlen;
                    if (cur > litlen) {
                        price = opt[cur - litlen].price + ZSTD_getPrice(seqStorePtr, litlen, inr-litlen, 0, mlen - MINMATCH);
                        ZSTD_LOG_TRY_PRICE("%d: TRY5 opt[%d].price=%d price=%d cur=%d litlen=%d\n", (int)(inr-base), cur - litlen, opt[cur - litlen].price, price, cur, litlen);
                    } else
                        price = ZSTD_getPrice(seqStorePtr, litlen, anchor, 0, mlen - MINMATCH);
                } else {
                    litlen = 0;
                    price = opt[cur].price + ZSTD_getPrice(seqStorePtr, 0, NULL, 0, mlen - MINMATCH);
                    ZSTD_LOG_TRY_PRICE("%d: TRY7 price=%d cur=%d litlen=0 getprice=%d\n", (int)(inr-base), price, cur, (int)ZSTD_getPrice(seqStorePtr, 0, NULL, 0, mlen - MINMATCH));
                }

                best_mlen = mlen;
                if (faster_get_matches) skip_num = best_mlen;

                ZSTD_LOG_PARSER("%d: Found REP mlen=%d off=%d price=%d litlen=%d price[%d]=%d\n", (int)(inr-base), mlen, 0, price, litlen, cur - litlen, opt[cur - litlen].price);

                do {
                    if (cur + mlen > last_pos || price <= opt[cur + mlen].price) // || ((price == opt[cur + mlen].price) && (opt[cur].mlen == 1) && (cur != litlen))) // at equal price prefer REP instead of MATCH
                        SET_PRICE(cur + mlen, mlen, 0, litlen, price);
                    mlen--;
                } while (mlen >= MINMATCH);
            }

            if (faster_get_matches && skip_num > 0) { skip_num--; continue; }

            best_mlen = (best_mlen > MINMATCH) ? best_mlen : MINMATCH;

            match_num = getAllMatches(ctx, inr, ip, iend, maxSearches, mls, matches, best_mlen);
            ZSTD_LOG_PARSER("%d: ZSTD_GetAllMatches match_num=%d\n", (int)(inr-base), match_num);

            if (match_num > 0 && matches[match_num-1].len > sufficient_len) {
                cur -= matches[match_num-1].back;
                best_mlen = matches[match_num-1].len;
                best_off = matches[match_num-1].off;
                last_pos = cur + 1;
                goto _storeSequence;
            }

            // set prices using matches at position = cur
            for (u = 0; u < match_num; u++) {
                mlen = (u>0) ? matches[u-1].len+1 : best_mlen;
                cur2 = cur - matches[u].back;
                best_mlen = (cur2 + matches[u].len < ZSTD_OPT_NUM) ? matches[u].len : ZSTD_OPT_NUM - cur2;

                ZSTD_LOG_PARSER("%d: Found1 cur=%d cur2=%d mlen=%d off=%d best_mlen=%d last_pos=%d\n", (int)(inr-base), cur, cur2, matches[u].len, matches[u].off, best_mlen, last_pos);
                if (mlen < matches[u].back + 1)
                    mlen = matches[u].back + 1;

                while (mlen <= best_mlen) {
                    if (opt[cur2].mlen == 1) {
                        litlen = opt[cur2].litlen;
                        if (cur2 > litlen)
                            price = opt[cur2 - litlen].price + ZSTD_getPrice(seqStorePtr, litlen, ip+cur2-litlen, matches[u].off, mlen - MINMATCH);
                        else
                            price = ZSTD_getPrice(seqStorePtr, litlen, anchor, matches[u].off, mlen - MINMATCH);
                    } else {
                        litlen = 0;
                        price = opt[cur2].price + ZSTD_getPrice(seqStorePtr, 0, NULL, matches[u].off, mlen - MINMATCH);
                    }

                    ZSTD_LOG_PARSER("%d: Found2 pred=%d mlen=%d best_mlen=%d off=%d price=%d litlen=%d price[%d]=%d\n", (int)(inr-base), matches[u].back, mlen, best_mlen, matches[u].off, price, litlen, cur - litlen, opt[cur - litlen].price);
                    ZSTD_LOG_TRY_PRICE("%d: TRY8 price=%d opt[%d].price=%d\n", (int)(inr-base), price, cur2 + mlen, opt[cur2 + mlen].price);

                    if (cur2 + mlen > last_pos || (price < opt[cur2 + mlen].price))
                        SET_PRICE(cur2 + mlen, mlen, matches[u].off, litlen, price);

                    mlen++;
        }   }   }   //  for (skip_num = 0, cur = 1; cur <= last_pos; cur++)

        best_mlen = opt[last_pos].mlen;
        best_off = opt[last_pos].off;
        cur = last_pos - best_mlen;
        // printf("%d: start=%d best_mlen=%d best_off=%d cur=%d\n", (int)(ip - base), (int)(start - ip), (int)best_mlen, (int)best_off, cur);

        /* store sequence */
_storeSequence: // cur, last_pos, best_mlen, best_off have to be set
        for (u = 1; u <= last_pos; u++)
            ZSTD_LOG_PARSER("%d: price[%u/%d]=%d off=%d mlen=%d litlen=%d rep=%d rep2=%d\n", (int)(ip-base+u), u, last_pos, opt[u].price, opt[u].off, opt[u].mlen, opt[u].litlen, opt[u].rep, opt[u].rep2);
        ZSTD_LOG_PARSER("%d: cur=%d/%d best_mlen=%d best_off=%d rep=%d\n", (int)(ip-base+cur), (int)cur, (int)last_pos, (int)best_mlen, (int)best_off, opt[cur].rep);

        opt[0].mlen = 1;

        while (1) {
            mlen = opt[cur].mlen;
            ZSTD_LOG_PARSER("%d: cur=%d mlen=%d\n", (int)(ip-base), cur, mlen);
            offset = opt[cur].off;
            opt[cur].mlen = best_mlen;
            opt[cur].off = best_off;
            best_mlen = mlen;
            best_off = offset;
            if (mlen > cur) break;
            cur -= mlen;
        }

        for (u = 0; u <= last_pos; ) {
            ZSTD_LOG_PARSER("%d: price2[%d/%d]=%d off=%d mlen=%d litlen=%d rep=%d rep2=%d\n", (int)(ip-base+u), u, last_pos, opt[u].price, opt[u].off, opt[u].mlen, opt[u].litlen, opt[u].rep, opt[u].rep2);
            u += opt[u].mlen;
        }

        for (cur=0; cur < last_pos; ) {
            U32 litLength;
            ZSTD_LOG_PARSER("%d: price3[%d/%d]=%d off=%d mlen=%d litlen=%d rep=%d rep2=%d\n", (int)(ip-base+cur), cur, last_pos, opt[cur].price, opt[cur].off, opt[cur].mlen, opt[cur].litlen, opt[cur].rep, opt[cur].rep2);
            mlen = opt[cur].mlen;
            if (mlen == 1) { ip++; cur++; continue; }
            offset = opt[cur].off;
            cur += mlen;

            litLength = (U32)(ip - anchor);
            ZSTD_LOG_ENCODE("%d/%d: ENCODE1 literals=%d mlen=%d off=%d rep1=%d rep2=%d\n", (int)(ip-base), (int)(iend-base), (int)(litLength), (int)mlen, (int)(offset), (int)rep_1, (int)rep_2);

            if (offset) {
                rep_2 = rep_1;
                rep_1 = offset;
            } else {
                if (litLength == 0) {
                    best_off = rep_2;
                    rep_2 = rep_1;
                    rep_1 = best_off;
            }   }

            ZSTD_LOG_ENCODE("%d/%d: ENCODE2 literals=%d mlen=%d off=%d rep1=%d rep2=%d\n", (int)(ip-base), (int)(iend-base), (int)(litLength), (int)mlen, (int)(offset), (int)rep_1, (int)rep_2);

#if ZSTD_OPT_DEBUG >= 5
            int ml2;
            if (offset)
                ml2 = ZSTD_count(ip, ip-offset, iend);
            else
                ml2 = ZSTD_count(ip, ip-rep_1, iend);
            if (ml2 < mlen && ml2 < MINMATCH) {
                printf("%d: ERROR iend=%d mlen=%d offset=%d ml2=%d\n", (int)(ip - base), (int)(iend - ip), (int)mlen, (int)offset, (int)ml2); exit(0); }
            if (ip < anchor) {
                printf("%d: ERROR ip < anchor iend=%d mlen=%d offset=%d\n", (int)(ip - base), (int)(iend - ip), (int)mlen, (int)offset); exit(0); }
            if (ip - offset < ctx->base) {
                printf("%d: ERROR ip - offset < base iend=%d mlen=%d offset=%d\n", (int)(ip - base), (int)(iend - ip), (int)mlen, (int)offset); exit(0); }
            if ((int)offset >= (1 << ctx->params.windowLog)) {
                printf("%d: offset >= (1 << params.windowLog) iend=%d mlen=%d offset=%d\n", (int)(ip - base), (int)(iend - ip), (int)mlen, (int)offset); exit(0); }
            if (mlen < MINMATCH) {
                printf("%d: ERROR mlen < MINMATCH iend=%d mlen=%d offset=%d\n", (int)(ip - base), (int)(iend - ip), (int)mlen, (int)offset); exit(0); }
            if (ip + mlen > iend) {
                printf("%d: ERROR ip + mlen >= iend iend=%d mlen=%d offset=%d\n", (int)(ip - base), (int)(iend - ip), (int)mlen, (int)offset); exit(0); }
#endif

            ZSTD_updatePrice(seqStorePtr, litLength, anchor, offset, mlen-MINMATCH);
            ZSTD_storeSeq(seqStorePtr, litLength, anchor, offset, mlen-MINMATCH);
            anchor = ip = ip + mlen;
        }

        /* check immediate repcode */
        while (anchor <= ilimit) {
            const U32 repIndex = (U32)((anchor-base) - rep_2);
            const BYTE* const repBase = repIndex < dictLimit ? dictBase : base;
            const BYTE* const repMatch = repBase + repIndex;
            if ( ((U32)((dictLimit-1) - repIndex) >= 3)   /* intentional overflow */
               && (MEM_read32(anchor) == MEM_read32(repMatch)) ) {
                /* repcode detected, let's take it */
                const BYTE* const repEnd = repIndex < dictLimit ? dictEnd : iend;
                mlen = (U32)ZSTD_count_2segments(anchor+MINMATCH, repMatch+MINMATCH, iend, repEnd, prefixStart) + MINMATCH;
                offset = rep_2; rep_2 = rep_1; rep_1 = offset;   /* swap offset history */
                ZSTD_LOG_ENCODE("%d/%d: ENCODE REP literals=%d mlen=%d off=%d rep1=%d rep2=%d\n", (int)(anchor-base), (int)(iend-base), (int)(0), (int)best_mlen, (int)(0), (int)rep_1, (int)rep_2);
                ZSTD_updatePrice(seqStorePtr, 0, anchor, 0, mlen-MINMATCH);
                ZSTD_storeSeq(seqStorePtr, 0, anchor, 0, mlen-MINMATCH);
                anchor += mlen;
                continue;   /* faster when present ... (?) */
            }
            break;
        }
        if (anchor > ip) ip = anchor;
    }

    {   /* Last Literals */
        size_t lastLLSize = iend - anchor;
        ZSTD_LOG_ENCODE("%d: lastLLSize literals=%u\n", (int)(ip-base), (U32)(lastLLSize));
        memcpy(seqStorePtr->lit, anchor, lastLLSize);
        seqStorePtr->lit += lastLLSize;
    }
}
