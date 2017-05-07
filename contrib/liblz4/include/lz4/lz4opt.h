/*
    lz4opt.h - Optimal Mode of LZ4
    Copyright (C) 2015-2017, Przemyslaw Skibinski <inikep@gmail.com>
    Note : this file is intended to be included within lz4hc.c

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
       - LZ4 source repository : https://github.com/lz4/lz4
       - LZ4 public forum : https://groups.google.com/forum/#!forum/lz4c
*/

#define LZ4_OPT_NUM   (1<<12)


typedef struct {
    int off;
    int len;
} LZ4HC_match_t;

typedef struct {
    int price;
    int off;
    int mlen;
    int litlen;
} LZ4HC_optimal_t;


/* price in bytes */
FORCE_INLINE size_t LZ4HC_literalsPrice(size_t litlen)
{
    size_t price = litlen;
    if (litlen >= (size_t)RUN_MASK)
        price += 1 + (litlen-RUN_MASK)/255;
    return price;
}


/* requires mlen >= MINMATCH */
FORCE_INLINE size_t LZ4HC_sequencePrice(size_t litlen, size_t mlen)
{
    size_t price = 2 + 1; /* 16-bit offset + token */

    price += LZ4HC_literalsPrice(litlen);

    if (mlen >= (size_t)(ML_MASK+MINMATCH))
        price+= 1 + (mlen-(ML_MASK+MINMATCH))/255;

    return price;
}


/*-*************************************
*  Binary Tree search
***************************************/
FORCE_INLINE int LZ4HC_BinTree_InsertAndGetAllMatches (
    LZ4HC_CCtx_internal* ctx,
    const BYTE* const ip,
    const BYTE* const iHighLimit,
    size_t best_mlen,
    LZ4HC_match_t* matches,
    int* matchNum)
{
    U16* const chainTable = ctx->chainTable;
    U32* const HashTable = ctx->hashTable;
    const BYTE* const base = ctx->base;
    const U32 dictLimit = ctx->dictLimit;
    const U32 current = (U32)(ip - base);
    const U32 lowLimit = (ctx->lowLimit + MAX_DISTANCE > current) ? ctx->lowLimit : current - (MAX_DISTANCE - 1);
    const BYTE* const dictBase = ctx->dictBase;
    const BYTE* match;
    int nbAttempts = ctx->searchNum;
    int mnum = 0;
    U16 *ptr0, *ptr1, delta0, delta1;
    U32 matchIndex;
    size_t matchLength = 0;
    U32* HashPos;

    if (ip + MINMATCH > iHighLimit) return 1;

    /* HC4 match finder */
    HashPos = &HashTable[LZ4HC_hashPtr(ip)];
    matchIndex = *HashPos;
    *HashPos = current;

    ptr0 = &DELTANEXTMAXD(current*2+1);
    ptr1 = &DELTANEXTMAXD(current*2);
    delta0 = delta1 = (U16)(current - matchIndex);

    while ((matchIndex < current) && (matchIndex>=lowLimit) && (nbAttempts)) {
        nbAttempts--;
        if (matchIndex >= dictLimit) {
            match = base + matchIndex;
            matchLength = LZ4_count(ip, match, iHighLimit);
        } else {
            const BYTE* vLimit = ip + (dictLimit - matchIndex);
            match = dictBase + matchIndex;
            if (vLimit > iHighLimit) vLimit = iHighLimit;
            matchLength = LZ4_count(ip, match, vLimit);
            if ((ip+matchLength == vLimit) && (vLimit < iHighLimit))
                matchLength += LZ4_count(ip+matchLength, base+dictLimit, iHighLimit);
        }

        if (matchLength > best_mlen) {
            best_mlen = matchLength;
            if (matches) {
                if (matchIndex >= dictLimit)
                    matches[mnum].off = (int)(ip - match);
                else
                    matches[mnum].off = (int)(ip - (base + matchIndex)); /* virtual matchpos */
                matches[mnum].len = (int)matchLength;
                mnum++;
            }
            if (best_mlen > LZ4_OPT_NUM) break;
        }

        if (ip+matchLength >= iHighLimit)   /* equal : no way to know if inf or sup */
            break;   /* drop , to guarantee consistency ; miss a bit of compression, but other solutions can corrupt the tree */

        if (*(ip+matchLength) < *(match+matchLength)) {
            *ptr0 = delta0;
            ptr0 = &DELTANEXTMAXD(matchIndex*2);
            if (*ptr0 == (U16)-1) break;
            delta0 = *ptr0;
            delta1 += delta0;
            matchIndex -= delta0;
        } else {
            *ptr1 = delta1;
            ptr1 = &DELTANEXTMAXD(matchIndex*2+1);
            if (*ptr1 == (U16)-1) break;
            delta1 = *ptr1;
            delta0 += delta1;
            matchIndex -= delta1;
        }
    }

    *ptr0 = (U16)-1;
    *ptr1 = (U16)-1;
    if (matchNum) *matchNum = mnum;
  /*  if (best_mlen > 8) return best_mlen-8; */
    if (!matchNum) return 1;
    return 1;
}


FORCE_INLINE void LZ4HC_updateBinTree(LZ4HC_CCtx_internal* ctx, const BYTE* const ip, const BYTE* const iHighLimit)
{
    const BYTE* const base = ctx->base;
    const U32 target = (U32)(ip - base);
    U32 idx = ctx->nextToUpdate;
    while(idx < target)
        idx += LZ4HC_BinTree_InsertAndGetAllMatches(ctx, base+idx, iHighLimit, 8, NULL, NULL);
}


/** Tree updater, providing best match */
FORCE_INLINE int LZ4HC_BinTree_GetAllMatches (
                        LZ4HC_CCtx_internal* ctx,
                        const BYTE* const ip, const BYTE* const iHighLimit,
                        size_t best_mlen, LZ4HC_match_t* matches, const int fullUpdate)
{
    int mnum = 0;
    if (ip < ctx->base + ctx->nextToUpdate) return 0;   /* skipped area */
    if (fullUpdate) LZ4HC_updateBinTree(ctx, ip, iHighLimit);
    best_mlen = LZ4HC_BinTree_InsertAndGetAllMatches(ctx, ip, iHighLimit, best_mlen, matches, &mnum);
    ctx->nextToUpdate = (U32)(ip - ctx->base + best_mlen);
    return mnum;
}


#define SET_PRICE(pos, ml, offset, ll, cost)           \
{                                                      \
    while (last_pos < pos)  { opt[last_pos+1].price = 1<<30; last_pos++; } \
    opt[pos].mlen = (int)ml;                           \
    opt[pos].off = (int)offset;                        \
    opt[pos].litlen = (int)ll;                         \
    opt[pos].price = (int)cost;                        \
}


static int LZ4HC_compress_optimal (
    LZ4HC_CCtx_internal* ctx,
    const char* const source,
    char* dest,
    int inputSize,
    int maxOutputSize,
    limitedOutput_directive limit,
    size_t sufficient_len,
    const int fullUpdate
    )
{
    LZ4HC_optimal_t opt[LZ4_OPT_NUM + 1];   /* this uses a bit too much stack memory to my taste ... */
    LZ4HC_match_t matches[LZ4_OPT_NUM + 1];

    const BYTE* ip = (const BYTE*) source;
    const BYTE* anchor = ip;
    const BYTE* const iend = ip + inputSize;
    const BYTE* const mflimit = iend - MFLIMIT;
    const BYTE* const matchlimit = (iend - LASTLITERALS);
    BYTE* op = (BYTE*) dest;
    BYTE* const oend = op + maxOutputSize;

    /* init */
    if (sufficient_len >= LZ4_OPT_NUM) sufficient_len = LZ4_OPT_NUM-1;
    ctx->end += inputSize;
    ip++;

    /* Main Loop */
    while (ip < mflimit) {
        size_t const llen = ip - anchor;
        size_t last_pos = 0;
        size_t match_num, cur, best_mlen, best_off;
        memset(opt, 0, sizeof(LZ4HC_optimal_t));  /* memset only the first one */

        match_num = LZ4HC_BinTree_GetAllMatches(ctx, ip, matchlimit, MINMATCH-1, matches, fullUpdate);
        if (!match_num) { ip++; continue; }

        if ((size_t)matches[match_num-1].len > sufficient_len) {
            /* good enough solution : immediate encoding */
            best_mlen = matches[match_num-1].len;
            best_off = matches[match_num-1].off;
            cur = 0;
            last_pos = 1;
            goto encode;
        }

        /* set prices using matches at position = 0 */
        {   size_t matchNb;
            for (matchNb = 0; matchNb < match_num; matchNb++) {
                size_t mlen = (matchNb>0) ? (size_t)matches[matchNb-1].len+1 : MINMATCH;
                best_mlen = matches[matchNb].len;   /* necessarily < sufficient_len < LZ4_OPT_NUM */
                for ( ; mlen <= best_mlen ; mlen++) {
                    size_t const cost = LZ4HC_sequencePrice(llen, mlen) - LZ4HC_literalsPrice(llen);
                    SET_PRICE(mlen, mlen, matches[matchNb].off, 0, cost);   /* updates last_pos and opt[pos] */
        }   }   }

        if (last_pos < MINMATCH) { ip++; continue; }  /* note : on clang at least, this test improves performance */

        /* check further positions */
        opt[0].mlen = opt[1].mlen = 1;
        for (cur = 1; cur <= last_pos; cur++) {
            const BYTE* const curPtr = ip + cur;

            /* establish baseline price if cur is literal */
            {   size_t price, litlen;
                if (opt[cur-1].mlen == 1) {
                    /* no match at previous position */
                    litlen = opt[cur-1].litlen + 1;
                    if (cur > litlen) {
                        price = opt[cur - litlen].price + LZ4HC_literalsPrice(litlen);
                    } else {
                        price = LZ4HC_literalsPrice(llen + litlen) - LZ4HC_literalsPrice(llen);
                    }
                } else {
                    litlen = 1;
                    price = opt[cur - 1].price + LZ4HC_literalsPrice(1);
                }

                if (price < (size_t)opt[cur].price)
                    SET_PRICE(cur, 1 /*mlen*/, 0 /*off*/, litlen, price);   /* note : increases last_pos */
            }

            if (cur == last_pos || curPtr >= mflimit) break;

            match_num = LZ4HC_BinTree_GetAllMatches(ctx, curPtr, matchlimit, MINMATCH-1, matches, fullUpdate);
            if ((match_num > 0) && (size_t)matches[match_num-1].len > sufficient_len) {
                /* immediate encoding */
                best_mlen = matches[match_num-1].len;
                best_off = matches[match_num-1].off;
                last_pos = cur + 1;
                goto encode;
            }

            /* set prices using matches at position = cur */
            {   size_t matchNb;
                for (matchNb = 0; matchNb < match_num; matchNb++) {
                    size_t ml = (matchNb>0) ? (size_t)matches[matchNb-1].len+1 : MINMATCH;
                    best_mlen = (cur + matches[matchNb].len < LZ4_OPT_NUM) ?
                                (size_t)matches[matchNb].len : LZ4_OPT_NUM - cur;

                    for ( ; ml <= best_mlen ; ml++) {
                        size_t ll, price;
                        if (opt[cur].mlen == 1) {
                            ll = opt[cur].litlen;
                            if (cur > ll)
                                price = opt[cur - ll].price + LZ4HC_sequencePrice(ll, ml);
                            else
                                price = LZ4HC_sequencePrice(llen + ll, ml) - LZ4HC_literalsPrice(llen);
                        } else {
                            ll = 0;
                            price = opt[cur].price + LZ4HC_sequencePrice(0, ml);
                        }

                        if (cur + ml > last_pos || price < (size_t)opt[cur + ml].price) {
                            SET_PRICE(cur + ml, ml, matches[matchNb].off, ll, price);
            }   }   }   }
        } /* for (cur = 1; cur <= last_pos; cur++) */

        best_mlen = opt[last_pos].mlen;
        best_off = opt[last_pos].off;
        cur = last_pos - best_mlen;

encode: /* cur, last_pos, best_mlen, best_off must be set */
        opt[0].mlen = 1;
        while (1) {  /* from end to beginning */
            size_t const ml = opt[cur].mlen;
            int const offset = opt[cur].off;
            opt[cur].mlen = (int)best_mlen;
            opt[cur].off = (int)best_off;
            best_mlen = ml;
            best_off = offset;
            if (ml > cur) break;   /* can this happen ? */
            cur -= ml;
        }

        /* encode all recorded sequences */
        cur = 0;
        while (cur < last_pos) {
            int const ml = opt[cur].mlen;
            int const offset = opt[cur].off;
            if (ml == 1) { ip++; cur++; continue; }
            cur += ml;
            if ( LZ4HC_encodeSequence(&ip, &op, &anchor, ml, ip - offset, limit, oend) ) return 0;
        }
    }  /* while (ip < mflimit) */

    /* Encode Last Literals */
    {   int lastRun = (int)(iend - anchor);
        if ((limit) && (((char*)op - dest) + lastRun + 1 + ((lastRun+255-RUN_MASK)/255) > (U32)maxOutputSize)) return 0;  /* Check output limit */
        if (lastRun>=(int)RUN_MASK) { *op++=(RUN_MASK<<ML_BITS); lastRun-=RUN_MASK; for(; lastRun > 254 ; lastRun-=255) *op++ = 255; *op++ = (BYTE) lastRun; }
        else *op++ = (BYTE)(lastRun<<ML_BITS);
        memcpy(op, anchor, iend - anchor);
        op += iend-anchor;
    }

    /* End */
    return (int) ((char*)op-dest);
}
