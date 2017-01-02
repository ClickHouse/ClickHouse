/*
 * Set match_start to the longest match starting at the given string and
 * return its length. Matches shorter or equal to prev_length are discarded,
 * in which case the result is equal to prev_length and match_start is garbage.
 *
 * IN assertions: cur_match is the head of the hash chain for the current
 * string (strstart) and its distance is <= MAX_DIST, and prev_length >=1
 * OUT assertion: the match length is not greater than s->lookahead
 */

#include "deflate.h"

#if (defined(UNALIGNED_OK) && MAX_MATCH == 258)

   /* Only use std3_longest_match for little_endian systems, also avoid using it with
      non-gcc compilers since the __builtin_ctzl() function might not be optimized. */
#  if defined(__GNUC__) && defined(HAVE_BUILTIN_CTZL) && ((__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) \
        || defined(__LITTLE_ENDIAN__))
#    define std3_longest_match
#  elif(defined(_MSC_VER) && defined(_WIN32))
#    define std3_longest_match
#  else
#    define std2_longest_match
#  endif

#else
#  define std1_longest_match
#endif


#if defined(_MSC_VER) && !defined(__clang__)
#include <intrin.h>
/* This is not a general purpose replacement for __builtin_ctzl. The function expects that value is != 0
 * Because of that assumption trailing_zero is not initialized and the return value of _BitScanForward is not checked
 */
static __forceinline unsigned long __builtin_ctzl(unsigned long value)
{
	unsigned long trailing_zero;
	_BitScanForward(&trailing_zero, value);
	return trailing_zero;
}
#endif



#ifdef std1_longest_match

/*
 * Standard longest_match
 *
 */
ZLIB_INTERNAL unsigned longest_match(deflate_state *const s, IPos cur_match) {
    const unsigned wmask = s->w_mask;
    const Pos *prev = s->prev;

    unsigned chain_length;
    IPos limit;
    unsigned int len, best_len, nice_match;
    unsigned char *scan, *match, *strend, scan_end, scan_end1;

    /*
     * The code is optimized for HASH_BITS >= 8 and MAX_MATCH-2 multiple
     * of 16. It is easy to get rid of this optimization if necessary.
     */
    Assert(s->hash_bits >= 8 && MAX_MATCH == 258, "Code too clever");

    /*
     * Do not waste too much time if we already have a good match
     */
    best_len = s->prev_length;
    chain_length = s->max_chain_length;
    if (best_len >= s->good_match)
        chain_length >>= 2;

    /*
     * Do not looks for matches beyond the end of the input. This is
     * necessary to make deflate deterministic
     */
    nice_match = (unsigned int)s->nice_match > s->lookahead ? s->lookahead : s->nice_match;

    /*
     * Stop when cur_match becomes <= limit. To simplify the code,
     * we prevent matches with the string of window index 0
     */
    limit = s->strstart > MAX_DIST(s) ? s->strstart - MAX_DIST(s) : 0;

    scan = s->window + s->strstart;
    strend = s->window + s->strstart + MAX_MATCH;
    scan_end1 = scan[best_len-1];
    scan_end = scan[best_len];

    Assert((unsigned long)s->strstart <= s->window_size - MIN_LOOKAHEAD, "need lookahead");
    do {
        Assert(cur_match < s->strstart, "no future");
        match = s->window + cur_match;

        /*
         * Skip to next match if the match length cannot increase
         * or if the match length is less than 2. Note that the checks
         * below for insufficient lookahead only occur occasionally
         * for performance reasons. Therefore uninitialized memory
         * will be accessed and conditional jumps will be made that
         * depend on those values. However the length of the match
         * is limited to the lookahead, so the output of deflate is not
         * affected by the uninitialized values.
         */
        if (match[best_len] != scan_end ||
            match[best_len-1] != scan_end1 ||
            *match != *scan ||
            *++match != scan[1])
            continue;

        /*
         * The check at best_len-1 can be removed because it will
         * be made again later. (This heuristic is not always a win.)
         * It is not necessary to compare scan[2] and match[2] since
         * they are always equal when the other bytes match, given
         * that the hash keys are equal and that HASH_BITS >= 8.
         */
        scan += 2;
        match++;
        Assert(*scan == *match, "match[2]?");

        /*
         * We check for insufficient lookahead only every 8th
         * comparision; the 256th check will be made at strstart + 258.
         */
        do {
        } while (*++scan == *++match && *++scan == *++match &&
             *++scan == *++match && *++scan == *++match &&
             *++scan == *++match && *++scan == *++match &&
             *++scan == *++match && *++scan == *++match &&
             scan < strend);

        Assert(scan <= s->window+(unsigned int)(s->window_size-1), "wild scan");

        len = MAX_MATCH - (int)(strend - scan);
        scan = strend - MAX_MATCH;

        if (len > best_len) {
            s->match_start = cur_match;
            best_len = len;
            if (len >= nice_match)
                break;
            scan_end1 = scan[best_len-1];
            scan_end = scan[best_len];
        } else {
            /*
             * The probability of finding a match later if we here
             * is pretty low, so for performance it's best to
             * outright stop here for the lower compression levels
             */
            if (s->level < 6)
                break;
        }
    } while ((cur_match = prev[cur_match & wmask]) > limit && --chain_length);

    if ((unsigned int)best_len <= s->lookahead)
        return best_len;
    return s->lookahead;
}
#endif

#ifdef std2_longest_match
/*
 * UNALIGNED_OK longest_match
 *
 */
ZLIB_INTERNAL unsigned longest_match(deflate_state *const s, IPos cur_match) {
    const unsigned wmask = s->w_mask;
    const Pos *prev = s->prev;

    uint16_t scan_start, scan_end;
    unsigned chain_length;
    IPos limit;
    unsigned int len, best_len, nice_match;
    unsigned char *scan, *strend;

    /*
     * The code is optimized for HASH_BITS >= 8 and MAX_MATCH-2 multiple
     * of 16. It is easy to get rid of this optimization if necessary.
     */
    Assert(s->hash_bits >= 8 && MAX_MATCH == 258, "Code too clever");

    /*
     * Do not waste too much time if we already have a good match
     */
    best_len = s->prev_length;
    chain_length = s->max_chain_length;
    if (best_len >= s->good_match)
        chain_length >>= 2;

    /*
     * Do not looks for matches beyond the end of the input. This is
     * necessary to make deflate deterministic
     */
    nice_match = (unsigned int)s->nice_match > s->lookahead ? s->lookahead : s->nice_match;

    /*
     * Stop when cur_match becomes <= limit. To simplify the code,
     * we prevent matches with the string of window index 0
     */
    limit = s->strstart > MAX_DIST(s) ? s->strstart - MAX_DIST(s) : 0;

    scan = s->window + s->strstart;
    strend = s->window + s->strstart + MAX_MATCH - 1;
    scan_start = *(uint16_t *)scan;
    scan_end = *(uint16_t *)(scan + best_len-1);

    Assert((unsigned long)s->strstart <= s->window_size - MIN_LOOKAHEAD, "need lookahead");
    do {
        unsigned char *match;
        Assert(cur_match < s->strstart, "no future");
        match = s->window + cur_match;

        /*
         * Skip to next match if the match length cannot increase
         * or if the match length is less than 2. Note that the checks
         * below for insufficient lookahead only occur occasionally
         * for performance reasons. Therefore uninitialized memory
         * will be accessed and conditional jumps will be made that
         * depend on those values. However the length of the match
         * is limited to the lookahead, so the output of deflate is not
         * affected by the uninitialized values.
         */
        if (likely((*(uint16_t *)(match + best_len - 1) != scan_end)))
            continue;
        if (*(uint16_t *)match != scan_start)
            continue;

        /* It is not necessary to compare scan[2] and match[2] since
         * they are always equal when the other bytes match, given that
         * the hash keys are equal and that HASH_BITS >= 8. Compare 2
         * bytes at a time at strstart+3, +5, ... up to strstart+257.
         * We check for insufficient lookahead only every 4th
         * comparison; the 128th check will be made at strstart+257.
         * If MAX_MATCH-2 is not a multiple of 8, it is necessary to
         * put more guard bytes at the end of the window, or to check
         * more often for insufficient lookahead.
         */
        Assert(scan[2] == match[2], "scan[2]?");
        scan++;
        match++;

        do {
        } while (*(uint16_t *)(scan += 2)== *(uint16_t *)(match += 2) &&
                 *(uint16_t *)(scan += 2)== *(uint16_t *)(match += 2) &&
                 *(uint16_t *)(scan += 2)== *(uint16_t *)(match += 2) &&
                 *(uint16_t *)(scan += 2)== *(uint16_t *)(match += 2) &&
                 scan < strend);

        /*
         * Here, scan <= window + strstart + 257
         */
        Assert(scan <= s->window+(unsigned)(s->window_size-1), "wild scan");
        if (*scan == *match)
            scan++;

        len = (MAX_MATCH -1) - (int)(strend-scan);
        scan = strend - (MAX_MATCH-1);

        if (len > best_len) {
            s->match_start = cur_match;
            best_len = len;
            if (len >= nice_match)
                break;
            scan_end = *(uint16_t *)(scan + best_len - 1);
        } else {
            /*
             * The probability of finding a match later if we here
             * is pretty low, so for performance it's best to
             * outright stop here for the lower compression levels
             */
            if (s->level < 6)
                break;
        }
    } while (--chain_length && (cur_match = prev[cur_match & wmask]) > limit);

    if ((unsigned)best_len <= s->lookahead)
        return best_len;
    return s->lookahead;
}
#endif

#ifdef std3_longest_match
/* longest_match() with minor change to improve performance (in terms of
 * execution time).
 *
 * The pristine longest_match() function is sketched bellow (strip the
 * then-clause of the "#ifdef UNALIGNED_OK"-directive)
 *
 * ------------------------------------------------------------
 * unsigned int longest_match(...) {
 *    ...
 *    do {
 *        match = s->window + cur_match;                //s0
 *        if (*(ushf*)(match+best_len-1) != scan_end || //s1
 *            *(ushf*)match != scan_start) continue;    //s2
 *        ...
 *
 *        do {
 *        } while (*(ushf*)(scan+=2) == *(ushf*)(match+=2) &&
 *                 *(ushf*)(scan+=2) == *(ushf*)(match+=2) &&
 *                 *(ushf*)(scan+=2) == *(ushf*)(match+=2) &&
 *                 *(ushf*)(scan+=2) == *(ushf*)(match+=2) &&
 *                 scan < strend); //s3
 *
 *        ...
 *    } while(cond); //s4
 *
 * -------------------------------------------------------------
 *
 * The change include:
 *
 *  1) The hottest statements of the function is: s0, s1 and s4. Pull them
 *     together to form a new loop. The benefit is two-fold:
 *
 *    o. Ease the compiler to yield good code layout: the conditional-branch
 *       corresponding to s1 and its biased target s4 become very close (likely,
 *       fit in the same cache-line), hence improving instruction-fetching
 *       efficiency.
 *
 *    o. Ease the compiler to promote "s->window" into register. "s->window"
 *       is loop-invariant; it is supposed to be promoted into register and keep
 *       the value throughout the entire loop. However, there are many such
 *       loop-invariant, and x86-family has small register file; "s->window" is
 *       likely to be chosen as register-allocation victim such that its value
 *       is reloaded from memory in every single iteration. By forming a new loop,
 *       "s->window" is loop-invariant of that newly created tight loop. It is
 *       lot easier for compiler to promote this quantity to register and keep
 *       its value throughout the entire small loop.
 *
 * 2) Transfrom s3 such that it examines sizeof(long)-byte-match at a time.
 *    This is done by:
 *        ------------------------------------------------
 *        v1 = load from "scan" by sizeof(long) bytes
 *        v2 = load from "match" by sizeof(lnog) bytes
 *        v3 = v1 xor v2
 *        match-bit = little-endian-machine(yes-for-x86) ?
 *                     count-trailing-zero(v3) :
 *                     count-leading-zero(v3);
 *
 *        match-byte = match-bit/8
 *
 *        "scan" and "match" advance if necessary
 *       -------------------------------------------------
 */

ZLIB_INTERNAL unsigned longest_match(deflate_state *const s, IPos cur_match) {
    unsigned chain_length = s->max_chain_length;/* max hash chain length */
    register unsigned char *scan = s->window + s->strstart; /* current string */
    register unsigned char *match;                       /* matched string */
    register unsigned int len;                  /* length of current match */
    unsigned int best_len = s->prev_length;     /* best match length so far */
    unsigned int nice_match = s->nice_match;    /* stop if match long enough */
    IPos limit = s->strstart > (IPos)MAX_DIST(s) ?
        s->strstart - (IPos)MAX_DIST(s) : NIL;
    /* Stop when cur_match becomes <= limit. To simplify the code,
     * we prevent matches with the string of window index 0.
     */
    Pos *prev = s->prev;
    unsigned int wmask = s->w_mask;

    register unsigned char *strend = s->window + s->strstart + MAX_MATCH;
    register uint16_t scan_start = *(uint16_t*)scan;
    register uint16_t scan_end   = *(uint16_t*)(scan+best_len-1);

    /* The code is optimized for HASH_BITS >= 8 and MAX_MATCH-2 multiple of 16.
     * It is easy to get rid of this optimization if necessary.
     */
    Assert(s->hash_bits >= 8 && MAX_MATCH == 258, "Code too clever");

    /* Do not waste too much time if we already have a good match: */
    if (s->prev_length >= s->good_match) {
        chain_length >>= 2;
    }
    /* Do not look for matches beyond the end of the input. This is necessary
     * to make deflate deterministic.
     */
    if ((unsigned int)nice_match > s->lookahead) nice_match = s->lookahead;

    Assert((unsigned long)s->strstart <= s->window_size-MIN_LOOKAHEAD, "need lookahead");

    do {
        Assert(cur_match < s->strstart, "no future");

        /* Skip to next match if the match length cannot increase
         * or if the match length is less than 2.  Note that the checks below
         * for insufficient lookahead only occur occasionally for performance
         * reasons.  Therefore uninitialized memory will be accessed, and
         * conditional jumps will be made that depend on those values.
         * However the length of the match is limited to the lookahead, so
         * the output of deflate is not affected by the uninitialized values.
         */
        unsigned char *win = s->window;
        int cont = 1;
        do {
            match = win + cur_match;
            if (likely(*(uint16_t*)(match+best_len-1) != scan_end)) {
                if ((cur_match = prev[cur_match & wmask]) > limit
                    && --chain_length != 0) {
                    continue;
                } else {
                    cont = 0;
                }
            }
            break;
        } while (1);

        if (!cont)
            break;

        if (*(uint16_t*)match != scan_start)
            continue;

        /* It is not necessary to compare scan[2] and match[2] since they are
         * always equal when the other bytes match, given that the hash keys
         * are equal and that HASH_BITS >= 8. Compare 2 bytes at a time at
         * strstart+3, +5, ... up to strstart+257. We check for insufficient
         * lookahead only every 4th comparison; the 128th check will be made
         * at strstart+257. If MAX_MATCH-2 is not a multiple of 8, it is
         * necessary to put more guard bytes at the end of the window, or
         * to check more often for insufficient lookahead.
         */
        scan += 2, match+=2;
        Assert(*scan == *match, "match[2]?");
        do {
            unsigned long sv = *(unsigned long*)(void*)scan;
            unsigned long mv = *(unsigned long*)(void*)match;
            unsigned long xor = sv ^ mv;
            if (xor) {
                int match_byte = __builtin_ctzl(xor) / 8;
                scan += match_byte;
                match += match_byte;
                break;
            } else {
                scan += sizeof(unsigned long);
                match += sizeof(unsigned long);
            }
        } while (scan < strend);

        if (scan > strend)
            scan = strend;

        Assert(scan <= s->window+(unsigned)(s->window_size-1), "wild scan");

        len = MAX_MATCH - (int)(strend - scan);
        scan = strend - MAX_MATCH;

        if (len > best_len) {
            s->match_start = cur_match;
            best_len = len;
            if (len >= nice_match)
                break;
            scan_end = *(uint16_t*)(scan+best_len-1);
        } else {
            /*
             * The probability of finding a match later if we here
             * is pretty low, so for performance it's best to
             * outright stop here for the lower compression levels
             */
            if (s->level < 6)
                break;
        }
    } while ((cur_match = prev[cur_match & wmask]) > limit && --chain_length != 0);

    if ((unsigned int)best_len <= s->lookahead)
        return (unsigned int)best_len;
    return s->lookahead;
}
#endif
