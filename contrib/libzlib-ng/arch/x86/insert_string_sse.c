/* insert_string_sse -- insert_string variant using SSE4.2's CRC instructions
 *
 * Copyright (C) 1995-2013 Jean-loup Gailly and Mark Adler
 * For conditions of distribution and use, see copyright notice in zlib.h
 *
 */

#include "deflate.h"

/* ===========================================================================
 * Insert string str in the dictionary and set match_head to the previous head
 * of the hash chain (the most recent string with same hash key). Return
 * the previous length of the hash chain.
 * IN  assertion: all calls to to INSERT_STRING are made with consecutive
 *    input characters and the first MIN_MATCH bytes of str are valid
 *    (except for the last MIN_MATCH-1 bytes of the input file).
 */
#ifdef X86_SSE4_2_CRC_HASH
Pos insert_string_sse(deflate_state *const s, const Pos str, unsigned int count) {
    Pos ret = 0;
    unsigned int idx;
    unsigned *ip, val, h = 0;

    for (idx = 0; idx < count; idx++) {
        ip = (unsigned *)&s->window[str+idx];
        val = *ip;
        h = 0;

        if (s->level >= 6)
            val &= 0xFFFFFF;

#ifdef _MSC_VER
        h = _mm_crc32_u32(h, val);
#else
        __asm__ __volatile__ (
            "crc32 %1,%0\n\t"
            : "+r" (h)
            : "r" (val)
        );
#endif

        if (s->head[h & s->hash_mask] != str+idx) {
            s->prev[(str+idx) & s->w_mask] = s->head[h & s->hash_mask];
            s->head[h & s->hash_mask] = str+idx;
        }
    }
    ret = s->prev[(str+count-1) & s->w_mask];
    return ret;
}
#endif
