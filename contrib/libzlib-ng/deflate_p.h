/* deflate_p.h -- Private inline functions and macros shared with more than
 *                one deflate method
 *
 * Copyright (C) 1995-2013 Jean-loup Gailly and Mark Adler
 * For conditions of distribution and use, see copyright notice in zlib.h
 *
 */

#ifndef DEFLATE_P_H
#define DEFLATE_P_H

#if defined(X86_CPUID)
# include "arch/x86/x86.h"
#endif

/* Forward declare common non-inlined functions declared in deflate.c */

#ifdef DEBUG
void check_match(deflate_state *s, IPos start, IPos match, int length);
#else
#define check_match(s, start, match, length)
#endif
void fill_window(deflate_state *s);
void flush_pending(z_stream *strm);

/* ===========================================================================
 * Insert string str in the dictionary and set match_head to the previous head
 * of the hash chain (the most recent string with same hash key). Return
 * the previous length of the hash chain.
 * IN  assertion: all calls to to INSERT_STRING are made with consecutive
 *    input characters and the first MIN_MATCH bytes of str are valid
 *    (except for the last MIN_MATCH-1 bytes of the input file).
 */

#ifdef X86_SSE4_2_CRC_HASH
extern Pos insert_string_sse(deflate_state *const s, const Pos str, uInt count);
#endif

local inline Pos insert_string_c(deflate_state *const s, const Pos str, uInt count) {
    Pos ret = 0;
    uInt idx;

    for (idx = 0; idx < count; idx++) {
        UPDATE_HASH(s, s->ins_h, str+idx);
        if (s->head[s->ins_h] != str+idx) {
            s->prev[(str+idx) & s->w_mask] = s->head[s->ins_h];
            s->head[s->ins_h] = str+idx;
        }
    }
    ret = s->prev[(str+count-1) & s->w_mask];
    return ret;
}

local inline Pos insert_string(deflate_state *const s, const Pos str) {
#ifdef X86_SSE4_2_CRC_HASH
    if (x86_cpu_has_sse42)
        return insert_string_sse(s, str, 1);
#endif
    return insert_string_c(s, str, 1);
}

#ifndef NOT_TWEAK_COMPILER
local inline void bulk_insert_str(deflate_state *const s, Pos startpos, uInt count) {
# ifdef X86_SSE4_2_CRC_HASH
    if (x86_cpu_has_sse42) {
        insert_string_sse(s, startpos, count);
    } else
# endif
    {
        insert_string_c(s, startpos, count);
    }
}
#endif /* NOT_TWEAK_COMPILER */

/* ===========================================================================
 * Flush the current block, with given end-of-file flag.
 * IN assertion: strstart is set to the end of the current match.
 */
#define FLUSH_BLOCK_ONLY(s, last) { \
    _tr_flush_block(s, (s->block_start >= 0L ? \
                   (char *)&s->window[(unsigned)s->block_start] : \
                   (char *)Z_NULL), \
                   (ulg)((long)s->strstart - s->block_start), \
                   (last)); \
    s->block_start = s->strstart; \
    flush_pending(s->strm); \
    Tracev((stderr, "[FLUSH]")); \
}

/* Same but force premature exit if necessary. */
#define FLUSH_BLOCK(s, last) { \
    FLUSH_BLOCK_ONLY(s, last); \
    if (s->strm->avail_out == 0) return (last) ? finish_started : need_more; \
}

#endif
