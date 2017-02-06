/* deflate_medium.c -- The deflate_medium deflate strategy
 *
 * Copyright (C) 2013 Intel Corporation. All rights reserved.
 * Authors:
 *  Arjan van de Ven    <arjan@linux.intel.com>
 *
 * For conditions of distribution and use, see copyright notice in zlib.h
 */
#ifdef MEDIUM_STRATEGY
#include "deflate.h"
#include "deflate_p.h"
#include "match.h"

struct match {
    unsigned int match_start;
    unsigned int match_length;
    unsigned int strstart;
    unsigned int orgstart;
};

#define MAX_DIST2  ((1 << MAX_WBITS) - MIN_LOOKAHEAD)

static int tr_tally_dist(deflate_state *s, int distance, int length) {
    return _tr_tally(s, distance, length);
}

static int tr_tally_lit(deflate_state *s, int c) {
    return  _tr_tally(s, 0, c);
}

static int emit_match(deflate_state *s, struct match match) {
    int flush = 0;

    /* matches that are not long enough we need to emit as litterals */
    if (match.match_length < MIN_MATCH) {
        while (match.match_length) {
            flush += tr_tally_lit(s, s->window[match.strstart]);
            s->lookahead--;
            match.strstart++;
            match.match_length--;
        }
        return flush;
    }

    check_match(s, match.strstart, match.match_start, match.match_length);

    flush += tr_tally_dist(s, match.strstart - match.match_start, match.match_length - MIN_MATCH);

    s->lookahead -= match.match_length;
    return flush;
}

static void insert_match(deflate_state *s, struct match match) {
    if (unlikely(s->lookahead <= match.match_length + MIN_MATCH))
        return;

    /* matches that are not long enough we need to emit as litterals */
    if (match.match_length < MIN_MATCH) {
#ifdef NOT_TWEAK_COMPILER
        while (match.match_length) {
            match.strstart++;
            match.match_length--;

            if (match.match_length) {
                if (match.strstart >= match.orgstart) {
                    insert_string(s, match.strstart);
                }
            }
        }
#else
        if (likely(match.match_length == 1)) {
            match.strstart++;
            match.match_length = 0;
        }else{
            match.strstart++;
            match.match_length--;
            if (match.strstart >= match.orgstart) {
                bulk_insert_str(s, match.strstart, match.match_length);
            }
            match.strstart += match.match_length;
            match.match_length = 0;
        }
#endif
        return;
    }

    /* Insert new strings in the hash table only if the match length
     * is not too large. This saves time but degrades compression.
     */
    if (match.match_length <= 16* s->max_insert_length && s->lookahead >= MIN_MATCH) {
        match.match_length--; /* string at strstart already in table */
        match.strstart++;
#ifdef NOT_TWEAK_COMPILER
        do {
            if (likely(match.strstart >= match.orgstart)) {
                insert_string(s, match.strstart);
            }
            match.strstart++;
            /* strstart never exceeds WSIZE-MAX_MATCH, so there are
             * always MIN_MATCH bytes ahead.
             */
        } while (--match.match_length != 0);
#else
        if (likely(match.strstart >= match.orgstart)) {
            bulk_insert_str(s, match.strstart, match.match_length);
        }
        match.strstart += match.match_length;
        match.match_length = 0;
#endif
    } else {
        match.strstart += match.match_length;
        match.match_length = 0;
        s->ins_h = s->window[match.strstart];
        if (match.strstart >= 1)
            UPDATE_HASH(s, s->ins_h, match.strstart+2-MIN_MATCH);
#if MIN_MATCH != 3
#warning Call UPDATE_HASH() MIN_MATCH-3 more times
#endif
    /* If lookahead < MIN_MATCH, ins_h is garbage, but it does not
     * matter since it will be recomputed at next deflate call.
     */
    }
}

static void fizzle_matches(deflate_state *s, struct match *current, struct match *next) {
    IPos limit;
    unsigned char *match, *orig;
    int changed = 0;
    struct match c, n;
    /* step zero: sanity checks */

    if (current->match_length <= 1)
        return;

    if (unlikely(current->match_length > 1 + next->match_start))
        return;

    if (unlikely(current->match_length > 1 + next->strstart))
        return;

    match = s->window - current->match_length + 1 + next->match_start;
    orig  = s->window - current->match_length + 1 + next->strstart;

    /* quick exit check.. if this fails then don't bother with anything else */
    if (likely(*match != *orig))
        return;

    /* check the overlap case and just give up. We can do better in theory,
     * but unlikely to be worth it
     */
    if (next->match_start + next->match_length >= current->strstart)
        return;

    c = *current;
    n = *next;

    /* step one: try to move the "next" match to the left as much as possible */
    limit = next->strstart > MAX_DIST2 ? next->strstart - MAX_DIST2 : 0;

    match = s->window + n.match_start - 1;
    orig = s->window + n.strstart - 1;

    while (*match == *orig) {
        if (c.match_length < 1)
            break;
        if (n.strstart <= limit)
            break;
        if (n.match_length >= 256)
            break;
        if (n.match_start <= 0)
            break;

        n.strstart--;
        n.match_start--;
        n.match_length++;
        c.match_length--;
        match--;
        orig--;
        changed++;
    }

    if (!changed)
        return;

    if (c.match_length <= 1 && n.match_length != 2) {
        n.orgstart++;
        *current = c;
        *next = n;
    } else {
        return;
    }
}

block_state deflate_medium(deflate_state *s, int flush) {
    struct match current_match, next_match;

    memset(&current_match, 0, sizeof(struct match));
    memset(&next_match, 0, sizeof(struct match));

    for (;;) {
        IPos hash_head = 0;   /* head of the hash chain */
        int bflush;           /* set if current block must be flushed */

        /* Make sure that we always have enough lookahead, except
         * at the end of the input file. We need MAX_MATCH bytes
         * for the next match, plus MIN_MATCH bytes to insert the
         * string following the next current_match.
         */
        if (s->lookahead < MIN_LOOKAHEAD) {
            fill_window(s);
            if (s->lookahead < MIN_LOOKAHEAD && flush == Z_NO_FLUSH) {
                return need_more;
            }
            if (s->lookahead == 0)
                break; /* flush the current block */
            next_match.match_length = 0;
        }
        s->prev_length = 2;

        /* Insert the string window[strstart .. strstart+2] in the
         * dictionary, and set hash_head to the head of the hash chain:
         */

        /* If we already have a future match from a previous round, just use that */
        if (next_match.match_length > 0) {
            current_match = next_match;
            next_match.match_length = 0;

        } else {
            hash_head = 0;
            if (s->lookahead >= MIN_MATCH) {
                hash_head = insert_string(s, s->strstart);
            }

            /* set up the initial match to be a 1 byte literal */
            current_match.match_start = 0;
            current_match.match_length = 1;
            current_match.strstart = s->strstart;
            current_match.orgstart = current_match.strstart;

            /* Find the longest match, discarding those <= prev_length.
             * At this point we have always match_length < MIN_MATCH
             */

            if (hash_head != 0 && s->strstart - hash_head <= MAX_DIST2) {
                /* To simplify the code, we prevent matches with the string
                 * of window index 0 (in particular we have to avoid a match
                 * of the string with itself at the start of the input file).
                 */
                current_match.match_length = longest_match(s, hash_head);
                current_match.match_start = s->match_start;
                if (current_match.match_length < MIN_MATCH)
                    current_match.match_length = 1;
                if (current_match.match_start >= current_match.strstart) {
                    /* this can happen due to some restarts */
                    current_match.match_length = 1;
                }
            }
        }

        insert_match(s, current_match);

        /* now, look ahead one */
        if (s->lookahead > MIN_LOOKAHEAD) {
            s->strstart = current_match.strstart + current_match.match_length;
            hash_head = insert_string(s, s->strstart);

            /* set up the initial match to be a 1 byte literal */
            next_match.match_start = 0;
            next_match.match_length = 1;
            next_match.strstart = s->strstart;
            next_match.orgstart = next_match.strstart;

            /* Find the longest match, discarding those <= prev_length.
             * At this point we have always match_length < MIN_MATCH
             */
            if (hash_head != 0 && s->strstart - hash_head <= MAX_DIST2) {
                /* To simplify the code, we prevent matches with the string
                 * of window index 0 (in particular we have to avoid a match
                 * of the string with itself at the start of the input file).
                 */
                next_match.match_length = longest_match(s, hash_head);
                next_match.match_start = s->match_start;
                if (next_match.match_start >= next_match.strstart) {
                    /* this can happen due to some restarts */
                    next_match.match_length = 1;
                }
                if (next_match.match_length < MIN_MATCH)
                    next_match.match_length = 1;
                else
                    fizzle_matches(s, &current_match, &next_match);
            }

            /* short matches with a very long distance are rarely a good idea encoding wise */
            if (next_match.match_length == 3 && (next_match.strstart - next_match.match_start) > 12000)
                    next_match.match_length = 1;
            s->strstart = current_match.strstart;

        } else {
            next_match.match_length = 0;
        }

        /* now emit the current match */
        bflush = emit_match(s, current_match);

        /* move the "cursor" forward */
        s->strstart += current_match.match_length;

        if (bflush)
            FLUSH_BLOCK(s, 0);
    }
    s->insert = s->strstart < MIN_MATCH-1 ? s->strstart : MIN_MATCH-1;
    if (flush == Z_FINISH) {
        FLUSH_BLOCK(s, 1);
        return finish_done;
    }
    if (s->last_lit)
        FLUSH_BLOCK(s, 0);

    return block_done;
}
#endif
