#undef LOC_TABLE_ENTRY
#undef LOC_TABLE_INDEX
#define LOC_TABLE_ENTRY        LOC_PREFIX(stem_table_entry_)
#define LOC_TABLE_INDEX        LOC_PREFIX(stem_table_index_)


struct LOC_TABLE_ENTRY {
    LOC_CHAR_TYPE suffix[8];
    int remove, len;
};


struct LOC_TABLE_INDEX {
    LOC_CHAR_TYPE first;
    int count;
};


// TableStringN, where N is a number of chars
#undef TS1
#undef TS2
#undef TS3
#undef TS4
#undef TS5
#define TS1(c1) { RUS::c1 }
#define TS2(c1, c2) { RUS::c1, RUS::c2 }
#define TS3(c1, c2, c3) { RUS::c1, RUS::c2, RUS::c3 }
#define TS4(c1, c2, c3, c4) { RUS::c1, RUS::c2, RUS::c3, RUS::c4 }
#define TS5(c1, c2, c3, c4, c5) { RUS::c1, RUS::c2, RUS::c3, RUS::c4, RUS::c5 }


static LOC_TABLE_INDEX LOC_PREFIX(ru_adj_i)[] =
        {
                {RUS::E,  4},
                {RUS::I,  2},
                {RUS::IY, 4},
                {RUS::M,  7},
                {RUS::O,  2},
                {RUS::U,  2},
                {RUS::H,  2},
                {RUS::YU, 4},
                {RUS::YA, 2},
        };


static LOC_TABLE_ENTRY LOC_PREFIX(ru_adj)[] =
        {
                {TS2(E, E),     2, -1},
                {TS2(I, E),     2, -1},
                {TS2(Y, E),     2, -1},
                {TS2(O, E),     2, -1},

                {TS3(I, M, I),  3, -1},
                {TS3(Y, M, I),  3, -1},

                {TS2(E, IY),    2, -1},
                {TS2(I, IY),    2, -1},
                {TS2(Y, IY),    2, -1},
                {TS2(O, IY),    2, -1},

                {TS3(A, E, M),  0, -1},
                {TS3(U, E, M),  0, -1},
                {TS3(YA, E, M), 0, -1},
                {TS2(E, M),     2, -1},
                {TS2(I, M),     2, -1},
                {TS2(Y, M),     2, -1},
                {TS2(O, M),     2, -1},

                {TS3(E, G, O),  3, -1},
                {TS3(O, G, O),  3, -1},

                {TS3(E, M, U),  3, -1},
                {TS3(O, M, U),  3, -1},

                {TS2(I, H),     2, -1},
                {TS2(Y, H),     2, -1},

                {TS2(E, YU),    2, -1},
                {TS2(O, YU),    2, -1},
                {TS2(U, YU),    2, -1},
                {TS2(YU, YU),   2, -1},

                {TS2(A, YA),    2, -1},
                {TS2(YA, YA),   2, -1}
        };


static LOC_TABLE_INDEX LOC_PREFIX(ru_part_i)[] =
        {
                {RUS::A,   3},
                {RUS::M,   1},
                {RUS::N,   3},
                {RUS::O,   3},
                {RUS::Y,   3},
                {RUS::SH,  4},
                {RUS::SCH, 5}
        };


static LOC_TABLE_ENTRY LOC_PREFIX(ru_part)[] =
        {
                {TS4(A, N, N, A),  2, -1},
                {TS4(E, N, N, A),  2, -1},
                {TS4(YA, N, N, A), 2, -1},

                {TS3(YA, E, M),    2, -1},

                {TS3(A, N, N),     1, -1},
                {TS3(E, N, N),     1, -1},
                {TS3(YA, N, N),    1, -1},

                {TS4(A, N, N, O),  2, -1},
                {TS4(E, N, N, O),  2, -1},
                {TS4(YA, N, N, O), 2, -1},

                {TS4(A, N, N, Y),  2, -1},
                {TS4(E, N, N, Y),  2, -1},
                {TS4(YA, N, N, Y), 2, -1},

                {TS3(A, V, SH),    2, -1},
                {TS3(I, V, SH),    3, -1},
                {TS3(Y, V, SH),    3, -1},
                {TS3(YA, V, SH),   2, -1},

                {TS3(A, YU, SCH),  2, -1},
                {TS2(A, SCH),      1, -1},
                {TS3(YA, YU, SCH), 2, -1},
                {TS2(YA, SCH),     1, -1},
                {TS3(U, YU, SCH),  3, -1}
        };


static LOC_TABLE_INDEX LOC_PREFIX(ru_verb_i)[] =
        {
                {RUS::A,   7},
                {RUS::E,   9},
                {RUS::I,   4},
                {RUS::IY,  4},
                {RUS::L,   4},
                {RUS::M,   5},
                {RUS::O,   7},
                {RUS::T,   9},
                {RUS::Y,   3},
                {RUS::MYA, 10},
                {RUS::YU,  4},
                {RUS::YA,  1}
        };


static LOC_TABLE_ENTRY LOC_PREFIX(ru_verb)[] =
        {
                {TS3(A, L, A),        3, -1},
                {TS3(A, N, A),        3, -1},
                {TS3(YA, L, A),       3, -1},
                {TS3(YA, N, A),       3, -1},
                {TS3(I, L, A),        3, -1},
                {TS3(Y, L, A),        3, -1},
                {TS3(E, N, A),        3, -1},

                {TS4(A, E, T, E),     4, -1},
                {TS4(A, IY, T, E),    4, -1},
                {TS3(MYA, T, E),      3, -1},
                {TS4(U, E, T, E),     4, -1},
                {TS4(YA, E, T, E),    4, -1},
                {TS4(YA, IY, T, E),   4, -1},
                {TS4(E, IY, T, E),    4, -1},
                {TS4(U, IY, T, E),    4, -1},
                {TS3(I, T, E),        3, -1},

                {TS3(A, L, I),        3, -1},
                {TS3(YA, L, I),       3, -1},
                {TS3(I, L, I),        3, -1},
                {TS3(Y, L, I),        3, -1},

                {TS2(A, IY),          2, -1},
                {TS2(YA, IY),         2, -1},
                {TS2(E, IY),          2, -1},
                {TS2(U, IY),          2, -1},

                {TS2(A, L),           2, -1},
                {TS2(YA, L),          2, -1},
                {TS2(I, L),           2, -1},
                {TS2(Y, L),           2, -1},

                {TS3(A, E, M),        3, -1},
                {TS3(YA, E, M),       3, -1},
                {TS3(U, E, M),        3, -1},
                {TS2(I, M),           2, -1},
                {TS2(Y, M),           2, -1},

                {TS3(A, L, O),        3, -1},
                {TS3(A, N, O),        3, -1},
                {TS3(YA, L, O),       3, -1},
                {TS3(YA, N, O),       3, -1},
                {TS3(I, L, O),        3, -1},
                {TS3(Y, L, O),        3, -1},
                {TS3(E, N, O),        3, -1},

                {TS3(A, E, T),        3, -1},
                {TS3(A, YU, T),       3, -1},
                {TS3(YA, E, T),       3, -1},
                {TS3(YA, YU, T),      3, -1},
                {TS2(YA, T),          2, -1},
                {TS3(U, E, T),        3, -1},
                {TS3(U, YU, T),       3, -1},
                {TS2(I, T),           2, -1},
                {TS2(Y, T),           2, -1},

                {TS3(A, N, Y),        3, -1},
                {TS3(YA, N, Y),       3, -1},
                {TS3(E, N, Y),        3, -1},

                {TS4(A, E, SH, MYA),  4, -1},
                {TS4(U, E, SH, MYA),  4, -1},
                {TS4(YA, E, SH, MYA), 4, -1},
                {TS3(A, T, MYA),      3, -1},
                {TS3(E, T, MYA),      3, -1},
                {TS3(I, T, MYA),      3, -1},
                {TS3(U, T, MYA),      3, -1},
                {TS3(Y, T, MYA),      3, -1},
                {TS3(I, SH, MYA),     3, -1},
                {TS3(YA, T, MYA),     3, -1},

                {TS2(A, YU),          2, -1},
                {TS2(U, YU),          2, -1},
                {TS2(YA, YU),         2, -1},
                {TS1(YU),             1, -1},

                {TS2(U, YA),          2, -1}
        };


static LOC_TABLE_INDEX LOC_PREFIX(ru_dear_i)[] =
        {
                {RUS::K,  3},
                {RUS::A,  2},
                {RUS::V,  2},
                {RUS::E,  2},
                {RUS::I,  4},
                {RUS::IY, 2},
                {RUS::M,  4},
                {RUS::O,  2},
                {RUS::U,  2},
                {RUS::H,  2},
                {RUS::YU, 2}
        };


static LOC_TABLE_ENTRY LOC_PREFIX(ru_dear)[] =
        {
                {TS3(CH, E, K),       3, -1},
                {TS3(CH, O, K),       3, -1},
                {TS3(N, O, K),        3, -1},

                {TS3(CH, K, A),       3, -1},
                {TS3(N, K, A),        3, -1},
                {TS4(CH, K, O, V),    4, -1},
                {TS4(N, K, O, V),     4, -1},
                {TS3(CH, K, E),       3, -1},
                {TS3(N, K, E),        3, -1},
                {TS3(CH, K, I),       3, -1},
                {TS3(N, K, I),        3, -1},
                {TS5(CH, K, A, M, I), 5, -1},
                {TS5(N, K, A, M, I),  5, -1},
                {TS4(CH, K, O, IY),   4, -1},
                {TS4(N, K, O, IY),    4, -1},
                {TS4(CH, K, A, M),    4, -1},
                {TS4(N, K, A, M),     4, -1},
                {TS4(CH, K, O, M),    4, -1},
                {TS4(N, K, O, M),     4, -1},
                {TS3(CH, K, O),       3, -1},
                {TS3(N, K, O),        3, -1},
                {TS3(CH, K, U),       3, -1},
                {TS3(N, K, U),        3, -1},
                {TS4(CH, K, A, H),    4, -1},
                {TS4(N, K, A, H),     4, -1},
                {TS4(CH, K, O, YU),   4, -1},
                {TS4(N, K, O, YU),    4, -1}
        };


static LOC_TABLE_INDEX LOC_PREFIX(ru_noun_i)[] =
        {
                {RUS::A,   1},
                {RUS::V,   2},
                {RUS::E,   3},
                {RUS::I,   6},
                {RUS::IY,  4},
                {RUS::M,   5},
                {RUS::O,   1},
                {RUS::U,   1},
                {RUS::H,   3},
                {RUS::Y,   1},
                {RUS::MYA, 1},
                {RUS::YU,  3},
                {RUS::YA,  3}
        };


static LOC_TABLE_ENTRY LOC_PREFIX(ru_noun)[] =
        {
                {TS1(A),           1, -1},

                {TS2(E, V),        2, -1},
                {TS2(O, V),        2, -1},

                {TS2(I, E),        2, -1},
                {TS2(MYA, E),      2, -1},
                {TS1(E),           1, -1},

                {TS4(I, YA, M, I), 4, -1},
                {TS3(YA, M, I),    3, -1},
                {TS3(A, M, I),     3, -1},
                {TS2(E, I),        2, -1},
                {TS2(I, I),        2, -1},
                {TS1(I),           1, -1},

                {TS3(I, E, IY),    3, -1},
                {TS2(E, IY),       2, -1},
                {TS2(O, IY),       2, -1},
                {TS2(I, IY),       2, -1},

                {TS3(I, YA, M),    3, -1},
                {TS2(YA, M),       2, -1},
                {TS3(I, E, M),     3, -1},
                {TS2(A, M),        2, -1},
                {TS2(O, M),        2, -1},

                {TS1(O),           1, -1},

                {TS1(U),           1, -1},

                {TS2(A, H),        2, -1},
                {TS3(I, YA, H),    3, -1},
                {TS2(YA, H),       2, -1},

                {TS1(Y),           1, -1},

                {TS1(MYA),         1, -1},

                {TS2(I, YU),       2, -1},
                {TS2(MYA, YU),     2, -1},
                {TS1(YU),          1, -1},

                {TS2(I, YA),       2, -1},
                {TS2(MYA, YA),     2, -1},
                {TS1(YA),          1, -1}
        };


int stem_ru_table_i(LOC_CHAR_TYPE *word, int len, LOC_TABLE_ENTRY *table, LOC_TABLE_INDEX *itable, int icount) {
    int i, j, k, m;
    LOC_CHAR_TYPE l = word[--len];

    for (i = 0, j = 0; i < icount; i++) {
        if (l == itable[i].first) {
            m = itable[i].count;
            i = j - 1;
            while (m--) {
                i++;
                j = table[i].len;
                k = len;
                if (j > k)
                    continue;
                for (; j >= 0; k--, j--)
                    if (word[k] != table[i].suffix[j])
                        break;
                if (j >= 0)
                    continue;
                return table[i].remove;
            }
            return 0;
        }
        j += itable[i].count;
    }
    return 0;
}


#undef STEM_RU_FUNC
#define STEM_RU_FUNC(func, table) \
    int func ( LOC_CHAR_TYPE * word, int len ) \
    { \
        return stem_ru_table ( word, len, LOC_PREFIX(table), \
            sizeof(LOC_PREFIX(table))/sizeof(LOC_TABLE_ENTRY) ); \
    }

#undef STEM_RU_FUNC_I
#define STEM_RU_FUNC_I(table) \
    int LOC_PREFIX(stem_##table##_i) ( LOC_CHAR_TYPE * word, int len ) \
    { \
        return stem_ru_table_i ( word, len, LOC_PREFIX(table), LOC_PREFIX(table##_i), \
            sizeof(LOC_PREFIX(table##_i))/sizeof(LOC_TABLE_INDEX) ); \
    }


STEM_RU_FUNC_I(ru_adj)

STEM_RU_FUNC_I(ru_part)

STEM_RU_FUNC_I(ru_dear)

STEM_RU_FUNC_I(ru_verb)

STEM_RU_FUNC_I(ru_noun)


static int LOC_PREFIX(stem_ru_adjectival)(LOC_CHAR_TYPE *word, int len) {
    int i = LOC_PREFIX(stem_ru_adj_i)(word, len);
    if (i)
        i += LOC_PREFIX(stem_ru_part_i)(word, len - i);
    return i;
}


static int LOC_PREFIX(stem_ru_verb_ov)(LOC_CHAR_TYPE *word, int len) {
    int i = LOC_PREFIX(stem_ru_verb_i)(word, len);
    if (i && (len >= i + 2) && word[len - i - 2] == RUS::O && word[len - i - 1] == RUS::V)
        return i + 2;
    return i;
}


void LOC_PREFIX(stem_ru_init)() {
    int i;

#undef STEM_RU_INIT_TABLE
#define STEM_RU_INIT_TABLE(table) \
        for ( i=0; i<static_cast<int>(sizeof(LOC_PREFIX(table))/sizeof(LOC_TABLE_ENTRY)); i++ ) \
            LOC_PREFIX(table)[i].len = (static_cast<int>(strlen(reinterpret_cast<char*>(LOC_PREFIX(table)[i].suffix)))/sizeof(LOC_CHAR_TYPE))- 1;

    STEM_RU_INIT_TABLE(ru_adj)
    STEM_RU_INIT_TABLE(ru_part)
    STEM_RU_INIT_TABLE(ru_verb)
    STEM_RU_INIT_TABLE(ru_noun)
    STEM_RU_INIT_TABLE(ru_dear)
}


void LOC_PREFIX(stem_ru)(LOC_CHAR_TYPE *word) {
    int r1, r2;
    int i, len;

    // IsVowel
#undef IV
#define IV(c) ( \
        c==RUS::A || c==RUS::E || c==RUS::YO || c==RUS::I || c==RUS::O || \
        c==RUS::U || c==RUS::Y || c==RUS::EE || c==RUS::YU || c==RUS::YA )

    // NotEndOfWord
#undef NEOW
#define NEOW(_arg) (*(reinterpret_cast<unsigned char *>(_arg)) && *(reinterpret_cast<unsigned char *>(_arg)+1))

    while (NEOW(word)) if (IV(*word)) break; else ++word;
    if (NEOW(word)) ++word; else return;
    len = 0;
    while (NEOW(word + len)) ++len;

    r1 = r2 = len;
    for (i = -1; i < len - 1; ++i)
        if (IV(word[i]) && !IV(word[i + 1])) {
            r1 = i + 2;
            break;
        }
    for (i = r1; i < len - 1; ++i)
        if (IV(word[i]) && !IV(word[i + 1])) {
            r2 = i + 2;
            break;
        }

#define C(p) word[len-p]
#undef W
#define W(p, c) ( C(p)==c )
#define XSUFF2(c2, c1) ( W(1,c1) && W(2,c2) )
#define XSUFF3(c3, c2, c1) ( W(1,c1) && W(2,c2) && W(3,c3) )
#define XSUFF4(c4, c3, c2, c1) ( W(1,c1) && W(2,c2) && W(3,c3) && W(4,c4) )
#define XSUFF5(c5, c4, c3, c2, c1) ( W(1,c1) && W(2,c2) && W(3,c3) && W(4,c4) && W(5,c5) )
#define BRK(_arg) { len -= _arg; break; }
#define CHK(_func) { i = LOC_PREFIX(_func) ( word, len ); if ( i ) BRK ( i ); }

    while (true) {
        CHK (stem_ru_dear_i)

        if (C(1) == RUS::V && len >= 2) {
            if (C(2) == RUS::I || C(2) == RUS::Y || C(2) == RUS::YA) BRK(2)

            if (C(2) == RUS::A) {
                if (C(3) == RUS::V && C(4) == RUS::A) BRK(4)
                BRK(2)
            }
        }

        if (len >= 3 && XSUFF3 (RUS::V, RUS::SH, RUS::I)
            && (C(4) == RUS::A || C(4) == RUS::I || C(4) == RUS::Y || C(4) == RUS::YA)) BRK(4)

        if (len >= 5 && XSUFF5 (RUS::V, RUS::SH, RUS::I, RUS::S, RUS::MYA)
            && (C(6) == RUS::A || C(6) == RUS::I || C(6) == RUS::Y || C(6) == RUS::YA)) BRK(6)

        CHK (stem_ru_adjectival)

        if (len >= 2 && (XSUFF2 (RUS::S, RUS::MYA) || XSUFF2 (RUS::S, RUS::YA))) {
            len -= 2;
            CHK (stem_ru_adjectival)
            CHK (stem_ru_verb_ov)
        } else {
            CHK (stem_ru_verb_ov)
        }

        CHK (stem_ru_noun_i)
        break;
    }

    if (len > 0 && (W(1, RUS::IY) || W(1, RUS::I)))
        len--;

    if (len - r2 >= 3 && XSUFF3 (RUS::O, RUS::S, RUS::T))
        len -= 3;
    else if (len - r2 >= 4 && XSUFF4 (RUS::O, RUS::S, RUS::T, RUS::MYA))
        len -= 4;

    if (len >= 3 && XSUFF3 (RUS::E, RUS::IY, RUS::SH))
        len -= 3;
    else if (len >= 4 && XSUFF4 (RUS::E, RUS::IY, RUS::SH, RUS::E))
        len -= 4;

    if (len >= 2 && XSUFF2 (RUS::N, RUS::N))
        len--;

    if (len > 0 && W(1, RUS::MYA))
        len--;

    *(reinterpret_cast<unsigned char *>(word + len)) = '\0';
}

// undefine externally defined stuff
#undef LOC_CHAR_TYPE
#undef LOC_PREFIX
#undef RUS
