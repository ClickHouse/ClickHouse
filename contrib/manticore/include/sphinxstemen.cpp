//
// Copyright (c) 2001-2016, Andrew Aksyonoff
// Copyright (c) 2008-2016, Sphinx Technologies Inc
// All rights reserved
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License. You should have
// received a copy of the GPL license along with this program; if you
// did not, you can find it at http://www.gnu.org/
//

// #include "sphinx.h" // for UNALIGNED_RAM_ACCESS

// #if defined(_MSC_VER) && !defined(__cplusplus)
//#define inline
// #endif


#define SNOWBALL2011

static unsigned char stem_en_doubles[] = "bdfgmnprt";

static unsigned char vowel_map[] =
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" // 0
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" // 1
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" // 2
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" // 3
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" // 4
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" // 5
        //` a b c d e f g h i j k l m n o - NOLINT
        "\0\1\0\0\0\1\0\0\0\1\0\0\0\0\0\1" // 6
        //p q r s t u v w x y z - NOLINT
        "\0\0\0\0\0\1\0\0\0\1\0\0\0\0\0\0" // 7
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" // 8
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" // 9
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" // a
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" // b
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" // c
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" // d
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" // e
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"; // f

#define is_vowel(idx) vowel_map[word[idx]]

static inline int stem_en_id(unsigned char l) {
    unsigned char *v = stem_en_doubles;
    while (*v && *v != l) v++;
    return (*v == l) ? 1 : 0;
}

static inline int stem_en_ivwxy(unsigned char l) {
    return vowel_map[l] || l == 'w' || l == 'x' || l == 'Y';
}

void stem_en_init() {
}

#define EXCBASE(b) ( iword==( ( static_cast<int>(b[3])<<24 ) + ( static_cast<int>(b[2])<<16 ) + ( static_cast<int>(b[1])<<8 ) + static_cast<int>(b[0]) ) )
#define EXC4(a, b) ( len==4 && EXCBASE(b) )
#define EXC5(a, b) ( len==5 && EXCBASE(b) )
#define EXC6(a, b) ( len==6 && EXCBASE(b) && a[4]==b[4] )
#define EXC7(a, b) ( len==7 && EXCBASE(b) && a[4]==b[4] && a[5]==b[5] )
#define EXC8(a, b) ( len==8 && EXCBASE(b) && a[4]==b[4] && a[5]==b[5] && a[6]==b[6] )

void stem_en(BYTE *word, int len) {
    int i, first_vowel, r1, r2, iword;
    unsigned char has_Y = 0;

    if (len <= 2)
        return;

// #if UNALIGNED_RAM_ACCESS
// 	iword = *(int*)word;
// #else
    iword = (static_cast<int>(word[3]) << 24) + (static_cast<int>(word[2]) << 16) + (static_cast<int>(word[1]) << 8) + static_cast<int>(word[0]);
// #endif

    // check for 3-letter exceptions (currently just one, "sky") and shortcuts
    if (len == 3) {
#define CHECK3(c1, c2, c3) if ( iword==( (c1<<0)+(c2<<8)+(c3<<16) ) ) return;
#ifdef SNOWBALL2011
#define CHECK3A CHECK3
#else
#define CHECK3A(c1,c2,c3) if ( iword==( (c1<<0)+(c2<<8)+(c3<<16) ) ) { word[2] = '\0'; return; }
#endif
        CHECK3 ('t', 'h', 'e')
        CHECK3 ('a', 'n', 'd')
        CHECK3 ('y', 'o', 'u')
        CHECK3A ('w', 'a', 's')
        CHECK3A ('h', 'i', 's')
        CHECK3 ('f', 'o', 'r')
        CHECK3 ('h', 'e', 'r')
        CHECK3 ('s', 'h', 'e')
        CHECK3 ('b', 'u', 't')
        CHECK3 ('h', 'a', 'd')
        CHECK3 ('s', 'k', 'y')
    }

    // check for 4..8-letter exceptions
    if (len >= 4 && len <= 8) {
        // check for 4-letter exceptions and shortcuts
        if (len == 4) {
            // shortcuts
            if (iword == 0x74616874) return; // that
            if (iword == 0x68746977) return; // with
            if (iword == 0x64696173) return; // said
            if (iword == 0x6d6f7266) return; // from

            // exceptions
            if (iword == 0x7377656e) return; // news
            if (iword == 0x65776f68) return; // howe
        }

        // all those exceptions only have a few valid endings; early check
        switch (word[len - 1]) {
            case 'd':
                if (EXC7 (word, "proceed")) return;
                if (EXC6 (word, "exceed")) return;
                if (EXC7 (word, "succeed")) return;
                break;

            case 'g':
                if (EXC5 (word, "dying")) {
                    word[1] = 'i';
                    word[2] = 'e';
                    word[3] = '\0';
                    return;
                }
                if (EXC5 (word, "lying")) {
                    word[1] = 'i';
                    word[2] = 'e';
                    word[3] = '\0';
                    return;
                }
                if (EXC5 (word, "tying")) {
                    word[1] = 'i';
                    word[2] = 'e';
                    word[3] = '\0';
                    return;
                }
                if (EXC6 (word, "inning")) return;
                if (EXC6 (word, "outing")) return;
                if (EXC7 (word, "canning")) return;
#ifdef SNOWBALL2011
                if (EXC7 (word, "herring")) return;
                if (EXC7 (word, "earring")) return;
#endif
                break;

            case 's':
                if (EXC5 (word, "skies")) {
                    word[2] = 'y';
                    word[3] = '\0';
                    return;
                }
                if (EXC7 (word, "innings")) {
                    word[6] = '\0';
                    return;
                }
                if (EXC7 (word, "outings")) {
                    word[6] = '\0';
                    return;
                }
                if (EXC8 (word, "cannings")) {
                    word[7] = '\0';
                    return;
                }
#ifdef SNOWBALL2011
                if (EXC4 (word, "skis")) {
                    word[3] = '\0';
                    return;
                }
                if (EXC5 (word, "atlas")) return;
                if (EXC6 (word, "cosmos")) return;
                if (EXC4 (word, "bias")) return;
                if (EXC5 (word, "andes")) return;
                if (EXC8 (word, "herrings")) {
                    word[7] = '\0';
                    return;
                }
                if (EXC8 (word, "earrings")) {
                    word[7] = '\0';
                    return;
                }
                if (EXC8 (word, "proceeds")) {
                    word[7] = '\0';
                    return;
                }
                if (EXC7 (word, "exceeds")) {
                    word[6] = '\0';
                    return;
                }
                if (EXC8 (word, "succeeds")) {
                    word[7] = '\0';
                    return;
                }
#endif
                break;

            case 'y':
                if (EXC4 (word, "idly")) {
                    word[3] = '\0';
                    return;
                }
                if (EXC6 (word, "gently")) {
                    word[5] = '\0';
                    return;
                }
                if (EXC4 (word, "ugly")) {
                    word[3] = 'i';
                    word[4] = '\0';
                    return;
                }
                if (EXC5 (word, "early")) {
                    word[4] = 'i';
                    word[5] = '\0';
                    return;
                }
                if (EXC4 (word, "only")) {
                    word[3] = 'i';
                    word[4] = '\0';
                    return;
                }
                if (EXC6 (word, "singly")) {
                    word[5] = '\0';
                    return;
                }
                break;
        }
    }

    // hide consonant-style y's
    if (word[0] == 'y')
        word[0] = has_Y = 'Y';
    for (i = 1; i < len; i++)
        if (word[i] == 'y' && is_vowel (i - 1))
            word[i] = has_Y = 'Y';

    // mark regions
    // R1 begins after first "vowel, consonant" sequence in the word
    // R2 begins after second "vowel, consonant" sequence
    if (len >= 5 && EXCBASE("gene") && word[4] == 'r') {
        r1 = 5; // gener-
        first_vowel = 1;
    }
#ifdef SNOWBALL2011
            else if (len >= 6 && EXCBASE("comm") && word[4] == 'u' && word[5] == 'n') {
        r1 = 6; // commun-
        first_vowel = 1;
    } else if (len >= 5 && EXCBASE("arse") && word[4] == 'n') {
        r1 = 5; // arsen-
        first_vowel = 0;
    }
#endif
            else {
        for (i = 0; i < len && !is_vowel(i); i++);
        first_vowel = i;

        for (i = first_vowel; i < len - 2; i++)
            if (is_vowel(i) && !is_vowel(i + 1))
                break;
        r1 = i + 2;
    }
    for (i = r1; i < len - 2; i++)
        if (is_vowel(i) && !is_vowel(i + 1))
            break;
    r2 = i + 2;

#define W(p, c) ( word[len-p]==c )

#define SUFF2(c2, c1) ( len>=2 && W(1,c1) && W(2,c2) )
#define SUFF3(c3, c2, c1) ( len>=3 && W(1,c1) && W(2,c2) && W(3,c3) )
#define SUFF4(c4, c3, c2, c1) ( len>=4 && W(1,c1) && W(2,c2) && W(3,c3) && W(4,c4) )
#define SUFF5(c5, c4, c3, c2, c1) ( len>=5 && W(1,c1) && W(2,c2) && W(3,c3) && W(4,c4) && W(5,c5) )
// #define SUFF6(c6, c5, c4, c3, c2, c1) ( len>=6 && W(1,c1) && W(2,c2) && W(3,c3) && W(4,c4) && W(5,c5) && W(6,c6) )
// #define SUFF7(c7, c6, c5, c4, c3, c2, c1) ( len>=7 && W(1,c1) && W(2,c2) && W(3,c3) && W(4,c4) && W(5,c5) && W(6,c6) && W(7,c7) )

#define SUFF3A(c3, c2) ( len>=3 && W(2,c2) && W(3,c3) )
#define SUFF4A(c4, c3, c2) ( len>=4 && W(2,c2) && W(3,c3) && W(4,c4) )
#define SUFF5A(c5, c4, c3, c2) ( len>=5 && W(2,c2) && W(3,c3) && W(4,c4) && W(5,c5) )
#define SUFF6A(c6, c5, c4, c3, c2) ( len>=6 && W(2,c2) && W(3,c3) && W(4,c4) && W(5,c5) && W(6,c6) )
#define SUFF7A(c7, c6, c5, c4, c3, c2) ( len>=6 && W(2,c2) && W(3,c3) && W(4,c4) && W(5,c5) && W(6,c6) && W(7,c7) )

    ///////////
    // STEP 1A
    ///////////

#ifdef SNOWBALL2011
#define IED_ACTION { if ( len-->4 ) len--; }
#else
#define IED_ACTION { if ( len--!=4 ) len--; }
#endif

    switch (word[len - 1]) {
        case 'd':
            if (word[len - 3] == 'i' && word[len - 2] == 'e') IED_ACTION
            break;
        case 's':
            if (SUFF4 ('s', 's', 'e', 's')) // faster that suff4a for some reason!
                len -= 2;
            else if (word[len - 3] == 'i' && word[len - 2] == 'e') IED_ACTION
            else if (word[len - 2] != 'u' && word[len - 2] != 's') {
#ifdef SNOWBALL2011
                if (first_vowel <= len - 3)
#endif

                    len--;
            }
            break;
    }

    ///////////
    // STEP 1B
    ///////////

    i = 0;
    switch (word[len - 1]) {
        case 'd':
            if (SUFF3A ('e', 'e')) {
                if (len - 3 >= r1) len--;
                break;
            }
            if (word[len - 2] == 'e') i = 2;
            break;

        case 'y':
            if (word[len - 2] == 'l') {
                if (SUFF5A ('e', 'e', 'd', 'l')) {
                    if (len - 5 >= r1) len -= 3;
                    break;
                }
                if (SUFF4A ('e', 'd', 'l')) {
                    i = 4;
                    break;
                }
                if (SUFF5A ('i', 'n', 'g', 'l')) {
                    i = 5;
                    break;
                }
            }
            break;

        case 'g':
            if (SUFF3A ('i', 'n')) i = 3;
            break;
    }

    if (i && first_vowel < len - i) {
        len -= i;
        if (SUFF2 ('a', 't') || SUFF2 ('b', 'l') || SUFF2 ('i', 'z'))
            word[len++] = 'e';
        else if (len >= 2 && word[len - 1] == word[len - 2] && stem_en_id(word[len - 1]))
            len--;
        else if ((len == 2 && is_vowel(0) && !is_vowel(1))
                 || (len == r1 && !is_vowel (len - 3) && is_vowel (len - 2) && !stem_en_ivwxy(word[len - 1]))) {
            word[len++] = 'e';
        }
    }

    ///////////
    // STEP 1C
    ///////////

    if (len > 2
        && (word[len - 1] == 'y' || word[len - 1] == 'Y')
        && !is_vowel (len - 2)) {
        word[len - 1] = 'i';
    }

    //////////
    // STEP 2
    //////////

    if (len - 2 >= r1)
        switch (word[len - 1]) {
            case 'i':
                if (len >= 3 && (W (2, 'c') || W (2, 'l') || W (2, 't'))) {
                    if (SUFF4A ('e', 'n', 'c')) {
                        if (len - 4 >= r1) word[len - 1] = 'e';
                        break;
                    }
                    if (SUFF4A ('a', 'n', 'c')) {
                        if (len - 4 >= r1) word[len - 1] = 'e';
                        break;
                    }
                    if (SUFF4A ('a', 'b', 'l')) {
                        if (len - 4 >= r1) word[len - 1] = 'e';
                        break;
                    }
                    if (SUFF3A ('b', 'l')) {
                        if (len - 3 >= r1) word[len - 1] = 'e';
                        break;
                    }

                    if (SUFF5A ('e', 'n', 't', 'l')) {
                        if (len - 5 >= r1) len -= 2;
                        break;
                    }
                    if (SUFF5A ('a', 'l', 'i', 't')) {
                        if (len - 5 >= r1) len -= 3;
                        break;
                    }
                    if (SUFF5A ('o', 'u', 's', 'l')) {
                        if (len - 5 >= r1) len -= 2;
                        break;
                    }

                    if (SUFF5A ('i', 'v', 'i', 't')) {
                        if (len - 5 >= r1) {
                            word[len - 3] = 'e';
                            len -= 2;
                        }
                        break;
                    }
                    if (SUFF6A ('b', 'i', 'l', 'i', 't')) {
                        if (len - 6 >= r1) {
                            word[len - 5] = 'l';
                            word[len - 4] = 'e';
                            len -= 3;
                        }
                        break;
                    }
                    if (SUFF5A ('f', 'u', 'l', 'l')) {
                        if (len - 5 >= r1) len -= 2;
                        break;
                    }
                    if (SUFF6A ('l', 'e', 's', 's', 'l')) {
                        if (len - 6 >= r1) len -= 2;
                        break;
                    }
                }

#ifdef SNOWBALL2011
                if (len - 3 >= r1 && SUFF3A ('o', 'g') && word[len - 4] == 'l') {
                    len -= 1;
                    break;
                }
#else
                if ( len-3>=r1 && SUFF3A ( 'o', 'g' ) ) { len -= 1; break; }
#endif

                if (len - 2 >= r1 && word[len - 2] == 'l')
                    len -= 2;
                else
                    break;

                if (len - 2 >= r1 && SUFF2 ('a', 'l')) {
                    len -= 2;
                    if (len - 5 >= r1 && SUFF5 ('a', 't', 'i', 'o', 'n')) {
                        len -= 3;
                        word[len++] = 'e';
                        break;
                    }
                    if (SUFF4 ('t', 'i', 'o', 'n'))
                        break;
                    len += 2;
                } else {
                    switch (word[len - 1]) {
                        case 'b':
                        case 'c':
                        case 'd':
                        case 'e':
                        case 'g':
                        case 'h':
                        case 'k':
                        case 'm':
                        case 'n':
                        case 'r':
                        case 't':
                            break;
                        default:
                            len += 2;
                            break;
                    }
                }
                break;

            case 'l':
                if (SUFF7A ('a', 't', 'i', 'o', 'n', 'a')) {
                    if (len - 7 >= r1) {
                        word[len - 5] = 'e';
                        len -= 4;
                    }
                    break;
                }
                if (SUFF6A ('t', 'i', 'o', 'n', 'a')) {
                    if (len - 6 >= r1) len -= 2;
                    break;
                }
                break;

            case 'm':
                if (SUFF5A ('a', 'l', 'i', 's')) {
                    if (len - 5 >= r1) len -= 3;
                    break;
                }
                break;

            case 'n':
                if (SUFF7A ('i', 'z', 'a', 't', 'i', 'o')) {
                    if (len - 7 >= r1) {
                        word[len - 5] = 'e';
                        len -= 4;
                    }
                    break;
                }
                if (SUFF5A ('a', 't', 'i', 'o')) {
                    if (len - 5 >= r1) {
                        word[len - 3] = 'e';
                        len -= 2;
                    }
                    break;
                }
                break;

            case 'r':
                if (SUFF4A ('i', 'z', 'e')) {
                    if (len - 4 >= r1) len -= 1;
                    break;
                }
                if (SUFF4A ('a', 't', 'o')) {
                    if (len - 4 >= r1) {
                        word[len - 2] = 'e';
                        len -= 1;
                    }
                    break;
                }
                break;

            case 's':
                if (len - 7 >= r1 && (
                        SUFF7A ('f', 'u', 'l', 'n', 'e', 's') ||
                        SUFF7A ('o', 'u', 's', 'n', 'e', 's') ||
                        SUFF7A ('i', 'v', 'e', 'n', 'e', 's'))) {
                    len -= 4;
                }
                break;
        }

    //////////
    // STEP 3
    //////////

    if (len - 3 >= r1)
        switch (word[len - 1]) {
            case 'e':
                if (SUFF5A ('a', 'l', 'i', 'z')) {
                    if (len - 5 >= r1) len -= 3;
                    break;
                }
                if (SUFF5A ('i', 'c', 'a', 't')) {
                    if (len - 5 >= r1) len -= 3;
                    break;
                }
#ifdef SNOWBALL2011
                if (SUFF5A ('a', 't', 'i', 'v')) {
                    if (len - 5 >= r2) len -= 5;
                    break;
                }
#else
                if ( SUFF5A ( 'a', 't', 'i', 'v' ) )	{ if ( len-5>=r1 ) len -= 5; break; }
#endif
                break;

            case 'i':
                if (SUFF5A ('i', 'c', 'i', 't')) {
                    if (len - 5 >= r1) len -= 3;
                    break;
                }
                break;

            case 'l':
                if (SUFF4A ('i', 'c', 'a')) {
                    if (len - 4 >= r1) len -= 2;
                    break;
                }
                if (SUFF3A ('f', 'u')) {
                    len -= 3;
                    break;
                }
                break;

            case 's':
                if (SUFF4A ('n', 'e', 's')) {
                    if (len - 4 >= r1) len -= 4;
                    break;
                }
                break;
        }

    //////////
    // STEP 4
    //////////

    if (len - 2 >= r2)
        switch (word[len - 1]) {
            case 'c':
                if (word[len - 2] == 'i') len -= 2; // -ic
                break;

            case 'e':
                if (len - 3 >= r2) {
                    if (SUFF4A ('a', 'n', 'c')) {
                        if (len - 4 >= r2) len -= 4;
                        break;
                    }
                    if (SUFF4A ('e', 'n', 'c')) {
                        if (len - 4 >= r2) len -= 4;
                        break;
                    }
                    if (SUFF4A ('a', 'b', 'l')) {
                        if (len - 4 >= r2) len -= 4;
                        break;
                    }
                    if (SUFF4A ('i', 'b', 'l')) {
                        if (len - 4 >= r2) len -= 4;
                        break;
                    }
                    if (SUFF3A ('a', 't')) {
                        len -= 3;
                        break;
                    }
                    if (SUFF3A ('i', 'v')) {
                        len -= 3;
                        break;
                    }
                    if (SUFF3A ('i', 'z')) {
                        len -= 3;
                        break;
                    }
                }
                break;

            case 'i':
                if (SUFF3A ('i', 't')) {
                    if (len - 3 >= r2) len -= 3;
                    break;
                }
                break;

            case 'l':
                if (word[len - 2] == 'a') len -= 2; // -al
                break;

            case 'm':
                if (SUFF3A ('i', 's')) {
                    if (len - 3 >= r2) len -= 3;
                    break;
                }
                break;

            case 'n':
                if (len - 3 >= r2 && SUFF3 ('i', 'o', 'n') && (word[len - 4] == 't' || word[len - 4] == 's'))
                    len -= 3;
                break;

            case 'r':
                if (word[len - 2] == 'e') len -= 2; // -er
                break;

            case 's':
                if (SUFF3A ('o', 'u')) {
                    if (len - 3 >= r2) len -= 3;
                    break;
                }
                break;

            case 't':
                if (word[len - 2] == 'n') {
                    if (SUFF5A ('e', 'm', 'e', 'n')) {
                        if (len - 5 >= r2) len -= 5;
                        break;
                    }
                    if (SUFF4A ('m', 'e', 'n')) {
                        if (len - 4 >= r2) len -= 4;
                        break;
                    }
                    if (SUFF3A ('a', 'n')) {
                        if (len - 3 >= r2) len -= 3;
                        break;
                    }
                    if (SUFF3A ('e', 'n')) {
                        if (len - 3 >= r2) len -= 3;
                        break;
                    }
                }
                break;
        }

    //////////
    // STEP 5
    //////////

#ifdef SNOWBALL2011
    if (len > r2 && word[len - 1] == 'l' && word[len - 2] == 'l')
        len--;
    else
#endif
        while (word[len - 1] == 'e') {
            if (len > r2) {
                len--;
                break;
            }
            if (len <= r1)
                break;
            if (len > 3 && !is_vowel (len - 4) && is_vowel (len - 3) && !stem_en_ivwxy(word[len - 2]))
                break;
            if (len == 3 && is_vowel(0) && !is_vowel(1))
                break;
            len--;
            break;
        }
#ifndef SNOWBALL2011
    if ( len>r2 && word[len-1]=='l' && word[len-2]=='l' )
        len--;
#endif

    ////////////
    // FINALIZE
    ////////////

    word[len] = 0;
    if (has_Y)
        for (i = 0; i < len; i++)
            if (word[i] == 'Y')
                word[i] = 'y';
}

