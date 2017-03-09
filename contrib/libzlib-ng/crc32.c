/* crc32.c -- compute the CRC-32 of a data stream
 * Copyright (C) 1995-2006, 2010, 2011, 2012 Mark Adler
 * For conditions of distribution and use, see copyright notice in zlib.h
 *
 * Thanks to Rodney Brown <rbrown64@csc.com.au> for his contribution of faster
 * CRC methods: exclusive-oring 32 bits of data at a time, and pre-computing
 * tables for updating the shift register in one step with three exclusive-ors
 * instead of four steps with four exclusive-ors.  This results in about a
 * factor of two increase in speed on a Power PC G4 (PPC7455) using gcc -O3.
 */

/* @(#) $Id$ */

#ifdef __MINGW32__
# include <sys/param.h>
#elif defined(WIN32) || defined(_WIN32)
# define LITTLE_ENDIAN 1234
# define BIG_ENDIAN 4321
# if defined(_M_IX86) || defined(_M_AMD64) || defined(_M_IA64)
#  define BYTE_ORDER LITTLE_ENDIAN
# else
#  error Unknown endianness!
# endif
#elif __APPLE__
# include <machine/endian.h>
#elif defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__) || defined(__bsdi__) || defined(__DragonFly__)
# include <sys/endian.h>
#elif defined(__sun) || defined(sun)
# include <sys/byteorder.h>
# if !defined(LITTLE_ENDIAN)
#  define LITTLE_ENDIAN 4321
# endif
# if !defined(BIG_ENDIAN)
#  define BIG_ENDIAN 1234
# endif
# if !defined(BYTE_ORDER)
#  if defined(_BIG_ENDIAN)
#   define BYTE_ORDER BIG_ENDIAN
#  else
#   define BYTE_ORDER LITTLE_ENDIAN
#  endif
# endif
#else
# include <endian.h>
#endif

/*
  Note on the use of DYNAMIC_CRC_TABLE: there is no mutex or semaphore
  protection on the static variables used to control the first-use generation
  of the crc tables.  Therefore, if you #define DYNAMIC_CRC_TABLE, you should
  first call get_crc_table() to initialize the tables before allowing more than
  one thread to use crc32().

  DYNAMIC_CRC_TABLE and MAKECRCH can be #defined to write out crc32.h.
 */

#ifdef MAKECRCH
#  include <stdio.h>
#  ifndef DYNAMIC_CRC_TABLE
#    define DYNAMIC_CRC_TABLE
#  endif /* !DYNAMIC_CRC_TABLE */
#endif /* MAKECRCH */

#include "deflate.h"

#if BYTE_ORDER == LITTLE_ENDIAN
static uint32_t crc32_little(uint32_t, const unsigned char *, z_off64_t);
#elif BYTE_ORDER == BIG_ENDIAN
static uint32_t crc32_big(uint32_t, const unsigned char *, z_off64_t);
#endif

/* Local functions for crc concatenation */
static uint32_t gf2_matrix_times(uint32_t *mat, uint32_t vec);
static void gf2_matrix_square(uint32_t *square, uint32_t *mat);
static uint32_t crc32_combine_(uint32_t crc1, uint32_t crc2, z_off64_t len2);


#ifdef DYNAMIC_CRC_TABLE
static volatile int crc_table_empty = 1;
static uint32_t crc_table[8][256];
static void make_crc_table(void);
#ifdef MAKECRCH
static void write_table(FILE *, const uint32_t *);
#endif /* MAKECRCH */
/*
  Generate tables for a byte-wise 32-bit CRC calculation on the polynomial:
  x^32+x^26+x^23+x^22+x^16+x^12+x^11+x^10+x^8+x^7+x^5+x^4+x^2+x+1.

  Polynomials over GF(2) are represented in binary, one bit per coefficient,
  with the lowest powers in the most significant bit.  Then adding polynomials
  is just exclusive-or, and multiplying a polynomial by x is a right shift by
  one.  If we call the above polynomial p, and represent a byte as the
  polynomial q, also with the lowest power in the most significant bit (so the
  byte 0xb1 is the polynomial x^7+x^3+x+1), then the CRC is (q*x^32) mod p,
  where a mod b means the remainder after dividing a by b.

  This calculation is done using the shift-register method of multiplying and
  taking the remainder.  The register is initialized to zero, and for each
  incoming bit, x^32 is added mod p to the register if the bit is a one (where
  x^32 mod p is p+x^32 = x^26+...+1), and the register is multiplied mod p by
  x (which is shifting right by one and adding x^32 mod p if the bit shifted
  out is a one).  We start with the highest power (least significant bit) of
  q and repeat for all eight bits of q.

  The first table is simply the CRC of all possible eight bit values.  This is
  all the information needed to generate CRCs on data a byte at a time for all
  combinations of CRC register values and incoming bytes.  The remaining tables
  allow for word-at-a-time CRC calculation for both big-endian and little-
  endian machines, where a word is four bytes.
*/
static void make_crc_table() {
    uint32_t c;
    int n, k;
    uint32_t poly;                       /* polynomial exclusive-or pattern */
    /* terms of polynomial defining this crc (except x^32): */
    static volatile int first = 1;      /* flag to limit concurrent making */
    static const unsigned char p[] = {0, 1, 2, 4, 5, 7, 8, 10, 11, 12, 16, 22, 23, 26};

    /* See if another task is already doing this (not thread-safe, but better
       than nothing -- significantly reduces duration of vulnerability in
       case the advice about DYNAMIC_CRC_TABLE is ignored) */
    if (first) {
        first = 0;

        /* make exclusive-or pattern from polynomial (0xedb88320) */
        poly = 0;
        for (n = 0; n < (int)(sizeof(p)/sizeof(unsigned char)); n++)
            poly |= (uint32_t)1 << (31 - p[n]);

        /* generate a crc for every 8-bit value */
        for (n = 0; n < 256; n++) {
            c = (uint32_t)n;
            for (k = 0; k < 8; k++)
                c = c & 1 ? poly ^ (c >> 1) : c >> 1;
            crc_table[0][n] = c;
        }

        /* generate crc for each value followed by one, two, and three zeros,
           and then the byte reversal of those as well as the first table */
        for (n = 0; n < 256; n++) {
            c = crc_table[0][n];
            crc_table[4][n] = ZSWAP32(c);
            for (k = 1; k < 4; k++) {
                c = crc_table[0][c & 0xff] ^ (c >> 8);
                crc_table[k][n] = c;
                crc_table[k + 4][n] = ZSWAP32(c);
            }
        }

        crc_table_empty = 0;
    } else {      /* not first */
        /* wait for the other guy to finish (not efficient, but rare) */
        while (crc_table_empty)
            {}
    }

#ifdef MAKECRCH
    /* write out CRC tables to crc32.h */
    {
        FILE *out;

        out = fopen("crc32.h", "w");
        if (out == NULL) return;
        fprintf(out, "/* crc32.h -- tables for rapid CRC calculation\n");
        fprintf(out, " * Generated automatically by crc32.c\n */\n\n");
        fprintf(out, "static const uint32_t ");
        fprintf(out, "crc_table[8][256] =\n{\n  {\n");
        write_table(out, crc_table[0]);
        for (k = 1; k < 8; k++) {
            fprintf(out, "  },\n  {\n");
            write_table(out, crc_table[k]);
        }
        fprintf(out, "  }\n};\n");
        fclose(out);
    }
#endif /* MAKECRCH */
}

#ifdef MAKECRCH
static void write_table(FILE *out, const uint32_t *table) {
    int n;

    for (n = 0; n < 256; n++)
        fprintf(out, "%s0x%08lx%s", n % 5 ? "" : "    ",
                (uint32_t)(table[n]),
                n == 255 ? "\n" : (n % 5 == 4 ? ",\n" : ", "));
}
#endif /* MAKECRCH */

#else /* !DYNAMIC_CRC_TABLE */
/* ========================================================================
 * Tables of CRC-32s of all single-byte values, made by make_crc_table().
 */
#include "crc32.h"
#endif /* DYNAMIC_CRC_TABLE */

/* =========================================================================
 * This function can be used by asm versions of crc32()
 */
const uint32_t * ZEXPORT get_crc_table(void) {
#ifdef DYNAMIC_CRC_TABLE
    if (crc_table_empty)
        make_crc_table();
#endif /* DYNAMIC_CRC_TABLE */
    return (const uint32_t *)crc_table;
}

/* ========================================================================= */
#define DO1 crc = crc_table[0][((int)crc ^ (*buf++)) & 0xff] ^ (crc >> 8)
#define DO8 DO1; DO1; DO1; DO1; DO1; DO1; DO1; DO1
#define DO4 DO1; DO1; DO1; DO1

/* ========================================================================= */
uint32_t ZEXPORT crc32(uint32_t crc, const unsigned char *buf, z_off64_t len) {
    if (buf == Z_NULL) return 0;

#ifdef DYNAMIC_CRC_TABLE
    if (crc_table_empty)
        make_crc_table();
#endif /* DYNAMIC_CRC_TABLE */

    if (sizeof(void *) == sizeof(ptrdiff_t)) {
#if BYTE_ORDER == LITTLE_ENDIAN
        return crc32_little(crc, buf, len);
#elif BYTE_ORDER == BIG_ENDIAN
        return crc32_big(crc, buf, len);
#endif
    }
    crc = crc ^ 0xffffffff;

#ifdef UNROLL_LESS
    while (len >= 4) {
        DO4;
        len -= 4;
    }
#else
    while (len >= 8) {
        DO8;
        len -= 8;
    }
#endif

    if (len) do {
        DO1;
    } while (--len);
    return crc ^ 0xffffffff;
}


/* ========================================================================= */
#if BYTE_ORDER == LITTLE_ENDIAN
#define DOLIT4 c ^= *buf4++; \
        c = crc_table[3][c & 0xff] ^ crc_table[2][(c >> 8) & 0xff] ^ \
            crc_table[1][(c >> 16) & 0xff] ^ crc_table[0][c >> 24]
#define DOLIT32 DOLIT4; DOLIT4; DOLIT4; DOLIT4; DOLIT4; DOLIT4; DOLIT4; DOLIT4

/* ========================================================================= */
static uint32_t crc32_little(uint32_t crc, const unsigned char *buf, z_off64_t len) {
    register uint32_t c;
    register const uint32_t *buf4;

    c = crc;
    c = ~c;
    while (len && ((ptrdiff_t)buf & 3)) {
        c = crc_table[0][(c ^ *buf++) & 0xff] ^ (c >> 8);
        len--;
    }

    buf4 = (const uint32_t *)(const void *)buf;

#ifndef UNROLL_LESS
    while (len >= 32) {
        DOLIT32;
        len -= 32;
    }
#endif

    while (len >= 4) {
        DOLIT4;
        len -= 4;
    }
    buf = (const unsigned char *)buf4;

    if (len) do {
        c = crc_table[0][(c ^ *buf++) & 0xff] ^ (c >> 8);
    } while (--len);
    c = ~c;
    return c;
}
#endif /* BYTE_ORDER == LITTLE_ENDIAN */

/* ========================================================================= */
#if BYTE_ORDER == BIG_ENDIAN
#define DOBIG4 c ^= *++buf4; \
        c = crc_table[4][c & 0xff] ^ crc_table[5][(c >> 8) & 0xff] ^ \
            crc_table[6][(c >> 16) & 0xff] ^ crc_table[7][c >> 24]
#define DOBIG32 DOBIG4; DOBIG4; DOBIG4; DOBIG4; DOBIG4; DOBIG4; DOBIG4; DOBIG4

/* ========================================================================= */
static uint32_t crc32_big(uint32_t crc, const unsigned char *buf, z_off64_t len) {
    register uint32_t c;
    register const uint32_t *buf4;

    c = ZSWAP32(crc);
    c = ~c;
    while (len && ((ptrdiff_t)buf & 3)) {
        c = crc_table[4][(c >> 24) ^ *buf++] ^ (c << 8);
        len--;
    }

    buf4 = (const uint32_t *)(const void *)buf;
    buf4--;

#ifndef UNROLL_LESS
    while (len >= 32) {
        DOBIG32;
        len -= 32;
    }
#endif

    while (len >= 4) {
        DOBIG4;
        len -= 4;
    }
    buf4++;
    buf = (const unsigned char *)buf4;

    if (len) do {
        c = crc_table[4][(c >> 24) ^ *buf++] ^ (c << 8);
    } while (--len);
    c = ~c;
    return ZSWAP32(c);
}
#endif /* BYTE_ORDER == BIG_ENDIAN */


#define GF2_DIM 32      /* dimension of GF(2) vectors (length of CRC) */

/* ========================================================================= */
static uint32_t gf2_matrix_times(uint32_t *mat, uint32_t vec) {
    uint32_t sum;

    sum = 0;
    while (vec) {
        if (vec & 1)
            sum ^= *mat;
        vec >>= 1;
        mat++;
    }
    return sum;
}

/* ========================================================================= */
static void gf2_matrix_square(uint32_t *square, uint32_t *mat) {
    int n;

    for (n = 0; n < GF2_DIM; n++)
        square[n] = gf2_matrix_times(mat, mat[n]);
}

/* ========================================================================= */
static uint32_t crc32_combine_(uint32_t crc1, uint32_t crc2, z_off64_t len2) {
    int n;
    uint32_t row;
    uint32_t even[GF2_DIM];    /* even-power-of-two zeros operator */
    uint32_t odd[GF2_DIM];     /* odd-power-of-two zeros operator */

    /* degenerate case (also disallow negative lengths) */
    if (len2 <= 0)
        return crc1;

    /* put operator for one zero bit in odd */
    odd[0] = 0xedb88320;          /* CRC-32 polynomial */
    row = 1;
    for (n = 1; n < GF2_DIM; n++) {
        odd[n] = row;
        row <<= 1;
    }

    /* put operator for two zero bits in even */
    gf2_matrix_square(even, odd);

    /* put operator for four zero bits in odd */
    gf2_matrix_square(odd, even);

    /* apply len2 zeros to crc1 (first square will put the operator for one
       zero byte, eight zero bits, in even) */
    do {
        /* apply zeros operator for this bit of len2 */
        gf2_matrix_square(even, odd);
        if (len2 & 1)
            crc1 = gf2_matrix_times(even, crc1);
        len2 >>= 1;

        /* if no more bits set, then done */
        if (len2 == 0)
            break;

        /* another iteration of the loop with odd and even swapped */
        gf2_matrix_square(odd, even);
        if (len2 & 1)
            crc1 = gf2_matrix_times(odd, crc1);
        len2 >>= 1;

        /* if no more bits set, then done */
    } while (len2 != 0);

    /* return combined crc */
    crc1 ^= crc2;
    return crc1;
}

/* ========================================================================= */
uint32_t ZEXPORT crc32_combine(uint32_t crc1, uint32_t crc2, z_off_t len2) {
    return crc32_combine_(crc1, crc2, len2);
}

uint32_t ZEXPORT crc32_combine64(uint32_t crc1, uint32_t crc2, z_off64_t len2) {
    return crc32_combine_(crc1, crc2, len2);
}


#ifdef X86_PCLMULQDQ_CRC
#include "arch/x86/x86.h"
extern void ZLIB_INTERNAL crc_fold_init(deflate_state *const s);
extern void ZLIB_INTERNAL crc_fold_copy(deflate_state *const s,
    unsigned char *dst, const unsigned char *src, long len);
extern uint32_t ZLIB_INTERNAL crc_fold_512to32(deflate_state *const s);
#endif

ZLIB_INTERNAL void crc_reset(deflate_state *const s) {
#ifdef X86_PCLMULQDQ_CRC
    if (x86_cpu_has_pclmulqdq) {
        crc_fold_init(s);
        return;
    }
#endif
    s->strm->adler = crc32(0L, Z_NULL, 0);
}

ZLIB_INTERNAL void crc_finalize(deflate_state *const s) {
#ifdef X86_PCLMULQDQ_CRC
    if (x86_cpu_has_pclmulqdq)
        s->strm->adler = crc_fold_512to32(s);
#endif
}

ZLIB_INTERNAL void copy_with_crc(z_stream *strm, unsigned char *dst, long size) {
#ifdef X86_PCLMULQDQ_CRC
    if (x86_cpu_has_pclmulqdq) {
        crc_fold_copy(strm->state, dst, strm->next_in, size);
        return;
    }
#endif
    memcpy(dst, strm->next_in, size);
    strm->adler = crc32(strm->adler, dst, size);
}

