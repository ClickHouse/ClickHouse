//=====================================================================
//
// FastMemcpy.c - skywind3000@163.com, 2015
//
// feature:
// 50% speed up in avg. vs standard memcpy (tested in vc2012/gcc5.1)
//
//=====================================================================
#ifndef __FAST_MEMCPY_H__
#define __FAST_MEMCPY_H__

#include <stddef.h>
#include <stdint.h>
#include <emmintrin.h>


//---------------------------------------------------------------------
// force inline for compilers
//---------------------------------------------------------------------
#ifndef INLINE
#ifdef __GNUC__
#if (__GNUC__ > 3) || ((__GNUC__ == 3) && (__GNUC_MINOR__ >= 1))
    #define INLINE         __inline__ __attribute__((always_inline))
#else
    #define INLINE         __inline__
#endif
#elif defined(_MSC_VER)
	#define INLINE __forceinline
#elif (defined(__BORLANDC__) || defined(__WATCOMC__))
    #define INLINE __inline
#else
    #define INLINE
#endif
#endif

typedef __attribute__((__aligned__(1))) uint16_t uint16_unaligned_t;
typedef __attribute__((__aligned__(1))) uint32_t uint32_unaligned_t;
typedef __attribute__((__aligned__(1))) uint64_t uint64_unaligned_t;

//---------------------------------------------------------------------
// fast copy for different sizes
//---------------------------------------------------------------------
static INLINE void memcpy_sse2_16(void *dst, const void *src) {
	__m128i m0 = _mm_loadu_si128(((const __m128i*)src) + 0);
	_mm_storeu_si128(((__m128i*)dst) + 0, m0);
}

static INLINE void memcpy_sse2_32(void *dst, const void *src) {
	__m128i m0 = _mm_loadu_si128(((const __m128i*)src) + 0);
	__m128i m1 = _mm_loadu_si128(((const __m128i*)src) + 1);
	_mm_storeu_si128(((__m128i*)dst) + 0, m0);
	_mm_storeu_si128(((__m128i*)dst) + 1, m1);
}

static INLINE void memcpy_sse2_64(void *dst, const void *src) {
	__m128i m0 = _mm_loadu_si128(((const __m128i*)src) + 0);
	__m128i m1 = _mm_loadu_si128(((const __m128i*)src) + 1);
	__m128i m2 = _mm_loadu_si128(((const __m128i*)src) + 2);
	__m128i m3 = _mm_loadu_si128(((const __m128i*)src) + 3);
	_mm_storeu_si128(((__m128i*)dst) + 0, m0);
	_mm_storeu_si128(((__m128i*)dst) + 1, m1);
	_mm_storeu_si128(((__m128i*)dst) + 2, m2);
	_mm_storeu_si128(((__m128i*)dst) + 3, m3);
}

static INLINE void memcpy_sse2_128(void *dst, const void *src) {
	__m128i m0 = _mm_loadu_si128(((const __m128i*)src) + 0);
	__m128i m1 = _mm_loadu_si128(((const __m128i*)src) + 1);
	__m128i m2 = _mm_loadu_si128(((const __m128i*)src) + 2);
	__m128i m3 = _mm_loadu_si128(((const __m128i*)src) + 3);
	__m128i m4 = _mm_loadu_si128(((const __m128i*)src) + 4);
	__m128i m5 = _mm_loadu_si128(((const __m128i*)src) + 5);
	__m128i m6 = _mm_loadu_si128(((const __m128i*)src) + 6);
	__m128i m7 = _mm_loadu_si128(((const __m128i*)src) + 7);
	_mm_storeu_si128(((__m128i*)dst) + 0, m0);
	_mm_storeu_si128(((__m128i*)dst) + 1, m1);
	_mm_storeu_si128(((__m128i*)dst) + 2, m2);
	_mm_storeu_si128(((__m128i*)dst) + 3, m3);
	_mm_storeu_si128(((__m128i*)dst) + 4, m4);
	_mm_storeu_si128(((__m128i*)dst) + 5, m5);
	_mm_storeu_si128(((__m128i*)dst) + 6, m6);
	_mm_storeu_si128(((__m128i*)dst) + 7, m7);
}


//---------------------------------------------------------------------
// tiny memory copy with jump table optimized
//---------------------------------------------------------------------
/// Attribute is used to avoid an error with undefined behaviour sanitizer
/// ../contrib/FastMemcpy/FastMemcpy.h:91:56: runtime error: applying zero offset to null pointer
/// Found by 01307_orc_output_format.sh, cause - ORCBlockInputFormat and external ORC library.
__attribute__((__no_sanitize__("undefined"))) static INLINE void *memcpy_tiny(void *dst, const void *src, size_t size) {
	unsigned char *dd = ((unsigned char*)dst) + size;
	const unsigned char *ss = ((const unsigned char*)src) + size;

	switch (size) {
	case 64:
		memcpy_sse2_64(dd - 64, ss - 64);
	case 0:
		break;

	case 65:
		memcpy_sse2_64(dd - 65, ss - 65);
	case 1:
		dd[-1] = ss[-1];
		break;

	case 66:
		memcpy_sse2_64(dd - 66, ss - 66);
	case 2:
		*((uint16_unaligned_t*)(dd - 2)) = *((uint16_unaligned_t*)(ss - 2));
		break;

	case 67:
		memcpy_sse2_64(dd - 67, ss - 67);
	case 3:
		*((uint16_unaligned_t*)(dd - 3)) = *((uint16_unaligned_t*)(ss - 3));
		dd[-1] = ss[-1];
		break;

	case 68:
		memcpy_sse2_64(dd - 68, ss - 68);
	case 4:
		*((uint32_unaligned_t*)(dd - 4)) = *((uint32_unaligned_t*)(ss - 4));
		break;

	case 69:
		memcpy_sse2_64(dd - 69, ss - 69);
	case 5:
		*((uint32_unaligned_t*)(dd - 5)) = *((uint32_unaligned_t*)(ss - 5));
		dd[-1] = ss[-1];
		break;

	case 70:
		memcpy_sse2_64(dd - 70, ss - 70);
	case 6:
		*((uint32_unaligned_t*)(dd - 6)) = *((uint32_unaligned_t*)(ss - 6));
		*((uint16_unaligned_t*)(dd - 2)) = *((uint16_unaligned_t*)(ss - 2));
		break;

	case 71:
		memcpy_sse2_64(dd - 71, ss - 71);
	case 7:
		*((uint32_unaligned_t*)(dd - 7)) = *((uint32_unaligned_t*)(ss - 7));
		*((uint32_unaligned_t*)(dd - 4)) = *((uint32_unaligned_t*)(ss - 4));
		break;

	case 72:
		memcpy_sse2_64(dd - 72, ss - 72);
	case 8:
		*((uint64_unaligned_t*)(dd - 8)) = *((uint64_unaligned_t*)(ss - 8));
		break;

	case 73:
		memcpy_sse2_64(dd - 73, ss - 73);
	case 9:
		*((uint64_unaligned_t*)(dd - 9)) = *((uint64_unaligned_t*)(ss - 9));
		dd[-1] = ss[-1];
		break;

	case 74:
		memcpy_sse2_64(dd - 74, ss - 74);
	case 10:
		*((uint64_unaligned_t*)(dd - 10)) = *((uint64_unaligned_t*)(ss - 10));
		*((uint16_unaligned_t*)(dd - 2)) = *((uint16_unaligned_t*)(ss - 2));
		break;

	case 75:
		memcpy_sse2_64(dd - 75, ss - 75);
	case 11:
		*((uint64_unaligned_t*)(dd - 11)) = *((uint64_unaligned_t*)(ss - 11));
		*((uint32_unaligned_t*)(dd - 4)) = *((uint32_unaligned_t*)(ss - 4));
		break;

	case 76:
		memcpy_sse2_64(dd - 76, ss - 76);
	case 12:
		*((uint64_unaligned_t*)(dd - 12)) = *((uint64_unaligned_t*)(ss - 12));
		*((uint32_unaligned_t*)(dd - 4)) = *((uint32_unaligned_t*)(ss - 4));
		break;

	case 77:
		memcpy_sse2_64(dd - 77, ss - 77);
	case 13:
		*((uint64_unaligned_t*)(dd - 13)) = *((uint64_unaligned_t*)(ss - 13));
		*((uint32_unaligned_t*)(dd - 5)) = *((uint32_unaligned_t*)(ss - 5));
		dd[-1] = ss[-1];
		break;

	case 78:
		memcpy_sse2_64(dd - 78, ss - 78);
	case 14:
		*((uint64_unaligned_t*)(dd - 14)) = *((uint64_unaligned_t*)(ss - 14));
		*((uint64_unaligned_t*)(dd - 8)) = *((uint64_unaligned_t*)(ss - 8));
		break;

	case 79:
		memcpy_sse2_64(dd - 79, ss - 79);
	case 15:
		*((uint64_unaligned_t*)(dd - 15)) = *((uint64_unaligned_t*)(ss - 15));
		*((uint64_unaligned_t*)(dd - 8)) = *((uint64_unaligned_t*)(ss - 8));
		break;

	case 80:
		memcpy_sse2_64(dd - 80, ss - 80);
	case 16:
		memcpy_sse2_16(dd - 16, ss - 16);
		break;

	case 81:
		memcpy_sse2_64(dd - 81, ss - 81);
	case 17:
		memcpy_sse2_16(dd - 17, ss - 17);
		dd[-1] = ss[-1];
		break;

	case 82:
		memcpy_sse2_64(dd - 82, ss - 82);
	case 18:
		memcpy_sse2_16(dd - 18, ss - 18);
		*((uint16_unaligned_t*)(dd - 2)) = *((uint16_unaligned_t*)(ss - 2));
		break;

	case 83:
		memcpy_sse2_64(dd - 83, ss - 83);
	case 19:
		memcpy_sse2_16(dd - 19, ss - 19);
		*((uint16_unaligned_t*)(dd - 3)) = *((uint16_unaligned_t*)(ss - 3));
		dd[-1] = ss[-1];
		break;

	case 84:
		memcpy_sse2_64(dd - 84, ss - 84);
	case 20:
		memcpy_sse2_16(dd - 20, ss - 20);
		*((uint32_unaligned_t*)(dd - 4)) = *((uint32_unaligned_t*)(ss - 4));
		break;

	case 85:
		memcpy_sse2_64(dd - 85, ss - 85);
	case 21:
		memcpy_sse2_16(dd - 21, ss - 21);
		*((uint32_unaligned_t*)(dd - 5)) = *((uint32_unaligned_t*)(ss - 5));
		dd[-1] = ss[-1];
		break;

	case 86:
		memcpy_sse2_64(dd - 86, ss - 86);
	case 22:
		memcpy_sse2_16(dd - 22, ss - 22);
		*((uint32_unaligned_t*)(dd - 6)) = *((uint32_unaligned_t*)(ss - 6));
		*((uint16_unaligned_t*)(dd - 2)) = *((uint16_unaligned_t*)(ss - 2));
		break;

	case 87:
		memcpy_sse2_64(dd - 87, ss - 87);
	case 23:
		memcpy_sse2_16(dd - 23, ss - 23);
		*((uint32_unaligned_t*)(dd - 7)) = *((uint32_unaligned_t*)(ss - 7));
		*((uint32_unaligned_t*)(dd - 4)) = *((uint32_unaligned_t*)(ss - 4));
		break;

	case 88:
		memcpy_sse2_64(dd - 88, ss - 88);
	case 24:
		memcpy_sse2_16(dd - 24, ss - 24);
		memcpy_sse2_16(dd - 16, ss - 16);
		break;

	case 89:
		memcpy_sse2_64(dd - 89, ss - 89);
	case 25:
		memcpy_sse2_16(dd - 25, ss - 25);
		memcpy_sse2_16(dd - 16, ss - 16);
		break;

	case 90:
		memcpy_sse2_64(dd - 90, ss - 90);
	case 26:
		memcpy_sse2_16(dd - 26, ss - 26);
		memcpy_sse2_16(dd - 16, ss - 16);
		break;

	case 91:
		memcpy_sse2_64(dd - 91, ss - 91);
	case 27:
		memcpy_sse2_16(dd - 27, ss - 27);
		memcpy_sse2_16(dd - 16, ss - 16);
		break;

	case 92:
		memcpy_sse2_64(dd - 92, ss - 92);
	case 28:
		memcpy_sse2_16(dd - 28, ss - 28);
		memcpy_sse2_16(dd - 16, ss - 16);
		break;

	case 93:
		memcpy_sse2_64(dd - 93, ss - 93);
	case 29:
		memcpy_sse2_16(dd - 29, ss - 29);
		memcpy_sse2_16(dd - 16, ss - 16);
		break;

	case 94:
		memcpy_sse2_64(dd - 94, ss - 94);
	case 30:
		memcpy_sse2_16(dd - 30, ss - 30);
		memcpy_sse2_16(dd - 16, ss - 16);
		break;

	case 95:
		memcpy_sse2_64(dd - 95, ss - 95);
	case 31:
		memcpy_sse2_16(dd - 31, ss - 31);
		memcpy_sse2_16(dd - 16, ss - 16);
		break;

	case 96:
		memcpy_sse2_64(dd - 96, ss - 96);
	case 32:
		memcpy_sse2_32(dd - 32, ss - 32);
		break;

	case 97:
		memcpy_sse2_64(dd - 97, ss - 97);
	case 33:
		memcpy_sse2_32(dd - 33, ss - 33);
		dd[-1] = ss[-1];
		break;

	case 98:
		memcpy_sse2_64(dd - 98, ss - 98);
	case 34:
		memcpy_sse2_32(dd - 34, ss - 34);
		*((uint16_unaligned_t*)(dd - 2)) = *((uint16_unaligned_t*)(ss - 2));
		break;

	case 99:
		memcpy_sse2_64(dd - 99, ss - 99);
	case 35:
		memcpy_sse2_32(dd - 35, ss - 35);
		*((uint16_unaligned_t*)(dd - 3)) = *((uint16_unaligned_t*)(ss - 3));
		dd[-1] = ss[-1];
		break;

	case 100:
		memcpy_sse2_64(dd - 100, ss - 100);
	case 36:
		memcpy_sse2_32(dd - 36, ss - 36);
		*((uint32_unaligned_t*)(dd - 4)) = *((uint32_unaligned_t*)(ss - 4));
		break;

	case 101:
		memcpy_sse2_64(dd - 101, ss - 101);
	case 37:
		memcpy_sse2_32(dd - 37, ss - 37);
		*((uint32_unaligned_t*)(dd - 5)) = *((uint32_unaligned_t*)(ss - 5));
		dd[-1] = ss[-1];
		break;

	case 102:
		memcpy_sse2_64(dd - 102, ss - 102);
	case 38:
		memcpy_sse2_32(dd - 38, ss - 38);
		*((uint32_unaligned_t*)(dd - 6)) = *((uint32_unaligned_t*)(ss - 6));
		*((uint16_unaligned_t*)(dd - 2)) = *((uint16_unaligned_t*)(ss - 2));
		break;

	case 103:
		memcpy_sse2_64(dd - 103, ss - 103);
	case 39:
		memcpy_sse2_32(dd - 39, ss - 39);
		*((uint32_unaligned_t*)(dd - 7)) = *((uint32_unaligned_t*)(ss - 7));
		*((uint32_unaligned_t*)(dd - 4)) = *((uint32_unaligned_t*)(ss - 4));
		break;

	case 104:
		memcpy_sse2_64(dd - 104, ss - 104);
	case 40:
		memcpy_sse2_32(dd - 40, ss - 40);
		*((uint64_unaligned_t*)(dd - 8)) = *((uint64_unaligned_t*)(ss - 8));
		break;

	case 105:
		memcpy_sse2_64(dd - 105, ss - 105);
	case 41:
		memcpy_sse2_32(dd - 41, ss - 41);
		*((uint64_unaligned_t*)(dd - 9)) = *((uint64_unaligned_t*)(ss - 9));
		dd[-1] = ss[-1];
		break;

	case 106:
		memcpy_sse2_64(dd - 106, ss - 106);
	case 42:
		memcpy_sse2_32(dd - 42, ss - 42);
		*((uint64_unaligned_t*)(dd - 10)) = *((uint64_unaligned_t*)(ss - 10));
		*((uint16_unaligned_t*)(dd - 2)) = *((uint16_unaligned_t*)(ss - 2));
		break;

	case 107:
		memcpy_sse2_64(dd - 107, ss - 107);
	case 43:
		memcpy_sse2_32(dd - 43, ss - 43);
		*((uint64_unaligned_t*)(dd - 11)) = *((uint64_unaligned_t*)(ss - 11));
		*((uint32_unaligned_t*)(dd - 4)) = *((uint32_unaligned_t*)(ss - 4));
		break;

	case 108:
		memcpy_sse2_64(dd - 108, ss - 108);
	case 44:
		memcpy_sse2_32(dd - 44, ss - 44);
		*((uint64_unaligned_t*)(dd - 12)) = *((uint64_unaligned_t*)(ss - 12));
		*((uint32_unaligned_t*)(dd - 4)) = *((uint32_unaligned_t*)(ss - 4));
		break;

	case 109:
		memcpy_sse2_64(dd - 109, ss - 109);
	case 45:
		memcpy_sse2_32(dd - 45, ss - 45);
		*((uint64_unaligned_t*)(dd - 13)) = *((uint64_unaligned_t*)(ss - 13));
		*((uint32_unaligned_t*)(dd - 5)) = *((uint32_unaligned_t*)(ss - 5));
		dd[-1] = ss[-1];
		break;

	case 110:
		memcpy_sse2_64(dd - 110, ss - 110);
	case 46:
		memcpy_sse2_32(dd - 46, ss - 46);
		*((uint64_unaligned_t*)(dd - 14)) = *((uint64_unaligned_t*)(ss - 14));
		*((uint64_unaligned_t*)(dd - 8)) = *((uint64_unaligned_t*)(ss - 8));
		break;

	case 111:
		memcpy_sse2_64(dd - 111, ss - 111);
	case 47:
		memcpy_sse2_32(dd - 47, ss - 47);
		*((uint64_unaligned_t*)(dd - 15)) = *((uint64_unaligned_t*)(ss - 15));
		*((uint64_unaligned_t*)(dd - 8)) = *((uint64_unaligned_t*)(ss - 8));
		break;

	case 112:
		memcpy_sse2_64(dd - 112, ss - 112);
	case 48:
		memcpy_sse2_32(dd - 48, ss - 48);
		memcpy_sse2_16(dd - 16, ss - 16);
		break;

	case 113:
		memcpy_sse2_64(dd - 113, ss - 113);
	case 49:
		memcpy_sse2_32(dd - 49, ss - 49);
		memcpy_sse2_16(dd - 17, ss - 17);
		dd[-1] = ss[-1];
		break;

	case 114:
		memcpy_sse2_64(dd - 114, ss - 114);
	case 50:
		memcpy_sse2_32(dd - 50, ss - 50);
		memcpy_sse2_16(dd - 18, ss - 18);
		*((uint16_unaligned_t*)(dd - 2)) = *((uint16_unaligned_t*)(ss - 2));
		break;

	case 115:
		memcpy_sse2_64(dd - 115, ss - 115);
	case 51:
		memcpy_sse2_32(dd - 51, ss - 51);
		memcpy_sse2_16(dd - 19, ss - 19);
		*((uint16_unaligned_t*)(dd - 3)) = *((uint16_unaligned_t*)(ss - 3));
		dd[-1] = ss[-1];
		break;

	case 116:
		memcpy_sse2_64(dd - 116, ss - 116);
	case 52:
		memcpy_sse2_32(dd - 52, ss - 52);
		memcpy_sse2_16(dd - 20, ss - 20);
		*((uint32_unaligned_t*)(dd - 4)) = *((uint32_unaligned_t*)(ss - 4));
		break;

	case 117:
		memcpy_sse2_64(dd - 117, ss - 117);
	case 53:
		memcpy_sse2_32(dd - 53, ss - 53);
		memcpy_sse2_16(dd - 21, ss - 21);
		*((uint32_unaligned_t*)(dd - 5)) = *((uint32_unaligned_t*)(ss - 5));
		dd[-1] = ss[-1];
		break;

	case 118:
		memcpy_sse2_64(dd - 118, ss - 118);
	case 54:
		memcpy_sse2_32(dd - 54, ss - 54);
		memcpy_sse2_16(dd - 22, ss - 22);
		*((uint32_unaligned_t*)(dd - 6)) = *((uint32_unaligned_t*)(ss - 6));
		*((uint16_unaligned_t*)(dd - 2)) = *((uint16_unaligned_t*)(ss - 2));
		break;

	case 119:
		memcpy_sse2_64(dd - 119, ss - 119);
	case 55:
		memcpy_sse2_32(dd - 55, ss - 55);
		memcpy_sse2_16(dd - 23, ss - 23);
		*((uint32_unaligned_t*)(dd - 7)) = *((uint32_unaligned_t*)(ss - 7));
		*((uint32_unaligned_t*)(dd - 4)) = *((uint32_unaligned_t*)(ss - 4));
		break;

	case 120:
		memcpy_sse2_64(dd - 120, ss - 120);
	case 56:
		memcpy_sse2_32(dd - 56, ss - 56);
		memcpy_sse2_16(dd - 24, ss - 24);
		memcpy_sse2_16(dd - 16, ss - 16);
		break;

	case 121:
		memcpy_sse2_64(dd - 121, ss - 121);
	case 57:
		memcpy_sse2_32(dd - 57, ss - 57);
		memcpy_sse2_16(dd - 25, ss - 25);
		memcpy_sse2_16(dd - 16, ss - 16);
		break;

	case 122:
		memcpy_sse2_64(dd - 122, ss - 122);
	case 58:
		memcpy_sse2_32(dd - 58, ss - 58);
		memcpy_sse2_16(dd - 26, ss - 26);
		memcpy_sse2_16(dd - 16, ss - 16);
		break;

	case 123:
		memcpy_sse2_64(dd - 123, ss - 123);
	case 59:
		memcpy_sse2_32(dd - 59, ss - 59);
		memcpy_sse2_16(dd - 27, ss - 27);
		memcpy_sse2_16(dd - 16, ss - 16);
		break;

	case 124:
		memcpy_sse2_64(dd - 124, ss - 124);
	case 60:
		memcpy_sse2_32(dd - 60, ss - 60);
		memcpy_sse2_16(dd - 28, ss - 28);
		memcpy_sse2_16(dd - 16, ss - 16);
		break;

	case 125:
		memcpy_sse2_64(dd - 125, ss - 125);
	case 61:
		memcpy_sse2_32(dd - 61, ss - 61);
		memcpy_sse2_16(dd - 29, ss - 29);
		memcpy_sse2_16(dd - 16, ss - 16);
		break;

	case 126:
		memcpy_sse2_64(dd - 126, ss - 126);
	case 62:
		memcpy_sse2_32(dd - 62, ss - 62);
		memcpy_sse2_16(dd - 30, ss - 30);
		memcpy_sse2_16(dd - 16, ss - 16);
		break;

	case 127:
		memcpy_sse2_64(dd - 127, ss - 127);
	case 63:
		memcpy_sse2_32(dd - 63, ss - 63);
		memcpy_sse2_16(dd - 31, ss - 31);
		memcpy_sse2_16(dd - 16, ss - 16);
		break;

	case 128:
		memcpy_sse2_128(dd - 128, ss - 128);
		break;
	}

	return dst;
}


//---------------------------------------------------------------------
// main routine
//---------------------------------------------------------------------
static void* memcpy_fast(void *destination, const void *source, size_t size)
{
	unsigned char *dst = (unsigned char*)destination;
	const unsigned char *src = (const unsigned char*)source;
	static size_t cachesize = 0x200000; // L2-cache size
	size_t padding;

	// small memory copy
	if (size <= 128) {
		return memcpy_tiny(dst, src, size);
	}

	// align destination to 16 bytes boundary
	padding = (16 - (((size_t)dst) & 15)) & 15;

	if (padding > 0) {
		__m128i head = _mm_loadu_si128((const __m128i*)src);
		_mm_storeu_si128((__m128i*)dst, head);
		dst += padding;
		src += padding;
		size -= padding;
	}

	// medium size copy
	if (size <= cachesize) {
		__m128i c0, c1, c2, c3, c4, c5, c6, c7;

		for (; size >= 128; size -= 128) {
			c0 = _mm_loadu_si128(((const __m128i*)src) + 0);
			c1 = _mm_loadu_si128(((const __m128i*)src) + 1);
			c2 = _mm_loadu_si128(((const __m128i*)src) + 2);
			c3 = _mm_loadu_si128(((const __m128i*)src) + 3);
			c4 = _mm_loadu_si128(((const __m128i*)src) + 4);
			c5 = _mm_loadu_si128(((const __m128i*)src) + 5);
			c6 = _mm_loadu_si128(((const __m128i*)src) + 6);
			c7 = _mm_loadu_si128(((const __m128i*)src) + 7);
			_mm_prefetch((const char*)(src + 256), _MM_HINT_NTA);
			src += 128;
			_mm_store_si128((((__m128i*)dst) + 0), c0);
			_mm_store_si128((((__m128i*)dst) + 1), c1);
			_mm_store_si128((((__m128i*)dst) + 2), c2);
			_mm_store_si128((((__m128i*)dst) + 3), c3);
			_mm_store_si128((((__m128i*)dst) + 4), c4);
			_mm_store_si128((((__m128i*)dst) + 5), c5);
			_mm_store_si128((((__m128i*)dst) + 6), c6);
			_mm_store_si128((((__m128i*)dst) + 7), c7);
			dst += 128;
		}
	}
	else {		// big memory copy
		__m128i c0, c1, c2, c3, c4, c5, c6, c7;

		_mm_prefetch((const char*)(src), _MM_HINT_NTA);

		if ((((size_t)src) & 15) == 0) {	// source aligned
			for (; size >= 128; size -= 128) {
				c0 = _mm_load_si128(((const __m128i*)src) + 0);
				c1 = _mm_load_si128(((const __m128i*)src) + 1);
				c2 = _mm_load_si128(((const __m128i*)src) + 2);
				c3 = _mm_load_si128(((const __m128i*)src) + 3);
				c4 = _mm_load_si128(((const __m128i*)src) + 4);
				c5 = _mm_load_si128(((const __m128i*)src) + 5);
				c6 = _mm_load_si128(((const __m128i*)src) + 6);
				c7 = _mm_load_si128(((const __m128i*)src) + 7);
				_mm_prefetch((const char*)(src + 256), _MM_HINT_NTA);
				src += 128;
				_mm_stream_si128((((__m128i*)dst) + 0), c0);
				_mm_stream_si128((((__m128i*)dst) + 1), c1);
				_mm_stream_si128((((__m128i*)dst) + 2), c2);
				_mm_stream_si128((((__m128i*)dst) + 3), c3);
				_mm_stream_si128((((__m128i*)dst) + 4), c4);
				_mm_stream_si128((((__m128i*)dst) + 5), c5);
				_mm_stream_si128((((__m128i*)dst) + 6), c6);
				_mm_stream_si128((((__m128i*)dst) + 7), c7);
				dst += 128;
			}
		}
		else {							// source unaligned
			for (; size >= 128; size -= 128) {
				c0 = _mm_loadu_si128(((const __m128i*)src) + 0);
				c1 = _mm_loadu_si128(((const __m128i*)src) + 1);
				c2 = _mm_loadu_si128(((const __m128i*)src) + 2);
				c3 = _mm_loadu_si128(((const __m128i*)src) + 3);
				c4 = _mm_loadu_si128(((const __m128i*)src) + 4);
				c5 = _mm_loadu_si128(((const __m128i*)src) + 5);
				c6 = _mm_loadu_si128(((const __m128i*)src) + 6);
				c7 = _mm_loadu_si128(((const __m128i*)src) + 7);
				_mm_prefetch((const char*)(src + 256), _MM_HINT_NTA);
				src += 128;
				_mm_stream_si128((((__m128i*)dst) + 0), c0);
				_mm_stream_si128((((__m128i*)dst) + 1), c1);
				_mm_stream_si128((((__m128i*)dst) + 2), c2);
				_mm_stream_si128((((__m128i*)dst) + 3), c3);
				_mm_stream_si128((((__m128i*)dst) + 4), c4);
				_mm_stream_si128((((__m128i*)dst) + 5), c5);
				_mm_stream_si128((((__m128i*)dst) + 6), c6);
				_mm_stream_si128((((__m128i*)dst) + 7), c7);
				dst += 128;
			}
		}
		_mm_sfence();
	}

	memcpy_tiny(dst, src, size);

	return destination;
}


#endif
