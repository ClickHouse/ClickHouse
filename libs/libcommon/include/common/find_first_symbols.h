#pragma once

#if defined(__x86_64__)
	#include <nmmintrin.h>
#endif


/** Позволяет найти в куске памяти следующий символ содержащийся в symbols...
  * Функция похожа на strpbrk, но со следующими отличиями:
  * - работает с любыми кусками памяти, в том числе, с нулевыми байтами;
  * - не требует нулевого байта в конце - в функцию передаётся конец данных;
  * - в случае, если не найдено, возвращает указатель на конец, а не NULL;
  * - максимальное количество символов для поиска - 16.
  *
  * Использует SSE 2 в случае маленького количества символов для поиска и SSE 4.2 в случае большого,
  *  что даёт прирост скорости более чем в 2 раза по сравнению с тривиальным циклом
  *  при парсинге типичного tab-separated файла со строками.
  * При парсинге файла с короткими строками, падения производительности нет.
  *
  * Замечание: оптимальный порог использования SSE 4.2 зависит от модели процессора.
  */

namespace detail
{

template <char s0>
inline bool is_in(char x)
{
	return x == s0;
}

template <char s0, char s1, char... tail>
inline bool is_in(char x)
{
	return x == s0 || is_in<s1, tail...>(x);
}

template <char s0>
inline __m128i mm_is_in(__m128i bytes)
{
	__m128i eq0 = _mm_cmpeq_epi8(bytes, _mm_set1_epi8(s0));
	return eq0;
}

template <char s0, char s1, char... tail>
inline __m128i mm_is_in(__m128i bytes)
{
	__m128i eq0 = _mm_cmpeq_epi8(bytes, _mm_set1_epi8(s0));
	__m128i eq = mm_is_in<s1, tail...>(bytes);
	return _mm_or_si128(eq0, eq);
}


template <char... symbols>
inline const char * find_first_symbols_sse2(const char * begin, const char * end)
{
#if defined(__x86_64__)
	for (; begin + 15 < end; begin += 16)
	{
		__m128i bytes = _mm_loadu_si128(reinterpret_cast<const __m128i *>(begin));

		__m128i eq = mm_is_in<symbols...>(bytes);

		UInt16 bit_mask = _mm_movemask_epi8(eq);
		if (bit_mask)
			return begin + __builtin_ctz(bit_mask);
	}
#endif

	for (; begin < end; ++begin)
		if (is_in<symbols...>(*begin))
			return begin;
	return end;
}


template <size_t num_chars,
	char c01,     char c02 = 0, char c03 = 0, char c04 = 0,
	char c05 = 0, char c06 = 0, char c07 = 0, char c08 = 0,
	char c09 = 0, char c10 = 0, char c11 = 0, char c12 = 0,
	char c13 = 0, char c14 = 0, char c15 = 0, char c16 = 0>
inline const char * find_first_symbols_sse42_impl(const char * begin, const char * end)
{
#if defined(__x86_64__)
#define MODE (_SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_ANY | _SIDD_LEAST_SIGNIFICANT)
	__m128i set = _mm_setr_epi8(c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16);

	for (; begin + 15 < end; begin += 16)
	{
		__m128i bytes = _mm_loadu_si128(reinterpret_cast<const __m128i *>(begin));

		if (_mm_cmpestrc(set, num_chars, bytes, 16, MODE))
			return begin + _mm_cmpestri(set, num_chars, bytes, 16, MODE);
	}
#undef MODE
#endif

	for (; begin < end; ++begin)
		if (   (num_chars >= 1 && *begin == c01)
			|| (num_chars >= 2 && *begin == c02)
			|| (num_chars >= 3 && *begin == c03)
			|| (num_chars >= 4 && *begin == c04)
			|| (num_chars >= 5 && *begin == c05)
			|| (num_chars >= 6 && *begin == c06)
			|| (num_chars >= 7 && *begin == c07)
			|| (num_chars >= 8 && *begin == c08)
			|| (num_chars >= 9 && *begin == c09)
			|| (num_chars >= 10 && *begin == c10)
			|| (num_chars >= 11 && *begin == c11)
			|| (num_chars >= 12 && *begin == c12)
			|| (num_chars >= 13 && *begin == c13)
			|| (num_chars >= 14 && *begin == c14)
			|| (num_chars >= 15 && *begin == c15)
			|| (num_chars >= 16 && *begin == c16))
			return begin;
	return end;
}


template <char... symbols>
inline const char * find_first_symbols_sse42(const char * begin, const char * end)
{
	return find_first_symbols_sse42_impl<sizeof...(symbols), symbols...>(begin, end);
}

}


template <char... symbols>
inline const char * find_first_symbols(const char * begin, const char * end)
{
	if (sizeof...(symbols) >= 5)
		return detail::find_first_symbols_sse42<symbols...>(begin, end);
	else
		return detail::find_first_symbols_sse2<symbols...>(begin, end);
}
