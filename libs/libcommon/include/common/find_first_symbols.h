#pragma once

#if defined(__x86_64__)
	#include <emmintrin.h>
#endif

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

}

/** Позволяет найти в куске памяти следующий символ содержащийся в symbols...
  * Функция похожа на strpbrk, но со следующими отличиями:
  * - работает с любыми кусками памяти, в том числе, с нулевыми байтами;
  * - не требует нулевого байта в конце - в функцию передаётся конец данных;
  * - в случае, если не найдено, возвращает указатель на конец, а не NULL.
  *
  * Использует SSE2, что даёт прирост скорости примерно в 1.7 раза (по сравнению с тривиальным циклом)
  *  при парсинге типичного tab-separated файла со строками.
  * Можно было бы использовать SSE4.2, но он на момент написания кода поддерживался не на всех наших серверах (сейчас уже поддерживается везде).
  * При парсинге файла с короткими строками, падения производительности нет.
  */
template <char... symbols>
inline const char * find_first_symbols(const char * begin, const char * end)
{
#if defined(__x86_64__)
	for (; begin + 15 < end; begin += 16)
	{
		__m128i bytes = _mm_loadu_si128(reinterpret_cast<const __m128i *>(begin));

		__m128i eq = detail::mm_is_in<symbols...>(bytes);

		UInt16 bit_mask = _mm_movemask_epi8(eq);
		if (bit_mask)
			return begin + __builtin_ctz(bit_mask);
	}
#endif

	for (; begin < end; ++begin)
		if (detail::is_in<symbols...>(*begin))
			return begin;
	return end;
}
