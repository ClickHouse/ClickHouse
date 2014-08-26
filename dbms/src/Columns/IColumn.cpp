#include <emmintrin.h>

#include <DB/Columns/IColumn.h>


namespace DB
{

size_t countBytesInFilter(const IColumn::Filter & filt)
{
	size_t count = 0;

	/** NOTE: По идее, filt должен содержать только нолики и единички.
	  * Но, на всякий случай, здесь используется условие > 0 (на знаковые байты).
	  * Лучше было бы использовать != 0, то это не позволяет SSE2.
	  */

	const __m128i zero16 = _mm_set1_epi8(0);

	const Int8 * pos = reinterpret_cast<const Int8 *>(&filt[0]);
	const Int8 * end = pos + filt.size();
	const Int8 * end64 = pos + filt.size() / 64 * 64;

	for (; pos < end64; pos += 64)
		count += __builtin_popcountll(
			static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpgt_epi8(
				_mm_loadu_si128(reinterpret_cast<const __m128i *>(pos)),
				zero16)))
			| (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpgt_epi8(
				_mm_loadu_si128(reinterpret_cast<const __m128i *>(pos + 16)),
				zero16))) << 16)
			| (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpgt_epi8(
				_mm_loadu_si128(reinterpret_cast<const __m128i *>(pos + 32)),
				zero16))) << 32)
			| (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpgt_epi8(
				_mm_loadu_si128(reinterpret_cast<const __m128i *>(pos + 48)),
				zero16))) << 48));

	for (; pos < end; ++pos)
		count += *pos > 0;

	return count;
}

}
