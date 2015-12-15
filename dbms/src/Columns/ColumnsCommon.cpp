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

	const __m128i zero16 = _mm_setzero_si128();

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


template <typename T>
void filterArraysImpl(
	const PODArray<T> & src_elems, const IColumn::Offsets_t & src_offsets,
	PODArray<T> & res_elems, IColumn::Offsets_t & res_offsets,
	const IColumn::Filter & filt, ssize_t result_size_hint)
{
	const size_t size = src_offsets.size();
	if (size != filt.size())
		throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

	if (result_size_hint)
	{
		res_offsets.reserve(result_size_hint > 0 ? result_size_hint : size);

		if (result_size_hint < 0)
			res_elems.reserve(src_elems.size());
		else if (result_size_hint < 1000000000 && src_elems.size() < 1000000000)	/// Избегаем переполнения.
			res_elems.reserve((result_size_hint * src_elems.size() + size - 1) / size);
	}

	IColumn::Offset_t current_src_offset = 0;

	static constexpr size_t SIMD_BYTES = 16;

	const UInt8 * filt_pos = &filt[0];
	const auto filt_end = filt_pos + size;
	const auto filt_end_aligned = filt_pos + size / SIMD_BYTES * SIMD_BYTES;

	auto offsets_pos = &src_offsets[0];
	const auto offsets_begin = offsets_pos;

	const __m128i zero_vec = _mm_setzero_si128();

	/// copy array ending at *end_offset_ptr
	const auto copy_array = [&] (const IColumn::Offset_t * offset_ptr)
	{
		const auto offset = offset_ptr == offsets_begin ? 0 : offset_ptr[-1];
		const auto size = *offset_ptr - offset;

		current_src_offset += size;
		res_offsets.push_back(current_src_offset);

		const auto elems_size_old = res_elems.size();
		res_elems.resize(elems_size_old + size);
		memcpy(&res_elems[elems_size_old], &src_elems[offset], size * sizeof(T));
	};

	while (filt_pos < filt_end_aligned)
	{
		const auto mask = _mm_movemask_epi8(_mm_cmpgt_epi8(
			_mm_loadu_si128(reinterpret_cast<const __m128i *>(filt_pos)),
			zero_vec));

		if (mask == 0)
		{
			/// SIMD_BYTES consecutive rows do not pass the filter
		}
		else if (mask == 0xffff)
		{
			/// SIMD_BYTES consecutive rows pass the filter
			const auto first = offsets_pos == offsets_begin;

			const auto chunk_offset = first ? 0 : offsets_pos[-1];
			const auto chunk_size = offsets_pos[SIMD_BYTES - 1] - chunk_offset;

			const auto offsets_size_old = res_offsets.size();
			res_offsets.resize(offsets_size_old + SIMD_BYTES);
			memcpy(&res_offsets[offsets_size_old], offsets_pos, SIMD_BYTES * sizeof(IColumn::Offset_t));

			if (!first)
			{
				/// difference between current and actual offset
				const auto diff_offset = chunk_offset - current_src_offset;

				if (diff_offset > 0)
				{
					const auto res_offsets_pos = &res_offsets[offsets_size_old];

					/// adjust offsets
					for (size_t i = 0; i < SIMD_BYTES; ++i)
						res_offsets_pos[i] -= diff_offset;
				}
			}
			current_src_offset += chunk_size;

			/// copy elements for SIMD_BYTES arrays at once
			const auto elems_size_old = res_elems.size();
			res_elems.resize(elems_size_old + chunk_size);
			memcpy(&res_elems[elems_size_old], &src_elems[chunk_offset], chunk_size * sizeof(T));
		}
		else
		{
			for (size_t i = 0; i < SIMD_BYTES; ++i)
				if (filt_pos[i])
					copy_array(offsets_pos + i);
		}

		filt_pos += SIMD_BYTES;
		offsets_pos += SIMD_BYTES;
	}

	while (filt_pos < filt_end)
	{
		if (*filt_pos)
			copy_array(offsets_pos);

		++filt_pos;
		++offsets_pos;
	}
}


/// Явные инстанцирования - чтобы не размещать реализацию функции выше в заголовочном файле.
template void filterArraysImpl<UInt8>(
	const PODArray<UInt8> &, const IColumn::Offsets_t &, PODArray<UInt8> &, IColumn::Offsets_t &, const IColumn::Filter &, ssize_t);
template void filterArraysImpl<UInt16>(
	const PODArray<UInt16> &, const IColumn::Offsets_t &, PODArray<UInt16> &, IColumn::Offsets_t &, const IColumn::Filter &, ssize_t);
template void filterArraysImpl<UInt32>(
	const PODArray<UInt32> &, const IColumn::Offsets_t &, PODArray<UInt32> &, IColumn::Offsets_t &, const IColumn::Filter &, ssize_t);
template void filterArraysImpl<UInt64>(
	const PODArray<UInt64> &, const IColumn::Offsets_t &, PODArray<UInt64> &, IColumn::Offsets_t &, const IColumn::Filter &, ssize_t);
template void filterArraysImpl<Int8>(
	const PODArray<Int8> &, const IColumn::Offsets_t &, PODArray<Int8> &, IColumn::Offsets_t &, const IColumn::Filter &, ssize_t);
template void filterArraysImpl<Int16>(
	const PODArray<Int16> &, const IColumn::Offsets_t &, PODArray<Int16> &, IColumn::Offsets_t &, const IColumn::Filter &, ssize_t);
template void filterArraysImpl<Int32>(
	const PODArray<Int32> &, const IColumn::Offsets_t &, PODArray<Int32> &, IColumn::Offsets_t &, const IColumn::Filter &, ssize_t);
template void filterArraysImpl<Int64>(
	const PODArray<Int64> &, const IColumn::Offsets_t &, PODArray<Int64> &, IColumn::Offsets_t &, const IColumn::Filter &, ssize_t);
template void filterArraysImpl<Float32>(
	const PODArray<Float32> &, const IColumn::Offsets_t &, PODArray<Float32> &, IColumn::Offsets_t &, const IColumn::Filter &, ssize_t);
template void filterArraysImpl<Float64>(
	const PODArray<Float64> &, const IColumn::Offsets_t &, PODArray<Float64> &, IColumn::Offsets_t &, const IColumn::Filter &, ssize_t);

}
