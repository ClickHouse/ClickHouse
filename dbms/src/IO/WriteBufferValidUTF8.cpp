#include <Poco/UTF8Encoding.h>
#include <DB/IO/WriteBufferValidUTF8.h>

#ifdef __x86_64__
#include <emmintrin.h>
#endif


namespace DB
{

const size_t WriteBufferValidUTF8::DEFAULT_SIZE = 4096;

/** Index into the table below with the first byte of a UTF-8 sequence to
  * get the number of trailing bytes that are supposed to follow it.
  * Note that *legal* UTF-8 values can't have 4 or 5-bytes. The table is
  * left as-is for anyone who may want to do such conversion, which was
  * allowed in earlier algorithms.
  */
const char WriteBufferValidUTF8::trailingBytesForUTF8[256] =
{
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
	1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
	2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2, 3,3,3,3,3,3,3,3,4,4,4,4,5,5,5,5
};


WriteBufferValidUTF8::WriteBufferValidUTF8(
	WriteBuffer & output_buffer, bool group_replacements, const char * replacement, size_t size)
	: BufferWithOwnMemory<WriteBuffer>(std::max(4LU, size)), output_buffer(output_buffer),
	group_replacements(group_replacements), replacement(replacement)
{
}


inline void WriteBufferValidUTF8::putReplacement()
{
	if (replacement.empty() || (group_replacements && just_put_replacement))
		return;

	just_put_replacement = true;
	output_buffer.write(replacement.data(), replacement.size());
}


inline void WriteBufferValidUTF8::putValid(char *data, size_t len)
{
	if (len == 0)
		return;

	just_put_replacement = false;
	output_buffer.write(data, len);
}


void WriteBufferValidUTF8::nextImpl()
{
	char *p = memory.data();
	char *valid_start = p;

	while (p < pos)
	{
#ifdef __x86_64__
		/// Быстрый пропуск ASCII
		static constexpr size_t SIMD_BYTES = 16;
		const char * simd_end = p + (pos - p) / SIMD_BYTES * SIMD_BYTES;

		while (p < simd_end && !_mm_movemask_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(p))))
			p += SIMD_BYTES;

		if (!(p < pos))
			break;
#endif

		size_t len = 1 + static_cast<size_t>(trailingBytesForUTF8[static_cast<unsigned char>(*p)]);

		if (len > 4)
		{
			/// Невалидное начало последовательности. Пропустим один байт.
			putValid(valid_start, p - valid_start);
			putReplacement();
			++p;
			valid_start = p;
		}
		else if (p + len > pos)
		{
			/// Еще не вся последовательность записана.
			break;
		}
		else if (Poco::UTF8Encoding::isLegal(reinterpret_cast<unsigned char*>(p), len))
		{
			/// Валидная последовательность.
			p += len;
		}
		else
		{
			/// Невалидная последовательность. Пропустим только первый байт.
			putValid(valid_start, p - valid_start);
			putReplacement();
			++p;
			valid_start = p;
		}
	}

	putValid(valid_start, p - valid_start);

	size_t cnt = pos - p;
	/// Сдвинем незаконченную последовательность в начало буфера.
	for (size_t i = 0; i < cnt; ++i)
		memory[i] = p[i];

	working_buffer = Buffer(&memory[cnt], &memory[0] + memory.size());
}


void WriteBufferValidUTF8::finish()
{
	/// Выпишем все полные последовательности из буфера.
	nextImpl();

	/// Если осталась незаконченная последовательность, запишем replacement.
	if (working_buffer.begin() != memory.data())
		putReplacement();
}

}
