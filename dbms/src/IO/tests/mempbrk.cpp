#include <emmintrin.h>

#include <string>
#include <iostream>
#include <iomanip>

#include <statdaemons/Stopwatch.h>

#include <DB/Core/Types.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>


namespace test
{
	/** Позволяет найти в куске памяти следующий символ \t, \n или \\.
	  * Функция похожа на strpbrk, но со следующими отличиями:
	  * - работает с любыми кусками памяти, в том числе, с нулевыми байтами;
	  * - не требует нулевого байта в конце - в функцию передаётся конец данных;
	  * - в случае, если не найдено, возвращает указатель на конец, а не NULL.
	  *
	  * Использует SSE2, что даёт прирост скорости примерно в 1.7 раза (по сравнению с тривиальным циклом)
	  *  при парсинге типичного tab-separated файла со строками.
	  * При парсинге файла с короткими строками, падения производительности нет.
	  */
	static inline const char * find_first_tab_lf_or_backslash(const char * begin, const char * end)
	{
		static const char tab_chars[16] = {'\t', '\t', '\t', '\t', '\t', '\t', '\t', '\t', '\t', '\t', '\t', '\t', '\t', '\t', '\t', '\t'};
		static const char lf_chars[16]	= {'\n', '\n', '\n', '\n', '\n', '\n', '\n', '\n', '\n', '\n', '\n', '\n', '\n', '\n', '\n', '\n'};
		static const char bs_chars[16] 	= {'\\', '\\', '\\', '\\', '\\', '\\', '\\', '\\', '\\', '\\', '\\', '\\', '\\', '\\', '\\', '\\'};

		static const __m128i tab	= *reinterpret_cast<const __m128i *>(tab_chars);
		static const __m128i lf		= *reinterpret_cast<const __m128i *>(lf_chars);
		static const __m128i bs		= *reinterpret_cast<const __m128i *>(bs_chars);

		for (; (reinterpret_cast<ptrdiff_t>(begin) & 0x0F) && begin < end; ++begin)
			if (*begin == '\t' || *begin == '\n' || *begin == '\\')
				return begin;

		for (; begin + 15 < end; begin += 16)
		{
			__m128i bytes = *reinterpret_cast<const __m128i *>(begin);

			__m128i eq1 = _mm_cmpeq_epi8(bytes, tab);
			__m128i eq2 = _mm_cmpeq_epi8(bytes, lf);
			__m128i eq3 = _mm_cmpeq_epi8(bytes, bs);

			eq1 = _mm_or_si128(eq1, eq2);
			eq1 = _mm_or_si128(eq1, eq3);

			UInt16 bit_mask = _mm_movemask_epi8(eq1);

			if (bit_mask)
				return begin + __builtin_ctz(bit_mask);
		}

		for (; begin < end; ++begin)
			if (*begin == '\t' || *begin == '\n' || *begin == '\\')
				return begin;

		return end;
	}


	void readEscapedString(DB::String & s, DB::ReadBuffer & buf)
	{
		s = "";
		while (!buf.eof())
		{
			const char * next_pos = find_first_tab_lf_or_backslash(buf.position(), buf.buffer().end());

			s.append(buf.position(), next_pos - buf.position());
			buf.position() += next_pos - buf.position();

			if (!buf.hasPendingData())
				continue;

			if (*buf.position() == '\t' || *buf.position() == '\n')
				return;

			if (*buf.position() == '\\')
			{
				++buf.position();
				if (buf.eof())
					throw DB::Exception("Cannot parse escape sequence", DB::ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);
				s += DB::parseEscapeSequence(*buf.position());
				++buf.position();
			}
		}
	}
}


int main(int argc, char ** argv)
{
	try
	{
		DB::ReadBufferFromFileDescriptor in(STDIN_FILENO);
//		DB::WriteBufferFromFileDescriptor out(STDOUT_FILENO);
		std::string s;
		size_t rows = 0;

		Stopwatch watch;

		while (!in.eof())
		{
			test::readEscapedString(s, in);
			in.ignore();
			
			++rows;

/*			DB::writeEscapedString(s, out);
			DB::writeChar('\n', out);*/
		}

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "Read " << rows << " rows (" << in.count() / 1000000.0 << " MB) in " << watch.elapsedSeconds() << " sec., "
			<< rows / watch.elapsedSeconds() << " rows/sec. (" << in.count() / watch.elapsedSeconds() / 1000000 << " MB/s.)"
			<< std::endl;
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
