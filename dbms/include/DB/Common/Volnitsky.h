#pragma once

#include <statdaemons/ext/range.hpp>
#include <Poco/UTF8Encoding.h>
#include <Poco/Unicode.h>
#include <x86intrin.h>
#include <stdint.h>
#include <string.h>


/** Поиск подстроки в строке по алгоритму Вольницкого:
  * http://volnitsky.com/project/str_search/
  *
  * haystack и needle могут содержать нулевые байты.
  *
  * Алгоритм:
  * - при слишком маленьком или слишком большом размере needle, или слишком маленьком haystack, используем std::search или memchr;
  * - при инициализации, заполняем open-addressing linear probing хэш-таблицу вида:
  *    хэш от биграммы из needle -> позиция этой биграммы в needle + 1.
  *    (прибавлена единица только чтобы отличить смещение ноль от пустой ячейки)
  * - в хэш-таблице ключи не хранятся, хранятся только значения;
  * - биграммы могут быть вставлены несколько раз, если они встречаются в needle несколько раз;
  * - при поиске, берём из haystack биграмму, которая должна соответствовать последней биграмме needle (сравниваем с конца);
  * - ищем её в хэш-таблице, если нашли - достаём смещение из хэш-таблицы и сравниваем строку побайтово;
  * - если сравнить не получилось - проверяем следующую ячейку хэш-таблицы из цепочки разрешения коллизий;
  * - если не нашли, пропускаем в haystack почти размер needle байт;
  *
  * Используется невыровненный доступ к памяти.
  */

/// @todo store lowercase needle to speed up in case there are numerous occurrences of bigrams from needle in haystack
template <typename CRTP>
class VolnitskyBase
{
protected:
	using offset_t = uint8_t;	/// Смещение в needle. Для основного алгоритма, длина needle не должна быть больше 255.
	using ngram_t = uint16_t;	/// n-грамма (2 байта).

	const char * needle;
	size_t needle_size;
	const char * needle_end;
	size_t step;				/// Насколько двигаемся, если n-грамма из haystack не нашлась в хэш-таблице.

	static const size_t hash_size = 64 * 1024;	/// Помещается в L2-кэш.
	offset_t hash[hash_size];	/// Хэш-таблица.

	bool fallback;				/// Нужно ли использовать fallback алгоритм.

	/// fallback алгоритм
	const char * search_fallback(const char * haystack, const size_t haystack_size) const
	{
		const char * pos = haystack;
		const char * end = haystack + haystack_size;

		while (nullptr != (pos = static_cast<const char *>(memchr(pos, needle[0], end - pos))) &&
			   pos + needle_size <= end)
		{
			if (0 == memcmp(pos, needle, needle_size))
				return pos;

			++pos;
		}

		return end;
	}

public:
	/** haystack_size_hint - ожидаемый суммарный размер haystack при вызовах search. Можно не указывать.
	  * Если указать его достаточно маленьким, то будет использован fallback алгоритм,
	  *  так как считается, что тратить время на инициализацию хэш-таблицы не имеет смысла.
	  */
	VolnitskyBase(const char * const needle, const size_t needle_size, size_t haystack_size_hint = 0)
	: needle{needle}, needle_size{needle_size}, needle_end{needle + needle_size},
	  step{needle_size - sizeof(ngram_t) + 1}
	{
		if (needle_size < 2 * sizeof(ngram_t)
			|| needle_size >= std::numeric_limits<offset_t>::max()
			|| (haystack_size_hint && haystack_size_hint < 20000))
		{
			fallback = true;
			return;
		}
		else
			fallback = false;

		memset(hash, 0, sizeof(hash));

		for (int i = needle_size - sizeof(ngram_t); i >= 0; --i)
			self().putNGram(needle + i, i + 1);
	}


	/// Если не найдено - возвращается конец haystack.
	const char * search(const char * haystack, size_t haystack_size) const
	{
		if (needle_size == 0)
			return haystack;

		const char * haystack_end = haystack + haystack_size;

		if (needle_size == 1)
		{
			if (const auto res = static_cast<const char *>(
					memchr(haystack, /*CaseSensitive*/true ? needle[0] : std::tolower(needle[0]), haystack_size)))
				return res;

//			if (!CaseSensitive)
//				if (const auto res = static_cast<const char *>(memchr(haystack, std::toupper(this->needle[0]), haystack_size)))
//					return res;

			return haystack_end;
		}

		if (fallback || haystack_size <= needle_size)
			return self().search_fallback(haystack, haystack_size);

		/// Будем "прикладывать" needle к haystack и сравнивать n-грам из конца needle.
		const char * pos = haystack + needle_size - sizeof(ngram_t);
		for (; pos <= haystack_end - needle_size; pos += step)
		{
			/// Смотрим все ячейки хэш-таблицы, которые могут соответствовать n-граму из haystack.
			for (size_t cell_num = toNGram(pos) % hash_size; hash[cell_num];
				 cell_num = (cell_num + 1) % hash_size)
			{
				/// Когда нашли - сравниваем побайтово, используя смещение из хэш-таблицы.
				const char * res = pos - (hash[cell_num] - 1);
				for (size_t i = 0; i < needle_size;)
					if (!self().compare(res + i, needle + i, i))
						goto next_hash_cell;

				return res;
				next_hash_cell:;
			}
		}

		/// Оставшийся хвостик.
		return self().search_fallback(pos - step + 1, haystack_end - (pos - step + 1));
	}

	const unsigned char * search(const unsigned char * haystack, size_t haystack_size) const
	{
		return reinterpret_cast<const unsigned char *>(search(reinterpret_cast<const char *>(haystack), haystack_size));
	}

protected:
	CRTP & self() { return static_cast<CRTP &>(*this); }
	const CRTP & self() const { return const_cast<VolnitskyBase *>(this)->self(); }

	static const ngram_t & toNGram(const char * const pos)
	{
		return *reinterpret_cast<const ngram_t *>(pos);
	}

	void putNGramBase(const ngram_t ngram, const int offset)
	{
		/// Кладём смещение для n-грама в соответствующую ему ячейку или ближайшую свободную.
		size_t cell_num = ngram % hash_size;

		while (hash[cell_num])
			cell_num = (cell_num + 1) % hash_size; /// Поиск следующей свободной ячейки.

		hash[cell_num] = offset;
	}
};


/// Primary template for case sensitive comparison
template <bool CaseSensitive, bool ASCII> struct VolnitskyImpl : VolnitskyBase<VolnitskyImpl<CaseSensitive, ASCII>>
{
	using VolnitskyBase<VolnitskyImpl<CaseSensitive, ASCII>>::VolnitskyBase;

	void putNGram(const char * const pos, const int offset)
	{
		this->putNGramBase(this->toNGram(pos), offset);
	}

	static bool compare(const char * const lhs, const char * const rhs, std::size_t & offset)
	{
		++offset;
		return *lhs == *rhs;
	}
};

/// Case-insensitive ASCII
template <> struct VolnitskyImpl<false, true> : VolnitskyBase<VolnitskyImpl<false, true>>
{
	VolnitskyImpl(const char * const needle, const size_t needle_size, const size_t haystack_size_hint = 0)
	: VolnitskyBase{needle, needle_size, haystack_size_hint}, fallback_searcher{needle, needle_size}
	{
	}

	void putNGram(const char * const pos, const int offset)
	{
		union {
			ngram_t n;
			UInt8 c[2];
		};

		n = toNGram(pos);
		const auto c0_alpha = std::isalpha(c[0]);
		const auto c1_alpha = std::isalpha(c[1]);

		if (c0_alpha && c1_alpha)
		{
			/// 4 combinations: AB, aB, Ab, ab
			c[0] = std::tolower(c[0]);
			c[1] = std::tolower(c[1]);
			putNGramBase(n, offset);

			c[0] = std::toupper(c[0]);
			putNGramBase(n, offset);

			c[1] = std::toupper(c[1]);
			putNGramBase(n, offset);

			c[0] = std::tolower(c[0]);
			putNGramBase(n, offset);
		}
		else if (c0_alpha)
		{
			/// 2 combinations: A1, a1
			c[0] = std::tolower(c[0]);
			putNGramBase(n, offset);

			c[0] = std::toupper(c[0]);
			putNGramBase(n, offset);
		}
		else if (c1_alpha)
		{
			/// 2 combinations: 0B, 0b
			c[1] = std::tolower(c[1]);
			putNGramBase(n, offset);

			c[1] = std::toupper(c[1]);
			putNGramBase(n, offset);
		}
		else
			/// 1 combination: 01
			putNGramBase(n, offset);
	}

	static bool compare(const char * const lhs, const char * const rhs, std::size_t & offset)
	{
		++offset;
		return std::tolower(*lhs) == std::tolower(*rhs);
	}

	class CaseInsensitiveSearcher
	{
		static constexpr auto n = sizeof(__m128i);

		const int page_size = getpagesize();

		/// string to be searched for
		const char * const needle;
		const std::size_t needle_size;
		/// lower and uppercase variants of the first character in `needle`
		UInt8 l{};
		UInt8 u{};
		/// vectors filled with `l` and `u`, for determining leftmost position of the first symbol
		__m128i patl, patu;
		/// lower and uppercase vectors of first 16 characters of `needle`
		__m128i cachel = _mm_setzero_si128(), cacheu = _mm_setzero_si128();
		int cachemask{};

		bool page_safe(const void * const ptr) const
		{
			return ((page_size - 1) & reinterpret_cast<std::uintptr_t>(ptr)) <= page_size - n;
		}

	public:
		CaseInsensitiveSearcher(const char * const needle, const std::size_t needle_size)
		: needle{needle}, needle_size{needle_size}
		{
			if (0 == needle_size)
				return;

			auto needle_pos = needle;

			l = std::tolower(*needle_pos);
			u = std::toupper(*needle_pos);

			patl = _mm_set1_epi8(l);
			patu = _mm_set1_epi8(u);

			const auto needle_end = needle_pos + needle_size;

			for (const auto i : ext::range(0, n))
			{
				cachel = _mm_srli_si128(cachel, 1);
				cacheu = _mm_srli_si128(cacheu, 1);

				if (needle_pos != needle_end)
				{
					cachel = _mm_insert_epi8(cachel, std::tolower(*needle_pos), n - 1);
					cacheu = _mm_insert_epi8(cacheu, std::toupper(*needle_pos), n - 1);
					cachemask |= 1 << i;
					++needle_pos;
				}
			}
		}

		const UInt8 * find(const UInt8 * haystack, const UInt8 * const haystack_end) const
		{
			if (0 == needle_size)
				return haystack;

			const auto needle_begin = reinterpret_cast<const UInt8 *>(needle);
			const auto needle_end = needle_begin + needle_size;

			while (haystack < haystack_end)
			{
				/// @todo supposedly for long strings spanning across multiple pages. Why don't we use this technique in other places?
				if (haystack + n <= haystack_end && page_safe(haystack))
				{
					const auto v_haystack = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack));
					const auto v_against_l = _mm_cmpeq_epi8(v_haystack, patl);
					const auto v_against_u = _mm_cmpeq_epi8(v_haystack, patu);
					const auto v_against_l_or_u = _mm_or_si128(v_against_l, v_against_u);

					const auto mask = _mm_movemask_epi8(v_against_l_or_u);

					if (mask == 0)
					{
						haystack += n;
						continue;
					}

					const auto offset = _bit_scan_forward(mask);
					haystack += offset;

					if (haystack < haystack_end && haystack + n <= haystack_end && page_safe(haystack))
					{
						const auto v_haystack = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack));
						const auto v_against_l = _mm_cmpeq_epi8(v_haystack, cachel);
						const auto v_against_u = _mm_cmpeq_epi8(v_haystack, cacheu);
						const auto v_against_l_or_u = _mm_or_si128(v_against_l, v_against_u);
						const auto mask = _mm_movemask_epi8(v_against_l_or_u);

						if (0xffff == cachemask)
						{
							if (mask == cachemask)
							{
								auto haystack_pos = haystack + n;
								auto needle_pos = needle_begin + n;

								while (haystack_pos < haystack_end && needle_pos < needle_end &&
									   std::tolower(*haystack_pos) == std::tolower(*needle_pos))
									++haystack_pos, ++needle_pos;

								if (needle_pos == needle_end)
									return haystack;
							}
						}
						else if ((mask & cachemask) == cachemask)
							return haystack;

						++haystack;
						continue;
					}
				}

				if (haystack == haystack_end)
					return haystack_end;

				if (*haystack == l || *haystack == u)
				{
					auto haystack_pos = haystack + 1;
					auto needle_pos = needle_begin + 1;

					while (haystack_pos < haystack_end && needle_pos < needle_end &&
						   std::tolower(*haystack_pos) == std::tolower(*needle_pos))
						++haystack_pos, ++needle_pos;

					if (needle_pos == needle_end)
						return haystack;
				}

				++haystack;
			}

			return haystack_end;
		}
	};

	CaseInsensitiveSearcher fallback_searcher;

	const char * search_fallback(const char * haystack, const size_t haystack_size) const
	{
		return reinterpret_cast<const char *>(fallback_searcher.find(reinterpret_cast<const UInt8 *>(haystack),
			reinterpret_cast<const UInt8 *>(haystack) + haystack_size));
	}
};

/// Case-sensitive UTF-8
template <> struct VolnitskyImpl<false, false> : VolnitskyBase<VolnitskyImpl<false, false>>
{
	VolnitskyImpl(const char * const needle, const size_t needle_size, const size_t haystack_size_hint = 0)
		: VolnitskyBase{needle, needle_size, haystack_size_hint}, fallback_searcher{needle, needle_size}
	{
	}

	void putNGram(const char * const pos, const int offset)
	{
		union
		{
			ngram_t n;
			UInt8 c[2];
		};

		n = toNGram(pos);

		if (isascii(c[0]) && isascii(c[1]))
		{
			const auto c0_al = std::isalpha(c[0]);
			const auto c1_al = std::isalpha(c[1]);

			if (c0_al && c1_al)
			{
				/// 4 combinations: AB, aB, Ab, ab
				c[0] = std::tolower(c[0]);
				c[1] = std::tolower(c[1]);
				putNGramBase(n, offset);

				c[0] = std::toupper(c[0]);
				putNGramBase(n, offset);

				c[1] = std::toupper(c[1]);
				putNGramBase(n, offset);

				c[0] = std::tolower(c[0]);
				putNGramBase(n, offset);
			}
			else if (c0_al)
			{
				/// 2 combinations: A1, a1
				c[0] = std::tolower(c[0]);
				putNGramBase(n, offset);

				c[0] = std::toupper(c[0]);
				putNGramBase(n, offset);
			}
			else if (c1_al)
			{
				/// 2 combinations: 0B, 0b
				c[1] = std::tolower(c[1]);
				putNGramBase(n, offset);

				c[1] = std::toupper(c[1]);
				putNGramBase(n, offset);
			}
			else
				/// 1 combination: 01
				putNGramBase(n, offset);
		}
		else
		{
			using Seq = UInt8[6];

			const auto u_pos = reinterpret_cast<const UInt8 *>(pos);
			static const Poco::UTF8Encoding utf8;

			if (utf8_is_continuation_octet(c[1]))
			{
				/// ngram is inside a sequence
				auto seq_pos = u_pos;
				utf8_sync_backward(seq_pos);

				const auto u32 = utf8.convert(seq_pos);
				const auto l_u32 = Poco::Unicode::toLower(u32);
				const auto u_u32 = Poco::Unicode::toUpper(u32);

				/// symbol is case-independent
				if (l_u32 == u_u32)
					putNGramBase(n, offset);
				else
				{
					/// where is the given ngram in respect to UTF-8 sequence start?
					const auto seq_ngram_offset = u_pos - seq_pos;

					Seq seq;

					/// put ngram from lowercase
					utf8.convert(l_u32, seq, sizeof(seq));
					c[0] = seq[seq_ngram_offset];
					c[1] = seq[seq_ngram_offset + 1];
					putNGramBase(n, offset);

					/// put ngram for uppercase
					utf8.convert(u_u32, seq, sizeof(seq));
					c[0] = seq[seq_ngram_offset];
					c[1] = seq[seq_ngram_offset + 1];
					putNGramBase(n, offset);
				}
			}
			else
			{
				/// ngram is on the boundary of two sequences
				/// first sequence may start before u_pos if it is not ASCII
				auto first_seq_pos = u_pos;
				utf8_sync_backward(first_seq_pos);

				const auto first_u32 = utf8.convert(first_seq_pos);
				const auto first_l_u32 = Poco::Unicode::toLower(first_u32);
				const auto first_u_u32 = Poco::Unicode::toUpper(first_u32);

				/// second sequence always start immediately after u_pos
				auto second_seq_pos = u_pos + 1;

				const auto second_u32 = utf8.convert(second_seq_pos);
				const auto second_l_u32 = Poco::Unicode::toLower(second_u32);
				const auto second_u_u32 = Poco::Unicode::toUpper(second_u32);

				/// both symbols are case-independent
				if (first_l_u32 == first_u_u32 && second_l_u32 == second_u_u32)
					putNGramBase(n, offset);
				else if (first_l_u32 == first_u_u32)
				{
					/// first symbol is case-independent
					Seq seq;

					/// put ngram for lowercase
					utf8.convert(second_l_u32, seq, sizeof(seq));
					c[1] = seq[0];
					putNGramBase(n, offset);

					/// put ngram from uppercase
					utf8.convert(second_u_u32, seq, sizeof(seq));
					c[1] = seq[0];
					putNGramBase(n, offset);
				}
				else if (second_l_u32 == second_u_u32)
				{
					/// second symbol is case-independent

					/// where is the given ngram in respect to the first UTF-8 sequence start?
					const auto seq_ngram_offset = u_pos - first_seq_pos;

					Seq seq;

					/// put ngram for lowercase
					utf8.convert(second_l_u32, seq, sizeof(seq));
					c[0] = seq[seq_ngram_offset];
					putNGramBase(n, offset);

					/// put ngram for uppercase
					utf8.convert(second_u_u32, seq, sizeof(seq));
					c[0] = seq[seq_ngram_offset];
					putNGramBase(n, offset);
				}
				else
				{
					/// where is the given ngram in respect to the first UTF-8 sequence start?
					const auto seq_ngram_offset = u_pos - first_seq_pos;

					Seq first_l_seq, first_u_seq, second_l_seq, second_u_seq;

					utf8.convert(first_l_u32, first_l_seq, sizeof(first_l_seq));
					utf8.convert(first_u_u32, first_u_seq, sizeof(first_u_seq));
					utf8.convert(second_l_u32, second_l_seq, sizeof(second_l_seq));
					utf8.convert(second_u_u32, second_u_seq, sizeof(second_u_seq));

					/// ngram for ll
					c[0] = first_l_seq[seq_ngram_offset];
					c[1] = second_l_seq[0];
					putNGramBase(n, offset);

					/// ngram for lU
					c[0] = first_l_seq[seq_ngram_offset];
					c[1] = second_u_seq[0];
					putNGramBase(n, offset);

					/// ngram for Ul
					c[0] = first_u_seq[seq_ngram_offset];
					c[1] = second_l_seq[0];
					putNGramBase(n, offset);

					/// ngram for UU
					c[0] = first_u_seq[seq_ngram_offset];
					c[1] = second_u_seq[0];
					putNGramBase(n, offset);
				}
			}
		}
	}

	static const UInt8 utf8_continuation_octet_mask = 0b11000000u;
	static const UInt8 utf8_continuation_octet = 0b10000000u;

	/// return true if `octet` binary repr starts with 10 (octet is a UTF-8 sequence continuation)
	static bool utf8_is_continuation_octet(const UInt8 octet)
	{
		return (octet & utf8_continuation_octet_mask) == utf8_continuation_octet;
	}

	/// moves `s` backward until either first non-continuation octet
	static void utf8_sync_backward(const UInt8 * & s)
	{
		while (utf8_is_continuation_octet(*s))
			--s;
	}

	/// moves `s` forward until either first non-continuation octet or string end is met
	static void utf8_sync_forward(const UInt8 * & s, const UInt8 * const end = nullptr)
	{
		while (s < end && utf8_is_continuation_octet(*s))
			++s;
	}

	/// returns UTF-8 code point sequence length judging by it's first octet
	static std::size_t utf8_seq_length(const UInt8 first_octet)
	{
		if (first_octet < 0x80u)
			return 1;

		const std::size_t bits = 8;
		const auto first_zero = _bit_scan_reverse(static_cast<UInt8>(~first_octet));

		return bits - 1 - first_zero;
	}

	static bool compare(const char * const lhs, const char * const rhs, std::size_t & offset)
	{
		offset += utf8_seq_length(*reinterpret_cast<const UInt8 *>(lhs));

		static const Poco::UTF8Encoding utf8;

		return Poco::Unicode::toLower(utf8.convert(reinterpret_cast<const UInt8 *>(lhs))) ==
			   Poco::Unicode::toLower(utf8.convert(reinterpret_cast<const UInt8 *>(rhs)));
	}

	class CaseInsensitiveSearcher
	{
		using UTF8SequenceBuffer = UInt8[6];

		static constexpr auto n = sizeof(__m128i);

		const int page_size = getpagesize();

		/// string to be searched for
		const char * const needle;
		const std::size_t needle_size;
		bool first_needle_symbol_is_ascii{};
		/// lower and uppercase variants of the first octet of the first character in `needle`
		UInt8 l{};
		UInt8 u{};
		/// vectors filled with `l` and `u`, for determining leftmost position of the first symbol
		__m128i patl, patu;
		/// lower and uppercase vectors of first 16 characters of `needle`
		__m128i cachel = _mm_setzero_si128(), cacheu = _mm_setzero_si128();
		int cachemask{};
		std::size_t cache_valid_len{};
		std::size_t cache_actual_len{};

		bool page_safe(const void * const ptr) const
		{
			return ((page_size - 1) & reinterpret_cast<std::uintptr_t>(ptr)) <= page_size - n;
		}

	public:
		CaseInsensitiveSearcher(const char * const needle, const std::size_t needle_size)
		: needle{needle}, needle_size{needle_size}
		{
			if (0 == needle_size)
				return;

			static const Poco::UTF8Encoding utf8;
			UTF8SequenceBuffer l_seq, u_seq;

			auto needle_pos = reinterpret_cast<const UInt8 *>(needle);
			if (*needle_pos < 0x80u)
			{
				first_needle_symbol_is_ascii = true;
				l = std::tolower(*needle_pos);
				u = std::toupper(*needle_pos);
			}
			else
			{
				const auto first_u32 = utf8.convert(needle_pos);
				const auto first_l_u32 = Poco::Unicode::toLower(first_u32);
				const auto first_u_u32 = Poco::Unicode::toUpper(first_u32);

				/// lower and uppercase variants of the first octet of the first character in `needle`
				utf8.convert(first_l_u32, l_seq, sizeof(l_seq));
				l = l_seq[0];
				utf8.convert(first_u_u32, u_seq, sizeof(u_seq));
				u = u_seq[0];
			}

			/// for detecting leftmost position of the first symbol
			patl = _mm_set1_epi8(l);
			patu = _mm_set1_epi8(u);
			/// lower and uppercase vectors of first 16 octets of `needle`

			const auto needle_end = needle_pos + needle_size;

			for (std::size_t i = 0; i < n;)
			{
				if (needle_pos == needle_end)
				{
					cachel = _mm_srli_si128(cachel, 1);
					cacheu = _mm_srli_si128(cacheu, 1);
					++i;

					continue;
				}

				const auto src_len = utf8_seq_length(*needle_pos);
				const auto c_u32 = utf8.convert(needle_pos);

				const auto c_l_u32 = Poco::Unicode::toLower(c_u32);
				const auto c_u_u32 = Poco::Unicode::toUpper(c_u32);

				const auto dst_l_len = static_cast<UInt8>(utf8.convert(c_l_u32, l_seq, sizeof(l_seq)));
				const auto dst_u_len = static_cast<UInt8>(utf8.convert(c_u_u32, u_seq, sizeof(u_seq)));

				/// @note Unicode standard states it is a rare but possible occasion
				if (!(dst_l_len == dst_u_len && dst_u_len == src_len))
					throw DB::Exception{
						"UTF8 sequences with different lowercase and uppercase lengths are not supported",
						DB::ErrorCodes::UNSUPPORTED_PARAMETER
					};

				cache_actual_len += src_len;
				if (cache_actual_len < n)
					cache_valid_len += src_len;

				for (std::size_t j = 0; j < src_len && i < n; ++j, ++i)
				{
					cachel = _mm_srli_si128(cachel, 1);
					cacheu = _mm_srli_si128(cacheu, 1);

					if (needle_pos != needle_end)
					{
						cachel = _mm_insert_epi8(cachel, l_seq[j], n - 1);
						cacheu = _mm_insert_epi8(cacheu, u_seq[j], n - 1);

						cachemask |= 1 << i;
						++needle_pos;
					}
				}
			}
		}

		const UInt8 * find(const UInt8 * haystack, const UInt8 * const haystack_end) const
		{
			if (0 == needle_size)
				return haystack;

			static const Poco::UTF8Encoding utf8;

			const auto needle_begin = reinterpret_cast<const UInt8 *>(needle);
			const auto needle_end = needle_begin + needle_size;

			while (haystack < haystack_end)
			{
				if (haystack + n <= haystack_end && page_safe(haystack))
				{
					const auto v_haystack = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack));
					const auto v_against_l = _mm_cmpeq_epi8(v_haystack, patl);
					const auto v_against_u = _mm_cmpeq_epi8(v_haystack, patu);
					const auto v_against_l_or_u = _mm_or_si128(v_against_l, v_against_u);

					const auto mask = _mm_movemask_epi8(v_against_l_or_u);

					if (mask == 0)
					{
						haystack += n;
						utf8_sync_forward(haystack, haystack_end);
						continue;
					}

					const auto offset = _bit_scan_forward(mask);
					haystack += offset;

					if (haystack < haystack_end && haystack + n <= haystack_end && page_safe(haystack))
					{
						const auto v_haystack = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack));
						const auto v_against_l = _mm_cmpeq_epi8(v_haystack, cachel);
						const auto v_against_u = _mm_cmpeq_epi8(v_haystack, cacheu);
						const auto v_against_l_or_u = _mm_or_si128(v_against_l, v_against_u);
						const auto mask = _mm_movemask_epi8(v_against_l_or_u);

						if (0xffff == cachemask)
						{
							if (mask == cachemask)
							{
								auto haystack_pos = haystack + cache_valid_len;
								auto needle_pos = needle_begin + cache_valid_len;

								while (haystack_pos < haystack_end && needle_pos < needle_end &&
									   Poco::Unicode::toLower(utf8.convert(haystack_pos)) ==
									   Poco::Unicode::toLower(utf8.convert(needle_pos)))
								{
									/// @note assuming sequences for lowercase and uppercase have exact same length
									const auto len = utf8_seq_length(*haystack_pos);
									haystack_pos += len, needle_pos += len;
								}

								if (needle_pos == needle_end)
									return haystack;
							}
						}
						else if ((mask & cachemask) == cachemask)
							return haystack;

						/// first octet was ok, but not the first 16, move to start of next sequence and reapply
						haystack += utf8_seq_length(*haystack);
						continue;
					}
				}

				if (haystack == haystack_end)
					return haystack_end;

				if (*haystack == l || *haystack == u)
				{
					auto haystack_pos = haystack + first_needle_symbol_is_ascii;
					auto needle_pos = needle_begin + first_needle_symbol_is_ascii;

					while (haystack_pos < haystack_end && needle_pos < needle_end &&
						   Poco::Unicode::toLower(utf8.convert(haystack_pos)) ==
						   Poco::Unicode::toLower(utf8.convert(needle_pos)))
					{
						const auto len = utf8_seq_length(*haystack_pos);
						haystack_pos += len, needle_pos += len;
					}

					if (needle_pos == needle_end)
						return haystack;
				}

				/// advance to the start of the next sequence
				haystack += utf8_seq_length(*haystack);
			}

			return haystack_end;
		}
	};

	CaseInsensitiveSearcher fallback_searcher;

	const char * search_fallback(const char * haystack, const size_t haystack_size) const
	{
		return reinterpret_cast<const char *>(fallback_searcher.find(reinterpret_cast<const UInt8 *>(haystack),
				reinterpret_cast<const UInt8 *>(haystack) + haystack_size));
	}
};


using Volnitsky = VolnitskyImpl<true, true>;
using VolnitskyUTF8 = VolnitskyImpl<true, false>;	/// exactly same as Volnitsky
using VolnitskyCaseInsensitive = VolnitskyImpl<false, true>;	/// ignores non-ASCII bytes
using VolnitskyCaseInsensitiveUTF8 = VolnitskyImpl<false, false>;
