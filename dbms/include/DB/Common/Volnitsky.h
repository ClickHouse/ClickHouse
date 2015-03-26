#pragma once

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
class Volnitsky
{
private:
	typedef uint8_t offset_t;	/// Смещение в needle. Для основного алгоритма, длина needle не должна быть больше 255.
	typedef uint16_t ngram_t;	/// n-грамма (2 байта).

	const char * needle;
	size_t needle_size;
	const char * needle_end;
	size_t step;				/// Насколько двигаемся, если n-грамма из haystack не нашлась в хэш-таблице.

	static const size_t hash_size = 64 * 1024;	/// Помещается в L2-кэш.
	offset_t hash[hash_size];	/// Хэш-таблица.

	bool fallback;				/// Нужно ли использовать fallback алгоритм.

	/// fallback алгоритм
	static const char * naive_memmem(const char * haystack, size_t haystack_size, const char * needle, size_t needle_size)
	{
		const char * pos = haystack;
		const char * end = haystack + haystack_size;
		while (nullptr != (pos = reinterpret_cast<const char *>(memchr(pos, needle[0], end - pos))) && pos + needle_size <= end)
		{
			if (0 == memcmp(pos, needle, needle_size))
				return pos;
			else
				++pos;
		}

		return end;
	}

public:
	/** haystack_size_hint - ожидаемый суммарный размер haystack при вызовах search. Можно не указывать.
	  * Если указать его достаточно маленьким, то будет использован fallback алгоритм,
	  *  так как считается, что тратить время на инициализацию хэш-таблицы не имеет смысла.
	  */
	Volnitsky(const char * needle_, size_t needle_size_, size_t haystack_size_hint = 0)
		: needle(needle_), needle_size(needle_size_), needle_end(needle + needle_size), step(needle_size - sizeof(ngram_t) + 1)
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

		memset(hash, 0, hash_size * sizeof(hash[0]));

		for (int i = needle_size - sizeof(ngram_t); i >= 0; --i)
		{
			/// Кладём смещение для n-грама в соответствующую ему ячейку или ближайшую свободную.
			size_t cell_num = *reinterpret_cast<const ngram_t *>(needle + i) % hash_size;
			while (hash[cell_num])
				cell_num = (cell_num + 1) % hash_size; /// Поиск следующей свободной ячейки.

			hash[cell_num] = i + 1;
		}
	}

	/// Если не найдено - возвращается конец haystack.
	const char * search(const char * haystack, size_t haystack_size) const
	{
		const char * haystack_end = haystack + haystack_size;

		if (needle_size == 1)
		{
			const char * res = reinterpret_cast<const char *>(memchr(haystack, needle[0], haystack_size));
			return res ? res : haystack_end;
		}
		if (fallback || haystack_size <= needle_size)
		{
			return naive_memmem(haystack, haystack_size, needle, needle_size);
		}

		/// Будем "прикладывать" needle к haystack и сравнивать n-грам из конца needle.
		const char * pos = haystack + needle_size - sizeof(ngram_t);
		for (; pos <= haystack_end - needle_size; pos += step)
		{
			/// Смотрим все ячейки хэш-таблицы, которые могут соответствовать n-граму из haystack.
			for (size_t cell_num = *reinterpret_cast<const ngram_t *>(pos) % hash_size; hash[cell_num]; cell_num = (cell_num + 1) % hash_size)
			{
				/// Когда нашли - сравниваем побайтово, используя смещение из хэш-таблицы.
				const char * res = pos - (hash[cell_num] - 1);
				for (size_t i = 0; i < needle_size; ++i)
					if (res[i] != needle[i])
						goto next_hash_cell;

				return res;
				next_hash_cell:;
			}
		}

		/// Оставшийся хвостик.
		return naive_memmem(pos - step + 1, haystack_end - (pos - step + 1), needle, needle_size);
	}

	const unsigned char * search(const unsigned char * haystack, size_t haystack_size) const
	{
		return reinterpret_cast<const unsigned char *>(search(reinterpret_cast<const char *>(haystack), haystack_size));
	}
};
