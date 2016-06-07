#pragma once

#include <string.h>
#include <DB/Core/Defines.h>


#if defined(__x86_64__)

#include <emmintrin.h>


/** Функция memcpy может работать неоптимально при выполнении одновременно следующих условий:
  * 1. Размер куска памяти сравнительно небольшой (как правило меньше, приблизительно, 50 байт).
  * 2. Размер куска памяти неизвестен во время компиляции.
  *
  * В этом случае, она работает неоптимально по следующим причинам:
  * 1. Функция не инлайнится.
  * 2. Тратится много времени/инструкций на обработку "хвостиков" данных.
  *
  * Существуют ситуации, когда функцию можно сделать быстрее, воспользовавшись некоторыми допущениями.
  * Одно из таких допущений - возможность читать и писать какое-то количество байт после конца соответствующих диапазонов памяти.
  * Тогда можно не тратить код на обработку хвостиков, а выполнять копирование всегда большими кусками.
  *
  * Эта ситуация является типичной, например, когда короткие куски памяти копируются подряд в один непрервыный кусок памяти
  * - так как каждое следующее копирование будет перетирать лишние данные от предыдущего копирования.
  *
  * Допущение о том, что размер небольшой, позволяет нам не разворачивать цикл.
  * Это работает медленее, когда размер, на самом деле, большой.
  */

namespace detail
{
	inline void memcpySmallAllowReadWriteOverflow15Impl(char * __restrict dst, const char * __restrict src, ssize_t n)
	{
		while (n > 0)
		{
			_mm_storeu_si128(reinterpret_cast<__m128i *>(dst),
				_mm_loadu_si128(reinterpret_cast<const __m128i *>(src)));

			dst += 16;
			src += 16;
			n -= 16;
		}
	}
}

/** Исходит из допущения, что можно читать до 15 лишних байт после конца массива src,
  *  и записывать любой мусор до 15 байт после конца массива dst.
  */
inline void memcpySmallAllowReadWriteOverflow15(void * __restrict dst, const void * __restrict src, size_t n)
{
	detail::memcpySmallAllowReadWriteOverflow15Impl(reinterpret_cast<char *>(dst), reinterpret_cast<const char *>(src), n);
}

/** Исходит из допущения, что можно записывать любой мусор до 15 байт после конца массива dst.
  */
inline void memcpySmallAllowWriteOverflow15(void * __restrict dst, const void * __restrict src, size_t n)
{
	if (reinterpret_cast<intptr_t>(src) % 4096 <= 4096 - 16)
		memcpySmallAllowReadWriteOverflow15(dst, src, n);
	else
		memcpy(dst, src, n);
}


#else	/// Реализации для других платформ.

inline void memcpySmallAllowReadWriteOverflow15(void * __restrict dst, const void * __restrict src, size_t n)
{
	memcpy(dst, src, n);
}

inline void memcpySmallAllowWriteOverflow15(void * __restrict dst, const void * __restrict src, size_t n)
{
	memcpy(dst, src, n);
}

#endif
