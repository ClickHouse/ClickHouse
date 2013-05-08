#pragma once

#include <limits>
#include <boost/type_traits.hpp>
#include <boost/utility/enable_if.hpp>

#include <Yandex/likely.h>

#include <DB/Core/Types.h>
#include <DB/IO/WriteBuffer.h>

/// 20 цифр или 19 цифр и знак
#define WRITE_HELPERS_MAX_INT_WIDTH 20U


namespace DB
{

namespace detail
{
	/** Смотрите:
	  * https://github.com/localvoid/cxx-benchmark-itoa
	  * http://www.slideshare.net/andreialexandrescu1/three-optimization-tips-for-c-15708507
	  * http://vimeo.com/55639112
	  */

	
	/// Обычный способ, если в буфере не хватает места, чтобы там поместилось любое число.
	template <typename T>
	void writeUIntTextFallback(T x, WriteBuffer & buf)
	{
		if (x == 0)
		{
			buf.nextIfAtEnd();
			*buf.position() = '0';
			++buf.position();

			return;
		}
		
		char tmp[WRITE_HELPERS_MAX_INT_WIDTH];

		char * pos;
		for (pos = tmp + WRITE_HELPERS_MAX_INT_WIDTH - 1; x != 0; --pos)
		{
			*pos = '0' + x % 10;
			x /= 10;
		}

		++pos;

		buf.write(pos, tmp + WRITE_HELPERS_MAX_INT_WIDTH - pos);
	}


	/** Подсчёт количества десятичных цифр в числе.
	  * Хорошо работает для неравномерного распределения чисел, которое обычно бывает.
	  * Если большинство чисел длинные, то лучше работал бы branchless код с инструкцией bsr и хитрым преобразованием.
	  */
	template <typename T>
	UInt32 digits10(T x)
	{
		if (x < 10ULL)
			return 1;
		if (x < 100ULL)
			return 2;
		if (x < 1000ULL)
			return 3;

		if (x < 1000000000000ULL)
		{
			if (x < 100000000ULL)
			{
				if (x < 1000000ULL)
				{
					if (x < 10000ULL)
						return 4;
					else
						return 5 + (x >= 100000ULL);
				}

				return 7 + (x >= 10000000ULL);
			}

			if (x < 10000000000ULL)
				return 9 + (x >= 1000000000ULL);

			return 11 + (x >= 100000000000ULL);
		}

		return 12 + digits10(x / 1000000000000ULL);
	}


	/** Преобразует по две цифры за итерацию.
	  * Хорошо работает для неравномерного распределения чисел, которое обычно бывает.
	  * Если большинство чисел длинные, и если не жалко кэш, то лучше работал бы вариант
	  *  с большой таблицей и четырьмя цифрами за итерацию.
	  */
	template <typename T>
	UInt32 writeUIntText(T x, char * dst)
	{
		static const char digits[201] =
			"00010203040506070809"
			"10111213141516171819"
			"20212223242526272829"
			"30313233343536373839"
			"40414243444546474849"
			"50515253545556575859"
			"60616263646566676869"
			"70717273747576777879"
			"80818283848586878889"
			"90919293949596979899";

		const UInt32 length = digits10(x);
		UInt32 next = length - 1;

		while (x >= 100)
		{
			const UInt32 i = (x % 100) * 2;
			x /= 100;
			dst[next] = digits[i + 1];
			dst[next - 1] = digits[i];
			next -= 2;
		}

		if (x < 10)
		{
			dst[next] = '0' + x;
		}
		else
		{
			const UInt32 i = x * 2;
			dst[next] = digits[i + 1];
			dst[next - 1] = digits[i];
		}

		return length;
	}


	/** Если в буфере есть достаточно места - вызывает оптимизированный вариант, иначе - обычный вариант.
	  */
	template <typename T>
	void writeUIntText(T x, WriteBuffer & buf)
	{
		if (likely(buf.position() + WRITE_HELPERS_MAX_INT_WIDTH < buf.buffer().end()))
			buf.position() += writeUIntText(x, buf.position());
		else
			writeUIntTextFallback(x, buf);
	}


	/** Обёртка для знаковых чисел.
	  */
	template <typename T>
	void writeSIntText(T x, WriteBuffer & buf)
	{
		/// Особый случай для самого маленького отрицательного числа
		if (unlikely(x == std::numeric_limits<T>::min()))
		{
			if (sizeof(x) == 1)
				buf.write("-128", 4);
			else if (sizeof(x) == 2)
				buf.write("-32768", 6);
			else if (sizeof(x) == 4)
				buf.write("-2147483648", 11);
			else
				buf.write("-9223372036854775808", 20);
			return;
		}

		if (x < 0)
		{
			x = -x;
			buf.nextIfAtEnd();
			*buf.position() = '-';
			++buf.position();
		}

		writeUIntText(static_cast<typename boost::make_unsigned<T>::type>(x), buf);
	}

}


template <typename T>
typename boost::disable_if<boost::is_unsigned<T>, void>::type writeIntText(T x, WriteBuffer & buf)
{
	detail::writeSIntText(x, buf);
}

template <typename T>
typename boost::enable_if<boost::is_unsigned<T>, void>::type writeIntText(T x, WriteBuffer & buf)
{
	detail::writeUIntText(x, buf);
}

}
