#pragma once

#include <string.h>
#include <malloc.h>
#include <cstdint>
#include <type_traits>

#include <ext/bit_cast.hpp>
#include <DB/Core/Defines.h>


/** Поразрядная сортировка, обладает следующей функциональностью:
  * Может сортировать unsigned, signed числа, а также float-ы.
  * Может сортировать массив элементов фиксированной длины, которые содержат что-то ещё кроме ключа.
  * Настраиваемый размер разряда.
  *
  * LSB, stable.
  * NOTE Для некоторых приложений имеет смысл добавить MSB-radix-sort,
  *  а также алгоритмы radix-select, radix-partial-sort, radix-get-permutation на его основе.
  */


/** Используется в качестве параметра шаблона. См. ниже.
  */
struct RadixSortMallocAllocator
{
	void * allocate(size_t size)
	{
		return malloc(size);
	}

	void deallocate(void * ptr, size_t size)
	{
		return free(ptr);
	}
};


/** Преобразование, которое переводит битовое представление ключа в такое целое беззнаковое число,
  *  что отношение порядка над ключами будет соответствовать отношению порядка над полученными беззнаковыми числами.
  * Для float-ов это преобразование делает следующее:
  *  если выставлен знаковый бит, то переворачивает все остальные биты.
  */
template <typename KeyBits>
struct RadixSortFloatTransform
{
	/// Стоит ли записывать результат в память, или лучше делать его каждый раз заново?
	static constexpr bool transform_is_simple = false;

	static KeyBits forward(KeyBits x)
	{
		return x ^ (-((x >> (sizeof(KeyBits) * 8 - 1) | (KeyBits(1) << (sizeof(KeyBits) * 8 - 1)))));
	}

	static KeyBits backward(KeyBits x)
	{
		return x ^ (((x >> (sizeof(KeyBits) * 8 - 1)) - 1) | (KeyBits(1) << (sizeof(KeyBits) * 8 - 1)));
	}
};


template <typename Float>
struct RadixSortFloatTraits
{
	using Element = Float;		/// Тип элемента. Это может быть структура с ключём и ещё каким-то payload-ом. Либо просто ключ.
	using Key = Float;			/// Ключ, по которому нужно сортировать.
	using CountType = uint32_t;	/// Тип для подсчёта гистограмм. В случае заведомо маленького количества элементов, может быть меньше чем size_t.

	/// Тип, в который переводится ключ, чтобы делать битовые операции. Это UInt такого же размера, как ключ.
	using KeyBits = typename std::conditional<sizeof(Float) == 8, uint64_t, uint32_t>::type;

	static constexpr size_t PART_SIZE_BITS = 8;	/// Какими кусочками ключа в количестве бит делать один проход - перестановку массива.

	/// Преобразования ключа в KeyBits такое, что отношение порядка над ключём соответствует отношению порядка над KeyBits.
	using Transform = RadixSortFloatTransform<KeyBits>;

	/// Объект с функциями allocate и deallocate.
	/// Может быть использован, например, чтобы выделить память для временного массива на стеке.
	/// Для этого сам аллокатор создаётся на стеке.
	using Allocator = RadixSortMallocAllocator;

	/// Функция получения ключа из элемента массива.
	static Key & extractKey(Element & elem) { return elem; }
};


template <typename KeyBits>
struct RadixSortIdentityTransform
{
	static constexpr bool transform_is_simple = true;

	static KeyBits forward(KeyBits x) 	{ return x; }
	static KeyBits backward(KeyBits x) 	{ return x; }
};


template <typename KeyBits>
struct RadixSortSignedTransform
{
	static constexpr bool transform_is_simple = true;

	static KeyBits forward(KeyBits x) 	{ return x ^ (KeyBits(1) << (sizeof(KeyBits) * 8 - 1)); }
	static KeyBits backward(KeyBits x) 	{ return x ^ (KeyBits(1) << (sizeof(KeyBits) * 8 - 1)); }
};


template <typename UInt>
struct RadixSortUIntTraits
{
	using Element = UInt;
	using Key = UInt;
	using CountType = uint32_t;
	using KeyBits = UInt;

	static constexpr size_t PART_SIZE_BITS = 8;

	using Transform = RadixSortIdentityTransform<KeyBits>;
	using Allocator = RadixSortMallocAllocator;

	/// Функция получения ключа из элемента массива.
	static Key & extractKey(Element & elem) { return elem; }
};

template <typename Int>
struct RadixSortIntTraits
{
	using Element = Int;
	using Key = Int;
	using CountType = uint32_t;
	using KeyBits = typename std::make_unsigned<Int>::type;

	static constexpr size_t PART_SIZE_BITS = 8;

	using Transform = RadixSortSignedTransform<KeyBits>;
	using Allocator = RadixSortMallocAllocator;

	/// Функция получения ключа из элемента массива.
	static Key & extractKey(Element & elem) { return elem; }
};


template <typename Traits>
struct RadixSort
{
private:
	using Element 	= typename Traits::Element;
	using Key 		= typename Traits::Key;
	using CountType = typename Traits::CountType;
	using KeyBits 	= typename Traits::KeyBits;

	static constexpr size_t HISTOGRAM_SIZE = 1 << Traits::PART_SIZE_BITS;
	static constexpr size_t PART_BITMASK = HISTOGRAM_SIZE - 1;
	static constexpr size_t KEY_BITS = sizeof(Key) * 8;
	static constexpr size_t NUM_PASSES = (KEY_BITS + (Traits::PART_SIZE_BITS - 1)) / Traits::PART_SIZE_BITS;

	static ALWAYS_INLINE KeyBits getPart(size_t N, KeyBits x)
	{
		if (Traits::Transform::transform_is_simple)
			x = Traits::Transform::forward(x);

		return (x >> (N * Traits::PART_SIZE_BITS)) & PART_BITMASK;
	}

	static KeyBits keyToBits(Key x) { return ext::bit_cast<KeyBits>(x); }
	static Key bitsToKey(KeyBits x) { return ext::bit_cast<Key>(x); }

public:
	static void execute(Element * arr, size_t size)
	{
		/// Если массив имеет размер меньше 256, то лучше использовать другой алгоритм.

		/// Здесь есть циклы по NUM_PASSES. Очень важно, что они разворачиваются в compile-time.

		/// Для каждого из NUM_PASSES кусков бит ключа, считаем, сколько раз каждое значение этого куска встретилось.
		CountType histograms[HISTOGRAM_SIZE * NUM_PASSES] = {0};

		typename Traits::Allocator allocator;

		/// Будем делать несколько проходов по массиву. На каждом проходе, данные перекладываются в другой массив. Выделим этот временный массив.
		Element * swap_buffer = reinterpret_cast<Element *>(allocator.allocate(size * sizeof(Element)));

		/// Трансформируем массив и вычисляем гистограмму.
		for (size_t i = 0; i < size; ++i)
		{
			if (!Traits::Transform::transform_is_simple)
				Traits::extractKey(arr[i]) = bitsToKey(Traits::Transform::forward(keyToBits(Traits::extractKey(arr[i]))));

			for (size_t j = 0; j < NUM_PASSES; ++j)
				++histograms[j * HISTOGRAM_SIZE + getPart(j, keyToBits(Traits::extractKey(arr[i])))];
		}

		{
			/// Заменяем гистограммы на суммы с накоплением: значение в позиции i равно сумме в предыдущих позициях минус один.
			size_t sums[NUM_PASSES] = {0};

			for (size_t i = 0; i < HISTOGRAM_SIZE; ++i)
			{
				for (size_t j = 0; j < NUM_PASSES; ++j)
				{
					size_t tmp = histograms[j * HISTOGRAM_SIZE + i] + sums[j];
					histograms[j * HISTOGRAM_SIZE + i] = sums[j] - 1;
					sums[j] = tmp;
				}
			}
		}

		/// Перекладываем элементы в порядке начиная от младшего куска бит, и далее делаем несколько проходов по количеству кусков.
		for (size_t j = 0; j < NUM_PASSES; ++j)
		{
			Element * writer = j % 2 ? arr : swap_buffer;
			Element * reader = j % 2 ? swap_buffer : arr;

			for (size_t i = 0; i < size; ++i)
			{
				size_t pos = getPart(j, keyToBits(Traits::extractKey(reader[i])));

				/// Размещаем элемент на следующей свободной позиции.
				auto & dest = writer[++histograms[j * HISTOGRAM_SIZE + pos]];
				dest = reader[i];

				/// На последнем перекладывании, делаем обратную трансформацию.
				if (!Traits::Transform::transform_is_simple && j == NUM_PASSES - 1)
					Traits::extractKey(dest) = bitsToKey(Traits::Transform::backward(keyToBits(Traits::extractKey(reader[i]))));
			}
		}

		/// Если число проходов нечётное, то результирующий массив находится во временном буфере. Скопируем его на место исходного массива.
		if (NUM_PASSES % 2)
			memcpy(arr, swap_buffer, size * sizeof(Element));

		allocator.deallocate(swap_buffer, size * sizeof(Element));
	}
};


template <typename T>
typename std::enable_if<std::is_unsigned<T>::value && std::is_integral<T>::value, void>::type
radixSort(T * arr, size_t size)
{
	return RadixSort<RadixSortUIntTraits<T>>::execute(arr, size);
}

template <typename T>
typename std::enable_if<std::is_signed<T>::value && std::is_integral<T>::value, void>::type
radixSort(T * arr, size_t size)
{
	return RadixSort<RadixSortIntTraits<T>>::execute(arr, size);
}

template <typename T>
typename std::enable_if<std::is_floating_point<T>::value, void>::type
radixSort(T * arr, size_t size)
{
	return RadixSort<RadixSortFloatTraits<T>>::execute(arr, size);
}

