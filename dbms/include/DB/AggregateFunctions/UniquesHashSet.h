#pragma once

#include <math.h>

#include <common/Common.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/VarInt.h>

#include <DB/Common/HashTable/HashTableAllocator.h>
#include <DB/Common/HashTable/Hash.h>


/** Приближённый рассчёт чего-угодно, как правило, построен по следующей схеме:
  * - для рассчёта значения X используется некоторая структура данных;
  * - в структуру данных добавляются не все значения, а только избранные (согласно некоторому критерию избранности);
  * - после обработки всех элементов, структура данных находится в некотором состоянии S;
  * - в качестве приближённого значения X возвращается значание, посчитанное по принципу максимального правдоподобия:
  *   при каком реальном значении X, вероятность нахождения структуры данных в полученном состоянии S максимальна.
  */

/** В частности, то, что описано ниже, можно найти по названию BJKST algorithm.
  */

/** Очень простое хэш-множество для приближённого подсчёта количества уникальных значений.
  * Работает так:
  * - вставлять можно UInt64;
  * - перед вставкой, сначала вычисляется хэш-функция UInt64 -> UInt32;
  * - исходное значение не сохраняется (теряется);
  * - далее все операции производятся с этими хэшами;
  * - хэш таблица построена по схеме:
  * -  open addressing (один буфер, позиция в буфере вычисляется взятием остатка от деления на его размер);
  * -  linear probing (если в ячейке уже есть значение, то берётся ячейка, следующая за ней и т. д.);
  * -  отсутствующее значение кодируется нулём; чтобы запомнить наличие в множестве нуля, используется отдельная переменная типа bool;
  * -  рост буфера в 2 раза при заполнении более чем на 50%;
  * - если в множестве больше UNIQUES_HASH_MAX_SIZE элементов, то из множества удаляются все элементы,
  *   не делящиеся на 2, и затем все элементы, которые не делятся на 2, не вставляются в множество;
  * - если ситуация повторяется, то берутся только элементы делящиеся на 4 и т. п.
  * - метод size() возвращает приблизительное количество элементов, которые были вставлены в множество;
  * - есть методы для быстрого чтения и записи в бинарный и текстовый вид.
  */


/// Максимальная степень размера буфера перед тем, как значения будут выкидываться
#define UNIQUES_HASH_MAX_SIZE_DEGREE 			17

/// Максимальное количество элементов перед тем, как значения будут выкидываться
#define UNIQUES_HASH_MAX_SIZE 					(1 << (UNIQUES_HASH_MAX_SIZE_DEGREE - 1))

/** Количество младших бит, использующихся для прореживания. Оставшиеся старшие биты используются для определения позиции в хэш-таблице.
  * (старшие биты берутся потому что младшие будут постоянными после выкидывания части значений)
  */
#define UNIQUES_HASH_BITS_FOR_SKIP 			(32 - UNIQUES_HASH_MAX_SIZE_DEGREE)

/// Начальная степень размера буфера
#define UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE 	4


/** Эта хэш-функция не самая оптимальная, но состояния UniquesHashSet, посчитанные с ней,
  *  хранятся много где на дисках (в Метраже), поэтому она продолжает использоваться.
  */
struct UniquesHashSetDefaultHash
{
	size_t operator() (UInt64 x) const
	{
		return intHash32<0>(x);
	}
};


template <typename Hash = UniquesHashSetDefaultHash>
class UniquesHashSet : private HashTableAllocatorWithStackMemory<(1 << UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE) * sizeof(UInt32)>
{
private:
	typedef UInt64 Value_t;
	typedef UInt32 HashValue_t;
	typedef HashTableAllocatorWithStackMemory<(1 << UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE) * sizeof(UInt32)> Allocator;

	UInt32 m_size;			/// Количество элементов
	UInt8 size_degree;		/// Размер таблицы в виде степени двух
	UInt8 skip_degree;		/// Пропускать элементы не делящиеся на 2 ^ skip_degree
	bool has_zero;			/// Хэш-таблица содержит элемент со значением хэш-функции = 0.

	HashValue_t * buf;

#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
	/// Для профилирования.
	mutable size_t collisions;
#endif

	void alloc(UInt8 new_size_degree)
	{
		buf = reinterpret_cast<HashValue_t *>(Allocator::alloc((1 << new_size_degree) * sizeof(buf[0])));
		size_degree = new_size_degree;
	}

	void free()
	{
		if (buf)
		{
			Allocator::free(buf, buf_size() * sizeof(buf[0]));
			buf = nullptr;
		}
	}

	inline size_t buf_size() const						{ return 1 << size_degree; }
	inline size_t max_fill() const						{ return 1 << (size_degree - 1); }
	inline size_t mask() const							{ return buf_size() - 1; }
	inline size_t place(HashValue_t x) const 			{ return (x >> UNIQUES_HASH_BITS_FOR_SKIP) & mask(); }

	/// Значение делится на 2 ^ skip_degree
	inline bool good(HashValue_t hash) const
	{
		return hash == ((hash >> skip_degree) << skip_degree);
	}

	HashValue_t hash(Value_t key) const
	{
		return Hash()(key);
	}

	/// Удалить все значения, хэши которых не делятся на 2 ^ skip_degree
	void rehash()
	{
		for (size_t i = 0; i < buf_size(); ++i)
		{
			if (buf[i] && !good(buf[i]))
			{
				buf[i] = 0;
				--m_size;
			}
		}

		/** После удаления элементов, возможно, освободилось место для элементов,
		  * которые были помещены дальше, чем нужно, из-за коллизии.
		  * Надо переместить их.
		  */
		for (size_t i = 0; i < buf_size(); ++i)
		{
			if (unlikely(buf[i] && i != place(buf[i])))
			{
				HashValue_t x = buf[i];
				buf[i] = 0;
				reinsertImpl(x);
			}
		}
	}

	/// Увеличить размер буфера в 2 раза или до new_size_degree, если указана ненулевая.
	void resize(size_t new_size_degree = 0)
	{
		size_t old_size = buf_size();

		if (!new_size_degree)
			new_size_degree = size_degree + 1;

		/// Расширим пространство.
		buf = reinterpret_cast<HashValue_t *>(Allocator::realloc(buf, old_size * sizeof(buf[0]), (1 << new_size_degree) * sizeof(buf[0])));
		size_degree = new_size_degree;

		/** Теперь некоторые элементы может потребоваться переместить на новое место.
		  * Элемент может остаться на месте, или переместиться в новое место "справа",
		  *  или переместиться левее по цепочке разрешения коллизий, из-за того, что элементы левее него были перемещены в новое место "справа".
		  * Также имеется особый случай:
		  *    если элемент должен был быть в конце старого буфера,                    [        x]
		  *    но находится в начале из-за цепочки разрешения коллизий,                [o       x]
		  *    то после ресайза, он сначала снова окажется не на своём месте,          [        xo        ]
		  *    и для того, чтобы перенести его куда надо,
		  *    надо будет после переноса всех элементов из старой половинки            [         o   x    ]
		  *    обработать ещё хвостик из цепочки разрешения коллизий сразу после неё   [        o    x    ]
		  * Именно для этого написано || buf[i] ниже.
		  */
		for (size_t i = 0; i < old_size || buf[i]; ++i)
		{
			HashValue_t x = buf[i];
			if (!x)
				continue;

			size_t place_value = place(x);

			/// Элемент на своём месте.
			if (place_value == i)
				continue;

			while (buf[place_value] && buf[place_value] != x)
			{
				++place_value;
				place_value &= mask();

#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
				++collisions;
#endif
			}

			/// Элемент остался на своём месте.
			if (buf[place_value] == x)
				continue;

			buf[place_value] = x;
			buf[i] = 0;
		}
	}

	/// Вставить значение.
	void insertImpl(HashValue_t x)
	{
		if (x == 0)
		{
			m_size += !has_zero;
			has_zero = true;
			return;
		}

		size_t place_value = place(x);
		while (buf[place_value] && buf[place_value] != x)
		{
			++place_value;
			place_value &= mask();

#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
			++collisions;
#endif
		}

		if (buf[place_value] == x)
			return;

		buf[place_value] = x;
		++m_size;
	}

	/** Вставить в новый буфер значение, которое было в старом буфере.
	  * Используется при увеличении размера буфера, а также при чтении из файла.
	  */
	void reinsertImpl(HashValue_t x)
	{
		size_t place_value = place(x);
		while (buf[place_value])
		{
			++place_value;
			place_value &= mask();

#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
			++collisions;
#endif
		}

		buf[place_value] = x;
	}

	/** Если хэш-таблица достаточно заполнена, то сделать resize.
	  * Если элементов слишком много - то выкидывать половину, пока их не станет достаточно мало.
	  */
	void shrinkIfNeed()
	{
		if (unlikely(m_size > max_fill()))
		{
			if (m_size > UNIQUES_HASH_MAX_SIZE)
			{
				while (m_size > UNIQUES_HASH_MAX_SIZE)
				{
					++skip_degree;
					rehash();
				}
			}
			else
				resize();
		}
	}


public:
	UniquesHashSet() :
		m_size(0),
		skip_degree(0),
		has_zero(false)
	{
		alloc(UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE);
#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
		collisions = 0;
#endif
	}

	UniquesHashSet(const UniquesHashSet & rhs)
		: m_size(rhs.m_size), skip_degree(rhs.skip_degree), has_zero(rhs.has_zero)
	{
		alloc(rhs.size_degree);
		memcpy(buf, rhs.buf, buf_size() * sizeof(buf[0]));
	}

	UniquesHashSet & operator= (const UniquesHashSet & rhs)
	{
		if (size_degree != rhs.size_degree)
		{
			free();
			alloc(rhs.size_degree);
		}

		m_size = rhs.m_size;
		skip_degree = rhs.skip_degree;
		has_zero = rhs.has_zero;

		memcpy(buf, rhs.buf, buf_size() * sizeof(buf[0]));

		return *this;
	}

	~UniquesHashSet()
	{
		free();
	}

	void insert(Value_t x)
	{
		HashValue_t hash_value = hash(x);
		if (!good(hash_value))
			return;

		insertImpl(hash_value);
		shrinkIfNeed();
	}

	size_t size() const
	{
		if (0 == skip_degree)
			return m_size;

		size_t res = m_size * (1 << skip_degree);

		/** Псевдослучайный остаток - для того, чтобы не было видно,
		  * что количество делится на степень двух.
		  */
		res += (intHashCRC32(m_size) & ((1 << skip_degree) - 1));

		/** Коррекция систематической погрешности из-за коллизий при хэшировании в UInt32.
		  * Формула fixed_res(res)
		  * - при каком количестве разных элементов fixed_res,
		  *   при их случайном разбрасывании по 2^32 корзинам,
		  *   получается в среднем res заполненных корзин.
		  */
		size_t p32 = 1ULL << 32;
		size_t fixed_res = round(p32 * (log(p32) - log(p32 - res)));
		return fixed_res;
	}

	void merge(const UniquesHashSet & rhs)
	{
		if (rhs.skip_degree > skip_degree)
		{
			skip_degree = rhs.skip_degree;
			rehash();
		}

		if (!has_zero && rhs.has_zero)
		{
			has_zero = true;
			++m_size;
			shrinkIfNeed();
		}

		for (size_t i = 0; i < rhs.buf_size(); ++i)
		{
			if (rhs.buf[i] && good(rhs.buf[i]))
			{
				insertImpl(rhs.buf[i]);
				shrinkIfNeed();
			}
		}
	}

	void write(DB::WriteBuffer & wb) const
	{
		if (m_size > UNIQUES_HASH_MAX_SIZE)
			throw Poco::Exception("Cannot write UniquesHashSet: too large size_degree.");

		DB::writeIntBinary(skip_degree, wb);
		DB::writeVarUInt(m_size, wb);

		if (has_zero)
		{
			HashValue_t x = 0;
			DB::writeIntBinary(x, wb);
		}

		for (size_t i = 0; i < buf_size(); ++i)
			if (buf[i])
				DB::writeIntBinary(buf[i], wb);
	}

	void read(DB::ReadBuffer & rb)
	{
		has_zero = false;

		DB::readIntBinary(skip_degree, rb);
		DB::readVarUInt(m_size, rb);

		if (m_size > UNIQUES_HASH_MAX_SIZE)
			throw Poco::Exception("Cannot read UniquesHashSet: too large size_degree.");

		free();

		UInt8 new_size_degree = m_size <= 1
			 ? UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE
			 : std::max(UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE, static_cast<int>(log2(m_size - 1)) + 2);

		alloc(new_size_degree);

		for (size_t i = 0; i < m_size; ++i)
		{
			HashValue_t x = 0;
			DB::readIntBinary(x, rb);
			if (x == 0)
				has_zero = true;
			else
				reinsertImpl(x);
		}
	}

	void readAndMerge(DB::ReadBuffer & rb)
	{
		UInt8 rhs_skip_degree = 0;
		DB::readIntBinary(rhs_skip_degree, rb);

		if (rhs_skip_degree > skip_degree)
		{
			skip_degree = rhs_skip_degree;
			rehash();
		}

		size_t rhs_size = 0;
		DB::readVarUInt(rhs_size, rb);

		if (rhs_size > UNIQUES_HASH_MAX_SIZE)
			throw Poco::Exception("Cannot read UniquesHashSet: too large size_degree.");

		if ((1U << size_degree) < rhs_size)
		{
			UInt8 new_size_degree = std::max(UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE, static_cast<int>(log2(rhs_size - 1)) + 2);
			resize(new_size_degree);
		}

		for (size_t i = 0; i < rhs_size; ++i)
		{
			HashValue_t x = 0;
			DB::readIntBinary(x, rb);
			insertHash(x);
		}
	}

	static void skip(DB::ReadBuffer & rb)
	{
		size_t size = 0;

		rb.ignore();
		DB::readVarUInt(size, rb);

		if (size > UNIQUES_HASH_MAX_SIZE)
			throw Poco::Exception("Cannot read UniquesHashSet: too large size_degree.");

		rb.ignore(sizeof(HashValue_t) * size);
	}

	void writeText(DB::WriteBuffer & wb) const
	{
		if (m_size > UNIQUES_HASH_MAX_SIZE)
			throw Poco::Exception("Cannot write UniquesHashSet: too large size_degree.");

		DB::writeIntText(skip_degree, wb);
		wb.write(",", 1);
		DB::writeIntText(m_size, wb);

		if (has_zero)
			wb.write(",0", 2);

		for (size_t i = 0; i < buf_size(); ++i)
		{
			if (buf[i])
			{
				wb.write(",", 1);
				DB::writeIntText(buf[i], wb);
			}
		}
	}

	void readText(DB::ReadBuffer & rb)
	{
		has_zero = false;

		DB::readIntText(skip_degree, rb);
		DB::assertChar(',', rb);
		DB::readIntText(m_size, rb);

		if (m_size > UNIQUES_HASH_MAX_SIZE)
			throw Poco::Exception("Cannot read UniquesHashSet: too large size_degree.");

		free();

		UInt8 new_size_degree = m_size <= 1
			 ? UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE
			 : std::max(UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE, static_cast<int>(log2(m_size - 1)) + 2);

		alloc(new_size_degree);

		for (size_t i = 0; i < m_size; ++i)
		{
			HashValue_t x = 0;
			DB::assertChar(',', rb);
			DB::readIntText(x, rb);
			if (x == 0)
				has_zero = true;
			else
				reinsertImpl(x);
		}
	}

	void insertHash(HashValue_t hash_value)
	{
		if (!good(hash_value))
			return;

		insertImpl(hash_value);
		shrinkIfNeed();
	}

#ifdef UNIQUES_HASH_SET_COUNT_COLLISIONS
	size_t getCollisions() const
	{
		return collisions;
	}
#endif
};


#undef UNIQUES_HASH_MAX_SIZE_DEGREE
#undef UNIQUES_HASH_MAX_SIZE
#undef UNIQUES_HASH_BITS_FOR_SKIP
#undef UNIQUES_HASH_SET_INITIAL_SIZE_DEGREE
