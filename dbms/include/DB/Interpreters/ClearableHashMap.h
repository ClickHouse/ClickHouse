#pragma once

#include <DB/Interpreters/HashMap.h>

#include <string.h>

#include <malloc.h>

#include <boost/noncopyable.hpp>

#include <Yandex/likely.h>

#include <stats/IntHash.h>

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>


namespace DB
{


/** Хеш-таблица, позволяющая очищать таблицу за O(1).
  * Еще более простая, чем HashMap: Key и Mapped должны быть POD-типами.
  *
  * Вместо этого класса можно было бы просто использовать в HashMap в качестве ключа пару <версия, ключ>,
  * но тогда таблица накапливала бы все ключи, которые в нее когда-либо складывали, и неоправданно росла.
  * Этот класс идет на шаг дальше и считает ключи со старой версией пустыми местами в хеш-таблице.
  */

template
<
	typename Key,
	typename Mapped,
	typename Hash = default_hash<Key>,
	typename GrowthTraits = default_growth_traits,
	typename Allocator = HashTableAllocator
>
class ClearableHashMap : private boost::noncopyable, private Hash, private Allocator		/// empty base optimization
{
private:
	struct Value
	{
		Key key;
		UInt32 version;
		Mapped mapped;
	};
	typedef size_t HashValue;

	size_t m_size;			/// Количество элементов
	Value * buf;			/// Кусок памяти для всех элементов кроме элемента с ключём 0.
	UInt32 version;
	UInt8 size_degree;		/// Размер таблицы в виде степени двух

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
	mutable size_t collisions;
#endif

	inline size_t hash(const Key & x) const 	{ return Hash::operator()(x); }
	inline size_t buf_size() const				{ return 1 << size_degree; }
	inline size_t buf_size_bytes() const		{ return buf_size() * sizeof(Value); }
	inline size_t max_fill() const				{ return 1 << (size_degree - 1); }
	inline size_t mask() const					{ return buf_size() - 1; }
	inline size_t place(HashValue x) const 		{ return x & mask(); }


	/// Увеличить размер буфера в 2 ^ N раз
	void resize()
	{
#ifdef DBMS_HASH_MAP_DEBUG_RESIZES
		Stopwatch watch;
#endif

		size_t old_size = buf_size();
		size_t old_size_bytes = buf_size_bytes();

		size_degree += size_degree >= GrowthTraits::GROWTH_CHANGE_THRESHOLD
			? 1
			: GrowthTraits::FAST_GROWTH_DEGREE;

		/// Расширим пространство.
		buf = reinterpret_cast<Value *>(Allocator::realloc(buf, old_size_bytes, buf_size_bytes()));

		/** Теперь некоторые элементы может потребоваться переместить на новое место.
		  * Элемент может остаться на месте, или переместиться в новое место "справа",
		  *  или переместиться левее по цепочке разрешения коллизий, из-за того, что элементы левее него были перемещены в новое место "справа".
		  */
		for (size_t i = 0; i < old_size; ++i)
			if (buf[i].version == version)
				reinsert(buf[i]);

#ifdef DBMS_HASH_MAP_DEBUG_RESIZES
		watch.stop();
		std::cerr << std::fixed << std::setprecision(3)
			<< "Resize from " << old_size << " to " << buf_size() << " took " << watch.elapsedSeconds() << " sec."
			<< std::endl;
#endif
	}


	/** Вставить в новый буфер значение, которое было в старом буфере.
	  * Используется при увеличении размера буфера.
	  */
	void reinsert(Value & x)
	{
		size_t place_value = place(hash(x.key));

		/// Если элемент на своём месте.
		if (&x == &buf[place_value])
			return;

		/// Вычисление нового места, с учётом цепочки разрешения коллизий.
		while (buf[place_value].version == version && x.key != buf[place_value].key)
		{
			++place_value;
			place_value &= mask();
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			++collisions;
#endif
		}

		/// Если элемент остался на своём месте в старой цепочке разрешения коллизий.
		if (buf[place_value].version == version && x.key == buf[place_value].key)
			return;

		/// Копирование на новое место и зануление старого.
		memcpy(&buf[place_value], &x, sizeof(x));
		x.version = 0;

		/// Потом на старое место могут переместиться элементы, которые раньше были в коллизии с этим.
	}


public:
	typedef Key key_type;
	typedef Mapped mapped_type;
	typedef Value value_type;


	ClearableHashMap() :
		m_size(0),
		version(1),
		size_degree(GrowthTraits::INITIAL_SIZE_DEGREE)
	{
		buf = reinterpret_cast<Value *>(Allocator::alloc(buf_size_bytes()));

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
		collisions = 0;
#endif
	}

	~ClearableHashMap()
	{
		Allocator::free(buf, buf_size_bytes());
	}


	size_t size() const
	{
	    return m_size;
	}

	bool empty() const
	{
	    return 0 == m_size;
	}

	Mapped & operator[](Key x)
	{
		size_t place_value = place(hash(x));
		while (buf[place_value].version == version && buf[place_value].key != x)
		{
			++place_value;
			place_value &= mask();
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			++collisions;
#endif
		}

		if (buf[place_value].version == version)
			return buf[place_value].mapped;

		buf[place_value].key = x;
		buf[place_value].mapped = Mapped();
		buf[place_value].version = version;
		++m_size;

		if (unlikely(m_size > max_fill()))
		{
			resize();
			return (*this)[x];
		}

		return buf[place_value].mapped;
	}

	void clear()
	{
		++version;
		m_size = 0;
	}

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
	size_t getCollisions() const
	{
		return collisions;
	}
#endif
};

}
