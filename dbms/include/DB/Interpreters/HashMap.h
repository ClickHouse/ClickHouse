#pragma once

#include <string.h>

#include <malloc.h>
#include <math.h>

#include <utility>

#include <boost/noncopyable.hpp>

#include <Yandex/likely.h>

#include <stats/IntHash.h>

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#ifdef DBMS_HASH_MAP_DEBUG_RESIZES
	#include <iostream>
	#include <iomanip>
	#include <statdaemons/Stopwatch.h>
#endif


namespace DB
{


/** Очень простая хэш-таблица. Предназначена для быстрой агрегации. Есть только необходимый минимум возможностей.
  * Требования:
  * - Key и Mapped - position independent типы (для перемещения значений которых достаточно сделать memcpy).
  *
  * Желательно, чтобы Key был числом, или маленьким агрегатом (типа UInt128).
  * 
  * Сценарий работы:
  * - вставлять в хэш-таблицу значения;
  * - проитерироваться по имеющимся в ней значениям.
  *
  * Open addressing.
  * Linear probing (подходит, если хэш функция хорошая!).
  * Значение с нулевым ключём хранится отдельно.
  * Удаления элементов нет.
  */


/** Хэш функции, которые лучше чем тривиальная функция std::hash.
  * (при агрегации по идентификатору посетителя, прирост производительности более чем в 5 раз)
  */
template <typename T> struct default_hash;

template <typename T>
inline size_t default_hash_64(T key)
{
	union
	{
		T in;
		UInt64 out;
	} u;
	u.out = 0;
	u.in = key;
	return intHash32<0>(u.out);
}

#define DEFAULT_HASH_64(T) \
template <> struct default_hash<T>\
{\
	size_t operator() (T key) const\
	{\
		return default_hash_64<T>(key);\
	}\
};

DEFAULT_HASH_64(UInt8)
DEFAULT_HASH_64(UInt16)
DEFAULT_HASH_64(UInt32)
DEFAULT_HASH_64(UInt64)
DEFAULT_HASH_64(Int8)
DEFAULT_HASH_64(Int16)
DEFAULT_HASH_64(Int32)
DEFAULT_HASH_64(Int64)
DEFAULT_HASH_64(Float32)
DEFAULT_HASH_64(Float64)

#undef DEFAULT_HASH_64


/** Способ проверить, что ключ нулевой,
  *  а также способ установить значение ключа в ноль.
  * При этом, нулевой ключ всё-равно должен быть представлен только нулевыми байтами
  *  (кроме, возможно, мусора из-за выравнивания).
  */
template <typename T> struct default_zero_traits
{
	static inline bool check(T x) { return 0 == x; }
	static inline void set(T & x) { x = 0; }
};


/** Описание, как хэш-таблица будет расти.
  */
struct default_growth_traits
{
	/** Изначально выделить кусок памяти для 64K элементов.
	  * Уменьшите значение для лучшей кэш-локальности в случае маленького количества уникальных ключей.
	  */
	static const int INITIAL_SIZE_DEGREE  = 16;

	/** Степень роста хэш таблицы, пока не превышен порог размера. (В 4 раза.)
	  */
	static const int FAST_GROWTH_DEGREE = 2;

	/** Порог размера, после которого степень роста уменьшается (до роста в 2 раза) - 8 миллионов элементов.
	  * После этого порога, максимально возможный оверхед по памяти будет всего лишь в 4, а не в 8 раз.
	  */
	static const int GROWTH_CHANGE_THRESHOLD = 23;
};


template
<
	typename Key,
	typename Mapped,
	typename Hash = default_hash<Key>,
	typename ZeroTraits = default_zero_traits<Key>,
	typename GrowthTraits = default_growth_traits
>
class HashMap : private boost::noncopyable
{
private:
	friend class const_iterator;
	friend class iterator;
	
	typedef std::pair<Key, Mapped> Value;	/// Без const Key для простоты.
	typedef size_t HashValue;
	typedef HashMap<Key, Mapped, Hash, ZeroTraits, GrowthTraits> Self;
	
	size_t m_size;			/// Количество элементов
	UInt8 size_degree;		/// Размер таблицы в виде степени двух
	bool has_zero;			/// Хэш-таблица содержит элемент со значением ключа = 0.
	Value * buf;			/// Кусок памяти для всех элементов кроме элемента с ключём 0.
	char zero_value_storage[sizeof(Value)];	/// Кусок памяти для элемента с ключём 0.

	Hash hash;

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
	mutable size_t collisions;
#endif

	inline size_t buf_size() const				{ return 1 << size_degree; }
	inline size_t buf_size_bytes() const		{ return buf_size() * sizeof(Value); }
	inline size_t max_fill() const				{ return 1 << (size_degree - 1); }
	inline size_t mask() const					{ return buf_size() - 1; }
	inline size_t place(HashValue x) const 		{ return x & mask(); }

	inline Value * zero_value()					{ return reinterpret_cast<Value*>(zero_value_storage); }


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
		buf = reinterpret_cast<Value *>(realloc(reinterpret_cast<void *>(buf), buf_size_bytes()));

		if (NULL == buf)
			throwFromErrno("HashMap: Cannot realloc.", ErrorCodes::CANNOT_ALLOCATE_MEMORY);

		/// Очистим новый кусок памяти.
		memset(buf + old_size, 0, buf_size_bytes() - old_size_bytes);

		/** Теперь некоторые элементы может потребоваться переместить на новое место.
		  * Элемент может остаться на месте, или переместиться в новое место "справа",
		  *  или переместиться левее по цепочке разрешения коллизий, из-за того, что элементы левее него были перемещены в новое место "справа".
		  */
		for (size_t i = 0; i < old_size; ++i)
			if (!ZeroTraits::check(buf[i].first))
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
		size_t place_value = place(hash(x.first));

		/// Если элемент на своём месте.
		if (&x == &buf[place_value])
			return;

		/// Вычисление нового места, с учётом цепочки разрешения коллизий.
		while (!ZeroTraits::check(buf[place_value].first))
		{
			++place_value;
			place_value &= mask();
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			++collisions;
#endif
		}

		/// Копирование на новое место и зануление старого.
		memcpy(&buf[place_value], &x, sizeof(x));
		ZeroTraits::set(x.first);

		/// Потом на старое место могут переместиться элементы, которые раньше были в коллизии с этим.
	}


public:
	typedef Key key_type;
	typedef Mapped mapped_type;
	typedef Value value_type;
	
	
	HashMap() :
		m_size(0),
		size_degree(GrowthTraits::INITIAL_SIZE_DEGREE),
		has_zero(false)
	{
		ZeroTraits::set(zero_value()->first);

		buf = reinterpret_cast<Value *>(calloc(buf_size_bytes(), 1));

		if (NULL == buf)
			throwFromErrno("HashMap: Cannot calloc.", ErrorCodes::CANNOT_ALLOCATE_MEMORY);

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
		collisions = 0;
#endif
	}

	~HashMap()
	{
		if (!__has_trivial_destructor(Key) || !__has_trivial_destructor(Mapped))
			for (iterator it = begin(); it != end(); ++it)
				it->~Value();

		free(reinterpret_cast<void *>(buf));
	}


	class iterator
	{
		Self * container;
		Value * ptr;

		friend class HashMap;

		iterator(Self * container_, Value * ptr_) : container(container_), ptr(ptr_) {}

	public:
		iterator() {}
		
		bool operator== (const iterator & rhs) const { return ptr == rhs.ptr; }
		bool operator!= (const iterator & rhs) const { return ptr != rhs.ptr; }

		iterator & operator++()
		{
			if (unlikely(ZeroTraits::check(ptr->first)))
				ptr = container->buf;
			else
				++ptr;

			while (ptr < container->buf + container->buf_size() && ZeroTraits::check(ptr->first))
				++ptr;

			return *this;
		}

		Value & operator* () const { return *ptr; }
		Value * operator->() const { return ptr; }
	};
	

	class const_iterator
	{
		const Self * container;
		const Value * ptr;

		friend class HashMap;

		const_iterator(const Self * container_, const Value * ptr_) : container(container_), ptr(ptr_) {}

	public:
		const_iterator() {}
		const_iterator(const iterator & rhs) : container(rhs.container), ptr(rhs.ptr) {}

		bool operator== (const const_iterator & rhs) const { return ptr == rhs.ptr; }
		bool operator!= (const const_iterator & rhs) const { return ptr != rhs.ptr; }

		const_iterator & operator++()
		{
			if (unlikely(ZeroTraits::check(ptr->first)))
				ptr = container->buf;
			else
				++ptr;

			while (ptr < container->buf + container->buf_size() && ZeroTraits::check(ptr->first))
				++ptr;

			return *this;
		}

		const Value & operator* () const { return *ptr; }
		const Value * operator->() const { return ptr; }
	};


	const_iterator begin() const
	{
		if (has_zero)
			return const_iterator(this, zero_value());

		const Value * ptr = buf;
		while (ptr < buf + buf_size() && ZeroTraits::check(ptr->first))
			++ptr;

		return const_iterator(this, ptr);
	}

	iterator begin()
	{
		if (has_zero)
			return iterator(this, zero_value());

		Value * ptr = buf;
		while (ptr < buf + buf_size() && ZeroTraits::check(ptr->first))
			++ptr;

		return iterator(this, ptr);
	}

	const_iterator end() const 		{ return const_iterator(this, buf + buf_size()); }
	iterator end() 					{ return iterator(this, buf + buf_size()); }
	
	
	/// Вставить значение. В случае хоть сколько-нибудь сложных значений, лучше используйте функцию emplace.
	std::pair<iterator, bool> insert(const Value & x)
	{
		if (ZeroTraits::check(x.first))
		{
			if (!has_zero)
			{
				++m_size;
				has_zero = true;
				zero_value()->second = x.second;
				return std::make_pair(begin(), true);
			}
			return std::make_pair(begin(), false);
		}

		size_t place_value = place(hash(x.first));
		while (!ZeroTraits::check(buf[place_value].first) && buf[place_value].first != x.first)
		{
			++place_value;
			place_value &= mask();
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			++collisions;
#endif
		}

		iterator res(this, &buf[place_value]);

		if (!ZeroTraits::check(buf[place_value].first) && buf[place_value].first == x.first)
			return std::make_pair(res, false);

		buf[place_value] = x;
		++m_size;

		if (unlikely(m_size > max_fill()))
		{
			resize();
			return std::make_pair(find(x.first), true);
		}

		return std::make_pair(res, true);
	}


	/** Вставить ключ,
	  * вернуть итератор на позицию, которую можно использовать для placement new значения,
	  * а также флаг - был ли вставлен новый ключ.
	  *
	  * Вы обязаны сделать placement new значения, если был вставлен новый ключ,
	  * так как при уничтожении хэш-таблицы для него будет вызываться деструктор!
	  *
	  * Пример использования:
	  *
	  * Map::iterator it;
	  * bool inserted;
	  * map.emplace(key, it, inserted);
	  * if (inserted)
	  * 	new(&it->second) Value(value);
	  */
	void emplace(Key x, iterator & it, bool & inserted)
	{
		if (ZeroTraits::check(x))
		{
			if (!has_zero)
			{
				++m_size;
				has_zero = true;
				inserted = true;
			}
			else
				inserted = false;

			it = begin();
			return;
		}

		size_t place_value = place(hash(x));
		while (!ZeroTraits::check(buf[place_value].first) && buf[place_value].first != x)
		{
			++place_value;
			place_value &= mask();
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			++collisions;
#endif
		}

		it = iterator(this, &buf[place_value]);

		if (!ZeroTraits::check(buf[place_value].first) && buf[place_value].first == x)
		{
			inserted = false;
			return;
		}

		new(&buf[place_value].first) Key(x);
		inserted = true;
		++m_size;

		if (unlikely(m_size > max_fill()))
		{
			resize();
			it = find(x);
		}
	}


	/// То же самое, но с заранее вычисленным значением хэш-функции.
	void emplace(Key x, iterator & it, bool & inserted, size_t hash_value)
	{
		if (ZeroTraits::check(x))
		{
			if (!has_zero)
			{
				++m_size;
				has_zero = true;
				inserted = true;
			}
			else
				inserted = false;

			it = begin();
			return;
		}

		size_t place_value = place(hash_value);
		while (!ZeroTraits::check(buf[place_value].first) && buf[place_value].first != x)
		{
			++place_value;
			place_value &= mask();
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			++collisions;
#endif
		}

		it = iterator(this, &buf[place_value]);

		if (!ZeroTraits::check(buf[place_value].first) && buf[place_value].first == x)
		{
			inserted = false;
			return;
		}

		new(&buf[place_value].first) Key(x);
		inserted = true;
		++m_size;

		if (unlikely(m_size > max_fill()))
		{
			resize();
			it = find(x);
		}
	}


	iterator find(Key x)
	{
		if (ZeroTraits::check(x))
			return has_zero ? begin() : end();

		size_t place_value = place(hash(x));
		while (!ZeroTraits::check(buf[place_value].first) && buf[place_value].first != x)
		{
			++place_value;
			place_value &= mask();
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			++collisions;
#endif
		}

		return !ZeroTraits::check(buf[place_value].first) ? iterator(this, &buf[place_value]) : end();
	}


	const_iterator find(Key x) const
	{
		if (ZeroTraits::check(x))
			return has_zero ? begin() : end();

		size_t place_value = place(hash(x.first));
		while (!ZeroTraits::check(buf[place_value].first) && buf[place_value].first != x)
		{
			++place_value;
			place_value &= mask();
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			++collisions;
#endif
		}

		return !ZeroTraits::check(buf[place_value].first) ? const_iterator(this, &buf[place_value]) : end();
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
		if (ZeroTraits::check(x))
		{
			if (!has_zero)
			{
				++m_size;
				has_zero = true;
			}
			return zero_value()->first;
		}

		size_t place_value = place(hash(x));
		while (!ZeroTraits::check(buf[place_value].first) && buf[place_value].first != x)
		{
			++place_value;
			place_value &= mask();
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			++collisions;
#endif
		}

		if (!ZeroTraits::check(buf[place_value].first))
			return buf[place_value].second;

		new(&buf[place_value].first) Key(x);
		new(&buf[place_value].second) Value();
		++m_size;

		if (unlikely(m_size > max_fill()))
		{
			resize();
			return (*this)[x];
		}

		return buf[place_value].second;
	}

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
	size_t getCollisions() const
	{
		return collisions;
	}
#endif
};


}
