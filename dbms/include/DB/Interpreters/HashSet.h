#pragma once

#include <DB/Interpreters/HashMap.h>


namespace DB
{


/** См. HashMap.h
  *
  * Очень простое хэш множество.
  * Можно только вставлять значения, а также проверять принадлежность.
  *
  * Итераторов нет. Не поддерживаются элементы с деструктором.
  */

template
<
	typename Key,
	typename Hash = default_hash<Key>,
	typename ZeroTraits = default_zero_traits<Key>,
	int INITIAL_SIZE_DEGREE = 16,
	int GROWTH_DEGREE = 2
>
class HashSet : private boost::noncopyable
{
private:
	typedef size_t HashValue;
	typedef HashSet<Key, Hash, ZeroTraits, INITIAL_SIZE_DEGREE, GROWTH_DEGREE> Self;
	
	size_t m_size;			/// Количество элементов
	UInt8 size_degree;		/// Размер таблицы в виде степени двух
	bool has_zero;			/// Хэш-таблица содержит элемент с ключём = 0.
	Key * buf;				/// Кусок памяти для всех элементов кроме элемента с ключём 0.

	Hash hash;

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
	mutable size_t collisions;
#endif

	inline size_t buf_size() const				{ return 1 << size_degree; }
	inline size_t max_fill() const				{ return 1 << (size_degree - 1); }
	inline size_t mask() const					{ return buf_size() - 1; }
	inline size_t place(HashValue x) const 		{ return x & mask(); }


	/// Увеличить размер буфера в 2 ^ GROWTH_DEGREE раз
	void resize()
	{
		size_t old_size = buf_size();
		
		size_degree += GROWTH_DEGREE;
		Key * new_buf = reinterpret_cast<Key*>(calloc(buf_size(), sizeof(Key)));
		Key * old_buf = buf;
		buf = new_buf;
		
		for (size_t i = 0; i < old_size; ++i)
			if (!ZeroTraits::check(old_buf[i]))
				reinsert(old_buf[i]);

		free(reinterpret_cast<void*>(old_buf));
	}


	/** Вставить в новый буфер значение, которое было в старом буфере.
	  * Используется при увеличении размера буфера.
	  */
	void reinsert(const Key & x)
	{
		size_t place_value = place(hash(x));
		while (!ZeroTraits::check(buf[place_value]))
		{
			++place_value;
			place_value &= mask();
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			++collisions;
#endif
		}
		memcpy(&buf[place_value], &x, sizeof(x));
	}


public:
	typedef Key key_type;
	typedef Key value_type;
	
	
	HashSet() :
		m_size(0),
		size_degree(INITIAL_SIZE_DEGREE),
		has_zero(false)
	{
		buf = reinterpret_cast<Key*>(calloc(buf_size(), sizeof(Key)));
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
		collisions = 0;
#endif
	}

	~HashSet()
	{
		free(reinterpret_cast<void*>(buf));
	}

	
	/// Вставить ключ.
	void insert(const Key & x)
	{
		if (ZeroTraits::check(x))
		{
			if (!has_zero)
			{
				++m_size;
				has_zero = true;
			}
			return;
		}

		size_t place_value = place(hash(x));
		while (!ZeroTraits::check(buf[place_value]) && buf[place_value] != x)
		{
			++place_value;
			place_value &= mask();
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			++collisions;
#endif
		}

		if (!ZeroTraits::check(buf[place_value]) && buf[place_value] == x)
			return;

		buf[place_value] = x;
		++m_size;

		if (unlikely(m_size > max_fill()))
			resize();

		return;
	}


	bool has(Key x) const
	{
		if (ZeroTraits::check(x))
			return has_zero;

		size_t place_value = place(hash(x));
		while (!ZeroTraits::check(buf[place_value]) && buf[place_value] != x)
		{
			++place_value;
			place_value &= mask();
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			++collisions;
#endif
		}

		return !ZeroTraits::check(buf[place_value]);
	}
	

	size_t size() const
	{
	    return m_size;
	}

	bool empty() const
	{
	    return 0 == m_size;
	}

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
	size_t getCollisions() const
	{
		return collisions;
	}
#endif
};

}
