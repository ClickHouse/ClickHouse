#pragma once

#include <DB/Interpreters/HashMap.h>


namespace DB
{


/** См. HashMap.h
  */

template
<
	typename Key,
	typename Hash = default_hash<Key>,
	typename ZeroTraits = default_zero_traits<Key>,
	typename GrowthTraits = default_growth_traits
>
class HashSet : private boost::noncopyable
{
private:
	friend class const_iterator;
	friend class iterator;
	
	typedef size_t HashValue;
	typedef HashSet<Key, Hash, ZeroTraits, GrowthTraits> Self;
	
	size_t m_size;			/// Количество элементов
	UInt8 size_degree;		/// Размер таблицы в виде степени двух
	bool has_zero;			/// Хэш-таблица содержит элемент со значением ключа = 0.
	Key * buf;				/// Кусок памяти для всех элементов кроме элемента с ключём 0.

	Hash hash;

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
	mutable size_t collisions;
#endif

	inline size_t buf_size() const				{ return 1 << size_degree; }
	inline size_t buf_size_bytes() const		{ return buf_size() * sizeof(Key); }
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
		buf = reinterpret_cast<Key *>(realloc(reinterpret_cast<void *>(buf), buf_size_bytes()));

		if (NULL == buf)
			throwFromErrno("HashSet: Cannot realloc.", ErrorCodes::CANNOT_ALLOCATE_MEMORY);

		/// Очистим новый кусок памяти.
		memset(buf + old_size, 0, buf_size_bytes() - old_size_bytes);
		
		/** Теперь некоторые элементы может потребоваться переместить на новое место.
		  * Элемент может остаться на месте, или переместиться в новое место "справа",
		  *  или переместиться левее по цепочке разрешения коллизий, из-за того, что элементы левее него были перемещены в новое место "справа".
		  */
		for (size_t i = 0; i < old_size; ++i)
			if (!ZeroTraits::check(buf[i]))
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
	void reinsert(Key & x)
	{
		size_t place_value = place(hash(x));

		/// Если элемент на своём месте.
		if (&x == &buf[place_value])
			return;

		/// Вычисление нового места, с учётом цепочки разрешения коллизий.
		while (!ZeroTraits::check(buf[place_value]))
		{
			++place_value;
			place_value &= mask();
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			++collisions;
#endif
		}

		/// Копирование на новое место и зануление старого.
		memcpy(&buf[place_value], &x, sizeof(x));
		ZeroTraits::set(x);

		/// Потом на старое место могут переместиться элементы, которые раньше были в коллизии с этим.
	}


public:
	typedef Key key_type;
	typedef Key value_type;


	HashSet() :
		m_size(0),
		size_degree(GrowthTraits::INITIAL_SIZE_DEGREE),
		has_zero(false)
	{
		buf = reinterpret_cast<Key *>(calloc(buf_size_bytes(), 1));

		if (NULL == buf)
			throwFromErrno("HashSet: Cannot calloc.", ErrorCodes::CANNOT_ALLOCATE_MEMORY);
	}

	~HashSet()
	{
		for (iterator it = begin(); it != end(); ++it)
			it->~Key();

		free(reinterpret_cast<void *>(buf));
	}


	class iterator
	{
		Self * container;
		Key * ptr;

		friend class HashSet;

		iterator(Self * container_, Key * ptr_) : container(container_), ptr(ptr_) {}

	public:
		iterator() {}
		
		bool operator== (const iterator & rhs) const { return ptr == rhs.ptr; }
		bool operator!= (const iterator & rhs) const { return ptr != rhs.ptr; }

		iterator & operator++()
		{
			if (unlikely(!ptr))
				ptr = container->buf;
			else
				++ptr;

			while (ptr < container->buf + container->buf_size() && ZeroTraits::check(*ptr))
				++ptr;

			return *this;
		}

		Key & operator* () const { return *ptr; }
		Key * operator->() const { return ptr; }
	};
	

	class const_iterator
	{
		const Self * container;
		const Key * ptr;

		friend class HashSet;

		const_iterator(const Self * container_, const Key * ptr_) : container(container_), ptr(ptr_) {}

	public:
		const_iterator() {}
		const_iterator(const iterator & rhs) : container(rhs.container), ptr(rhs.ptr) {}

		bool operator== (const const_iterator & rhs) const { return ptr == rhs.ptr; }
		bool operator!= (const const_iterator & rhs) const { return ptr != rhs.ptr; }

		const_iterator & operator++()
		{
			if (unlikely(!ptr))
				ptr = container->buf;
			else
				++ptr;

			while (ptr < container->buf + container->buf_size() && ZeroTraits::check(*ptr))
				++ptr;

			return *this;
		}

		const Key & operator* () const { return *ptr; }
		const Key * operator->() const { return ptr; }
	};


	const_iterator begin() const
	{
		if (has_zero)
			return const_iterator(this, NULL);

		const Key * ptr = buf;
		while (ptr < buf + buf_size() && ZeroTraits::check(*ptr))
			++ptr;

		return const_iterator(this, ptr);
	}

	iterator begin()
	{
		if (has_zero)
			return iterator(this, NULL);

		Key * ptr = buf;
		while (ptr < buf + buf_size() && ZeroTraits::check(*ptr))
			++ptr;

		return iterator(this, ptr);
	}

	const_iterator end() const 		{ return const_iterator(this, buf + buf_size()); }
	iterator end() 					{ return iterator(this, buf + buf_size()); }
	
	
	/// Вставить ключ.
	std::pair<iterator, bool> insert(const Key & x)
	{
		if (ZeroTraits::check(x))
		{
			if (!has_zero)
			{
				++m_size;
				has_zero = true;
				return std::make_pair(begin(), true);
			}
			return std::make_pair(begin(), false);
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

		iterator res(this, &buf[place_value]);

		if (!ZeroTraits::check(buf[place_value]) && buf[place_value] == x)
			return std::make_pair(res, false);

		buf[place_value] = x;
		++m_size;

		if (unlikely(m_size > max_fill()))
		{
			resize();
			return std::make_pair(find(x), true);
		}

		return std::make_pair(res, true);
	}


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
		while (!ZeroTraits::check(buf[place_value]) && buf[place_value] != x)
		{
			++place_value;
			place_value &= mask();
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			++collisions;
#endif
		}

		it = iterator(this, &buf[place_value]);

		if (!ZeroTraits::check(buf[place_value]) && buf[place_value] == x)
		{
			inserted = false;
			return;
		}

		new(&buf[place_value]) Key(x);
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
		while (!ZeroTraits::check(buf[place_value]) && buf[place_value] != x)
		{
			++place_value;
			place_value &= mask();
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			++collisions;
#endif
		}

		return !ZeroTraits::check(buf[place_value]) ? iterator(this, &buf[place_value]) : end();
	}


	const_iterator find(Key x) const
	{
		if (ZeroTraits::check(x))
			return has_zero ? begin() : end();

		size_t place_value = place(hash(x));
		while (!ZeroTraits::check(buf[place_value]) && buf[place_value] != x)
		{
			++place_value;
			place_value &= mask();
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			++collisions;
#endif
		}

		return !ZeroTraits::check(buf[place_value]) ? const_iterator(this, &buf[place_value]) : end();
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
