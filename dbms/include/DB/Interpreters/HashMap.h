#pragma once

#include <string.h>

#include <map>

#include <boost/noncopyable.hpp>

#include <Yandex/optimization.h>


namespace DB
{


/** Очень простая хэш-таблица. Предназначена для быстрой агрегации. Есть только необходимый минимум возможностей.
  * Сценарий работы:
  * - вставлять в хэш-таблицу значения;
  * - проитерироваться по имеющимся в ней значениям.
  *
  * Open addressing.
  * Quadratic probing.
  * Значение с нулевым ключём хранится отдельно.
  * Удаления элементов нет.
  */


template <typename T> struct zero_traits
{
	static inline bool check(T x) { return 0 == x; }
	static inline void set(T & x) { x = 0; }
};


template
<
	typename Key,
	typename Mapped,
	typename Hash = std::tr1::hash<Key>,
	typename ZeroTraits = zero_traits<Key>,
	int INITIAL_SIZE_DEGREE = 16,
	int GROWTH_DEGREE = 1
>
class HashMap : private boost::noncopyable
{
private:
	friend class const_iterator;
	friend class iterator;
	
	typedef std::pair<Key, Mapped> Value;
	typedef size_t HashValue;
	typedef HashMap<Key, Mapped, Hash, ZeroTraits, INITIAL_SIZE_DEGREE, GROWTH_DEGREE> Self;
	
	size_t m_size;			/// Количество элементов
	UInt8 size_degree;		/// Размер таблицы в виде степени двух
	bool has_zero;			/// Хэш-таблица содержит элемент со значением ключа = 0.
	Value zero_value;
	Value * buf;

	Hash hash;


	void alloc()
	{
		buf = new Value[buf_size()];
	}
	
	void free()
	{
		delete[] buf;
	}

	void realloc()
	{
		free();
		alloc();
	}

	inline size_t buf_size() const				{ return 1 << size_degree; }
	inline size_t max_fill() const				{ return 1 << (size_degree - 1); }
	inline size_t mask() const					{ return buf_size() - 1; }
	inline size_t place(HashValue x) const 		{ return x & mask(); }


	/// Увеличить размер буфера в 2 ^ GROWTH_DEGREE раз
	void resize()
	{
		size_t old_size = buf_size();
		
		size_degree += GROWTH_DEGREE;
		Value * new_buf = new Value[buf_size()];
		Value * old_buf = buf;
		buf = new_buf;
		
		memset(new_buf, 0, buf_size() * sizeof(buf[0]));

		for (size_t i = 0; i < old_size; ++i)
			if (!ZeroTraits::check(old_buf[i].first))
				reinsert(old_buf[i]);

		delete[] old_buf;
	}


	/** Вставить в новый буфер значение, которое было в старом буфере.
	  * Используется при увеличении размера буфера.
	  */
	void reinsert(const Value & x)
	{
		size_t place_value = place(hash(x.first));
		unsigned increment = 1;
		while (!ZeroTraits::check(buf[place_value].first))
		{
			place_value += increment;
			++increment;
			place_value &= mask();
		}
		buf[place_value] = x;
	}


public:
	HashMap() :
		m_size(0),
		size_degree(INITIAL_SIZE_DEGREE),
		has_zero(false)
	{
		ZeroTraits::set(zero_value.first);
		alloc();
	}

	~HashMap()
	{
		free();
	}


	class iterator
	{
		Self * container;
		Value * ptr;

		friend class HashMap;

		iterator(Self * container_, Value * ptr_) : container(container_), ptr(ptr_) {}

	public:
		bool operator== (const iterator & rhs) const { return ptr == rhs.ptr; }
		bool operator!= (const iterator & rhs) const { return ptr != rhs.ptr; }

		iterator & operator++()
		{
			if (unlikely(ptr->first == 0))
				ptr = container->buf;
			else
				++ptr;

			while (ptr->first == 0 && ptr < container->buf + container->buf_size())
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

		const_iterator(const Self & container_, const Value * ptr_) : container(container_), ptr(ptr_) {}

	public:
		const_iterator(const iterator & rhs) : container(rhs.container), ptr(rhs.ptr) {}

		bool operator== (const const_iterator & rhs) const { return ptr == rhs.ptr; }
		bool operator!= (const const_iterator & rhs) const { return ptr != rhs.ptr; }

		const_iterator & operator++()
		{
			if (unlikely(ZeroTraits::check(ptr->first)))
				ptr = container->buf;
			else
				++ptr;

			while (ZeroTraits::check(ptr->first) && ptr < container->buf + container->buf_size())
				++ptr;

			return *this;
		}

		const Value & operator* () const { return *ptr; }
		const Value * operator->() const { return ptr; }
	};


	const_iterator begin() const
	{
		if (has_zero)
			return const_iterator(this, &zero_value);

		const Value * ptr = buf;
		while (ZeroTraits::check(ptr->first) && ptr < buf + buf_size())
			++ptr;

		return const_iterator(this, ptr);
	}

	iterator begin()
	{
		if (has_zero)
			return iterator(this, &zero_value);

		Value * ptr = buf;
		while (ZeroTraits::check(ptr->first) && ptr < buf + buf_size())
			++ptr;

		return iterator(this, ptr);
	}

	const_iterator end() const 		{ return const_iterator(this, buf + buf_size()); }
	iterator end() 					{ return iterator(this, buf + buf_size()); }
	
	
	/// Вставить значение.
	std::pair<iterator, bool> insert(const Value & x)
	{
		if (ZeroTraits::check(x.first))
		{
			if (!has_zero)
			{
				++m_size;
				has_zero = true;
				zero_value.second = x.second;
				return std::make_pair(begin(), true);
			}
			return std::make_pair(begin(), false);
		}

		size_t place_value = place(hash(x.first));
		unsigned increment = 1;
		while (!ZeroTraits::check(buf[place_value].first) && buf[place_value].first != x.first)
		{
			place_value += increment;
			++increment;
			place_value &= mask();
		}

		iterator res(this, &buf[place_value]);

		if (buf[place_value].first == x.first)
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


	iterator find(Key x)
	{
		if (ZeroTraits::check(x))
			return has_zero ? begin() : end();

		size_t place_value = place(hash(x));
		unsigned increment = 1;
		while (!ZeroTraits::check(buf[place_value].first) && buf[place_value].first != x)
		{
			place_value += increment;
			++increment;
			place_value &= mask();
		}

		return !ZeroTraits::check(buf[place_value].first) ? iterator(this, &buf[place_value]) : end();
	}


	const_iterator find(Key x) const
	{
		if (ZeroTraits::check(x))
			return has_zero ? begin() : end();

		size_t place_value = place(hash(x.first));
		unsigned increment = 1;
		while (!ZeroTraits::check(buf[place_value].first) && buf[place_value].first != x)
		{
			place_value += increment;
			++increment;
			place_value &= mask();
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
};

}
