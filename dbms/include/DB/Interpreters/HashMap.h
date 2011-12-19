#pragma once

#include <string.h>
#include <malloc.h>

#include <map>	/// pair

#include <boost/noncopyable.hpp>

#include <Yandex/optimization.h>


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
  * Quadratic probing (пока ещё не уверен, что оно не может зациклиться).
  * Значение с нулевым ключём хранится отдельно.
  * Удаления элементов нет.
  */


/** Способ проверить, что ключ нулевой,
  * а также способ установить значение ключа в ноль.
  */
template <typename T> struct default_zero_traits
{
	static inline bool check(T x) { return 0 == x; }
	static inline void set(T & x) { x = 0; }
};


template
<
	typename Key,
	typename Mapped,
	typename Hash = std::tr1::hash<Key>,
	typename ZeroTraits = default_zero_traits<Key>,
	int INITIAL_SIZE_DEGREE = 16,	/** Изначально выделить кусок памяти для 64K элементов.
									  * Уменьшите значение для лучшей кэш-локальности в случае маленького количества уникальных ключей.
									  */
	int GROWTH_DEGREE = 2			/// Рост буфера в 4 раза.
>
class HashMap : private boost::noncopyable
{
private:
	friend class const_iterator;
	friend class iterator;
	
	typedef std::pair<Key, Mapped> Value;	/// Без const Key для простоты.
	typedef size_t HashValue;
	typedef HashMap<Key, Mapped, Hash, ZeroTraits, INITIAL_SIZE_DEGREE, GROWTH_DEGREE> Self;
	
	size_t m_size;			/// Количество элементов
	UInt8 size_degree;		/// Размер таблицы в виде степени двух
	bool has_zero;			/// Хэш-таблица содержит элемент со значением ключа = 0.
	Value zero_value;
	Value * buf;

	Hash hash;


	inline size_t buf_size() const				{ return 1 << size_degree; }
	inline size_t max_fill() const				{ return 1 << (size_degree - 1); }
	inline size_t mask() const					{ return buf_size() - 1; }
	inline size_t place(HashValue x) const 		{ return x & mask(); }


	/// Увеличить размер буфера в 2 ^ GROWTH_DEGREE раз
	void resize()
	{
		size_t old_size = buf_size();
		
		size_degree += GROWTH_DEGREE;
		Value * new_buf = reinterpret_cast<Value*>(calloc(buf_size(), sizeof(Value)));
		Value * old_buf = buf;
		buf = new_buf;
		
		for (size_t i = 0; i < old_size; ++i)
			if (!ZeroTraits::check(old_buf[i].first))
				reinsert(old_buf[i]);

		free(reinterpret_cast<void*>(old_buf));
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
		memcpy(&buf[place_value], &x, sizeof(x));
	}


public:
	typedef Key key_type;
	typedef Mapped mapped_type;
	typedef Value value_type;
	
	
	HashMap() :
		m_size(0),
		size_degree(INITIAL_SIZE_DEGREE),
		has_zero(false)
	{
		ZeroTraits::set(zero_value.first);
		buf = reinterpret_cast<Value*>(calloc(buf_size(), sizeof(Value)));
	}

	~HashMap()
	{
		for (iterator it = begin(); it != end(); ++it)
			it->~Value();
		free(reinterpret_cast<void*>(buf));
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

			while (ZeroTraits::check(ptr->first) && ptr < container->buf + container->buf_size())
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
	
	
	/// Вставить значение. В случае хоть сколько-нибудь сложных значений, лучше используйте функцию emplace.
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


	/** Вставить ключ,
	  * вернуть итератор на позицию, которую можно использовать для placement new значения,
	  * а также флаг - был ли вставлен новый ключ.
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
			it = begin();
			
			if (!has_zero)
			{
				++m_size;
				has_zero = true;
				inserted = true;
			}
			else
				inserted = false;
			
			return;
		}

		size_t place_value = place(hash(x));
		unsigned increment = 1;
		while (!ZeroTraits::check(buf[place_value].first) && buf[place_value].first != x)
		{
			place_value += increment;
			++increment;
			place_value &= mask();
		}

		it = iterator(this, &buf[place_value]);

		if (buf[place_value].first == x)
		{
			inserted = false;
			return;
		}

		buf[place_value].first = x;
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
