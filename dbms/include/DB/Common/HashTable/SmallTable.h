#pragma once

#include <DB/Common/HashTable/HashMap.h>


/** Замена хэш-таблицы для маленького количества (единицы) ключей.
  * Реализована в виде массива с линейным поиском.
  * Массив расположен внутри объекта.
  * Интерфейс является подмножеством интерфейса HashTable.
  *
  * Вставка возможна только если метод full возвращает false.
  * При неизвестном количестве различных ключей,
  *  вы должны проверять, не заполнена ли таблица,
  *  и делать fallback в этом случае (например, использовать полноценную хэш-таблицу).
  */

template
<
	typename Key,
	typename Cell,
	size_t capacity
>
class SmallTable :
	private boost::noncopyable,
	protected Cell::State
{
protected:
	friend class const_iterator;
	friend class iterator;
	friend class Reader;

	typedef SmallTable<Key, Cell, capacity> Self;
	typedef Cell cell_type;

	size_t m_size = 0;		/// Количество элементов.
	Cell buf[capacity];		/// Кусок памяти для всех элементов.


	/// Найти ячейку с тем же ключём или пустую ячейку, начиная с заданного места и далее по цепочке разрешения коллизий.
	const Cell * ALWAYS_INLINE findCell(const Key & x) const
	{
		const Cell * it = buf;
		while (it < buf + m_size)
		{
			if (it->keyEquals(x))
				break;
			++it;
		}
		return it;
	}

	Cell * ALWAYS_INLINE findCell(const Key & x)
	{
		Cell * it = buf;
		while (it < buf + m_size)
		{
			if (it->keyEquals(x))
				break;
			++it;
		}
		return it;
	}


public:
	typedef Key key_type;
	typedef typename Cell::value_type value_type;


	class Reader final : private Cell::State
	{
	public:
		Reader(DB::ReadBuffer & in_)
		: in(in_)
		{
		}

		Reader(const Reader &) = delete;
		Reader & operator=(const Reader &) = delete;

		bool next()
		{
			if (!is_initialized)
			{
				Cell::State::read(in);
				DB::readVarUInt(size, in);

				if (size > capacity)
					throw DB::Exception("Illegal size");

				is_initialized = true;
			}

			if (read_count == size)
			{
				is_eof = true;
				return false;
			}

			cell.read(in);
			++read_count;

			return true;
		}

		inline const value_type & get() const
		{
			if (!is_initialized || is_eof)
				throw DB::Exception("No available data", DB::ErrorCodes::NO_AVAILABLE_DATA);

			return cell.getValue();
		}

	private:
		DB::ReadBuffer & in;
		Cell cell;
		size_t read_count = 0;
		size_t size;
		bool is_eof = false;
		bool is_initialized = false;
	};

	class iterator
	{
		Self * container;
		Cell * ptr;

		friend class SmallTable;

	public:
		iterator() {}
		iterator(Self * container_, Cell * ptr_) : container(container_), ptr(ptr_) {}

		bool operator== (const iterator & rhs) const { return ptr == rhs.ptr; }
		bool operator!= (const iterator & rhs) const { return ptr != rhs.ptr; }

		iterator & operator++()
		{
			++ptr;
			return *this;
		}

		value_type & operator* () const { return ptr->getValue(); }
		value_type * operator->() const { return &ptr->getValue(); }

		Cell * getPtr() const { return ptr; }
	};


	class const_iterator
	{
		const Self * container;
		const Cell * ptr;

		friend class SmallTable;

	public:
		const_iterator() {}
		const_iterator(const Self * container_, const Cell * ptr_) : container(container_), ptr(ptr_) {}
		const_iterator(const iterator & rhs) : container(rhs.container), ptr(rhs.ptr) {}

		bool operator== (const const_iterator & rhs) const { return ptr == rhs.ptr; }
		bool operator!= (const const_iterator & rhs) const { return ptr != rhs.ptr; }

		const_iterator & operator++()
		{
			++ptr;
			return *this;
		}

		const value_type & operator* () const { return ptr->getValue(); }
		const value_type * operator->() const { return &ptr->getValue(); }

		const Cell * getPtr() const { return ptr; }
	};


	const_iterator begin() const 	{ return iteratorTo(buf); }
	iterator begin() 				{ return iteratorTo(buf); }

	const_iterator end() const 		{ return iteratorTo(buf + m_size); }
	iterator end() 					{ return iteratorTo(buf + m_size); }


protected:
	const_iterator iteratorTo(const Cell * ptr) const 	{ return const_iterator(this, ptr); }
	iterator iteratorTo(Cell * ptr) 					{ return iterator(this, ptr); }


public:
	/** Таблица переполнена.
	  * В переполненную таблицу ничего нельзя вставлять.
	  */
	bool full()
	{
		return m_size == capacity;
	}


	/// Вставить значение. В случае хоть сколько-нибудь сложных значений, лучше используйте функцию emplace.
	std::pair<iterator, bool> ALWAYS_INLINE insert(const value_type & x)
	{
		std::pair<iterator, bool> res;

		emplace(Cell::getKey(x), res.first, res.second);

		if (res.second)
			res.first.ptr->setMapped(x);

		return res;
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
	  * 	new(&it->second) Mapped(value);
	  */
	void ALWAYS_INLINE emplace(Key x, iterator & it, bool & inserted)
	{
		Cell * res = findCell(x);
		it = iteratorTo(res);
		inserted = res == buf + m_size;
		if (inserted)
		{
			new(res) Cell(x, *this);
			++m_size;
		}
	}


	/// То же самое, но вернуть false, если переполнено.
	bool ALWAYS_INLINE tryEmplace(Key x, iterator & it, bool & inserted)
	{
		Cell * res = findCell(x);
		it = iteratorTo(res);
		inserted = res == buf + m_size;
		if (inserted)
		{
			if (res == buf + capacity)
				return false;

			new(res) Cell(x, *this);
			++m_size;
		}
		return true;
	}


	/// Скопировать ячейку из другой хэш-таблицы. Предполагается, что такого ключа в таблице ещё не было.
	void ALWAYS_INLINE insertUnique(const Cell * cell)
	{
		memcpy(&buf[m_size], cell, sizeof(*cell));
		++m_size;
	}

	void ALWAYS_INLINE insertUnique(Key x)
	{
		new(&buf[m_size]) Cell(x, *this);
		++m_size;
	}


	iterator ALWAYS_INLINE find(Key x) 				{ return iteratorTo(findCell(x)); }
	const_iterator ALWAYS_INLINE find(Key x) const 	{ return iteratorTo(findCell(x)); }


	void write(DB::WriteBuffer & wb) const
	{
		Cell::State::write(wb);
		DB::writeVarUInt(m_size, wb);

		for (size_t i = 0; i < m_size; ++i)
			buf[i].write(wb);
	}

	void writeText(DB::WriteBuffer & wb) const
	{
		Cell::State::writeText(wb);
		DB::writeText(m_size, wb);

		for (size_t i = 0; i < m_size; ++i)
		{
			DB::writeChar(',', wb);
			buf[i].writeText(wb);
		}
	}

	void read(DB::ReadBuffer & rb)
	{
		Cell::State::read(rb);

		m_size = 0;

		size_t new_size = 0;
		DB::readVarUInt(new_size, rb);

		if (new_size > capacity)
			throw DB::Exception("Illegal size");

		for (size_t i = 0; i < new_size; ++i)
			buf[i].read(rb);

		m_size = new_size;
	}

	void readText(DB::ReadBuffer & rb)
	{
		Cell::State::readText(rb);

		m_size = 0;

		size_t new_size = 0;
		DB::readText(new_size, rb);

		if (new_size > capacity)
			throw DB::Exception("Illegal size");

		for (size_t i = 0; i < new_size; ++i)
		{
			DB::assertChar(',', rb);
			buf[i].readText(rb);
		}

		m_size = new_size;
	}


	size_t size() const
	{
	    return m_size;
	}

	bool empty() const
	{
	    return 0 == m_size;
	}

	void clear()
	{
		if (!__has_trivial_destructor(Cell))
			for (iterator it = begin(); it != end(); ++it)
				it.ptr->~Cell();

		m_size = 0;
	}

	size_t getBufferSizeInBytes() const
	{
		return sizeof(buf);
	}
};


struct HashUnused {};


template
<
	typename Key,
	size_t capacity
>
using SmallSet = SmallTable<Key, HashTableCell<Key, HashUnused>, capacity>;


template
<
	typename Key,
	typename Cell,
	size_t capacity
>
class SmallMapTable : public SmallTable<Key, Cell, capacity>
{
public:
	typedef Key key_type;
	typedef typename Cell::Mapped mapped_type;
	typedef typename Cell::value_type value_type;

	mapped_type & ALWAYS_INLINE operator[](Key x)
	{
		typename SmallMapTable::iterator it;
		bool inserted;
		this->emplace(x, it, inserted);
		new(&it->second) mapped_type();
		return it->second;
	}
};


template
<
	typename Key,
	typename Mapped,
	size_t capacity
>
using SmallMap = SmallMapTable<Key, HashMapCell<Key, Mapped, HashUnused>, capacity>;
