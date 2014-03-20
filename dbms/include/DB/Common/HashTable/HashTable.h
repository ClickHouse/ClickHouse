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

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/VarInt.h>

#include <DB/Common/HashTable/HashTableAllocator.h>

#ifdef DBMS_HASH_MAP_DEBUG_RESIZES
	#include <iostream>
	#include <iomanip>
	#include <statdaemons/Stopwatch.h>
#endif



/** Compile-time интерфейс ячейки хэш-таблицы.
  * Разные ячейки используются для реализации разных хэш-таблиц.
  * Ячейка должна содержать ключ.
  * Также может содержать значение и произвольные дополнительные данные.
  */
template <typename Key, typename Hash>
struct HashTableCell
{
	typedef Key value_type;
	Key key;

	/// Создать ячейку с заданным ключём / ключём и значением.
	HashTableCell(const Key & key_) : key(key_) {}
///	HashTableCell(const value_type & value_) : key(value_) {}

	/// Получить то, что будет value_type контейнера.
	value_type & getValue()				{ return key; }
	const value_type & getValue() const { return key; }

	/// Получить ключ.
	static Key & getKey(value_type & value)	{ return value; }
	static const Key & getKey(const value_type & value) { return value; }

	/// Равны ли ключи у ячеек.
	bool keyEquals(const Key & key_) const { return key == key_; }
	bool keyEquals(const HashTableCell & other) const { return key == other.key; }

	/// Если ячейка умеет запоминать в себе значение хэш-функции, то запомнить его.
	void setHash(size_t hash_value) {}

	/// Если ячейка умеет запоминать в себе значение хэш-функции, то вернуть запомненное значение.
	/// Оно должно быть хотя бы один раз вычислено до этого.
	/// Если запоминание значения хэш-функции не предусмотрено, то просто вычислить хэш.
	size_t getHash(const Hash & hash) const { return hash(key); }

	/// Является ли ключ нулевым. Ячейка для нулевого ключа хранится отдельно, не в основном буфере.
	/// Нулевые ключи должны быть такими, что занулённый кусок памяти представляет собой нулевой ключ.
	static bool isZero(const Key & key) { return key == 0; }
	bool isZero() const { return isZero(key); }

	/// Установить значение ключа в ноль.
	void setZero() { key = 0; }

	/// Установить отображаемое значение, если есть (для HashMap), в соответствующиее из value.
	void setMapped(const value_type & value) {}

	/// Сериализация, в бинарном и текстовом виде.
	void write(DB::WriteBuffer & wb) const 		{ DB::writeBinary(key, wb); }
	void writeText(DB::WriteBuffer & wb) const 	{ DB::writeDoubleQuoted(key, wb); }

	/// Десериализация, в бинарном и текстовом виде.
	void read(DB::ReadBuffer & rb) 				{ DB::readBinary(key, rb); }
	void readText(DB::ReadBuffer & rb) 			{ DB::writeDoubleQuoted(key, rb); }
};


/** Определяет размер хэш-таблицы, а также когда и во сколько раз её надо ресайзить.
  */
struct HashTableGrower
{
	/// Состояние этой структуры достаточно, чтобы получить размер буфера хэш-таблицы.

	/// Определяет начальный размер хэш-таблицы.
	static const size_t initial_size_degree = 16;

	UInt8 size_degree = initial_size_degree;

	/// Размер хэш-таблицы в ячейках.
	size_t bufSize() const				{ return 1 << size_degree; }

	size_t maxFill() const				{ return 1 << (size_degree - 1); }
	size_t mask() const					{ return bufSize() - 1; }

	/// Из значения хэш-функции получить номер ячейки в хэш-таблице.
	size_t place(size_t x) const 		{ return x & mask(); }

	/// Следующая ячейка в цепочке разрешения коллизий.
	size_t next(size_t pos) const		{ ++pos; return pos & mask(); }

	/// Является ли хэш-таблица достаточно заполненной. Нужно увеличить размер хэш-таблицы, или удалить из неё что-нибудь ненужное.
	bool overflow(size_t elems) const	{ return elems > maxFill(); }

	/// Увеличить размер хэш-таблицы.
	void increaseSize()
	{
		size_degree += size_degree >= 23 ? 1 : 2;
	}

	/// Установить размер буфера по количеству элементов хэш-таблицы. Используется при десериализации хэш-таблицы.
	void set(size_t num_elems)
	{
		size_degree = num_elems <= 1
			 ? initial_size_degree
			 : ((initial_size_degree > static_cast<size_t>(log2(num_elems - 1)) + 2)
				 ? initial_size_degree
				 : (static_cast<size_t>(log2(num_elems - 1)) + 2));
	}
};


template
<
	typename Key,
	typename Cell,
	typename Hash,
	typename Grower,
	typename Allocator
>
class HashTable : private boost::noncopyable, private Hash, private Allocator		/// empty base optimization
{
private:
	friend class const_iterator;
	friend class iterator;

	typedef size_t HashValue;
	typedef HashTable<Key, Cell, Hash, Grower, Allocator> Self;

	size_t m_size;			/// Количество элементов
	Cell * buf;				/// Кусок памяти для всех элементов кроме элемента с ключём 0.
	Grower grower;
	bool has_zero;			/// Хэш-таблица содержит элемент со значением ключа = 0.

	char zero_value_storage[sizeof(Cell)];	/// Кусок памяти для элемента с ключём 0.

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
	mutable size_t collisions;
#endif

	Cell * zero_value() 			 { return reinterpret_cast<Cell*>(zero_value_storage); }
	const Cell * zero_value() const	 { return reinterpret_cast<const Cell*>(zero_value_storage); }

	size_t hash(const Key & x) const { return Hash::operator()(x); }

	/// Размер хэш-таблицы в байтах.
	size_t bufSizeBytes() const			{ return grower.bufSize() * sizeof(Cell); }

	/// Найти ячейку с тем же ключём или пустую ячейку, начиная с заданного места и далее по цепочке разрешения коллизий.
	size_t findCell(const Cell & x, size_t place_value)
	{
		while (!buf[place_value].isZero() && !buf[place_value].keyEquals(x))
		{
			place_value = grower.next(place_value);
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			++collisions;
#endif
		}

		return place_value;
	}


	void alloc()
	{
		buf = reinterpret_cast<Cell *>(Allocator::alloc(bufSizeBytes()));
	}

	void free()
	{
		Allocator::free(buf, bufSizeBytes());
	}


	/// Увеличить размер буфера.
	void resize(size_t for_num_elems = 0)
	{
#ifdef DBMS_HASH_MAP_DEBUG_RESIZES
		Stopwatch watch;
#endif

		size_t old_size = grower.bufSize();
		size_t old_size_bytes = bufSizeBytes();

		if (for_num_elems)
		{
			grower.set(for_num_elems);
			if (grower.bufSize() <= old_size)
				return;
		}
		else
			grower.increaseSize();

		/// Расширим пространство.
		buf = reinterpret_cast<Cell *>(Allocator::realloc(buf, old_size_bytes, bufSizeBytes()));

		/** Теперь некоторые элементы может потребоваться переместить на новое место.
		  * Элемент может остаться на месте, или переместиться в новое место "справа",
		  *  или переместиться левее по цепочке разрешения коллизий, из-за того, что элементы левее него были перемещены в новое место "справа".
		  */
		for (size_t i = 0; i < old_size; ++i)
			if (!buf[i].isZero())
				reinsert(buf[i]);

#ifdef DBMS_HASH_MAP_DEBUG_RESIZES
		watch.stop();
		std::cerr << std::fixed << std::setprecision(3)
			<< "Resize from " << old_size << " to " << grower.bufSize() << " took " << watch.elapsedSeconds() << " sec."
			<< std::endl;
#endif
	}


	/** Вставить в новый буфер значение, которое было в старом буфере.
	  * Используется при увеличении размера буфера.
	  */
	void reinsert(Cell & x)
	{
		size_t place_value = grower.place(x.getHash(*this));

		/// Если элемент на своём месте.
		if (&x == &buf[place_value])
			return;

		/// Вычисление нового места, с учётом цепочки разрешения коллизий.
		place_value = findCell(x, place_value);

		/// Если элемент остался на своём месте в старой цепочке разрешения коллизий.
		if (!buf[place_value].isZero() && x.keyEquals(buf[place_value]))
			return;

		/// Копирование на новое место и зануление старого.
		memcpy(&buf[place_value], &x, sizeof(x));
		x.setZero();

		/// Потом на старое место могут переместиться элементы, которые раньше были в коллизии с этим.
	}


public:
	typedef Key key_type;
	typedef typename Cell::value_type value_type;


	HashTable() :
		m_size(0),
		has_zero(false)
	{
		zero_value()->setZero();
		alloc();

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
		collisions = 0;
#endif
	}

	~HashTable()
	{
		if (!__has_trivial_destructor(Cell))
			for (iterator it = begin(); it != end(); ++it)
				it.ptr->~Cell();

		free();
	}


	class iterator
	{
		Self * container;
		Cell * ptr;

		friend class HashTable;

		iterator(Self * container_, Cell * ptr_) : container(container_), ptr(ptr_) {}

	public:
		iterator() {}

		bool operator== (const iterator & rhs) const { return ptr == rhs.ptr; }
		bool operator!= (const iterator & rhs) const { return ptr != rhs.ptr; }

		iterator & operator++()
		{
			if (unlikely(ptr->isZero()))
				ptr = container->buf;
			else
				++ptr;

			while (ptr < container->buf + container->grower.bufSize() && ptr->isZero())
				++ptr;

			return *this;
		}

		value_type & operator* () const { return ptr->getValue(); }
		value_type * operator->() const { return &ptr->getValue(); }
	};


	class const_iterator
	{
		const Self * container;
		const Cell * ptr;

		friend class HashTable;

		const_iterator(const Self * container_, const Cell * ptr_) : container(container_), ptr(ptr_) {}

	public:
		const_iterator() {}
		const_iterator(const iterator & rhs) : container(rhs.container), ptr(rhs.ptr) {}

		bool operator== (const const_iterator & rhs) const { return ptr == rhs.ptr; }
		bool operator!= (const const_iterator & rhs) const { return ptr != rhs.ptr; }

		const_iterator & operator++()
		{
			if (unlikely(ptr->isZero()))
				ptr = container->buf;
			else
				++ptr;

			while (ptr < container->buf + container->grower.bufSize() && ptr->isZero())
				++ptr;

			return *this;
		}

		const value_type & operator* () const { return ptr->getValue(); }
		const value_type * operator->() const { return &ptr->getValue(); }
	};


	const_iterator begin() const
	{
		if (has_zero)
			return const_iterator(this, zero_value());

		const Cell * ptr = buf;
		while (ptr < buf + grower.bufSize() && ptr->isZero())
			++ptr;

		return const_iterator(this, ptr);
	}

	iterator begin()
	{
		if (has_zero)
			return iterator(this, zero_value());

		Cell * ptr = buf;
		while (ptr < buf + grower.bufSize() && ptr->isZero())
			++ptr;

		return iterator(this, ptr);
	}

	const_iterator end() const 		{ return const_iterator(this, buf + grower.bufSize()); }
	iterator end() 					{ return iterator(this, buf + grower.bufSize()); }


private:
	/// Если ключ нулевой - вставить его в специальное место и вернуть true.
	bool emplaceIfZero(Key x, iterator & it, bool & inserted)
	{
		if (Cell::isZero(x))
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
			it.ptr->setHash(hash(x));
			return true;
		}

		return false;
	}


	/// Только для ненулевых ключей. Найти нужное место, вставить туда ключ, если его ещё нет, вернуть итератор на ячейку.
	void emplaceNonZero(Key x, iterator & it, bool & inserted, size_t hash_value)
	{
		size_t place_value = findCell(x, grower.place(hash_value));

		it = iterator(this, &buf[place_value]);

		if (!buf[place_value].isZero() && buf[place_value].keyEquals(x))
		{
			inserted = false;
			return;
		}

		new(&buf[place_value]) Cell(x);
		buf[place_value].setHash(hash_value);
		inserted = true;
		++m_size;

		if (unlikely(grower.overflow(m_size)))
		{
			resize();
			it = find(x);
		}
	}


public:
	/// Вставить значение. В случае хоть сколько-нибудь сложных значений, лучше используйте функцию emplace.
	std::pair<iterator, bool> insert(const value_type & x)
	{
		std::pair<iterator, bool> res;

		if (!emplaceIfZero(Cell::getKey(x), res.first, res.second))
			emplaceNonZero(Cell::getKey(x), res.first, res.second, hash(Cell::getKey(x)));

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
	void emplace(Key x, iterator & it, bool & inserted)
	{
		if (!emplaceIfZero(x, it, inserted))
			emplaceNonZero(x, it, inserted, hash(x));
	}


	/// То же самое, но с заранее вычисленным значением хэш-функции.
	void emplace(Key x, iterator & it, bool & inserted, size_t hash_value)
	{
		if (!emplaceIfZero(x, it, inserted))
			emplaceNonZero(x, it, inserted, hash_value);
	}


	iterator find(Key x)
	{
		if (Cell::isZero(x))
			return has_zero ? begin() : end();

		size_t place_value = findCell(x, grower.place(hash(x)));

		return !buf[place_value].isZero() ? iterator(this, &buf[place_value]) : end();
	}


	const_iterator find(Key x) const
	{
		if (Cell::isZero(x))
			return has_zero ? begin() : end();

		size_t place_value = findCell(x, grower.place(hash(x)));

		return !buf[place_value].isZero() ? const_iterator(this, &buf[place_value]) : end();
	}


	void write(DB::WriteBuffer & wb) const
	{
		DB::writeVarUInt(m_size, wb);

		if (has_zero)
			zero_value()->write(wb);

		for (size_t i = 0; i < grower.bufSize(); ++i)
			if (!buf[i].isZero())
				buf[i].write(wb);
	}

	void writeText(DB::WriteBuffer & wb) const
	{
		DB::writeText(m_size, wb);

		if (has_zero)
		{
			DB::writeChar(',', wb);
			zero_value()->writeText(wb);
		}

		for (size_t i = 0; i < grower.bufSize(); ++i)
		{
			if (!buf[i].isZero())
			{
				DB::writeChar(',', wb);
				buf[i].writeText(wb);
			}
		}
	}

	void read(DB::ReadBuffer & rb)
	{
		has_zero = false;
		m_size = 0;

		size_t new_size = 0;
		DB::readVarUInt(new_size, rb);

		free();
		grower.set(new_size);
		alloc();

		for (size_t i = 0; i < new_size; ++i)
		{
			Cell x;
			x.read(rb);
			insert(x);
		}
	}

	void readText(DB::ReadBuffer & rb)
	{
		has_zero = false;
		m_size = 0;

		size_t new_size = 0;
		DB::readText(new_size, rb);

		free();
		grower.set(new_size);
		alloc();

		for (size_t i = 0; i < new_size; ++i)
		{
			Cell x;
			DB::assertString(",", rb);
			x.readText(rb);
			insert(x);
		}
	}


	size_t size() const
	{
	    return m_size;
	}

	bool empty() const
	{
	    return 0 == m_size;
	}

	size_t getBufferSizeInBytes() const
	{
		return bufSizeBytes();
	}

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
	size_t getCollisions() const
	{
		return collisions;
	}
#endif
};
