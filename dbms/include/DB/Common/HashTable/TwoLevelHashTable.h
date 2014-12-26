#pragma once

#include <DB/Common/HashTable/HashTable.h>


/** Двухуровневая хэш-таблица.
  * Представляет собой 256 маленьких хэш-таблиц (bucket-ов первого уровня).
  * Для определения, какую из них использовать, берётся один из байтов хэш-функции.
  *
  * Обычно работает чуть-чуть медленнее простой хэш-таблицы.
  * Тем не менее, обладает преимуществами в некоторых случаях:
  * - если надо мерджить две хэш-таблицы вместе, то это можно легко распараллелить по bucket-ам;
  * - лаг при ресайзах размазан, так как маленькие хэш-таблицы ресайзятся по-отдельности;
  * - по идее, ресайзы кэш-локальны в большем диапазоне размеров.
  */

template <size_t initial_size_degree = 8>
struct TwoLevelHashTableGrower : public HashTableGrower<initial_size_degree>
{
	/// Увеличить размер хэш-таблицы.
	void increaseSize()
	{
		this->size_degree += this->size_degree >= 15 ? 1 : 2;
	}
};

template
<
	typename Key,
	typename Cell,
	typename Hash,
	typename Grower,
	typename Allocator,	/// TODO WithStackMemory
	typename ImplTable = HashTable<Key, Cell, Hash, Grower, Allocator>
>
class TwoLevelHashTable :
	private boost::noncopyable,
	protected Hash			/// empty base optimization
{
protected:
	friend class const_iterator;
	friend class iterator;

	typedef size_t HashValue;
	typedef TwoLevelHashTable<Key, Cell, Hash, Grower, Allocator, ImplTable> Self;
public:
	typedef ImplTable Impl;

	size_t hash(const Key & x) const { return Hash::operator()(x); }
	size_t getBucketFromHash(size_t hash_value) const { return (hash_value >> 24) & 0xFF; }	/// NOTE Плохо для хэш-таблиц больше чем на 2^32 ячеек.

protected:
	typename Impl::iterator beginOfNextNonEmptyBucket(size_t & bucket)
	{
		while (bucket != NUM_BUCKETS && impls[bucket].empty())
			++bucket;

		if (bucket != NUM_BUCKETS)
			return impls[bucket].begin();

		--bucket;
		return impls[MAX_BUCKET].end();
	}

	typename Impl::const_iterator beginOfNextNonEmptyBucket(size_t & bucket) const
	{
		while (bucket != NUM_BUCKETS && impls[bucket].empty())
			++bucket;

		if (bucket != NUM_BUCKETS)
			return impls[bucket].begin();

		--bucket;
		return impls[MAX_BUCKET].end();
	}

public:
	typedef typename Impl::key_type key_type;
	typedef typename Impl::value_type value_type;

	static constexpr size_t NUM_BUCKETS = 256;
	static constexpr size_t MAX_BUCKET = NUM_BUCKETS - 1;
	Impl impls[NUM_BUCKETS];


	class iterator
	{
		Self * container;
		size_t bucket;
		typename Impl::iterator current_it;

		friend class TwoLevelHashTable;

		iterator(Self * container_, size_t bucket_, typename Impl::iterator current_it_)
			: container(container_), bucket(bucket_), current_it(current_it_) {}

	public:
		iterator() {}

		bool operator== (const iterator & rhs) const { return bucket == rhs.bucket && current_it == rhs.current_it; }
		bool operator!= (const iterator & rhs) const { return !(*this == rhs); }

		iterator & operator++()
		{
			++current_it;
			if (current_it == container->impls[bucket].end())
			{
				++bucket;
				current_it = container->beginOfNextNonEmptyBucket(bucket);
			}

			return *this;
		}

		value_type & operator* () const { return *current_it; }
		value_type * operator->() const { return &*current_it; }
	};


	class const_iterator
	{
		Self * container;
		size_t bucket;
		typename Impl::const_iterator current_it;

		friend class TwoLevelHashTable;

		const_iterator(Self * container_, size_t bucket_, typename Impl::const_iterator current_it_)
			: container(container_), bucket(bucket_), current_it(current_it_) {}

	public:
		const_iterator() {}
		const_iterator(const iterator & rhs) : container(rhs.container), bucket(rhs.bucket), current_it(rhs.current_it) {}

		bool operator== (const const_iterator & rhs) const { return bucket == rhs.bucket && current_it == rhs.current_it; }
		bool operator!= (const const_iterator & rhs) const { return !(*this == rhs); }

		const_iterator & operator++()
		{
			++current_it;
			if (current_it == container->impls[bucket].end())
			{
				++bucket;
				current_it = container->beginOfNextNonEmptyBucket(bucket);
			}

			return *this;
		}

		const value_type & operator* () const { return *current_it; }
		const value_type * operator->() const { return &*current_it; }
	};


	const_iterator begin() const
	{
		size_t buck = 0;
		typename Impl::const_iterator impl_it = beginOfNextNonEmptyBucket(buck);
		return { this, buck, impl_it };
	}

	iterator begin()
	{
		size_t buck = 0;
		typename Impl::iterator impl_it = beginOfNextNonEmptyBucket(buck);
		return { this, buck, impl_it };
	}

	const_iterator end() const 		{ return { this, MAX_BUCKET, impls[MAX_BUCKET].end() }; }
	iterator end() 					{ return { this, MAX_BUCKET, impls[MAX_BUCKET].end() }; }


	/// Вставить значение. В случае хоть сколько-нибудь сложных значений, лучше используйте функцию emplace.
	std::pair<iterator, bool> insert(const value_type & x)
	{
		size_t hash_value = hash(Cell::getKey(x));

		std::pair<iterator, bool> res;
		emplace(Cell::getKey(x), res.first, res.second, hash_value);
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
		size_t hash_value = hash(x);
		emplace(x, it, inserted, hash_value);
	}


	/// То же самое, но с заранее вычисленным значением хэш-функции.
	void emplace(Key x, iterator & it, bool & inserted, size_t hash_value)
	{
		size_t buck = getBucketFromHash(hash_value);
		typename Impl::iterator impl_it;
		impls[buck].emplace(x, impl_it, inserted, hash_value);
		it = iterator(this, buck, impl_it);
	}


	iterator find(Key x)
	{
		size_t hash_value = hash(x);
		size_t buck = getBucketFromHash(hash_value);

		typename Impl::iterator found = impls[buck].find(x, hash_value);
		return found != impls[buck].end()
			? iterator(this, buck, found)
			: end();
	}


	const_iterator find(Key x) const
	{
		size_t hash_value = hash(x);
		size_t buck = getBucketFromHash(hash_value);

		typename Impl::const_iterator found = impls[buck].find(x, hash_value);
		return found != impls[buck].end()
			? const_iterator(this, buck, found)
			: end();
	}


	void write(DB::WriteBuffer & wb) const
	{
		for (size_t i = 0; i < NUM_BUCKETS; ++i)
			impls[i].write(wb);
	}

	void writeText(DB::WriteBuffer & wb) const
	{
		for (size_t i = 0; i < NUM_BUCKETS; ++i)
		{
			if (i != 0)
				DB::writeChar(',', wb);
			impls[i].writeText(wb);
		}
	}

	void read(DB::ReadBuffer & rb)
	{
		for (size_t i = 0; i < NUM_BUCKETS; ++i)
			impls[i].read(rb);
	}

	void readText(DB::ReadBuffer & rb)
	{
		for (size_t i = 0; i < NUM_BUCKETS; ++i)
		{
			if (i != 0)
				DB::assertString(",", rb);
			impls[i].readText(rb);
		}
	}


	size_t size() const
	{
		size_t res = 0;
		for (size_t i = 0; i < NUM_BUCKETS; ++i)
			res += impls[i].size();

		return res;
	}

	bool empty() const
	{
	    for (size_t i = 0; i < NUM_BUCKETS; ++i)
			if (!impls[i].empty())
				return false;

		return true;
	}

	size_t getBufferSizeInBytes() const
	{
		size_t res = 0;
		for (size_t i = 0; i < NUM_BUCKETS; ++i)
			res += impls[i].getBufferSizeInBytes();

		return res;
	}
};
