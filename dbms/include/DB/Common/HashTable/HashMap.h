#pragma once

#include <DB/Common/HashTable/Hash.h>
#include <DB/Common/HashTable/HashTable.h>
#include <DB/Common/HashTable/HashTableAllocator.h>


template <typename Key, typename Mapped, typename Hash>
struct HashMapCell
{
	typedef std::pair<Key, Mapped> value_type;
	value_type value;

	HashMapCell(const Key & key_) : value(key_, Mapped()) {}
	HashMapCell(const value_type & value_) : value(value_) {}

	value_type & getValue()				{ return value; }
	const value_type & getValue() const { return value; }

	static Key & getKey(value_type & value)	{ return value.first; }
	static const Key & getKey(const value_type & value) { return value.first; }

	bool keyEquals(const Key & key_) const { return value.first == key_; }
	bool keyEquals(const HashMapCell & other) const { return value.first == other.value.first; }

	void setHash(size_t hash_value) {}
	size_t getHash(const Hash & hash) const { return hash(value.first); }

	static bool isZero(const Key & key) { return key == 0; }
	bool isZero() const { return isZero(value.first); }

	void setZero() { value.first = 0; }

	void setMapped(const value_type & value_) { value.second = value_.second; }

	/// Сериализация, в бинарном и текстовом виде.
	void write(DB::WriteBuffer & wb) const
	{
		DB::writeBinary(value.first, wb);
		DB::writeBinary(value.second, wb);
	}

	void writeText(DB::WriteBuffer & wb) const
	{
		DB::writeDoubleQuoted(value.first, wb);
		DB::writeChar(',', wb);
		DB::writeDoubleQuoted(value.second, wb);
	}

	/// Десериализация, в бинарном и текстовом виде.
	void read(DB::ReadBuffer & rb)
	{
		DB::readBinary(value.first, rb);
		DB::readBinary(value.second, rb);
	}

	void readText(DB::ReadBuffer & rb)
	{
		DB::readDoubleQuoted(value.first, rb);
		DB::assertString(",", rb);
		DB::readDoubleQuoted(value.second, rb);
	}
};


template
<
	typename Key,
	typename Mapped,
	typename Hash = DefaultHash<Key>,
	typename Grower = HashTableGrower,
	typename Allocator = HashTableAllocator
>
class HashMap : public HashTable<Key, HashMapCell<Key, Mapped, Hash>, Hash, Grower, Allocator>
{
public:
	typedef Key key_type;
	typedef Mapped mapped_type;
	typedef HashMapCell<Key, Mapped, Hash> value_type;

	Mapped & operator[](Key x)
	{
		typename HashMap::iterator it;
		bool inserted;
		this->emplace(x, it, inserted);

		if (inserted)
			new(&it->second) Mapped();

		return it->second;
	}
};
