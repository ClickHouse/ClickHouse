#pragma once

#include <DB/Common/HashTable/HashMap.h>


/** Хеш-таблица, позволяющая очищать таблицу за O(1).
  * Еще более простая, чем HashMap: Key и Mapped должны быть POD-типами.
  *
  * Вместо этого класса можно было бы просто использовать в HashMap в качестве ключа пару <версия, ключ>,
  * но тогда таблица накапливала бы все ключи, которые в нее когда-либо складывали, и неоправданно росла.
  * Этот класс идет на шаг дальше и считает ключи со старой версией пустыми местами в хеш-таблице.
  */

struct ClearableHashMapState
{
	UInt32 version = 1;

	/// Сериализация, в бинарном и текстовом виде.
	void write(DB::WriteBuffer & wb) const 		{ DB::writeBinary(version, wb); }
	void writeText(DB::WriteBuffer & wb) const 	{ DB::writeText(version, wb); }

	/// Десериализация, в бинарном и текстовом виде.
	void read(DB::ReadBuffer & rb) 				{ DB::readBinary(version, rb); }
	void readText(DB::ReadBuffer & rb) 			{ DB::readText(version, rb); }
};


template <typename Key, typename Mapped, typename Hash>
struct ClearableHashMapCell : public HashMapCell<Key, Mapped, Hash, ClearableHashMapState>
{
	typedef ClearableHashMapState State;
	typedef HashMapCell<Key, Mapped, Hash, ClearableHashMapState> Base;
	typedef typename Base::value_type value_type;

	UInt32 version;

	ClearableHashMapCell() {}
	ClearableHashMapCell(const Key & key_, const State & state) : Base(key_, state), version(state.version) {}
	ClearableHashMapCell(const value_type & value_, const State & state) : Base(value_, state), version(state.version) {}

	bool isZero(const State & state) const { return version != state.version; }
	static bool isZero(const Key & key, const State & state) { return false; }

	/// Установить значение ключа в ноль.
	void setZero() { version = 0; }

	/// Нужно ли хранить нулевой ключ отдельно (то есть, могут ли в хэш-таблицу вставить нулевой ключ).
	static constexpr bool need_zero_value_storage = false;
};


template
<
	typename Key,
	typename Mapped,
	typename Hash = DefaultHash<Key>,
	typename Grower = HashTableGrower<>,
	typename Allocator = HashTableAllocator
>
class ClearableHashMap : public HashTable<Key, ClearableHashMapCell<Key, Mapped, Hash>, Hash, Grower, Allocator>
{
public:
	typedef Key key_type;
	typedef Mapped mapped_type;
	typedef typename ClearableHashMap::cell_type::value_type value_type;

	mapped_type & operator[](Key x)
	{
		typename ClearableHashMap::iterator it;
		bool inserted;
		this->emplace(x, it, inserted);

		if (inserted)
			new(&it->second) mapped_type();

		return it->second;
	}

	void clear()
	{
		++this->version;
		this->m_size = 0;
	}
};
