#pragma once

#include <type_traits>
#include <DB/Common/HashTable/HashSet.h>


/** Хеш-таблица, позволяющая очищать таблицу за O(1).
  * Еще более простая, чем HashSet: Key и Mapped должны быть POD-типами.
  *
  * Вместо этого класса можно было бы просто использовать в HashSet в качестве ключа пару <версия, ключ>,
  * но тогда таблица накапливала бы все ключи, которые в нее когда-либо складывали, и неоправданно росла.
  * Этот класс идет на шаг дальше и считает ключи со старой версией пустыми местами в хеш-таблице.
  */


struct ClearableHashSetState
{
	UInt32 version = 1;

	/// Сериализация, в бинарном и текстовом виде.
	void write(DB::WriteBuffer & wb) const 		{ DB::writeBinary(version, wb); }
	void writeText(DB::WriteBuffer & wb) const 	{ DB::writeText(version, wb); }

	/// Десериализация, в бинарном и текстовом виде.
	void read(DB::ReadBuffer & rb) 				{ DB::readBinary(version, rb); }
	void readText(DB::ReadBuffer & rb) 			{ DB::readText(version, rb); }
};


template <typename Key, typename BaseCell>
struct ClearableHashTableCell : public BaseCell
{
	typedef ClearableHashSetState State;
	typedef typename BaseCell::value_type value_type;

	UInt32 version;

	bool isZero(const State & state) const { return version != state.version; }
	static bool isZero(const Key & key, const State & state) { return false; }

	/// Установить значение ключа в ноль.
	void setZero() { version = 0; }

	/// Нужно ли хранить нулевой ключ отдельно (то есть, могут ли в хэш-таблицу вставить нулевой ключ).
	static constexpr bool need_zero_value_storage = false;

	ClearableHashTableCell() {}
	ClearableHashTableCell(const Key & key_, const State & state) : BaseCell(key_, state), version(state.version) {}
};


template
<
	typename Key,
	typename Hash = DefaultHash<Key>,
	typename Grower = HashTableGrower<>,
	typename Allocator = HashTableAllocator
>
class ClearableHashSet : public HashTable<Key, ClearableHashTableCell<Key, HashTableCell<Key, Hash, ClearableHashSetState>>, Hash, Grower, Allocator>
{
public:
	typedef Key key_type;
	typedef typename ClearableHashSet::cell_type::value_type value_type;

	void clear()
	{
		++this->version;
		this->m_size = 0;
	}
};
