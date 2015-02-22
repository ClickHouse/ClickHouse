#pragma once

#include <statdaemons/HyperLogLogCounter.h>
#include <DB/Common/HashTable/SmallTable.h>

namespace DB
{


/** Для маленького количества ключей - массив фиксированного размера "на стеке".
  * Для большого - выделяется HyperLogLog.
  * NOTE Возможно, имеет смысл сделать реализацию для среднего размера в виде хэш-таблицы.
  */
template <
	typename Key,
	UInt8 small_set_size,
	UInt8 K,
	typename Hash = IntHash32<Key>,
	typename DenominatorType = float>
class HyperLogLogWithSmallSetOptimization
{
private:
	using Small = SmallSet<Key, small_set_size>;
	using Large = HyperLogLogCounter<K, Hash, DenominatorType>;

	Small small;
	Large * large = nullptr;

	bool isLarge() const
	{
		return large != nullptr;
	}

	void toLarge()
	{
		if (current_memory_tracker)
			current_memory_tracker->alloc(sizeof(large));

		/// На время копирования данных из tiny, устанавливать значение large ещё нельзя (иначе оно перезатрёт часть данных).
		Large * tmp_large = new Large;

		for (const auto & x : small)
			tmp_large->insert(x);

		large = tmp_large;
	}

public:
	~HyperLogLogWithSmallSetOptimization()
	{
		if (isLarge())
		{
			delete large;

			if (current_memory_tracker)
				current_memory_tracker->free(sizeof(large));
		}
	}

	void insert(Key value)
	{
		if (!isLarge())
		{
			if (small.find(value) == small.end())
			{
				if (!small.full())
					small.insert(value);
				else
				{
					toLarge();
					large->insert(value);
				}
			}
		}
		else
			large->insert(value);
	}

	UInt32 size() const
	{
		return !isLarge() ? small.size() : large->size();
	}

	void merge(const HyperLogLogWithSmallSetOptimization & rhs)
	{
		if (rhs.isLarge())
		{
			if (!isLarge())
				toLarge();

			large->merge(*rhs.large);
		}
		else
		{
			for (const auto & x : rhs.small)
				insert(x);
		}
	}

	/// Можно вызывать только для пустого объекта.
	void read(DB::ReadBuffer & in)
	{
		bool is_large;
		readBinary(is_large, in);

		if (is_large)
		{
			toLarge();
			large->read(in);
		}
		else
			small.read(in);
	}

	void readAndMerge(DB::ReadBuffer & in)
	{
		/// Немного не оптимально.
		HyperLogLogWithSmallSetOptimization other;
		other.read(in);
		merge(other);
	}

	void write(DB::WriteBuffer & out) const
	{
		writeBinary(isLarge(), out);

		if (isLarge())
			large->write(out);
		else
			small.write(out);
	}
};


}
