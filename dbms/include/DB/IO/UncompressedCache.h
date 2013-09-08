#pragma once

#include <vector>

#include <Poco/SharedPtr.h>
#include <Poco/Mutex.h>

#include <DB/Common/SipHash.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/Interpreters/AggregationCommon.h>


namespace DB
{


/** Кэш разжатых блоков для CachedCompressedReadBuffer. thread-safe.
  * NOTE Использовать LRU вместо простой кэш-таблицы.
  */
class UncompressedCache
{
public:
	struct Cell
	{
		UInt128 key;
		Memory data;
		size_t compressed_size;

		Cell() { key.first = 0; key.second = 0; compressed_size = 0; }
	};

	/// В ячейках кэш-таблицы лежат SharedPtr-ы на разжатые блоки. Это нужно, чтобы можно было достать ячейку, захватив владение ею.
	typedef Poco::SharedPtr<Cell> CellPtr;
	typedef std::vector<CellPtr> Cells;

private:
	size_t num_cells;
	Cells cells;

	mutable Poco::FastMutex mutex;
	mutable size_t hits;
	mutable size_t misses;

public:
	UncompressedCache(size_t num_cells_)
		: num_cells(num_cells_), cells(num_cells), hits(0), misses(0)
	{
	}

	/// Посчитать ключ от пути к файлу и смещения.
	static UInt128 hash(const String & path_to_file, size_t offset)
	{
		UInt128 key;

		SipHash hash;
		hash.update(path_to_file.data(), path_to_file.size() + 1);
		hash.update(reinterpret_cast<const char *>(&offset), sizeof(offset));
		hash.get128(key.first, key.second);

		return key;
	}

	CellPtr get(UInt128 key) const
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		CellPtr cell = cells[key.first % num_cells];

		if (cell && cell->key == key)
		{
			++hits;
			return cell;
		}
		else
		{
			++misses;
			return NULL;
		}
	}

	void set(const CellPtr & new_cell)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		CellPtr & cell = cells[new_cell->key.first % num_cells];

		if (!cell || cell->key != new_cell->key)
			cell = new_cell;
	}

	void getStats(size_t & out_hits, size_t & out_misses) const volatile
	{
		/// Синхронизация не нужна.
		out_hits = hits;
		out_misses = misses;
	}
};

typedef Poco::SharedPtr<UncompressedCache> UncompressedCachePtr;

}
