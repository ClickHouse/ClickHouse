#pragma once

#include <DB/Common/LRUCache.h>
#include <DB/Common/SipHash.h>
#include <DB/Common/ProfileEvents.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/Interpreters/AggregationCommon.h>


namespace DB
{


struct UncompressedCacheCell
{
	Memory data;
	size_t compressed_size;
};


/** Кэш разжатых блоков для CachedCompressedReadBuffer. thread-safe.
  */
class UncompressedCache : public LRUCache<UInt128, UncompressedCacheCell, UInt128TrivialHash>
{
private:
	typedef LRUCache<UInt128, UncompressedCacheCell, UInt128TrivialHash> Base;

public:
	UncompressedCache(size_t max_size_in_cells)
		: Base(max_size_in_cells) {}

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

	MappedPtr get(const Key & key)
	{
		MappedPtr res = Base::get(key);

		if (res)
			ProfileEvents::increment(ProfileEvents::UncompressedCacheHits);
		else
			ProfileEvents::increment(ProfileEvents::UncompressedCacheMisses);

		return res;
	}

	void reset()
	{
		Base::reset();
	}
};

typedef Poco::SharedPtr<UncompressedCache> UncompressedCachePtr;

}
