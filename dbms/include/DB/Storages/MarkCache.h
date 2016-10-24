#pragma once

#include <memory>

#include <DB/Common/LRUCache.h>
#include <DB/Common/ProfileEvents.h>
#include <DB/Common/SipHash.h>
#include <DB/Interpreters/AggregationCommon.h>
#include <DB/DataStreams/MarkInCompressedFile.h>


namespace ProfileEvents
{
	extern const Event MarkCacheHits;
	extern const Event MarkCacheMisses;
}

namespace DB
{

/// Estimate of number of bytes in cache for marks.
struct MarksWeightFunction
{
	size_t operator()(const MarksInCompressedFile & marks) const
	{
		/// NOTE Could add extra 100 bytes for overhead of std::vector, cache structures and allocator.
		return marks.size() * sizeof(MarkInCompressedFile);
	}
};


/** Cache of 'marks' for StorageMergeTree.
  * Marks is an index structure that addresses ranges in column file, corresponding to ranges of primary key.
  */
class MarkCache : public LRUCache<UInt128, MarksInCompressedFile, UInt128TrivialHash, MarksWeightFunction>
{
private:
	using Base = LRUCache<UInt128, MarksInCompressedFile, UInt128TrivialHash, MarksWeightFunction>;

public:
	MarkCache(size_t max_size_in_bytes, const Delay & expiration_delay)
		: Base(max_size_in_bytes, expiration_delay) {}

	/// Calculate key from path to file and offset.
	static UInt128 hash(const String & path_to_file)
	{
		UInt128 key;

		SipHash hash;
		hash.update(path_to_file.data(), path_to_file.size() + 1);
		hash.get128(key.first, key.second);

		return key;
	}

	MappedPtr get(const Key & key)
	{
		MappedPtr res = Base::get(key);

		if (res)
			ProfileEvents::increment(ProfileEvents::MarkCacheHits);
		else
			ProfileEvents::increment(ProfileEvents::MarkCacheMisses);

		return res;
	}
};

using MarkCachePtr = std::shared_ptr<MarkCache>;

}
