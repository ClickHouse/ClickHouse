#pragma once

#include <memory>

#include <DB/Common/LRUCache.h>
#include <DB/Common/ProfileEvents.h>
#include <DB/Common/SipHash.h>
#include <DB/Interpreters/AggregationCommon.h>


namespace DB
{

struct MarkInCompressedFile
{
	size_t offset_in_compressed_file;
	size_t offset_in_decompressed_block;

	bool operator==(const MarkInCompressedFile & rhs) const
	{
		return std::forward_as_tuple(offset_in_compressed_file, offset_in_decompressed_block) ==
				std::forward_as_tuple(rhs.offset_in_compressed_file, rhs.offset_in_decompressed_block);
	}
	bool operator!=(const MarkInCompressedFile & rhs) const
	{
		return !(*this == rhs);
	}

	String toString() const
	{
		return "(" + DB::toString(offset_in_compressed_file) + "," + DB::toString(offset_in_decompressed_block) + ")";
	}
};

typedef std::vector<MarkInCompressedFile> MarksInCompressedFile;

/// Оценка количества байтов, занимаемых засечками в кеше.
struct MarksWeightFunction
{
	size_t operator()(const MarksInCompressedFile & marks) const
	{
		/// Можно еще добавить порядка 100 байт на накладные расходы вектора и кеша.
		return marks.size() * sizeof(MarkInCompressedFile);
	}
};


/** Кэш засечек в столбце из StorageMergeTree.
  */
class MarkCache : public LRUCache<UInt128, MarksInCompressedFile, UInt128TrivialHash, MarksWeightFunction>
{
private:
	typedef LRUCache<UInt128, MarksInCompressedFile, UInt128TrivialHash, MarksWeightFunction> Base;

public:
	MarkCache(size_t max_size_in_bytes)
		: Base(max_size_in_bytes) {}

	/// Посчитать ключ от пути к файлу и смещения.
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

typedef std::shared_ptr<MarkCache> MarkCachePtr;

}
