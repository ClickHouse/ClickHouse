#pragma once

#include <DB/Common/Arena.h>
#include <ext/bit_cast.hpp>
#include <ext/size.hpp>
#include <cstdlib>
#include <memory>
#include <array>


namespace DB
{


class ArenaWithFreeLists : private Allocator<false>
{
private:
	struct Block { Block * next; };

	static const std::array<std::size_t, 14> & getSizes()
	{
		static constexpr std::array<std::size_t, 14> sizes{
			8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536
		};

		static_assert(sizes.front() >= sizeof(Block), "Can't make allocations smaller than sizeof(Block)");

		return sizes;
	}

	static auto sizeToPreviousPowerOfTwo(const int size) { return _bit_scan_reverse(size - 1); }

	static auto getMinBucketNum()
	{
		static const auto val = sizeToPreviousPowerOfTwo(getSizes().front());
		return val;
	}
	static auto getMaxFixedBlockSize() { return getSizes().back(); }

	Arena pool;
	const std::unique_ptr<Block * []> free_lists = std::make_unique<Block * []>(ext::size(getSizes()));

	static std::size_t findFreeListIndex(const std::size_t size)
	{
		/// shift powers of two into previous bucket by subtracting 1
		const auto bucket_num = sizeToPreviousPowerOfTwo(size);

		return std::max(bucket_num, getMinBucketNum()) - getMinBucketNum();
	}

public:
	ArenaWithFreeLists(
		const std::size_t initial_size = 4096, const std::size_t growth_factor = 2,
		const std::size_t linear_growth_threshold = 128 * 1024 * 1024)
		: pool{initial_size, growth_factor, linear_growth_threshold}
	{
	}

	char * alloc(const std::size_t size)
	{
		if (size > getMaxFixedBlockSize())
			return static_cast<char *>(Allocator::alloc(size));

		/// find list of required size
		const auto list_idx = findFreeListIndex(size);

		if (auto & block = free_lists[list_idx])
		{
			const auto res = ext::bit_cast<char *>(block);
			block = block->next;
			return res;
		}

		/// no block of corresponding size, allocate a new one
		return pool.alloc(getSizes()[list_idx]);
	}

	void free(const void * ptr, const std::size_t size)
	{
		if (size > getMaxFixedBlockSize())
			return Allocator::free(const_cast<void *>(ptr), size);

		/// find list of required size
		const auto list_idx = findFreeListIndex(size);

		auto & block = free_lists[list_idx];
		const auto old = block;
		block = ext::bit_cast<Block *>(ptr);
		block->next = old;
	}

	/// Размер выделенного пула в байтах
	size_t size() const
	{
		return pool.size();
	}
};


}
