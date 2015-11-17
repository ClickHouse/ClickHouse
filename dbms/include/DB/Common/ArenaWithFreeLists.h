#pragma once

#include <cstdlib>
#include <memory>
#include <vector>
#include <Poco/SharedPtr.h>
#include <common/likely.h>
#include <DB/Core/Defines.h>
#include <DB/Common/ProfileEvents.h>
#include <DB/Common/Allocator.h>
#include <ext/range.hpp>
#include <ext/size.hpp>


namespace DB
{


class ArenaWithFreeLists
{
private:
	/// Непрерывный кусок памяти и указатель на свободное место в нём. Односвязный список.
	struct Chunk : private Allocator<false>	/// empty base optimization
	{
		char * begin;
		char * end;

		Chunk * prev;

		Chunk(const std::size_t size_, Chunk * prev_)
		{
			ProfileEvents::increment(ProfileEvents::ArenaAllocChunks);
			ProfileEvents::increment(ProfileEvents::ArenaAllocBytes, size_);

			begin = static_cast<char *>(Allocator::alloc(size_));
			end = begin + size_;
			prev = prev_;
		}

		~Chunk()
		{
			Allocator::free(begin, size());

			delete prev;
		}

		size_t size() { return end - begin; }
	};

	struct Block
	{
		std::size_t size;
		Block * next;
	};

	static size_t roundUpToPageSize(size_t s)
	{
		return (s + 4096 - 1) / 4096 * 4096;
	}

	size_t growth_factor;
	size_t linear_growth_threshold;

	/// Последний непрерывный кусок памяти.
	Chunk * head;
	size_t size_in_bytes;

	static constexpr std::size_t sizes[] {
		16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536,
		std::numeric_limits<std::size_t>::max()
	};
	static_assert(sizes[0] >= sizeof(Block), "Can't make allocations smaller than sizeof(Block)");
	static constexpr auto min_bucket_num = 3;
	static constexpr auto max_fixed_block_size = 65536;

	Block * free_lists[ext::size(sizes)] {};

	/// Если размер чанка меньше linear_growth_threshold, то рост экспоненциальный, иначе - линейный, для уменьшения потребления памяти.
	size_t nextSize(size_t min_next_size) const
	{
		size_t size_after_grow = 0;

		if (head->size() < linear_growth_threshold)
			size_after_grow = head->size() * growth_factor;
		else
			size_after_grow = linear_growth_threshold;

		if (size_after_grow < min_next_size)
			size_after_grow = min_next_size;

		return roundUpToPageSize(size_after_grow);
	}

	/// Добавить следующий непрерывный кусок памяти размера не меньше заданного.
	void NO_INLINE addChunk(size_t min_size)
	{
		head = new Chunk(nextSize(min_size), head);
		size_in_bytes += head->size();

		putBlock(head->begin, head->size());
	}

	static std::size_t findFreeListIndex(const std::size_t size)
	{
		/// last free list is for any blocks > 64k
		if (size > max_fixed_block_size)
			return ext::size(sizes) - 1;

		/// shift powers of two into previous bucket by subtracting 1
		const auto bucket_num = _bit_scan_reverse(size - 1);

		return std::max(bucket_num, min_bucket_num) - min_bucket_num;
	}

	/// @todo coalesce blocks
	void putBlock(const void * ptr, const std::size_t size)
	{
		const auto list_idx = findFreeListIndex(size);

		union {
			const void * p_c;
			Block * block;
		};

		p_c = ptr;
		block->size = size;
		block->next = free_lists[list_idx];

		free_lists[list_idx] = block;
	}

	char * splitBlock(Block * & block, const std::size_t size)
	{
		const auto block_pos = reinterpret_cast<char *>(block);
		/// calculate size of block remaining after cutting `size` bytes
		const auto remaining_size = block->size - size;

		/// we have claimed this block, redirect pointer to next block
		block = block->next;

		/// put remaining block to appropriate free list
		if (remaining_size != 0)
			putBlock(block_pos + size, remaining_size);

		/// return cut block to caller
		return block_pos;
	}

public:
	ArenaWithFreeLists(
		const std::size_t initial_size = 4096, const std::size_t growth_factor = 2,
		const std::size_t linear_growth_threshold = 128 * 1024 * 1024)
		: growth_factor{growth_factor}, linear_growth_threshold{linear_growth_threshold},
		  head{new Chunk(initial_size, nullptr)}, size_in_bytes{head->size()}
	{
		putBlock(head->begin, head->size());
	}

	~ArenaWithFreeLists()
	{
		delete head;
	}

	char * alloc(const std::size_t size)
	{
		/// find existing list of required size, possibly split a larger one
		for (const auto list_idx : ext::range(findFreeListIndex(size), ext::size(free_lists)))
			/// reference to a pointer to head of corresponding free list
			if (auto & block = free_lists[list_idx])
				return splitBlock(block, size);

		/// no block of corresponding size, add another chunk
		addChunk(size);

		/// find the newly created block and split it
		auto & newly_created_block = free_lists[findFreeListIndex(head->size())];

		return splitBlock(newly_created_block, size);
	}

	void free(const void * ptr, const std::size_t size)
	{
		putBlock(ptr, size);
	}
};


}
