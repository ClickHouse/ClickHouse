#pragma once

#include <malloc.h>
#include <string.h>
#include <sys/mman.h>

#include <DB/Common/MemoryTracker.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>


/** Отвечает за выделение/освобождение памяти. Используется, например, в PODArray, Arena.
  * Интерфейс отличается от std::allocator
  * - наличием метода realloc, который для больших кусков памяти использует mremap;
  * - передачей размера в метод free;
  * - наличием аргумента alignment;
  */
class Allocator
{
private:
	/** См. комментарий в HashTableAllocator.h
	  */
	static constexpr size_t MMAP_THRESHOLD = 64 * (1 << 20);
	static constexpr size_t HUGE_PAGE_SIZE = 2 * (1 << 20);
	static constexpr size_t MMAP_MIN_ALIGNMENT = 4096;
	static constexpr size_t MALLOC_MIN_ALIGNMENT = 8;

public:
	/// Выделить кусок памяти.
	void * alloc(size_t size, size_t alignment = 0)
	{
		if (current_memory_tracker)
			current_memory_tracker->alloc(size);

		void * buf;

		if (size >= MMAP_THRESHOLD)
		{
			if (alignment > MMAP_MIN_ALIGNMENT)
				throw DB::Exception("Too large alignment: more than page size.", DB::ErrorCodes::BAD_ARGUMENTS);

			buf = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
			if (MAP_FAILED == buf)
				DB::throwFromErrno("Allocator: Cannot mmap.", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);

			/// См. комментарий в HashTableAllocator.h
			if (size >= HUGE_PAGE_SIZE && 0 != madvise(buf, size, MADV_HUGEPAGE))
				DB::throwFromErrno("HashTableAllocator: Cannot madvise with MADV_HUGEPAGE.", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
		}
		else
		{
			if (alignment <= MALLOC_MIN_ALIGNMENT)
			{
				buf = ::malloc(size);

				if (nullptr == buf)
					DB::throwFromErrno("Allocator: Cannot malloc.", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
			}
			else
			{
				buf = nullptr;
				int res = posix_memalign(&buf, alignment, size);

				if (0 != res)
					DB::throwFromErrno("Cannot allocate memory (posix_memalign)", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, res);
			}
		}

		return buf;
	}

	/// Освободить память.
	void free(void * buf, size_t size)
	{
		if (size >= MMAP_THRESHOLD)
		{
			if (0 != munmap(buf, size))
				DB::throwFromErrno("Allocator: Cannot munmap.", DB::ErrorCodes::CANNOT_MUNMAP);
		}
		else
		{
			::free(buf);
		}

		if (current_memory_tracker)
			current_memory_tracker->free(size);
	}

	/** Увеличить размер куска памяти.
	  * Содержимое старого куска памяти переезжает в начало нового.
	  * Положение куска памяти может измениться.
	  */
	void * realloc(void * buf, size_t old_size, size_t new_size, size_t alignment = 0)
	{
		if (old_size < MMAP_THRESHOLD && new_size < MMAP_THRESHOLD && alignment <= MALLOC_MIN_ALIGNMENT)
		{
			if (current_memory_tracker)
				current_memory_tracker->realloc(old_size, new_size);

			buf = ::realloc(buf, new_size);

			if (nullptr == buf)
				DB::throwFromErrno("Allocator: Cannot realloc.", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
		}
		else if (old_size >= MMAP_THRESHOLD && new_size >= MMAP_THRESHOLD)
		{
			if (current_memory_tracker)
				current_memory_tracker->realloc(old_size, new_size);

			buf = mremap(buf, old_size, new_size, MREMAP_MAYMOVE);
			if (MAP_FAILED == buf)
				DB::throwFromErrno("Allocator: Cannot mremap.", DB::ErrorCodes::CANNOT_MREMAP);
		}
		else
		{
			void * new_buf = alloc(new_size, alignment);
			memcpy(new_buf, buf, old_size);
			free(buf, old_size);
			buf = new_buf;
		}

		return buf;
	}
};
