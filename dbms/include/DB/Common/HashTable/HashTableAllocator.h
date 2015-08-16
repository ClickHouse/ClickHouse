#pragma once

#include <malloc.h>
#include <string.h>
#include <sys/mman.h>

#include <DB/Common/MemoryTracker.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

/** При использовании HashTableAllocatorWithStackMemory, размещённом на стеке,
  *  GCC 4.9 ошибочно делает предположение, что мы можем вызывать free от указателя на стек.
  * На самом деле, комбинация условий внутри HashTableAllocatorWithStackMemory этого не допускает.
  */
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfree-nonheap-object"
#endif


/** Общая часть разных хэш-таблиц, отвечающая за выделение/освобождение памяти.
  * Отличается от Allocator тем, что зануляет память.
  * Используется в качестве параметра шаблона (есть несколько реализаций с таким же интерфейсом).
  */
class HashTableAllocator
{
private:
	/** Многие современные аллокаторы (например, tcmalloc) не умеют делать mremap для realloc,
	  *  даже в случае достаточно больших кусков памяти.
	  * Хотя это позволяет увеличить производительность и уменьшить потребление памяти во время realloc-а.
	  * Чтобы это исправить, делаем mremap самостоятельно, если кусок памяти достаточно большой.
	  * Порог (64 МБ) выбран достаточно большим, так как изменение адресного пространства
	  *  довольно сильно тормозит, особенно в случае наличия большого количества потоков.
	  * Рассчитываем, что набор операций mmap/что-то сделать/mremap может выполняться всего лишь около 1000 раз в секунду.
	  *
	  * PS. Также это требуется, потому что tcmalloc не может выделить кусок памяти больше 16 GB.
	  */
	static constexpr size_t MMAP_THRESHOLD = 64 * (1 << 20);
	static constexpr size_t HUGE_PAGE_SIZE = 2 * (1 << 20);

public:
	/// Выделить кусок памяти и заполнить его нулями.
	void * alloc(size_t size)
	{
		if (current_memory_tracker)
			current_memory_tracker->alloc(size);

		void * buf;

		if (size >= MMAP_THRESHOLD)
		{
			buf = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
			if (MAP_FAILED == buf)
				DB::throwFromErrno("HashTableAllocator: Cannot mmap.", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);

			/** Использование huge pages позволяет увеличить производительность более чем в три раза
			  *  в запросе SELECT number % 1000000 AS k, count() FROM system.numbers GROUP BY k,
			  *  (хэш-таблица на 1 000 000 элементов)
			  * и примерно на 15% в случае хэш-таблицы на 100 000 000 элементов.
			  */
			if (size >= HUGE_PAGE_SIZE && 0 != madvise(buf, size, MADV_HUGEPAGE))
				DB::throwFromErrno("HashTableAllocator: Cannot madvise with MADV_HUGEPAGE.", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);

			/// Заполнение нулями не нужно - mmap сам это делает.
		}
		else
		{
			buf = ::calloc(size, 1);
			if (nullptr == buf)
				DB::throwFromErrno("HashTableAllocator: Cannot calloc.", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
		}

		return buf;
	}

	/// Освободить память.
	void free(void * buf, size_t size)
	{
		if (size >= MMAP_THRESHOLD)
		{
			if (0 != munmap(buf, size))
				DB::throwFromErrno("HashTableAllocator: Cannot munmap.", DB::ErrorCodes::CANNOT_MUNMAP);
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
	  * Оставшаяся часть заполняется нулями.
	  * Положение куска памяти может измениться.
	  */
	void * realloc(void * buf, size_t old_size, size_t new_size)
	{
		if (old_size < MMAP_THRESHOLD && new_size < MMAP_THRESHOLD)
		{
			if (current_memory_tracker)
				current_memory_tracker->realloc(old_size, new_size);

			buf = ::realloc(buf, new_size);
			if (nullptr == buf)
				DB::throwFromErrno("HashTableAllocator: Cannot realloc.", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);

			memset(reinterpret_cast<char *>(buf) + old_size, 0, new_size - old_size);
		}
		else if (old_size >= MMAP_THRESHOLD && new_size >= MMAP_THRESHOLD)
		{
			if (current_memory_tracker)
				current_memory_tracker->realloc(old_size, new_size);

			buf = mremap(buf, old_size, new_size, MREMAP_MAYMOVE);
			if (MAP_FAILED == buf)
				DB::throwFromErrno("HashTableAllocator: Cannot mremap.", DB::ErrorCodes::CANNOT_MREMAP);

			/** Здесь не получается сделать madvise с MADV_HUGEPAGE.
			  * Похоже, что при mremap, huge pages сами расширяются на новую область.
			  */

			/// Заполнение нулями не нужно.
		}
		else
		{
			void * new_buf = alloc(new_size);
			memcpy(new_buf, buf, old_size);
			free(buf, old_size);
			buf = new_buf;
		}

		return buf;
	}
};


/** Аллокатор с оптимизацией для маленьких кусков памяти.
  */
template <size_t N = 64>
class HashTableAllocatorWithStackMemory : private HashTableAllocator
{
private:
	char stack_memory[N];

public:
	void * alloc(size_t size)
	{
		if (size <= N)
		{
			memset(stack_memory, 0, N);
			return stack_memory;
		}

		return HashTableAllocator::alloc(size);
	}

	void free(void * buf, size_t size)
	{
		if (size > N)
			HashTableAllocator::free(buf, size);
	}

	void * realloc(void * buf, size_t old_size, size_t new_size)
	{
		/// Было в stack_memory, там и останется.
		if (new_size <= N)
			return buf;

		/// Уже не помещалось в stack_memory.
		if (old_size > N)
			return HashTableAllocator::realloc(buf, old_size, new_size);

		/// Было в stack_memory, но теперь не помещается.
		void * new_buf = HashTableAllocator::alloc(new_size);
		memcpy(new_buf, buf, old_size);
		return new_buf;
	}
};

#if !__clang__
#pragma GCC diagnostic pop
#endif
