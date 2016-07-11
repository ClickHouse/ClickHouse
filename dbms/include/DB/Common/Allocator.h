#pragma once

#include <malloc.h>
#include <string.h>
#include <sys/mman.h>

#include <DB/Common/MemoryTracker.h>
#include <DB/Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
	extern const int BAD_ARGUMENTS;
	extern const int CANNOT_ALLOCATE_MEMORY;
	extern const int CANNOT_MUNMAP;
	extern const int CANNOT_MREMAP;
}
}


/** При использовании AllocatorWithStackMemory, размещённом на стеке,
  *  GCC 4.9 ошибочно делает предположение, что мы можем вызывать free от указателя на стек.
  * На самом деле, комбинация условий внутри AllocatorWithStackMemory этого не допускает.
  */
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfree-nonheap-object"
#endif


/** Отвечает за выделение/освобождение памяти. Используется, например, в PODArray, Arena.
  * Также используется в хэш-таблицах.
  * Интерфейс отличается от std::allocator
  * - наличием метода realloc, который для больших кусков памяти использует mremap;
  * - передачей размера в метод free;
  * - наличием аргумента alignment;
  * - возможностью зануления памяти (используется в хэш-таблицах);
  */
template <bool clear_memory_>
class Allocator
{
protected:
	static constexpr bool clear_memory = clear_memory_;

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

			/// Заполнение нулями не нужно - mmap сам это делает.
		}
		else
		{
			if (alignment <= MALLOC_MIN_ALIGNMENT)
			{
				if (clear_memory)
					buf = ::calloc(size, 1);
				else
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

				if (clear_memory)
					memset(buf, 0, size);
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

			if (clear_memory)
				memset(reinterpret_cast<char *>(buf) + old_size, 0, new_size - old_size);
		}
		else if (old_size >= MMAP_THRESHOLD && new_size >= MMAP_THRESHOLD)
		{
			if (current_memory_tracker)
				current_memory_tracker->realloc(old_size, new_size);

			buf = mremap(buf, old_size, new_size, MREMAP_MAYMOVE);
			if (MAP_FAILED == buf)
				DB::throwFromErrno("Allocator: Cannot mremap.", DB::ErrorCodes::CANNOT_MREMAP);

			/// Заполнение нулями не нужно.
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

protected:
	static constexpr size_t getStackThreshold()
	{
		return 0;
	}
};


/** Аллокатор с оптимизацией для маленьких кусков памяти.
  */
template <typename Base, size_t N = 64>
class AllocatorWithStackMemory : private Base
{
private:
	char stack_memory[N];

public:
	void * alloc(size_t size)
	{
		if (size <= N)
		{
			if (Base::clear_memory)
				memset(stack_memory, 0, N);
			return stack_memory;
		}

		return Base::alloc(size);
	}

	void free(void * buf, size_t size)
	{
		if (size > N)
			Base::free(buf, size);
	}

	void * realloc(void * buf, size_t old_size, size_t new_size)
	{
		/// Было в stack_memory, там и останется.
		if (new_size <= N)
			return buf;

		/// Уже не помещалось в stack_memory.
		if (old_size > N)
			return Base::realloc(buf, old_size, new_size);

		/// Было в stack_memory, но теперь не помещается.
		void * new_buf = Base::alloc(new_size);
		memcpy(new_buf, buf, old_size);
		return new_buf;
	}

protected:
	static constexpr size_t getStackThreshold()
	{
		return N;
	}
};


#if !__clang__
#pragma GCC diagnostic pop
#endif
