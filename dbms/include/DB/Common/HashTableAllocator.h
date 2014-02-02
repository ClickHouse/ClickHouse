#pragma once

#include <malloc.h>
#include <string.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>


namespace DB
{


/** Общая часть разных хэш-таблиц, отвечающая за выделение/освобождение памяти.
  * Используется в качестве параметра шаблона (есть несколько реализаций с таким же интерфейсом).
  */
class HashTableAllocator
{
public:
	/// Выделить кусок памяти и заполнить его нулями.
	void * alloc(size_t size)
	{
		void * buf = ::calloc(size, 1);
		if (NULL == buf)
			throwFromErrno("HashTableAllocator: Cannot calloc.", ErrorCodes::CANNOT_ALLOCATE_MEMORY);

		return buf;
	}

	/// Освободить память.
	void free(void * buf, size_t size)
	{
		::free(buf);
	}

	/** Увеличить размер куска памяти.
	  * Содержимое старого куска памяти переезжает в начало нового.
	  * Оставшаяся часть заполняется нулями.
	  * Положение куска памяти может измениться.
	  */
	void * realloc(void * buf, size_t old_size, size_t new_size)
	{
		buf = ::realloc(buf, new_size);
		if (NULL == buf)
			throwFromErrno("HashTableAllocator: Cannot realloc.", ErrorCodes::CANNOT_ALLOCATE_MEMORY);

		memset(reinterpret_cast<char *>(buf) + old_size, 0, new_size - old_size);
		return buf;
	}
};


/** Аллокатор с оптимизацией для маленьких кусков памяти.
  */
template <size_t N = 64>
class HashTableAllocatorWithStackMemory : private HashTableAllocator
{
private:
	char stack_memory[N]{};

public:
	void * alloc(size_t size)
	{
		if (size <= N)
			return stack_memory;

		return HashTableAllocator::alloc(size);
	}

	void free(void * buf, size_t size)
	{
		if (size > N)
			HashTableAllocator::free(buf, size);
	}

	void * realloc(void * buf, size_t old_size, size_t new_size)
	{
		if (new_size <= N)
			return buf;

		if (old_size > N)
			return HashTableAllocator::realloc(buf, old_size, new_size);

		buf = ::malloc(new_size);
		if (NULL == buf)
			throwFromErrno("HashTableAllocator: Cannot malloc.", ErrorCodes::CANNOT_ALLOCATE_MEMORY);

		memcpy(buf, stack_memory, old_size);
		memset(reinterpret_cast<char *>(buf) + old_size, 0, new_size - old_size);

		return buf;
	}
};


}
