#pragma once

#include <Yandex/Common.h>


/** Отслеживает потребление памяти.
  * Кидает исключение, если оно стало бы больше некоторого предельного значения.
  * Один объект может использоваться одновременно в разных потоках.
  */
class MemoryTracker
{
	Int64 amount = 0;
	Int64 peak = 0;
	Int64 limit = 0;

public:
	MemoryTracker(Int64 limit_) : limit(limit_) {}

	~MemoryTracker();

	/** Вызывайте эти функции перед соответствующими операциями с памятью.
	  */
	void alloc(Int64 size);

	void realloc(Int64 old_size, Int64 new_size)
	{
		alloc(new_size - old_size);
	}

	/** А эту функцию имеет смысл вызывать после освобождения памяти.
	  */
	void free(Int64 size)
	{
		__sync_sub_and_fetch(&amount, size);
	}

	Int64 get() const
	{
		return amount;
	}

	Int64 getPeak() const
	{
		return peak;
	}
};


/** Объект MemoryTracker довольно трудно протащить во все места, где выделяются существенные объёмы памяти.
  * Поэтому, используется thread-local указатель на используемый MemoryTracker или nullptr, если его не нужно использовать.
  * Этот указатель выставляется, когда в данном потоке следует отслеживать потребление памяти.
  * Таким образом, его нужно всего-лишь протащить во все потоки, в которых обрабатывается один запрос.
  */
extern __thread MemoryTracker * current_memory_tracker;
