#pragma once

#include <common/Common.h>


/** Отслеживает потребление памяти.
  * Кидает исключение, если оно стало бы больше некоторого предельного значения.
  * Один объект может использоваться одновременно в разных потоках.
  */
class MemoryTracker
{
	Int64 amount = 0;
	Int64 peak = 0;
	Int64 limit = 0;

	/// В целях тестирования exception safety - кидать исключение при каждом выделении памяти с указанной вероятностью.
	double fault_probability = 0;

	/// Односвязный список. Вся информация будет передаваться в следующие MemoryTracker-ы тоже.
	MemoryTracker * next = nullptr;

	/// Если задано (например, "for user") - в сообщениях в логе будет указываться это описание.
	const char * description = nullptr;

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
	void free(Int64 size);

	Int64 get() const
	{
		return amount;
	}

	Int64 getPeak() const
	{
		return peak;
	}

	void setFaultProbability(double value)
	{
		fault_probability = value;
	}

	void setNext(MemoryTracker * next_)
	{
		next = next_;
	}

	void setDescription(const char * description_)
	{
		description = description_;
	}

	/// Обнулить накопленные данные.
	void reset();

	/// Вывести в лог информацию о пиковом потреблении памяти.
	void logPeakMemoryUsage() const;
};


/** Объект MemoryTracker довольно трудно протащить во все места, где выделяются существенные объёмы памяти.
  * Поэтому, используется thread-local указатель на используемый MemoryTracker или nullptr, если его не нужно использовать.
  * Этот указатель выставляется, когда в данном потоке следует отслеживать потребление памяти.
  * Таким образом, его нужно всего-лишь протащить во все потоки, в которых обрабатывается один запрос.
  */
extern __thread MemoryTracker * current_memory_tracker;
