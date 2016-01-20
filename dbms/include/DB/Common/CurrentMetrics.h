#pragma once

#include <stddef.h>
#include <utility>


/** Позволяет считать количество одновременно происходящих событий или текущее значение какой-либо метрики.
  *  - для высокоуровневого профайлинга.
  *
  * Также смотрите ProfileEvents.h
  * В ProfileEvents считается общее количество произошедших (точечных) событий - например, сколько раз были выполнены запросы.
  * В CurrentMetrics считается количество одновременных событий - например, сколько сейчас одновременно выполняется запросов,
  *  или текущее значение метрики - например, величина отставания реплики в секундах.
  */

#define APPLY_FOR_METRICS(M) \
	M(Query) \
	\
	M(END)

namespace CurrentMetrics
{
	/// Виды метрик.
	enum Metric
	{
	#define M(NAME) NAME,
		APPLY_FOR_METRICS(M)
	#undef M
	};


	/// Получить текстовое описание метрики по его enum-у.
	inline const char * getDescription(Metric event)
	{
		static const char * descriptions[] =
		{
		#define M(NAME) #NAME,
			APPLY_FOR_METRICS(M)
		#undef M
		};

		return descriptions[event];
	}


	/// Счётчики - текущие значения метрик.
	extern size_t values[END];


	/// Выставить значение указанной метрики.
	inline void set(Metric metric, size_t value)
	{
		values[metric] = value;
	}

	/// На время жизни объекта, увеличивает указанное значение на указанную величину.
	class Increment
	{
	private:
		size_t * what;
		size_t amount;

		Increment(size_t * what, size_t amount)
			: what(what), amount(amount)
		{
			__sync_fetch_and_add(what, amount);
		}

	public:
		Increment(Metric metric, size_t amount = 1)
			: Increment(&values[metric], amount) {}

		~Increment()
		{
			if (what)
				__sync_fetch_and_sub(what, amount);
		}

		Increment(Increment && old)
		{
			*this = std::move(old);
		}

		Increment & operator= (Increment && old)
		{
			what = old.what;
			amount = old.amount;
			old.what = nullptr;
			return *this;
		}
	};
}


#undef APPLY_FOR_METRICS
