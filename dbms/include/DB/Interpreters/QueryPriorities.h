#pragma once

#include <map>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <chrono>


/** Реализует приоритеты запросов.
  * Позволяет приостанавливать выполнение запроса, если выполняется хотя бы один более приоритетный запрос.
  *
  * Величина приоритета - целое число, чем меньше - тем больше приоритет.
  *
  * Приоритет 0 считается особенным - запросы с таким приоритетом выполняются всегда,
  *  не зависят от других запросов и не влияют на другие запросы.
  * То есть 0 означает - не использовать приоритеты.
  *
  * NOTE Возможности сделать лучше:
  * - реализовать ограничение на максимальное количество запросов с таким приоритетом.
  */
class QueryPriorities
{
public:
	using Priority = int;

private:
	friend struct Handle;

	using Count = int;

	/// Количество выполняющихся сейчас запросов с заданным приоритетом.
	using Container = std::map<Priority, Count>;

	std::mutex mutex;
	std::condition_variable condvar;
	Container container;


	/** Если есть более приоритетные запросы - спать, пока они не перестанут быть или не истечёт таймаут.
	  * Возвращает true, если более приоритетные запросы исчезли на момент возврата из функции, false, если истёк таймаут.
	  */
	template <typename Duration>
	bool waitIfNeed(Priority priority, Duration timeout)
	{
		if (0 == priority)
			return true;

		std::unique_lock<std::mutex> lock(mutex);

		while (true)
		{
			/// Если ли хотя бы один более приоритетный запрос?
			bool found = false;
			for (const auto & value : container)
			{
				if (value.first >= priority)
					break;

				if (value.second > 0)
				{
					found = true;
					break;
				}
			}

			if (!found)
				return true;

			if (std::cv_status::timeout == condvar.wait_for(lock, timeout))
				return false;
		}
	}

public:
	struct HandleImpl
	{
	private:
		QueryPriorities & parent;
		QueryPriorities::Container::value_type & value;

	public:
		HandleImpl(QueryPriorities & parent_, QueryPriorities::Container::value_type & value_)
			: parent(parent_), value(value_) {}

		~HandleImpl()
		{
			{
				std::lock_guard<std::mutex> lock(parent.mutex);
				--value.second;
			}
			parent.condvar.notify_all();
		}

		template <typename Duration>
		bool waitIfNeed(Duration timeout)
		{
			return parent.waitIfNeed(value.first, timeout);
		}
	};

	using Handle = std::shared_ptr<HandleImpl>;

	/** Зарегистрировать, что запрос с заданным приоритетом выполняется.
	  * Возвращается объект, в деструкторе которого, запись о запросе удаляется.
	  */
	Handle insert(Priority priority)
	{
		if (0 == priority)
			return {};

		std::lock_guard<std::mutex> lock(mutex);
		auto it = container.emplace(priority, 0).first;
		++it->second;
		return std::make_shared<HandleImpl>(*this, *it);
	}
};
