#pragma once

#include <map>
#include <list>
#include <memory>
#include <Poco/Mutex.h>
#include <Poco/Condition.h>
#include <Poco/Net/IPAddress.h>
#include <statdaemons/Stopwatch.h>
#include <DB/Core/Defines.h>
#include <DB/Core/Progress.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Common/MemoryTracker.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Interpreters/QueryPriorities.h>


namespace DB
{

/** Список исполняющихся в данный момент запросов.
  * Также реализует ограничение на их количество.
  */

/// Запрос и данные о его выполнении.
struct ProcessListElement
{
	String query;
	String user;
	String query_id;
	Poco::Net::IPAddress ip_address;

	Stopwatch watch;

	Progress progress;

	MemoryTracker memory_tracker;

	QueryPriorities::Handle priority_handle;

	bool is_cancelled = false;


	ProcessListElement(const String & query_, const String & user_,
		const String & query_id_, const Poco::Net::IPAddress & ip_address_,
		size_t max_memory_usage, QueryPriorities::Handle && priority_handle_)
		: query(query_), user(user_), query_id(query_id_), ip_address(ip_address_), memory_tracker(max_memory_usage),
		priority_handle(std::move(priority_handle_))
	{
		current_memory_tracker = &memory_tracker;
	}

	~ProcessListElement()
	{
		current_memory_tracker = nullptr;
	}

	bool update(const Progress & value)
	{
		progress.incrementPiecewiseAtomically(value);

		if (priority_handle)
			priority_handle->waitIfNeed(std::chrono::seconds(1));		/// NOTE Можно сделать настраиваемым таймаут.

		return !is_cancelled;
	}
};


class ProcessList;


/// Держит итератор на список, и удаляет элемент из списка в деструкторе.
class ProcessListEntry
{
private:
	using Container = std::list<ProcessListElement>;

	ProcessList & parent;
	Container::iterator it;
public:
	ProcessListEntry(ProcessList & parent_, Container::iterator it_)
		: parent(parent_), it(it_) {}

	~ProcessListEntry();

	ProcessListElement * operator->() { return &*it; }
	const ProcessListElement * operator->() const { return &*it; }

	ProcessListElement & get() { return *it; }
	const ProcessListElement & get() const { return *it; }
};


class ProcessList
{
	friend class ProcessListEntry;
public:
	using Element = ProcessListElement;
	using Entry = ProcessListEntry;

	/// list, чтобы итераторы не инвалидировались. NOTE: можно заменить на cyclic buffer, но почти незачем.
	using Container = std::list<Element>;
	/// Query_id -> Element *
	using QueryToElement = std::unordered_map<String, Element *>;
	/// User -> Query_id -> Element *
	using UserToQueries = std::unordered_map<String, QueryToElement>;

private:
	mutable Poco::FastMutex mutex;
	mutable Poco::Condition have_space;		/// Количество одновременно выполняющихся запросов стало меньше максимального.

	Container cont;
	size_t cur_size;		/// В C++03 std::list::size не O(1).
	size_t max_size;		/// Если 0 - не ограничено. Иначе, если пытаемся добавить больше - кидается исключение.
	UserToQueries user_to_queries;
	QueryPriorities priorities;

public:
	ProcessList(size_t max_size_ = 0) : cur_size(0), max_size(max_size_) {}

	typedef std::shared_ptr<ProcessListEntry> EntryPtr;

	/** Зарегистрировать выполняющийся запрос. Возвращает refcounted объект, который удаляет запрос из списка при уничтожении.
	  * Если выполняющихся запросов сейчас слишком много - ждать не более указанного времени.
	  * Если времени не хватило - кинуть исключение.
	  */
	EntryPtr insert(const String & query_, const String & user_, const String & query_id_, const Poco::Net::IPAddress & ip_address_,
		size_t max_memory_usage, size_t max_wait_milliseconds, bool replace_running_query, QueryPriorities::Priority priority);

	/// Количество одновременно выполняющихся запросов.
	size_t size() const { return cur_size; }

	/// Получить текущее состояние (копию) списка запросов.
	Container get() const
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);
		return cont;
	}

	void setMaxSize(size_t max_size_)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);
		max_size = max_size_;
	}
};

}
