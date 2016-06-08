#pragma once

#include <map>
#include <list>
#include <memory>
#include <mutex>
#include <Poco/Condition.h>
#include <Poco/Net/IPAddress.h>
#include <DB/Common/Stopwatch.h>
#include <DB/Core/Defines.h>
#include <DB/Core/Progress.h>
#include <DB/Common/Exception.h>
#include <DB/Common/MemoryTracker.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Interpreters/QueryPriorities.h>
#include <DB/Storages/IStorage.h>
#include <DB/Common/CurrentMetrics.h>


namespace DB
{

/** Список исполняющихся в данный момент запросов.
  * Также реализует ограничение на их количество.
  */

/** Информационная составляющая элемента списка процессов.
  * Для вывода в SHOW PROCESSLIST. Не содержит никаких сложных объектов, которые что-то делают при копировании или в деструкторах.
  */
struct ProcessInfo
{
	String query;
	String user;
	String query_id;
	Poco::Net::IPAddress ip_address;
	double elapsed_seconds;
	size_t rows;
	size_t bytes;
	size_t total_rows;
	Int64 memory_usage;
};


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

	CurrentMetrics::Increment num_queries {CurrentMetrics::Query};

	bool is_cancelled = false;

	/// Здесь могут быть зарегистрированы временные таблицы. Изменять под mutex-ом.
	Tables temporary_tables;


	ProcessListElement(const String & query_, const String & user_,
		const String & query_id_, const Poco::Net::IPAddress & ip_address_,
		size_t max_memory_usage, double memory_tracker_fault_probability,
		QueryPriorities::Handle && priority_handle_)
		: query(query_), user(user_), query_id(query_id_), ip_address(ip_address_), memory_tracker(max_memory_usage),
		priority_handle(std::move(priority_handle_))
	{
		memory_tracker.setDescription("(for query)");
		current_memory_tracker = &memory_tracker;

		if (memory_tracker_fault_probability)
			memory_tracker.setFaultProbability(memory_tracker_fault_probability);
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

	ProcessInfo getInfo() const
	{
		return ProcessInfo{
			.query 				= query,
			.user 				= user,
			.query_id 			= query_id,
			.ip_address 		= ip_address,
			.elapsed_seconds 	= watch.elapsedSeconds(),
			.rows 				= progress.rows,
			.bytes 				= progress.bytes,
			.total_rows 		= progress.total_rows,
			.memory_usage 		= memory_tracker.get(),
		};
	}
};


/// Данные о запросах одного пользователя.
struct ProcessListForUser
{
	/// Query_id -> ProcessListElement *
	using QueryToElement = std::unordered_map<String, ProcessListElement *>;
	QueryToElement queries;

	/// Ограничение и счётчик памяти на все одновременно выполняющиеся запросы одного пользователя.
	MemoryTracker user_memory_tracker;
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
	using Info = std::vector<ProcessInfo>;
	/// User -> queries
	using UserToQueries = std::unordered_map<String, ProcessListForUser>;

private:
	mutable std::mutex mutex;
	mutable Poco::Condition have_space;		/// Количество одновременно выполняющихся запросов стало меньше максимального.

	Container cont;
	size_t cur_size;		/// В C++03 std::list::size не O(1).
	size_t max_size;		/// Если 0 - не ограничено. Иначе, если пытаемся добавить больше - кидается исключение.
	UserToQueries user_to_queries;
	QueryPriorities priorities;

	/// Ограничение и счётчик памяти на все одновременно выполняющиеся запросы.
	MemoryTracker total_memory_tracker;

public:
	ProcessList(size_t max_size_ = 0) : cur_size(0), max_size(max_size_) {}

	using EntryPtr = std::shared_ptr<ProcessListEntry>;

	/** Зарегистрировать выполняющийся запрос. Возвращает refcounted объект, который удаляет запрос из списка при уничтожении.
	  * Если выполняющихся запросов сейчас слишком много - ждать не более указанного времени.
	  * Если времени не хватило - кинуть исключение.
	  */
	EntryPtr insert(const String & query_, const String & user_, const String & query_id_, const Poco::Net::IPAddress & ip_address_,
		const Settings & settings);

	/// Количество одновременно выполняющихся запросов.
	size_t size() const { return cur_size; }

	/// Получить текущее состояние списка запросов.
	Info getInfo() const
	{
		std::lock_guard<std::mutex> lock(mutex);

		Info res;
		res.reserve(cur_size);
		for (const auto & elem : cont)
			res.emplace_back(elem.getInfo());

		return res;
	}

	void setMaxSize(size_t max_size_)
	{
		std::lock_guard<std::mutex> lock(mutex);
		max_size = max_size_;
	}

	/// Зарегистрировать временную таблицу. Потом её можно будет получить по query_id и по названию.
	void addTemporaryTable(ProcessListElement & elem, const String & table_name, StoragePtr storage);

	/// Найти временную таблицу по query_id и по названию. Замечание: плохо работает, если есть разные запросы с одним query_id.
	StoragePtr tryGetTemporaryTable(const String & query_id, const String & table_name) const;
};

}
