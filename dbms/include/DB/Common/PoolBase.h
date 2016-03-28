#pragma once

#include <Poco/SharedPtr.h>
#include <Poco/Mutex.h>
#include <Poco/Condition.h>
#include <Poco/Timespan.h>
#include <boost/noncopyable.hpp>

#include <common/logger_useful.h>

/// This type specifies the possible behaviors of an object pool allocator.
enum class PoolMode
{
	/// Get exactly one object from a given pool.
	GET_ONE = 0,
	/// Get a number of objects from a given pool, this number being
	/// read from a configuration parameter.
	GET_MANY,
	/// Get all the objects from a given pool.
	GET_ALL
};

/** Класс, от которого можно унаследоваться и получить пул чего-нибудь. Используется для пулов соединений с БД.
  * Наследник должен предоставить метод для создания нового объекта для помещения в пул.
  */

template <typename TObject>
class PoolBase : private boost::noncopyable
{
public:
	typedef TObject Object;
	typedef Poco::SharedPtr<Object> ObjectPtr;
	typedef Poco::SharedPtr<PoolBase<TObject> > Ptr;

private:

	/** Объект с флагом, используется ли он сейчас. */
	struct PooledObject
	{
		PooledObject(Poco::Condition & available_, ObjectPtr object_)
			: object(object_), available(available_)
		{
		}

		ObjectPtr object;
		bool in_use = false;
		Poco::Condition & available;
	};

	typedef std::vector<Poco::SharedPtr<PooledObject> > Objects;

	/** Помощник, который устанавливает флаг использования объекта, а в деструкторе - снимает,
	  *  а также уведомляет о событии с помощью condvar-а.
	  */
	struct PoolEntryHelper
	{
		PoolEntryHelper(PooledObject & data_) : data(data_) { data.in_use = true; }
		~PoolEntryHelper() { data.in_use = false; data.available.signal(); }

		PooledObject & data;
	};

public:
	/** То, что выдаётся пользователю. */
	class Entry
	{
	public:
		friend class PoolBase<Object>;

		Entry() {}	/// Для отложенной инициализации.

		/** Объект Entry защищает ресурс от использования другим потоком.
		 * Следующие методы запрещены для rvalue, чтобы нельзя было написать подобное
		 *
		 * auto q = pool.Get()->query("SELECT .."); // Упс, после этой строчки Entry уничтожился
		 * q.execute();  // Кто-то еще может использовать этот Connection
		 */
		Object * operator->() && = delete;
		const Object * operator->() const && = delete;
		Object & operator*() && = delete;
		const Object & operator*() const && = delete;

		Object * operator->() &			{ return &*data->data.object; }
		const Object * operator->() const &	{ return &*data->data.object; }
		Object & operator*() &				{ return *data->data.object; }
		const Object & operator*() const &	{ return *data->data.object; }

		bool isNull() const { return data.isNull(); }

	private:
		Poco::SharedPtr<PoolEntryHelper> data;

		Entry(PooledObject & object) : data(new PoolEntryHelper(object)) {}
	};

	virtual ~PoolBase() {}

	/** Выделяет объект для работы. При timeout < 0 таймаут бесконечный. */
	Entry get(Poco::Timespan::TimeDiff timeout)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		while (true)
		{
			for (typename Objects::iterator it = items.begin(); it != items.end(); it++)
				if (!(*it)->in_use)
					return Entry(**it);

			if (items.size() < max_items)
			{
				ObjectPtr object = allocObject();
				items.push_back(new PooledObject(available, object));
				return Entry(*items.back());
			}

			LOG_INFO(log, "No free connections in pool. Waiting.");

			if (timeout < 0)
				available.wait(mutex);
			else
				available.wait(mutex, timeout);
		}
	}

	void reserve(size_t count)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		while (items.size() < count)
			items.push_back(new PooledObject(available, allocObject()));
	}

private:
	/** Максимальный размер пула. */
	unsigned max_items;

	/** Пул. */
	Objects items;

	/** Блокировка для доступа к пулу. */
	Poco::FastMutex mutex;
	Poco::Condition available;

protected:

	Logger * log;

	PoolBase(unsigned max_items_, Logger * log_)
	   : max_items(max_items_), log(log_)
	{
		items.reserve(max_items);
	}

	/** Создает новый объект для помещения в пул. */
	virtual ObjectPtr allocObject() = 0;
};

