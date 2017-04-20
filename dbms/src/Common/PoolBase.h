#pragma once

#include <mutex>
#include <condition_variable>
#include <Poco/Timespan.h>
#include <boost/noncopyable.hpp>

#include <common/logger_useful.h>
#include <Common/Exception.h>

/** Класс, от которого можно унаследоваться и получить пул чего-нибудь. Используется для пулов соединений с БД.
  * Наследник должен предоставить метод для создания нового объекта для помещения в пул.
  */

template <typename TObject>
class PoolBase : private boost::noncopyable
{
public:
    using Object = TObject;
    using ObjectPtr = std::shared_ptr<Object>;
    using Ptr = std::shared_ptr<PoolBase<TObject>>;

private:

    /** Объект с флагом, используется ли он сейчас. */
    struct PooledObject
    {
        PooledObject(ObjectPtr object_, PoolBase & pool_)
            : object(object_), pool(pool_)
        {
        }

        ObjectPtr object;
        bool in_use = false;
        PoolBase & pool;
    };

    using Objects = std::vector<std::shared_ptr<PooledObject>>;

    /** Помощник, который устанавливает флаг использования объекта, а в деструкторе - снимает,
      *  а также уведомляет о событии с помощью condvar-а.
      */
    struct PoolEntryHelper
    {
        PoolEntryHelper(PooledObject & data_) : data(data_) { data.in_use = true; }
        ~PoolEntryHelper()
        {
            std::unique_lock<std::mutex> lock(data.pool.mutex);
            data.in_use = false;
            data.pool.available.notify_one();
        }

        PooledObject & data;
    };

public:
    /** То, что выдаётся пользователю. */
    class Entry
    {
    public:
        friend class PoolBase<Object>;

        Entry() {}    /// Для отложенной инициализации.

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

        Object * operator->() &            { return &*data->data.object; }
        const Object * operator->() const &    { return &*data->data.object; }
        Object & operator*() &                { return *data->data.object; }
        const Object & operator*() const &    { return *data->data.object; }

        bool isNull() const { return data == nullptr; }

        PoolBase * getPool() const
        {
            if (!data)
                throw DB::Exception("attempt to get pool from uninitialized entry");
            return &data->data.pool;
        }

    private:
        std::shared_ptr<PoolEntryHelper> data;

        Entry(PooledObject & object) : data(std::make_shared<PoolEntryHelper>(object)) {}
    };

    virtual ~PoolBase() {}

    /** Выделяет объект для работы. При timeout < 0 таймаут бесконечный. */
    Entry get(Poco::Timespan::TimeDiff timeout)
    {
        std::unique_lock<std::mutex> lock(mutex);

        while (true)
        {
            for (auto & item : items)
                if (!item->in_use)
                    return Entry(*item);

            if (items.size() < max_items)
            {
                ObjectPtr object = allocObject();
                items.emplace_back(std::make_shared<PooledObject>(object, *this));
                return Entry(*items.back());
            }

            LOG_INFO(log, "No free connections in pool. Waiting.");

            if (timeout < 0)
                available.wait(lock);
            else
                available.wait_for(lock, std::chrono::microseconds(timeout));
        }
    }

    void reserve(size_t count)
    {
        std::lock_guard<std::mutex> lock(mutex);

        while (items.size() < count)
            items.emplace_back(std::make_shared<PooledObject>(allocObject(), *this));
    }

private:
    /** Максимальный размер пула. */
    unsigned max_items;

    /** Пул. */
    Objects items;

    /** Блокировка для доступа к пулу. */
    std::mutex mutex;
    std::condition_variable available;

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

