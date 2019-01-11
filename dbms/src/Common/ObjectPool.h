#pragma once

#include <map>
#include <memory>
#include <stack>
#include <mutex>

#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int POOL_SIZE_EXCEEDED;
}


/** Pool for objects that cannot be used from different threads simultaneously.
  * Allows to create an object for each thread.
  *
  * Use it in cases when thread local storage is not appropriate
  *  (when maximum number of simultaneously used objects is less
  *   than number of running/sleeping threads, that has ever used object,
  *   and creation/destruction of objects is expensive).
  */
template <typename T>
class SimpleObjectPool
{
protected:

    /// Hold all avaiable objects in stack.
    std::mutex mutex;
    std::stack<std::unique_ptr<T>> stack;

    /// Maximum pool size. Zero means unlimited. When exceeded, an exception is thrown.
    size_t max_objects = 0;
    /// Maximum number of unused objects kept in the pool. Zero means unlimited. When exceeded, all excessive objects are destroyed.
    size_t max_free_objects = 0;

    /// Total objects in pool: free and in use.
    size_t size = 0;

    /// Specialized deleter for std::unique_ptr.
    /// Returns underlying pointer back to stack thus reclaiming its ownership.
    struct Deleter
    {
        SimpleObjectPool<T> * parent;

        Deleter(SimpleObjectPool<T> * parent_ = nullptr) : parent{parent_} {}

        void operator()(T * owning_ptr) const
        {
            std::lock_guard lock{parent->mutex};
            parent->stack.emplace(owning_ptr);

            if (parent->max_free_objects && parent->stack.size() > parent->max_free_objects)
            {
                while (parent->stack.size() > parent->max_free_objects)
                {
                    parent->stack.pop();
                    --parent->size;
                }
            }
        }
    };

public:
    SimpleObjectPool() {};

    SimpleObjectPool(size_t max_objects, size_t max_free_objects)
        : max_objects(max_objects), max_free_objects(max_free_objects) {};

    void setLimits(size_t max_objects_, size_t max_free_objects_)
    {
        std::lock_guard lock(mutex);
        max_objects = max_objects_;
        max_free_objects = max_free_objects_;
    }

    using Pointer = std::unique_ptr<T, Deleter>;

    /// Extracts and returns a pointer from the stack if it's not empty,
    ///  creates a new one by calling provided f() otherwise.
    template <typename Factory>
    Pointer get(Factory && f)
    {
        std::unique_lock lock(mutex);

        if (stack.empty())
        {
            if (max_objects && size >= max_objects)
                throw Exception("SimpleObjectPool size exceeded", ErrorCodes::POOL_SIZE_EXCEEDED);
            ++size;

            lock.unlock();
            try
            {
                return { f(), this };
            }
            catch (...)
            {
                {
                    std::unique_lock lock(mutex);
                    --size;
                }
                throw;
            }
        }

        auto object = stack.top().release();
        stack.pop();

        return { object, this };
    }

    /// Like get(), but creates object using default constructor.
    Pointer getDefault()
    {
        return get([] { return new T; });
    }
};


/// Like SimpleObjectPool, but additionally allows store different kind of objects that are identified by Key
template <typename T, typename Key>
class ObjectPoolMap
{
private:

    using Object = SimpleObjectPool<T>;

    /// Key -> objects
    using Container = std::map<Key, std::unique_ptr<Object>>;

    Container container;
    std::mutex mutex;

public:

    using Pointer = typename Object::Pointer;

    template <typename Factory>
    Pointer get(const Key & key, Factory && f)
    {
        std::unique_lock lock(mutex);

        auto it = container.find(key);
        if (container.end() == it)
            it = container.emplace(key, std::make_unique<Object>()).first;

        return it->second->get(std::forward<Factory>(f));
    }
};


}
