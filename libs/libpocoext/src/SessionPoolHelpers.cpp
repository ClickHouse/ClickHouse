#include <mutex>
#include <Poco/ThreadPool.h>
#include <Poco/Ext/SessionPoolHelpers.h>


std::shared_ptr<Poco::Data::SessionPool> createAndCheckResizePocoSessionPool(PocoSessionPoolConstructor pool_constr)
{
    static std::mutex mutex;

    Poco::ThreadPool & pool = Poco::ThreadPool::defaultPool();

    /// NOTE: The lock don't guarantee that external users of the pool don't change its capacity
    std::unique_lock lock(mutex);

    if (pool.available() == 0)
        pool.addCapacity(2 * std::max(pool.capacity(), 1));

    return pool_constr();
}
