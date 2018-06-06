#pragma once

#include <functional>
#include <memory>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
    #include <Poco/Data/SessionPool.h>
#pragma GCC diagnostic pop


using PocoSessionPoolConstructor = std::function<std::shared_ptr<Poco::Data::SessionPool>()>;

/** Is used to adjust max size of default Poco thread pool. See issue #750
  * Acquire the lock, resize pool and construct new Session.
  */
std::shared_ptr<Poco::Data::SessionPool> createAndCheckResizePocoSessionPool(PocoSessionPoolConstructor pool_constr);
