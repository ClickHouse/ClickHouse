#pragma once

#include <memory>
#include <vector>

namespace DB
{

class IConnectionPool;
using ConnectionPoolPtr = std::shared_ptr<IConnectionPool>;
using ConnectionPoolPtrs = std::vector<ConnectionPoolPtr>;

class ConnectionPoolWithFailover;
using ConnectionPoolWithFailoverPtr = std::shared_ptr<ConnectionPoolWithFailover>;
using ConnectionPoolWithFailoverPtrs = std::vector<ConnectionPoolWithFailoverPtr>;

}
