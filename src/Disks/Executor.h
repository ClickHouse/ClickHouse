#pragma once

#include "future"
#include "functional"

namespace DB
{

/// Interface to run task asynchronously with possibility to wait for execution.
class Executor
{
public:
    virtual ~Executor() = default;
    virtual std::future<void> execute(std::function<void()> task) = 0;
};

}
