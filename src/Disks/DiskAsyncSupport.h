#pragma once

#include "future"

namespace DB
{

class DiskAsyncSupport
{
public:
    virtual ~DiskAsyncSupport() = default;
    virtual std::future<void> runAsync(std::function<void()> task) = 0;
};

}
