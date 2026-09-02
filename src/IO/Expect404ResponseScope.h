#pragma once

#include <thread>

namespace DB
{

// Inside the inner scope
// Remote storage response with PATH_NOT_FOUND error is considered as expected
// No error logs are written, no profile events about errors are incremented
class Expect404ResponseScope
{
public:
    Expect404ResponseScope();
    ~Expect404ResponseScope();

    static bool is404Expected();

private:
    std::thread::id initial_thread_id;
};

}
