#pragma once

#include <boost/noncopyable.hpp>

namespace DB
{

/// Inside the scope a response of remote storage with a `PATH_NOT_FOUND` error is expected:
/// no error logs are written and no profile events about errors are incremented.
/// The scope belongs to the current thread or fiber.
class Expect404ResponseScope : private boost::noncopyable
{
public:
    Expect404ResponseScope();
    ~Expect404ResponseScope();

    static bool is404Expected();
};

}
