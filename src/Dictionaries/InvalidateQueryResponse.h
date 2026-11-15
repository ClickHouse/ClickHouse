#pragma once

#include <mutex>
#include <string>

namespace DB
{

/// Thread-safe holder for the last value returned by a dictionary source's invalidate query.
class InvalidateQueryResponse
{
public:
    InvalidateQueryResponse() = default;
    InvalidateQueryResponse(const InvalidateQueryResponse & other);
    InvalidateQueryResponse & operator=(const InvalidateQueryResponse &) = delete;

    bool updateAndCheckModified(const std::string & new_response);

private:
    mutable std::mutex mutex;
    std::string response;
};

}
