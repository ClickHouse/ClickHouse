#include <Dictionaries/InvalidateQueryResponse.h>

namespace DB
{

InvalidateQueryResponse::InvalidateQueryResponse(const InvalidateQueryResponse & other)
{
    std::lock_guard lock(other.mutex);
    response = other.response;
}

bool InvalidateQueryResponse::updateAndCheckModified(const std::string & new_response)
{
    std::lock_guard lock(mutex);
    if (response == new_response)
        return false;
    response = new_response;
    return true;
}

}
