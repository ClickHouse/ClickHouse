#pragma once

#include <base/types.h>

namespace DB
{

class IInputFormatErrorsHandler
{
public:
    struct ErrorEntry
    {
        time_t time;
        size_t offset;
        String reason;
        String raw_data;
    };

    virtual ~IInputFormatErrorsHandler() = default;
    virtual void logError(ErrorEntry entry) = 0;
};
using IInputFormatErrorsHandlerPtr = std::shared_ptr<IInputFormatErrorsHandler>;

}
