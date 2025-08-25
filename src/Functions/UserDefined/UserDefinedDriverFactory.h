#pragma once

#include <Core/Field.h>
#include <Functions/UserDefined/UserDefinedDriver.h>
#include <Interpreters/Context.h>


namespace DB
{

class UserDefinedDriverFactory
{
public:
    static UserDefinedDriverFactory & instance();

    static UserDefinedDriverPtr get(const String & driver_name, ContextPtr context);

    static UserDefinedDriverPtr tryGet(const String & driver_name, ContextPtr context);

    static bool has(const String & driver_name, ContextPtr context);

    static std::vector<String> getRegisteredNames(ContextPtr context);

};

}
