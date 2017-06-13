#pragma once

#include <string>
#include <memory>
#include <unordered_map>
#include <common/singleton.h>
#include <Common/Exception.h>


namespace DB
{

class Context;
class IFunction;
using FunctionPtr = std::shared_ptr<IFunction>;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


/** Creates function by name.
  * Function could use for initialization (take ownership of shared_ptr, for example)
  *  some dictionaries from Context.
  */
class FunctionFactory : public Singleton<FunctionFactory>
{
    friend class StorageSystemFunctions;

private:
    using Creator = FunctionPtr(*)(const Context & context);    /// Not std::function, for lower object size and less indirection.
    std::unordered_map<std::string, Creator> functions;

public:
    FunctionFactory();

    FunctionPtr get(const std::string & name, const Context & context) const;    /// Throws an exception if not found.
    FunctionPtr tryGet(const std::string & name, const Context & context) const; /// Returns nullptr if not found.

    /// No locking, you must register all functions before usage of get, tryGet.
    template <typename Function> void registerFunction()
    {
        static_assert(std::is_same<decltype(&Function::create), Creator>::value, "Function::create has incorrect type");

        if (!functions.emplace(std::string(Function::name), &Function::create).second)
            throw Exception("FunctionFactory: the function name '" + std::string(Function::name) + "' is not unique",
                ErrorCodes::LOGICAL_ERROR);
    }
};

}
