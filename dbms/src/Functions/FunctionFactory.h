#pragma once

#include <string>
#include <memory>
#include <unordered_map>
#include <ext/singleton.h>

#include <Common/Exception.h>
#include <Core/Types.h>


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
class FunctionFactory : public ext::singleton<FunctionFactory>
{
    friend class StorageSystemFunctions;

private:
    using Creator = FunctionPtr(*)(const Context & context);    /// Not std::function, for lower object size and less indirection.
    using Functions = std::unordered_map<String, Creator>;

    Functions functions;
    Functions case_insensitive_functions;

    /// For compatibility with SQL, it's possible to specify that certain function name is case insensitive.
    enum CaseSensitiveness
    {
        CaseSensitive,
        CaseInsensitive
    };

public:
    FunctionPtr get(const String & name, const Context & context) const;    /// Throws an exception if not found.
    FunctionPtr tryGet(const String & name, const Context & context) const; /// Returns nullptr if not found.

    /// No locking, you must register all functions before usage of get, tryGet.
    void registerFunction(const String & name, Creator creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

    template <typename Function>
    void registerFunction()
    {
        registerFunction(String(Function::name), &Function::create);
    }
};

}
