#pragma once

#include <Functions/IFunction.h>

#include <ext/singleton.h>

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>


namespace DB
{

class Context;


/** Creates function by name.
  * Function could use for initialization (take ownership of shared_ptr, for example)
  *  some dictionaries from Context.
  */
class FunctionFactory : public ext::singleton<FunctionFactory>
{
    friend class StorageSystemFunctions;

public:
    using Creator = std::function<FunctionPtr(const Context &)>;

    /// For compatibility with SQL, it's possible to specify that certain function name is case insensitive.
    enum CaseSensitiveness
    {
        CaseSensitive,
        CaseInsensitive
    };

    /// Register a function by its name.
    /// No locking, you must register all functions before usage of get.
    void registerFunction(
        const std::string & name,
        Creator creator,
        CaseSensitiveness case_sensitiveness = CaseSensitive);

    template <typename Function>
    void registerFunction()
    {
        registerFunction(Function::name, &Function::create);
    }

    /// Throws an exception if not found.
    FunctionPtr get(const std::string & name, const Context & context) const;

    /// Returns nullptr if not found.
    FunctionPtr tryGet(const std::string & name, const Context & context) const;

private:
    using Functions = std::unordered_map<std::string, Creator>;

    Functions functions;
    Functions case_insensitive_functions;
};

}
