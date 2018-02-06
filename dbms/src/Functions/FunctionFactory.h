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
    using Creator = std::function<FunctionBuilderPtr(const Context &)>;

    /// For compatibility with SQL, it's possible to specify that certain function name is case insensitive.
    enum CaseSensitiveness
    {
        CaseSensitive,
        CaseInsensitive
    };

    template <typename Function>
    void registerFunction(CaseSensitiveness case_sensitiveness = CaseSensitive)
    {
        registerFunction<Function>(Function::name, case_sensitiveness);
    }

    template <typename Function>
    void registerFunction(const std::string & name, CaseSensitiveness case_sensitiveness = CaseSensitive)
    {
        if constexpr (std::is_base_of<IFunction, Function>::value)
            registerFunction(name, &createDefaultFunction<Function>, case_sensitiveness);
        else
            registerFunction(name, &Function::create, case_sensitiveness);
    }

    /// Throws an exception if not found.
    FunctionBuilderPtr get(const std::string & name, const Context & context) const;

    /// Returns nullptr if not found.
    FunctionBuilderPtr tryGet(const std::string & name, const Context & context) const;

private:
    using Functions = std::unordered_map<std::string, Creator>;

    Functions functions;
    Functions case_insensitive_functions;

    template <typename Function>
    static FunctionBuilderPtr createDefaultFunction(const Context & context)
    {
        return std::make_shared<DefaultFunctionBuilder>(Function::create(context));
    }

    /// Register a function by its name.
    /// No locking, you must register all functions before usage of get.
    void registerFunction(
            const std::string & name,
            Creator creator,
            CaseSensitiveness case_sensitiveness = CaseSensitive);
};

}
