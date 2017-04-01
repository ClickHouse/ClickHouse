#pragma once

#include <string>
#include <memory>
#include <unordered_map>
#include <common/singleton.h>


namespace DB
{

class Context;
class IFunction;
using FunctionPtr = std::shared_ptr<IFunction>;


/** Creates function by name.
  * Function could use for initialization (take ownership of shared_ptr, for example)
  *  some dictionaries from Context.
  */
class FunctionFactory : public Singleton<FunctionFactory>
{
    friend class StorageSystemFunctions;

private:
    typedef FunctionPtr (*Creator)(const Context & context);    /// Not std::function, for lower object size and less indirection.
    std::unordered_map<std::string, Creator> functions;

public:
    FunctionFactory();

    FunctionPtr get(const std::string & name, const Context & context) const;    /// Throws an exception if not found.
    FunctionPtr tryGet(const std::string & name, const Context & context) const;    /// Returns nullptr if not found.

    /// No locking, you must register all functions before usage of get, tryGet.
    template <typename F> void registerFunction()
    {
        static_assert(std::is_same<decltype(&F::create), Creator>::value, "F::create has incorrect type");
        functions[F::name] = &F::create;
    }
};

}
