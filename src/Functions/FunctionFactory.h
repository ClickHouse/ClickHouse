#pragma once

#include <Interpreters/Context_fwd.h>
#include <Common/IFactoryWithAliases.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>


namespace DB
{

/** Creates function by name.
  * Function could use for initialization (take ownership of shared_ptr, for example)
  *  some dictionaries from Context.
  */
class FunctionFactory : private boost::noncopyable,
                        public IFactoryWithAliases<std::function<FunctionOverloadResolverPtr(ContextPtr)>>
{
public:
    static FunctionFactory & instance();

    template <typename Function>
    void registerFunction(CaseSensitiveness case_sensitiveness = CaseSensitive)
    {
        registerFunction<Function>(Function::name, case_sensitiveness);
    }

    template <typename Function>
    void registerFunction(const std::string & name, CaseSensitiveness case_sensitiveness = CaseSensitive)
    {

        if constexpr (std::is_base_of_v<IFunction, Function>)
            registerFunction(name, &adaptFunctionToOverloadResolver<Function>, case_sensitiveness);
        else
            registerFunction(name, &Function::create, case_sensitiveness);
    }

    /// This function is used by YQL - innovative transactional DBMS that depends on ClickHouse by source code.
    std::vector<std::string> getAllNames() const;

    bool has(const std::string & name) const;

    /// Throws an exception if not found.
    FunctionOverloadResolverPtr get(const std::string & name, ContextPtr context) const;

    /// Returns nullptr if not found.
    FunctionOverloadResolverPtr tryGet(const std::string & name, ContextPtr context) const;

    /// The same methods to get developer interface implementation.
    FunctionOverloadResolverPtr getImpl(const std::string & name, ContextPtr context) const;
    FunctionOverloadResolverPtr tryGetImpl(const std::string & name, ContextPtr context) const;

    /// Register a function by its name.
    /// No locking, you must register all functions before usage of get.
    void registerFunction(
        const std::string & name,
        Value creator,
        CaseSensitiveness case_sensitiveness = CaseSensitive);

private:
    using Functions = std::unordered_map<std::string, Value>;

    Functions functions;
    Functions case_insensitive_functions;

    template <typename Function>
    static FunctionOverloadResolverPtr adaptFunctionToOverloadResolver(ContextPtr context)
    {
        return std::make_unique<FunctionToOverloadResolverAdaptor>(Function::create(context));
    }

    const Functions & getMap() const override { return functions; }

    const Functions & getCaseInsensitiveMap() const override { return case_insensitive_functions; }

    String getFactoryName() const override { return "FunctionFactory"; }
};

}

typedef void (*func_entry)(DB::FunctionFactory &factory);

#define COLD_INIT __attribute((__section__(".init.text"))) __attribute__((__cold__))

#define FUNCTION_REGISTER(fn) \
static __attribute((__section__("func_list"))) __attribute__((used)) \
    func_entry fn_autocall_##fn = (fn);

#define FUNCTION_REGISTER_NAME(name, fn) \
static COLD_INIT void name(FunctionFactory & factory) \
{ factory.registerFunction<fn>(); } \
FUNCTION_REGISTER(name);

#define FUNCTION_REGISTER_NAME_WITH_ARGS(name, fn, ...) \
static COLD_INIT void name(FunctionFactory & factory) \
{ factory.registerFunction<fn>(__VA_ARGS__); } \
FUNCTION_REGISTER(name);

#define FUNC_CAT(a,b) FUNC_CAT_EXP(a,b)
#define FUNC_CAT_EXP(a,b) a##b
#define FUNC_UNIQUE_ID() FUNC_CAT(registerFunction, __COUNTER__)

#define FUNCTION_REGISTER_SIMPLE(fn) \
    FUNCTION_REGISTER_NAME(FUNC_UNIQUE_ID(), fn)

#define FUNCTION_REGISTER_SIMPLE_WITH_ARGS(fn, ...) \
    FUNCTION_REGISTER_NAME_WITH_ARGS(FUNC_UNIQUE_ID(), fn, __VA_ARGS__)
