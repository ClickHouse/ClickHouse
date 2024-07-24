#pragma once

#include <Common/FunctionDocumentation.h>
#include <Common/IFactoryWithAliases.h>
#include <Common/register_objects.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/Context_fwd.h>

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>


namespace DB
{

using FunctionCreator = std::function<FunctionOverloadResolverPtr(ContextPtr)>;
using FunctionSimpleCreator = std::function<FunctionPtr(ContextPtr)>;

struct FunctionFactoryData
{
    FunctionCreator creator;
    FunctionDocumentation documentation;
    FunctionProperties properties;
};

/** Creates function by name.
  * The provided Context is guaranteed to outlive the created function. Functions may use it for
  * things like settings, current database, permission checks, etc.
  */
class FunctionFactory : private boost::noncopyable, public IFactoryWithAliases<FunctionFactoryData>
{
    using Base = IFactoryWithAliases<FunctionFactoryData>;
public:
    static FunctionFactory & instance();

    template <typename Function>
    void registerFunction(FunctionDocumentation documentation = {}, FunctionProperties properties = {}, Case case_sensitiveness = Case::Sensitive)
    {
        registerFunction<Function>(Function::name, std::move(documentation), std::move(properties), case_sensitiveness);
    }

    /// Register a function by its name.
    /// No locking, you must register all functions before usage of get.
    void registerFunction(
        const std::string & name,
        FunctionCreator creator,
        FunctionDocumentation documentation = {},
        FunctionProperties properties = {},
        Case case_sensitiveness = Case::Sensitive);

    void registerFunction(
        const std::string & name,
        FunctionSimpleCreator creator,
        FunctionDocumentation documentation = {},
        FunctionProperties properties = {},
        Case case_sensitiveness = Case::Sensitive);

    bool has(const std::string & name) const;

    /// Throws an exception if not found.
    FunctionOverloadResolverPtr get(const std::string & name, ContextPtr context) const;

    /// Returns nullptr if not found.
    FunctionOverloadResolverPtr tryGet(const std::string & name, ContextPtr context) const;

    /// The same methods to get developer interface implementation.
    FunctionOverloadResolverPtr getImpl(const std::string & name, ContextPtr context) const;
    FunctionOverloadResolverPtr tryGetImpl(const std::string & name, ContextPtr context) const;

    /// This function is used by YQL - innovative transactional DBMS that depends on ClickHouse by source code.
    std::vector<std::string> getAllNames() const;

    FunctionDocumentation getDocumentation(const std::string & name) const;
    FunctionProperties getProperties(const std::string & name) const;

    std::optional<FunctionDocumentation> tryGetDocumentation(const std::string & name) const;
    std::optional<FunctionProperties> tryGetProperties(const std::string & name) const;

private:
    using Functions = std::unordered_map<std::string, Value>;

    Functions functions;
    Functions case_insensitive_functions;

    const Base::OriginalNameMap & getOriginalNameMap() const override { return functions; }
    const Base::OriginalNameMap & getOriginalCaseInsensitiveNameMap() const override { return case_insensitive_functions; }

    String getFactoryName() const override { return "FunctionFactory"; }

    template <typename Function>
    void registerFunction(const std::string & name, FunctionDocumentation documentation = {}, FunctionProperties properties = {}, Case case_sensitiveness = Case::Sensitive)
    {
        registerFunction(name, &Function::create, std::move(documentation), std::move(properties), case_sensitiveness);
    }

    std::optional<FunctionDocumentation> tryGetDocumentationImpl(const String & name_param) const;
    std::optional<FunctionProperties> tryGetPropertiesImpl(const String & name_param) const;
};

const String & getFunctionCanonicalNameIfAny(const String & name);

}
