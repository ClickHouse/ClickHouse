#pragma once

#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/SecretArgumentsSpec.h>
#include <Common/register_objects.h>
#include <Common/IFactoryWithAliases.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/FunctionDocumentation.h>
#include <Functions/FunctionResultSetting.h>
#include <Functions/IFunction.h>

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include <boost/noncopyable.hpp>


namespace DB
{

using FunctionSimpleCreator = std::function<FunctionPtr(ContextPtr)>;
using FunctionFactoryData = std::pair<FunctionCreator, FunctionDocumentation>;

/** Creates function by name.
  * The provided Context is guaranteed to outlive the created function. Functions may use it for
  * things like settings, current database, permission checks, etc.
  */
class FunctionFactory : private boost::noncopyable, public IFactoryWithAliases<FunctionFactoryData>
{
public:
    static FunctionFactory & instance();

    template <typename Function>
    void registerFunction(FunctionDocumentation documentation, Case case_sensitiveness = Case::Sensitive)
    {
        registerFunction<Function>(Function::name, std::move(documentation), case_sensitiveness);
    }

    /// For the few functions with a secret argument (`encrypt`, `HMAC`, ...).
    template <typename Function>
    void registerFunction(FunctionDocumentation documentation, Case case_sensitiveness, SecretArgumentsSpec secret_arguments_)
    {
        registerFunction<Function>(Function::name, std::move(documentation), case_sensitiveness);
        secret_arguments.emplace(Function::name, std::move(secret_arguments_));
    }

    /// This function is used by YQL - innovative transactional DBMS that depends on ClickHouse by source code.
    [[nodiscard]] Strings getAllNames() const;

    [[nodiscard]] bool has(const std::string & name) const;

    /// Throws an exception if not found.
    [[nodiscard]] FunctionOverloadResolverPtr get(const std::string & name, ContextPtr context) const;

    /// Returns nullptr if not found.
    [[nodiscard]] FunctionOverloadResolverPtr tryGet(const std::string & name, ContextPtr context) const;

    /// The same methods to get developer interface implementation.
    [[nodiscard]] FunctionOverloadResolverPtr getImpl(const std::string & name, ContextPtr context) const;
    [[nodiscard]] FunctionOverloadResolverPtr tryGetImpl(const std::string & name, ContextPtr context) const;

    /// Register a function by its name.
    /// No locking, you must register all functions before usage of get.
    /// `result_settings`: see `FunctionResultSetting`. Their values are captured when the function is created and become part
    /// of its identity (`IFunctionBase::updateHash`), and storage keys use them to tell whether a session computes the values
    /// stored with the key. A function registered by its class declares them in `Function::result_settings` instead.
    void registerFunction(
        const std::string & name,
        FunctionCreator creator,
        FunctionDocumentation documentation,
        Case case_sensitiveness = Case::Sensitive,
        FunctionResultSettings result_settings_ = {});

    void registerFunction(
        const std::string & name,
        FunctionSimpleCreator creator,
        FunctionDocumentation documentation,
        Case case_sensitiveness = Case::Sensitive,
        FunctionResultSettings result_settings_ = {});

    /// The declared result settings of `name` (an alias or any case of a case-insensitive name), or nullptr.
    [[nodiscard]] const RegisteredResultSettings * tryGetResultSettings(const std::string & name) const;

    [[nodiscard]] FunctionDocumentation getDocumentation(const std::string & name) const;

    [[nodiscard]] const SecretArgumentsSpec * tryGetSecretArgumentsSpec(const std::string & name) const;
    [[nodiscard]] bool hasArgumentSecretsIgnoringCase(const std::string & name) const;

private:
    using Functions = std::unordered_map<std::string, Value>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    Functions functions;
    Functions case_insensitive_functions;
    std::unordered_map<std::string, SecretArgumentsSpec> secret_arguments; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::unordered_map<std::string, RegisteredResultSettings> result_settings; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    /// The same, by the creator of the function: `tryGetImpl` has it already, and looking up a name costs more.
    std::unordered_map<const FunctionCreator *, const RegisteredResultSettings *> result_settings_by_creator; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    [[nodiscard]] const Functions & getMap() const override { return functions; }

    [[nodiscard]] const Functions & getCaseInsensitiveMap() const override { return case_insensitive_functions; }

    [[nodiscard]] String getFactoryName() const override { return "FunctionFactory"; }

    template <typename Function>
    void registerFunction(const std::string & name, FunctionDocumentation documentation, Case case_sensitiveness = Case::Sensitive)
    {
        FunctionResultSettings function_result_settings;
        if constexpr (requires { Function::result_settings; })
            function_result_settings = Function::result_settings;
        registerFunction(name, &Function::create, std::move(documentation), case_sensitiveness, function_result_settings);
    }

    void registerResultSettings(const std::string & name, FunctionResultSettings settings);
};

const String & getFunctionCanonicalNameIfAny(const String & name);

}
