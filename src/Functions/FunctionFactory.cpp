#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>

#include <Functions/DateTimeTransforms.h>

#include <Interpreters/Context.h>

#include <Common/Exception.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/FieldVisitorHash.h>
#include <Common/SipHash.h>
#include <Core/Settings.h>

#include <Poco/String.h>

#include <IO/WriteHelpers.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool log_queries;
    extern const SettingsBool use_legacy_to_time;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_FUNCTION;
    extern const int LOGICAL_ERROR;
}

const String & getFunctionCanonicalNameIfAny(const String & name)
{
    return FunctionFactory::instance().getCanonicalNameIfAny(name);
}

void FunctionFactory::registerFunction(
    const std::string & name,
    FunctionCreator creator,
    FunctionDocumentation documentation,
    Case case_sensitiveness)
{
    if (!functions.emplace(name, FunctionFactoryData{creator, documentation}).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FunctionFactory: the function name '{}' is not unique", name);

    String function_name_lowercase = Poco::toLower(name);
    if (isAlias(name) || isAlias(function_name_lowercase))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FunctionFactory: the function name '{}' is already registered as alias",
                        name);

    if (case_sensitiveness == Case::Insensitive)
    {
        if (!case_insensitive_functions.emplace(function_name_lowercase, FunctionFactoryData{creator, documentation}).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "FunctionFactory: the case insensitive function name '{}' is not unique",
                name);
        case_insensitive_name_mapping[function_name_lowercase] = name;
    }
}

void FunctionFactory::registerFunction(
    const std::string & name,
    FunctionSimpleCreator creator,
    FunctionDocumentation documentation,
    Case case_sensitiveness)
{
    registerFunction(name, [my_creator = std::move(creator)](ContextPtr context)
    {
        return std::make_unique<FunctionToOverloadResolverAdaptor>(my_creator(context));
    }, std::move(documentation), std::move(case_sensitiveness));
}


const SecretArgumentsSpec * FunctionFactory::tryGetSecretArgumentsSpec(const std::string & name) const
{
    auto it = secret_arguments.find(getCanonicalNameIfAny(getAliasToOrName(name)));
    return it == secret_arguments.end() ? nullptr : &it->second;
}

void FunctionFactory::registerResultSettings(const std::string & name, Names settings)
{
    auto function = functions.find(name);
    if (function == functions.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FunctionFactory: result settings of the unknown function '{}'", name);

    ResultSettings entry;
    for (const auto & setting : settings)
    {
        if (!Settings::hasBuiltin(setting))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "FunctionFactory: unknown result setting '{}' of the function '{}'", setting, name);
        entry.indices.push_back(Settings::getIndexOfBuiltin(setting));
    }
    entry.names = std::move(settings);

    auto [it, inserted] = result_settings.emplace(name, std::move(entry));
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FunctionFactory: result settings of the function '{}' are already registered", name);
    result_settings_by_creator.emplace(&function->second.first, &it->second);
}

const Names * FunctionFactory::tryGetResultSettings(const std::string & name) const
{
    auto it = result_settings.find(getCanonicalNameIfAny(getAliasToOrName(name)));
    return it == result_settings.end() ? nullptr : &it->second.names;
}

ContextPtr FunctionFactory::withGlobalResultSettings(const ContextPtr & context) const
{
    const auto & settings = context->getSettingsRef();
    const auto & global_settings = context->getGlobalContext()->getSettingsRef();
    ContextMutablePtr result;
    for (const auto & [_, entry] : result_settings)
    {
        for (const auto & name : entry.names)
        {
            Field global_value = global_settings.get(name);
            if (settings.get(name) == global_value)
                continue;
            if (!result)
                result = Context::createCopy(context);
            result->setSetting(name, global_value);
        }
    }
    return result ? result : context;
}

bool FunctionFactory::hasArgumentSecretsIgnoringCase(const std::string & name) const
{
    return std::ranges::any_of(secret_arguments, [&](const auto & entry)
        { return Poco::icompare(entry.first, name) == 0 && entry.second.hasArgumentSecrets(); });
}

FunctionOverloadResolverPtr FunctionFactory::getImpl(
    const std::string & name,
    ContextPtr context) const
{
    auto res = tryGetImpl(name, context);
    if (!res)
    {
        String extra_info;
        if (AggregateFunctionFactory::instance().hasNameOrAlias(name))
            extra_info = ". There is an aggregate function with the same name, but ordinary function is expected here";

        auto hints = this->getHints(name);
        if (!hints.empty())
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unknown function {}{}. Maybe you meant: {}", name, extra_info, toString(hints));
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unknown function {}{}", name, extra_info);
    }

    return res;
}

Strings FunctionFactory::getAllNames() const
{
    Strings res;
    res.reserve(functions.size());
    for (const auto & func : functions)
        res.emplace_back(func.first);
    return res;
}

FunctionOverloadResolverPtr FunctionFactory::get(
    const std::string & name,
    ContextPtr context) const
{
    return getImpl(name, context);
}

bool FunctionFactory::has(const std::string & name) const
{
    String canonical_name = getAliasToOrName(name);
    if (functions.contains(canonical_name))
        return true;
    canonical_name = Poco::toLower(canonical_name);
    return case_insensitive_functions.contains(canonical_name);
}

FunctionOverloadResolverPtr FunctionFactory::tryGetImpl(
    const std::string & name_param,
    ContextPtr context) const
{
    String name = getAliasToOrName(name_param);
    FunctionOverloadResolverPtr res;
    const FunctionCreator * canonical_creator = nullptr;

    auto it = functions.find(name);
    if (functions.end() != it)
    {
        res = it->second.first(context);
        canonical_creator = &it->second.first;
    }
    else
    {
        name = Poco::toLower(name);
        it = case_insensitive_functions.find(name);
        if (case_insensitive_functions.end() != it)
        {
            res = it->second.first(context);
            auto cn_it = case_insensitive_name_mapping.find(name);
            if (cn_it != case_insensitive_name_mapping.end())
            {
                auto fn_it = functions.find(cn_it->second);
                if (fn_it != functions.end())
                    canonical_creator = &fn_it->second.first;
            }
        }
    }

    if (!res)
        return nullptr;

    if (CurrentThread::isInitialized())
    {
        auto query_context = CurrentThread::get().tryGetQueryContext();
        if (query_context && query_context->getSettingsRef()[Setting::log_queries])
            query_context->addQueryFactoriesInfo(Context::QueryLogFactories::Function, name);

        /// There is a legacy toTime function that has the same name as toTime function for Time data type, so we need to
        /// check this setting here and decide if we need to change the function to get
        if (query_context && Poco::toLower(name) == "totime" && query_context->getSettingsRef()[Setting::use_legacy_to_time])
        {
            it = functions.find(ToTimeWithFixedDateImpl::name);
            if (functions.end() != it)
            {
                res = it->second.first(context);
                canonical_creator = &it->second.first;
            }
        }
    }

    if (canonical_creator)
    {
        res->setFactoryHandle(canonical_creator);

        if (auto settings_it = result_settings_by_creator.find(canonical_creator); settings_it != result_settings_by_creator.end())
        {
            /// A function created without a context uses the defaults of the settings.
            static const Settings default_settings;
            const auto & settings = context ? context->getSettingsRef() : default_settings;
            SipHash hash;
            for (size_t index : settings_it->second->indices)
                applyVisitor(FieldVisitorHash(hash), settings.getByIndex(index));
            res->setResultSettingsHash(hash.get128());
        }
    }

    return res;
}

FunctionOverloadResolverPtr FunctionFactory::tryGet(
    const std::string & name,
    ContextPtr context) const
{
    auto impl = tryGetImpl(name, context);
    return impl ? std::move(impl) : nullptr;
}

FunctionFactory & FunctionFactory::instance()
{
    static FunctionFactory ret;
    return ret;
}

FunctionDocumentation FunctionFactory::getDocumentation(const std::string & name) const
{
    auto it = functions.find(name);
    if (it == functions.end())
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unknown function {}", name);

    return it->second.second;
}

}
