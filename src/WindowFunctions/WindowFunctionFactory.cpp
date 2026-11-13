#include <WindowFunctions/WindowFunctionFactory.h>

#include <Interpreters/Context.h>

#include <DataTypes/DataTypeLowCardinality.h>

#include <Core/Settings.h>

#include <Common/CurrentThread.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool log_queries;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_FUNCTION;
}

namespace
{

ColumnsWithTypeAndName convertLowCardinalityTypesToNested(ColumnsWithTypeAndName arguments)
{
    for (auto & argument : arguments)
    {
        argument.type = recursiveRemoveLowCardinality(argument.type);
        if (argument.column)
            argument.column = recursiveRemoveLowCardinality(argument.column);
    }

    return arguments;
}

void addToQueryLog(const String & name)
{
    auto query_context = CurrentThread::tryGetQueryContext();
    if (query_context && query_context->getSettingsRef()[Setting::log_queries])
        query_context->addQueryFactoriesInfo(Context::QueryLogFactories::Function, name);
}

}

WindowFunctionFactory & WindowFunctionFactory::instance()
{
    static WindowFunctionFactory factory;
    return factory;
}

void WindowFunctionFactory::registerFunction(const String & name, Value registration, Case case_sensitiveness)
{
    if (!registration.creator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "WindowFunctionFactory: the window function {} has been provided a null creator", name);

    if (!functions.emplace(name, registration).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "WindowFunctionFactory: the window function name '{}' is not unique", name);

    if (case_sensitiveness == Case::Insensitive)
    {
        auto key = Poco::toLower(name);
        if (!case_insensitive_functions.emplace(key, registration).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "WindowFunctionFactory: the case insensitive window function name '{}' is not unique", name);

        case_insensitive_name_mapping[key] = name;
    }
}

bool WindowFunctionFactory::isWindowFunctionName(const String & name) const
{
    const String resolved_name = getAliasToOrName(name);
    return functions.contains(resolved_name) || case_insensitive_functions.contains(Poco::toLower(resolved_name));
}

DataTypes WindowFunctionFactory::bindArgumentTypes(const String & name, DataTypes argument_types) const
{
    const Value & registration = find(name);
    if (!registration.binder)
        return argument_types;

    return registration.binder(std::move(argument_types));
}

GeneralWindowFunctionPtr WindowFunctionFactory::get(const String & name, const ColumnsWithTypeAndName & arguments, const Array & parameters, const Settings & settings) const
{
    const Value & registration = find(name);
    const String resolved_name = getAliasToOrName(name);
    addToQueryLog(resolved_name);
    return registration.creator(resolved_name, convertLowCardinalityTypesToNested(arguments), parameters, settings);
}

FunctionDocumentation WindowFunctionFactory::getDocumentation(const String & name) const
{
    return find(name).documentation;
}

const WindowFunctionFactory::Value & WindowFunctionFactory::find(const String & name) const
{
    const String resolved_name = getAliasToOrName(name);
    if (auto it = functions.find(resolved_name); it != functions.end())
        return it->second;

    const String lowercase_name = Poco::toLower(resolved_name);
    if (auto it = case_insensitive_functions.find(lowercase_name); it != case_insensitive_functions.end())
        return it->second;

    throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unknown window function {}", name);
}

const WindowFunctionFactory::Functions & WindowFunctionFactory::getMap() const
{
    return functions;
}

const WindowFunctionFactory::Functions & WindowFunctionFactory::getCaseInsensitiveMap() const
{
    return case_insensitive_functions;
}

String WindowFunctionFactory::getFactoryName() const
{
    return "WindowFunctionFactory";
}

}
