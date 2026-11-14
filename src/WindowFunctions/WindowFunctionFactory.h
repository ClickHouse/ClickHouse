#pragma once

#include <WindowFunctions/IGeneralWindowFunction_fwd.h>

#include <DataTypes/IDataType_fwd.h>

#include <Common/FunctionDocumentation.h>
#include <Common/IFactoryWithAliases.h>

#include <boost/noncopyable.hpp>

#include <functional>
#include <unordered_map>

namespace DB
{

struct Array;
struct Settings;

using WindowFunctionCreator = std::function<GeneralWindowFunctionPtr(const String &, const DataTypes &, const Array &, const Settings *)>;
using WindowFunctionArgumentTypesBinder = std::function<DataTypes(DataTypes argument_types)>;

struct WindowFunctionRegistration
{
    WindowFunctionCreator creator;
    FunctionDocumentation documentation;
    WindowFunctionArgumentTypesBinder binder {};
};

class WindowFunctionFactory final : private boost::noncopyable, public IFactoryWithAliases<WindowFunctionRegistration>
{
    using Functions = std::unordered_map<String, Value>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    const Value & find(const String & name) const;
    const Functions & getMap() const override;
    const Functions & getCaseInsensitiveMap() const override;
    String getFactoryName() const override;

public:
    static WindowFunctionFactory & instance();

    void registerFunction(const String & name, Value registration, Case case_sensitiveness = Case::Sensitive);

    bool isWindowFunctionName(const String & name) const;
    DataTypes bindArgumentTypes(const String & name, DataTypes argument_types) const;
    GeneralWindowFunctionPtr get(const String & name, const DataTypes & argument_types, const Array & parameters) const;
    FunctionDocumentation getDocumentation(const String & name) const;

private:
    Functions functions;
    Functions case_insensitive_functions;
};

const String & getWindowFunctionCanonicalNameIfAny(const String & name);

}
