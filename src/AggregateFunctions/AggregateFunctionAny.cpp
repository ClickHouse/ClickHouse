#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/HelpersMinMaxAny.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/defines.h>


namespace DB
{
struct Settings;
//
//AggregateFunctionPtr createAggregateFunctionAny(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
//{
//    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyData>(name, argument_types, parameters, settings));
//}
//
//
//AggregateFunctionPtr createAggregateFunctionAnyLast(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
//{
//    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyLastData>(name, argument_types, parameters, settings));
//}
//
//
//AggregateFunctionPtr createAggregateFunctionAnyHeavy(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
//{
//    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionAnyHeavyData>(name, argument_types, parameters, settings));
//}
//
//}
//
void registerAggregateFunctionsAny(AggregateFunctionFactory &)
{
    //    AggregateFunctionProperties default_properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    //    AggregateFunctionProperties default_properties_for_respect_nulls
    //        = {.returns_default_when_only_null = false, .is_order_dependent = true, .is_window_function = true};
    //
    //    factory.registerFunction("any", {createAggregateFunctionAny, default_properties});
    //    factory.registerAlias("any_value", "any", AggregateFunctionFactory::CaseInsensitive);
    //    factory.registerAlias("first_value", "any", AggregateFunctionFactory::CaseInsensitive);
    ////
    //    factory.registerFunction("anyLast", {createAggregateFunctionAnyLast, default_properties});
    //    factory.registerAlias("last_value", "anyLast", AggregateFunctionFactory::CaseInsensitive);
    ////
    //    factory.registerFunction("anyHeavy", {createAggregateFunctionAnyHeavy, default_properties});
    //
    //    factory.registerNullsActionTransformation("any", "any_respect_nulls");
    //    factory.registerNullsActionTransformation("anyLast", "anyLast_respect_nulls");
}

}
