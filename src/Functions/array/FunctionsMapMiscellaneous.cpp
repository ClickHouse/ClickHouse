#include <Columns/ColumnArray.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnConst.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>

#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/like.h>
#include <Functions/array/arrayConcat.h>
#include <Functions/array/arrayFilter.h>
#include <Functions/array/arrayMap.h>
#include <Functions/array/arraySort.h>
#include <Functions/array/arrayIndex.h>
#include <Functions/array/arrayExists.h>
#include <Functions/array/arrayAll.h>
#include <Functions/identity.h>
#include <Functions/FunctionFactory.h>


#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

/** An adapter that allows to execute array* functions over Map types arguments.
  * E.g. transform mapConcat to arrayConcat.
  *
  * Impl - the implementation of function that is applied
  * to internal column of Map arguments (e.g. 'arrayConcat').
  *
  * Adapter - a struct that determines the way how to extract the internal array columns
  * from Map arguments and possibly modify other columns.
*/
template <typename Impl, typename Adapter, typename Name>
class FunctionMapToArrayAdapter : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapToArrayAdapter>(); }
    String getName() const override { return name; }

    bool isVariadic() const override { return impl.isVariadic(); }
    size_t getNumberOfArguments() const override { return impl.getNumberOfArguments(); }
    bool useDefaultImplementationForNulls() const override { return impl.useDefaultImplementationForNulls(); }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return impl.useDefaultImplementationForLowCardinalityColumns(); }
    bool useDefaultImplementationForConstants() const override { return impl.useDefaultImplementationForConstants(); }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override  { return false; }

    void getLambdaArgumentTypes(DataTypes & arguments) const override
    {
        Adapter::extractNestedTypes(arguments);
        impl.getLambdaArgumentTypes(arguments);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Function {} requires at least one argument, passed {}", getName(), arguments.size());

        auto nested_arguments = arguments;
        Adapter::extractNestedTypesAndColumns(nested_arguments);

        constexpr bool impl_has_get_return_type = requires
        {
            impl.getReturnTypeImpl(nested_arguments);
        };

        DataTypePtr nested_type;
        /// If method is not overloaded in the implementation call default implementation
        /// from IFunction. Here inheritance cannot be used for template parameterized field.
        if constexpr (impl_has_get_return_type)
            nested_type = impl.getReturnTypeImpl(nested_arguments);
        else
            nested_type = dynamic_cast<const IFunction &>(impl).getReturnTypeImpl(nested_arguments);

        if constexpr (std::is_same_v<Impl, FunctionArrayMap> || std::is_same_v<Impl, FunctionArrayConcat>)
        {
            /// Check if nested type is Array(Tuple(key, value))
            const auto * type_array = typeid_cast<const DataTypeArray *>(nested_type.get());
            if (!type_array)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Expected Array(Tuple(key, value)) type, got {}", nested_type->getName());

            const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type_array->getNestedType().get());
            if (!type_tuple)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Expected Array(Tuple(key, value)) type, got {}", nested_type->getName());

            if (type_tuple->getElements().size() != 2)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Expected Array(Tuple(key, value)) type, got {}", nested_type->getName());

            /// Recreate nested type with explicitly named tuple.
            return Adapter::wrapType(std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{type_tuple->getElement(0), type_tuple->getElement(1)}, Names{"keys", "values"})));
        }
        else
            return Adapter::wrapType(nested_type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto nested_arguments = arguments;
        Adapter::extractNestedTypesAndColumns(nested_arguments);
        return Adapter::wrapColumn(impl.executeImpl(nested_arguments, Adapter::extractResultType(result_type), input_rows_count));
    }

private:
    Impl impl;
};


template <typename Derived, typename Name>
struct MapAdapterBase
{
    static void extractNestedTypes(DataTypes & types)
    {
        bool has_map_column = false;
        for (auto & type : types)
        {
            if (const auto * type_map = typeid_cast<const DataTypeMap *>(type.get()))
            {
                has_map_column = true;
                type = Derived::extractNestedType(*type_map);
            }
        }

        if (!has_map_column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} requires at least one argument of type Map", Name::name);
    }

    static void extractNestedTypesAndColumns(ColumnsWithTypeAndName & arguments)
    {
        bool has_map_column = false;
        for (auto & argument : arguments)
        {
            if (const auto * type_map = typeid_cast<const DataTypeMap *>(argument.type.get()))
            {
                has_map_column = true;
                argument.type = Derived::extractNestedType(*type_map);

                if (argument.column)
                {
                    if (const auto * const_map = checkAndGetColumnConstData<ColumnMap>(argument.column.get()))
                        argument.column = ColumnConst::create(Derived::extractNestedColumn(*const_map), argument.column->size());
                    else
                        argument.column = Derived::extractNestedColumn(assert_cast<const ColumnMap &>(*argument.column));
                }
            }
        }

        if (!has_map_column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} requires at least one argument of type Map", Name::name);
    }
};

/// Adapter that extracts nested Array(Tuple(key, value)) from Map columns.
template <typename Name, bool returns_map = true>
struct MapToNestedAdapter : public MapAdapterBase<MapToNestedAdapter<Name, returns_map>, Name>
{
    using MapAdapterBase<MapToNestedAdapter, Name>::extractNestedTypes;
    using MapAdapterBase<MapToNestedAdapter, Name>::extractNestedTypesAndColumns;

    static DataTypePtr extractNestedType(const DataTypeMap & type_map)
    {
        return type_map.getNestedType();
    }

    static ColumnPtr extractNestedColumn(const ColumnMap & column_map)
    {
        return column_map.getNestedColumnPtr();
    }

    static DataTypePtr extractResultType(const DataTypePtr & result_type)
    {
        if constexpr (returns_map)
            return assert_cast<const DataTypeMap &>(*result_type).getNestedType();
        return result_type;
    }

    static DataTypePtr wrapType(DataTypePtr type)
    {
        if constexpr (returns_map)
            return std::make_shared<DataTypeMap>(std::move(type));
        return type;
    }

    static ColumnPtr wrapColumn(ColumnPtr column)
    {
        if constexpr (returns_map)
            return ColumnMap::create(std::move(column));
        return column;
    }
};

/// Adapter that extracts array with keys or values from Map columns.
template <typename Name, size_t position>
struct MapToSubcolumnAdapter
{
    static_assert(position <= 1, "position of Map subcolumn must be 0 or 1");

    static void extractNestedTypes(DataTypes & types)
    {
        if (types.empty())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be at least 1",
                Name::name,
                types.size());

        DataTypes new_types = {types[0]};
        MapAdapterBase<MapToSubcolumnAdapter, Name>::extractNestedTypes(new_types);
        types[0] = new_types[0];
    }

    static void extractNestedTypesAndColumns(ColumnsWithTypeAndName & arguments)
    {
        if (arguments.empty())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be at least 1",
                Name::name,
                arguments.size());

        ColumnsWithTypeAndName new_arguments = {arguments[0]};
        MapAdapterBase<MapToSubcolumnAdapter, Name>::extractNestedTypesAndColumns(new_arguments);
        arguments[0] = new_arguments[0];
    }

    static DataTypePtr extractNestedType(const DataTypeMap & type_map)
    {
        const auto & array_type = assert_cast<const DataTypeArray &>(*type_map.getNestedType());
        const auto & tuple_type = assert_cast<const DataTypeTuple &>(*array_type.getNestedType());
        return std::make_shared<DataTypeArray>(tuple_type.getElement(position));
    }

    static ColumnPtr extractNestedColumn(const ColumnMap & column_map)
    {
        const auto & array_column = column_map.getNestedColumn();
        const auto & tuple_column = column_map.getNestedData();
        return ColumnArray::create(tuple_column.getColumnPtr(position), array_column.getOffsetsPtr());
    }

    static DataTypePtr extractResultType(const DataTypePtr & result_type) { return result_type; }
    static DataTypePtr wrapType(DataTypePtr type) { return type; }
    static ColumnPtr wrapColumn(ColumnPtr column) { return column; }
};

/// A special function that works like the following:
/// mapKeyLike(pattern, key, value) <=> key LIKE pattern
/// It is used to mimic lambda: (key, value) -> key LIKE pattern.
class FunctionMapKeyLike : public IFunction
{
public:
    FunctionMapKeyLike() : impl(/*context*/ nullptr) {} /// nullptr because getting a context here is hard and FunctionLike doesn't need context
    String getName() const override { return "mapKeyLike"; }
    size_t getNumberOfArguments() const override { return 3; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        DataTypes new_arguments{arguments[1], arguments[0]};
        return impl.getReturnTypeImpl(new_arguments);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnsWithTypeAndName new_arguments{arguments[1], arguments[0]};
        return impl.executeImpl(new_arguments, result_type, input_rows_count);
    }

private:
    FunctionLike impl;
};

/// A special function that works like the following:
/// mapValueLike(pattern, key, value) <=> value LIKE pattern
/// It is used to mimic lambda: (key, value) -> value LIKE pattern.
class FunctionMapValueLike : public IFunction
{
public:
FunctionMapValueLike() : impl(/*context*/ nullptr) {} /// nullptr because getting a context here is hard and FunctionLike doesn't need context
    String getName() const override { return "mapValueLike"; }
    size_t getNumberOfArguments() const override { return 3; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        DataTypes new_arguments{arguments[2], arguments[0]};
        return impl.getReturnTypeImpl(new_arguments);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnsWithTypeAndName new_arguments{arguments[2], arguments[0]};
        return impl.executeImpl(new_arguments, result_type, input_rows_count);
    }

private:
    FunctionLike impl;
};

/// Adapter for map*KeyLike functions.
/// It extracts nested Array(Tuple(key, value)) from Map columns
/// and prepares ColumnFunction as first argument which works
/// like lambda (k, v) -> k LIKE pattern to pass it to the nested
/// function derived from FunctionArrayMapped.
template <typename Name, bool returns_map, size_t position>
struct MapLikeAdapter
{
    static_assert(position <= 1, "position of Map subcolumn must be 0 or 1");

    static void checkTypes(const DataTypes & types)
    {
        if (types.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2",
                Name::name, types.size());

        const auto * map_type = checkAndGetDataType<DataTypeMap>(types[0].get());
        if (!map_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a Map", Name::name);

        if (!isStringOrFixedString(types[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument for function {} must be String or FixedString", Name::name);

        auto subcolumn_type = position == 0 ? map_type->getKeyType() : map_type->getValueType();

        if (!isStringOrFixedString(subcolumn_type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{} type of map for function {} must be String or FixedString", position == 0 ? "Key" : "Value", Name::name);
    }

    static void extractNestedTypes(DataTypes & types)
    {
        checkTypes(types);
        const auto & map_type = assert_cast<const DataTypeMap &>(*types[0]);

        DataTypes lambda_argument_types{types[1], map_type.getKeyType(), map_type.getValueType()};

        DataTypePtr result_type;

        if constexpr (position == 0)
            result_type = FunctionMapKeyLike().getReturnTypeImpl(lambda_argument_types);
        else
            result_type = FunctionMapValueLike().getReturnTypeImpl(lambda_argument_types);

        DataTypes argument_types{map_type.getKeyType(), map_type.getValueType()};
        auto function_type = std::make_shared<DataTypeFunction>(argument_types, result_type);

        types = {function_type, types[0]};
        MapToNestedAdapter<Name, returns_map>::extractNestedTypes(types);
    }

    static void extractNestedTypesAndColumns(ColumnsWithTypeAndName & arguments)
    {
        checkTypes(DataTypes{std::from_range_t{}, arguments | std::views::transform([](auto & elem) { return elem.type; })});

        const auto & map_type = assert_cast<const DataTypeMap &>(*arguments[0].type);
        const auto & pattern_arg = arguments[1];

        ColumnPtr function_column;

        std::shared_ptr<IFunction> function;

        if constexpr (position == 0)
            function = std::make_shared<FunctionMapKeyLike>();
        else
            function = std::make_shared<FunctionMapValueLike>();

        DataTypes lambda_argument_types{pattern_arg.type, map_type.getKeyType(), map_type.getValueType()};
        auto result_type = function->getReturnTypeImpl(lambda_argument_types);

        DataTypes argument_types{map_type.getKeyType(), map_type.getValueType()};
        auto function_type = std::make_shared<DataTypeFunction>(argument_types, result_type);

        if (pattern_arg.column)
        {
            /// Here we create ColumnFunction with already captured pattern column.
            /// Nested function will append keys and values column and it will work as desired lambda.
            auto function_base = std::make_shared<FunctionToFunctionBaseAdaptor>(function, lambda_argument_types, result_type);
            function_column = ColumnFunction::create(pattern_arg.column->size(), std::move(function_base), ColumnsWithTypeAndName{pattern_arg});
        }

        ColumnWithTypeAndName function_arg{function_column, function_type, position == 0 ? "__function_map_key_like" :  "__function_map_value_like"};
        arguments = {function_arg, arguments[0]};
        MapToNestedAdapter<Name, returns_map>::extractNestedTypesAndColumns(arguments);
    }

    static DataTypePtr extractResultType(const DataTypePtr & result_type)
    {
        return MapToNestedAdapter<Name, returns_map>::extractResultType(result_type);
    }

    static DataTypePtr wrapType(DataTypePtr type)
    {
        return MapToNestedAdapter<Name, returns_map>::wrapType(std::move(type));
    }

    static ColumnPtr wrapColumn(ColumnPtr column)
    {
        return MapToNestedAdapter<Name, returns_map>::wrapColumn(std::move(column));
    }
};

struct NameMapConcat { static constexpr auto name = "mapConcat"; };
using FunctionMapConcat = FunctionMapToArrayAdapter<FunctionArrayConcat, MapToNestedAdapter<NameMapConcat>, NameMapConcat>;

struct NameMapKeys { static constexpr auto name = "mapKeys"; };
using FunctionMapKeys = FunctionMapToArrayAdapter<FunctionIdentity, MapToSubcolumnAdapter<NameMapKeys, 0>, NameMapKeys>;

struct NameMapValues { static constexpr auto name = "mapValues"; };
using FunctionMapValues = FunctionMapToArrayAdapter<FunctionIdentity, MapToSubcolumnAdapter<NameMapValues, 1>, NameMapValues>;

struct NameMapContainsKey { static constexpr auto name = "mapContainsKey"; };
using FunctionMapContainsKey = FunctionMapToArrayAdapter<FunctionArrayIndex<HasAction, NameMapContainsKey>, MapToSubcolumnAdapter<NameMapContainsKey, 0>, NameMapContainsKey>;

struct NameMapContainsValue { static constexpr auto name = "mapContainsValue"; };
using FunctionMapContainsValue = FunctionMapToArrayAdapter<FunctionArrayIndex<HasAction, NameMapContainsValue>, MapToSubcolumnAdapter<NameMapContainsValue, 1>, NameMapContainsValue>;

struct NameMapFilter { static constexpr auto name = "mapFilter"; };
using FunctionMapFilter = FunctionMapToArrayAdapter<FunctionArrayFilter, MapToNestedAdapter<NameMapFilter>, NameMapFilter>;

struct NameMapApply { static constexpr auto name = "mapApply"; };
using FunctionMapApply = FunctionMapToArrayAdapter<FunctionArrayMap, MapToNestedAdapter<NameMapApply>, NameMapApply>;

struct NameMapExists { static constexpr auto name = "mapExists"; };
using FunctionMapExists = FunctionMapToArrayAdapter<FunctionArrayExists, MapToNestedAdapter<NameMapExists, false>, NameMapExists>;

struct NameMapAll { static constexpr auto name = "mapAll"; };
using FunctionMapAll = FunctionMapToArrayAdapter<FunctionArrayAll, MapToNestedAdapter<NameMapAll, false>, NameMapAll>;

struct NameMapContainsKeyLike { static constexpr auto name = "mapContainsKeyLike"; };
using FunctionMapContainsKeyLike = FunctionMapToArrayAdapter<FunctionArrayExists, MapLikeAdapter<NameMapContainsKeyLike, false, 0>, NameMapContainsKeyLike>;

struct NameMapContainsValueLike { static constexpr auto name = "mapContainsValueLike"; };
using FunctionMapContainsValueLike = FunctionMapToArrayAdapter<FunctionArrayExists, MapLikeAdapter<NameMapContainsValueLike, false, 1>, NameMapContainsValueLike>;

struct NameMapExtractKeyLike { static constexpr auto name = "mapExtractKeyLike"; };
using FunctionMapExtractKeyLike = FunctionMapToArrayAdapter<FunctionArrayFilter, MapLikeAdapter<NameMapExtractKeyLike, true, 0>, NameMapExtractKeyLike>;

struct NameMapExtractValueLike { static constexpr auto name = "mapExtractValueLike"; };
using FunctionMapExtractValueLike = FunctionMapToArrayAdapter<FunctionArrayFilter, MapLikeAdapter<NameMapExtractValueLike, true, 1>, NameMapExtractValueLike>;

struct NameMapSort { static constexpr auto name = "mapSort"; };
struct NameMapReverseSort { static constexpr auto name = "mapReverseSort"; };
struct NameMapPartialSort { static constexpr auto name = "mapPartialSort"; };
struct NameMapPartialReverseSort { static constexpr auto name = "mapPartialReverseSort"; };

using FunctionMapSort = FunctionMapToArrayAdapter<FunctionArraySort, MapToNestedAdapter<NameMapSort>, NameMapSort>;
using FunctionMapReverseSort = FunctionMapToArrayAdapter<FunctionArrayReverseSort, MapToNestedAdapter<NameMapReverseSort>, NameMapReverseSort>;
using FunctionMapPartialSort = FunctionMapToArrayAdapter<FunctionArrayPartialSort, MapToNestedAdapter<NameMapPartialSort>, NameMapPartialSort>;
using FunctionMapPartialReverseSort = FunctionMapToArrayAdapter<FunctionArrayPartialReverseSort, MapToNestedAdapter<NameMapPartialReverseSort>, NameMapPartialReverseSort>;

REGISTER_FUNCTION(MapMiscellaneous)
{
    /// mapConcat documentation
    FunctionDocumentation::Description description_mapConcat = R"(
Concatenates multiple maps based on the equality of their keys.
If elements with the same key exist in more than one input map, all elements are added to the result map, but only the first one is accessible via operator [].
)";
    FunctionDocumentation::Syntax syntax_mapConcat = "mapConcat(maps)";
    FunctionDocumentation::Arguments arguments_mapConcat = {
        {"maps", "Arbitrarily many maps.", {"Map"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapConcat = {"Returns a map with concatenated maps passed as arguments.", {"Map"}};
    FunctionDocumentation::Examples examples_mapConcat = {
    {
        "Usage example",
        "SELECT mapConcat(map('k1', 'v1'), map('k2', 'v2'))",
        "{'k1':'v1','k2':'v2'}"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapConcat = {23, 4};
    FunctionDocumentation::Category category_mapConcat = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapConcat = {description_mapConcat, syntax_mapConcat, arguments_mapConcat, {}, returned_value_mapConcat, examples_mapConcat, introduced_in_mapConcat, category_mapConcat};
    factory.registerFunction<FunctionMapConcat>(documentation_mapConcat);

    /// mapKeys documentation
    FunctionDocumentation::Description description_mapKeys = R"(
Returns the keys of a given map.
This function can be optimized by enabling setting [`optimize_functions_to_subcolumns`](/operations/settings/settings#optimize_functions_to_subcolumns).
With the setting enabled, the function only reads the `keys` subcolumn instead of the entire map.
The query `SELECT mapKeys(m) FROM table` is transformed to `SELECT m.keys FROM table`.
)";
    FunctionDocumentation::Syntax syntax_mapKeys = "mapKeys(map)";
    FunctionDocumentation::Arguments arguments_mapKeys = {
        {"map", "Map to extract keys from.", {"Map(K, V)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapKeys = {"Returns array containing all keys from the map.", {"Array(T)"}};
    FunctionDocumentation::Examples examples_mapKeys = {
    {
        "Usage example",
        "SELECT mapKeys(map('k1', 'v1', 'k2', 'v2'))",
        "['k1','k2']"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapKeys = {21, 2};
    FunctionDocumentation::Category category_mapKeys = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapKeys = {description_mapKeys, syntax_mapKeys, arguments_mapKeys, {}, returned_value_mapKeys, examples_mapKeys, introduced_in_mapKeys, category_mapKeys};
    factory.registerFunction<FunctionMapKeys>(documentation_mapKeys);

    /// mapValues documentation
    FunctionDocumentation::Description description_mapValues = R"(
Returns the values of a given map.
This function can be optimized by enabling setting [`optimize_functions_to_subcolumns`](/operations/settings/settings#optimize_functions_to_subcolumns).
With the setting enabled, the function only reads the `values` subcolumn instead of the entire map.
The query `SELECT mapValues(m) FROM table` is transformed to `SELECT m.values FROM table`.
)";
    FunctionDocumentation::Syntax syntax_mapValues = "mapValues(map)";
    FunctionDocumentation::Arguments arguments_mapValues = {
        {"map", "Map to extract values from.", {"Map(K, V)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapValues = {"Returns an array containing all the values from the map.", {"Array(T)"}};
    FunctionDocumentation::Examples examples_mapValues = {
    {
        "Usage example",
        "SELECT mapValues(map('k1', 'v1', 'k2', 'v2'))",
        "['v1','v2']"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapValues = {21, 2};
    FunctionDocumentation::Category category_mapValues = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapValues = {description_mapValues, syntax_mapValues, arguments_mapValues, {}, returned_value_mapValues, examples_mapValues, introduced_in_mapValues, category_mapValues};
    factory.registerFunction<FunctionMapValues>(documentation_mapValues);

    /// mapContainsKey documentation
    FunctionDocumentation::Description description_mapContainsKey = R"(
Determines if a key is contained in a map.
)";
    FunctionDocumentation::Syntax syntax_mapContainsKey = "mapContainsKey(map, key)";
    FunctionDocumentation::Arguments arguments_mapContainsKey = {
        {"map", "Map to search in.", {"Map(K, V)"}},
        {"key", "Key to search for. Type must match the key type of the map.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapContainsKey = {"Returns 1 if map contains key, 0 if not.", {"UInt8"}};
    FunctionDocumentation::Examples examples_mapContainsKey = {
    {
        "Usage example",
        "SELECT mapContainsKey(map('k1', 'v1', 'k2', 'v2'), 'k1')",
        "1"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapContainsKey = {21, 2};
    FunctionDocumentation::Category category_mapContainsKey = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapContainsKey = {description_mapContainsKey, syntax_mapContainsKey, arguments_mapContainsKey, {}, returned_value_mapContainsKey, examples_mapContainsKey, introduced_in_mapContainsKey, category_mapContainsKey};
    factory.registerFunction<FunctionMapContainsKey>(documentation_mapContainsKey);

    factory.registerAlias("mapContains", "mapContainsKey", FunctionFactory::Case::Sensitive);

    /// mapContainsValue documentation
    FunctionDocumentation::Description description_mapContainsValue = R"(
Determines if a value is contained in a map.
)";
    FunctionDocumentation::Syntax syntax_mapContainsValue = "mapContainsValue(map, value)";
    FunctionDocumentation::Arguments arguments_mapContainsValue = {
        {"map", "Map to search in.", {"Map(K, V)"}},
        {"value", "Value to search for. Type must match the value type of map.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapContainsValue = {"Returns `1` if the map contains the value, `0` if not.", {"UInt8"}};
    FunctionDocumentation::Examples examples_mapContainsValue = {
    {
        "Usage example",
        "SELECT mapContainsValue(map('k1', 'v1', 'k2', 'v2'), 'v1')",
        "1"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapContainsValue = {25, 6};
    FunctionDocumentation::Category category_mapContainsValue = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapContainsValue = {description_mapContainsValue, syntax_mapContainsValue, arguments_mapContainsValue, {}, returned_value_mapContainsValue, examples_mapContainsValue, introduced_in_mapContainsValue, category_mapContainsValue};
    factory.registerFunction<FunctionMapContainsValue>(documentation_mapContainsValue);

    /// mapFilter documentation
    FunctionDocumentation::Description description_mapFilter = R"(
Filters a map by applying a function to each map element.
)";
    FunctionDocumentation::Syntax syntax_mapFilter = "mapFilter(func, map)";
    FunctionDocumentation::Arguments arguments_mapFilter = {
        {"func", "Lambda function.", {"Lambda function"}},
        {"map", "Map to filter.", {"Map(K, V)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapFilter = {"Returns a map containing only the elements in the map for which `func` returns something other than `0`.", {"Map(K, V)"}};
    FunctionDocumentation::Examples examples_mapFilter = {
    {
        "Usage example",
        "SELECT mapFilter((k, v) -> v > 1, map('k1', 1, 'k2', 2))",
        "{'k2':2}"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapFilter = {22, 3};
    FunctionDocumentation::Category category_mapFilter = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapFilter = {description_mapFilter, syntax_mapFilter, arguments_mapFilter, {}, returned_value_mapFilter, examples_mapFilter, introduced_in_mapFilter, category_mapFilter};
    factory.registerFunction<FunctionMapFilter>(documentation_mapFilter);

    /// mapApply documentation
    FunctionDocumentation::Description description_mapApply = R"(
Applies a function to each element of a map.
)";
    FunctionDocumentation::Syntax syntax_mapApply = "mapApply(func, map)";
    FunctionDocumentation::Arguments arguments_mapApply = {
        {"func", "Lambda function.", {"Lambda function"}},
        {"map", "Map to apply function to.", {"Map(K, V)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapApply = {"Returns a new map obtained from the original map by application of `func` for each element.", {"Map(K, V)"}};
    FunctionDocumentation::Examples examples_mapApply = {
    {
        "Usage example",
        "SELECT mapApply((k, v) -> (k, v * 2), map('k1', 1, 'k2', 2))",
        "{'k1':2,'k2':4}"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapApply = {22, 3};
    FunctionDocumentation::Category category_mapApply = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapApply = {description_mapApply, syntax_mapApply, arguments_mapApply, {}, returned_value_mapApply, examples_mapApply, introduced_in_mapApply, category_mapApply};
    factory.registerFunction<FunctionMapApply>(documentation_mapApply);

    /// mapExists documentation
    FunctionDocumentation::Description description_mapExists = R"(
Tests whether a condition holds for at least one key-value pair in a map.
`mapExists` is a higher-order function.
You can pass a lambda function to it as the first argument.
)";
    FunctionDocumentation::Syntax syntax_mapExists = "mapExists([func,] map)";
    FunctionDocumentation::Arguments arguments_mapExists = {
        {"func", "Optional. Lambda function.", {"Lambda function"}},
        {"map", "Map to check.", {"Map(K, V)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapExists = {"Returns `1` if at least one key-value pair satisfies the condition, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_mapExists = {
    {
        "Usage example",
        "SELECT mapExists((k, v) -> v = 1, map('k1', 1, 'k2', 2))",
        "1"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapExists = {23, 4};
    FunctionDocumentation::Category category_mapExists = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapExists = {description_mapExists, syntax_mapExists, arguments_mapExists, {}, returned_value_mapExists, examples_mapExists, introduced_in_mapExists, category_mapExists};
    factory.registerFunction<FunctionMapExists>(documentation_mapExists);

    /// mapAll documentation
    FunctionDocumentation::Description description_mapAll = R"(
Tests whether a condition holds for all key-value pairs in a map.
`mapAll` is a higher-order function.
You can pass a lambda function to it as the first argument.
)";
    FunctionDocumentation::Syntax syntax_mapAll = "mapAll([func,] map)";
    FunctionDocumentation::Arguments arguments_mapAll = {
        {"func", "Lambda function.", {"Lambda function"}},
        {"map", "Map to check.", {"Map(K, V)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapAll = {"Returns `1` if all key-value pairs satisfy the condition, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_mapAll = {
    {
        "Usage example",
        "SELECT mapAll((k, v) -> v = 1, map('k1', 1, 'k2', 2))",
        "0"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapAll = {23, 4};
    FunctionDocumentation::Category category_mapAll = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapAll = {description_mapAll, syntax_mapAll, arguments_mapAll, {}, returned_value_mapAll, examples_mapAll, introduced_in_mapAll, category_mapAll};
    factory.registerFunction<FunctionMapAll>(documentation_mapAll);

    /// mapSort documentation
    FunctionDocumentation::Description description_mapSort = R"(
Sorts the elements of a map in ascending order.
If the func function is specified, the sorting order is determined by the result of the func function applied to the keys and values of the map.
)";
    FunctionDocumentation::Syntax syntax_mapSort = "mapSort([func,] map)";
    FunctionDocumentation::Arguments arguments_mapSort = {
        {"func", "Optional. Lambda function.", {"Lambda function"}},
        {"map", "Map to sort.", {"Map(K, V)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapSort = {"Returns a map sorted in ascending order.", {"Map(K, V)"}};
    FunctionDocumentation::Examples examples_mapSort = {
    {
        "Usage example",
        "SELECT mapSort((k, v) -> v, map('k1', 3, 'k2', 1, 'k3', 2))",
        "{'k2':1,'k3':2,'k1':3}"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapSort = {23, 4};
    FunctionDocumentation::Category category_mapSort = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapSort = {description_mapSort, syntax_mapSort, arguments_mapSort, {}, returned_value_mapSort, examples_mapSort, introduced_in_mapSort, category_mapSort};
    factory.registerFunction<FunctionMapSort>(documentation_mapSort);

    /// mapReverseSort documentation
    FunctionDocumentation::Description description_mapReverseSort = R"(
Sorts the elements of a map in descending order.
If the func function is specified, the sorting order is determined by the result of the func function applied to the keys and values of the map.
)";
    FunctionDocumentation::Syntax syntax_mapReverseSort = "mapReverseSort([func,] map)";
    FunctionDocumentation::Arguments arguments_mapReverseSort = {
        {"func", "Optional. Lambda function.", {"Lambda function"}},
        {"map", "Map to sort.", {"Map(K, V)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapReverseSort = {"Returns a map sorted in descending order.", {"Map(K, V)"}};
    FunctionDocumentation::Examples examples_mapReverseSort = {
    {
        "Usage example",
        "SELECT mapReverseSort((k, v) -> v, map('k1', 3, 'k2', 1, 'k3', 2))",
        "{'k1':3,'k3':2,'k2':1}"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapReverseSort = {23, 4};
    FunctionDocumentation::Category category_mapReverseSort = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapReverseSort = {description_mapReverseSort, syntax_mapReverseSort, arguments_mapReverseSort, {}, returned_value_mapReverseSort, examples_mapReverseSort, introduced_in_mapReverseSort, category_mapReverseSort};
    factory.registerFunction<FunctionMapReverseSort>(documentation_mapReverseSort);

    /// mapPartialSort documentation
    FunctionDocumentation::Description description_mapPartialSort = R"(
Sorts the elements of a map in ascending order with additional limit argument allowing partial sorting.
If the func function is specified, the sorting order is determined by the result of the func function applied to the keys and values of the map.
)";
    FunctionDocumentation::Syntax syntax_mapPartialSort = "mapPartialSort([func,] limit, map)";
    FunctionDocumentation::Arguments arguments_mapPartialSort = {
        {"func", "Optional. Lambda function.", {"Lambda function"}},
        {"limit", "Elements in the range `[1..limit]` are sorted.", {"(U)Int*"}},
        {"map", "Map to sort.", {"Map(K, V)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapPartialSort = {"Returns a partially sorted map.", {"Map(K, V)"}};
    FunctionDocumentation::Examples examples_mapPartialSort = {
    {
        "Usage example",
        "SELECT mapPartialSort((k, v) -> v, 2, map('k1', 3, 'k2', 1, 'k3', 2))",
        "{'k2':1,'k3':2,'k1':3}"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapPartialSort = {23, 4};
    FunctionDocumentation::Category category_mapPartialSort = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapPartialSort = {description_mapPartialSort, syntax_mapPartialSort, arguments_mapPartialSort, {}, returned_value_mapPartialSort, examples_mapPartialSort, introduced_in_mapPartialSort, category_mapPartialSort};
    factory.registerFunction<FunctionMapPartialSort>(documentation_mapPartialSort);

    /// mapPartialReverseSort documentation
    FunctionDocumentation::Description description_mapPartialReverseSort = R"(
Sorts the elements of a map in descending order with additional limit argument allowing partial sorting.
If the func function is specified, the sorting order is determined by the result of the func function applied to the keys and values of the map.
)";
    FunctionDocumentation::Syntax syntax_mapPartialReverseSort = "mapPartialReverseSort([func,] limit, map)";
    FunctionDocumentation::Arguments arguments_mapPartialReverseSort = {
        {"func", "Optional. Lambda function.", {"Lambda function"}},
        {"limit", "Elements in the range `[1..limit]` are sorted.", {"(U)Int*"}},
        {"map", "Map to sort.", {"Map(K, V)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapPartialReverseSort = {"Returns a partially sorted map in descending order.", {"Map(K, V)"}};
    FunctionDocumentation::Examples examples_mapPartialReverseSort = {
    {
        "Usage example",
        "SELECT mapPartialReverseSort((k, v) -> v, 2, map('k1', 3, 'k2', 1, 'k3', 2))",
        "{'k1':3,'k3':2,'k2':1}"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapPartialReverseSort = {23, 4};
    FunctionDocumentation::Category category_mapPartialReverseSort = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapPartialReverseSort = {description_mapPartialReverseSort, syntax_mapPartialReverseSort, arguments_mapPartialReverseSort, {}, returned_value_mapPartialReverseSort, examples_mapPartialReverseSort, introduced_in_mapPartialReverseSort, category_mapPartialReverseSort};
    factory.registerFunction<FunctionMapPartialReverseSort>(documentation_mapPartialReverseSort);

    /// mapContainsKeyLike documentation
    FunctionDocumentation::Description description_mapContainsKeyLike = R"(
Checks whether map contains key `LIKE` specified pattern.
)";
    FunctionDocumentation::Syntax syntax_mapContainsKeyLike = "mapContainsKeyLike(map, pattern)";
    FunctionDocumentation::Arguments arguments_mapContainsKeyLike = {
        {"map", "Map to search in.", {"Map(K, V)"}},
        {"pattern", "Pattern to match keys against.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapContainsKeyLike = {"Returns `1` if `map` contains a key matching `pattern`, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_mapContainsKeyLike = {
    {
        "Usage example",
        R"(
CREATE TABLE tab (a Map(String, String))
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab VALUES ({'abc':'abc','def':'def'}), ({'hij':'hij','klm':'klm'});

SELECT mapContainsKeyLike(a, 'a%') FROM tab;
        )",
        R"(
┌─mapContainsKeyLike(a, 'a%')─┐
│                           1 │
│                           0 │
└─────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapContainsKeyLike = {23, 4};
    FunctionDocumentation::Category category_mapContainsKeyLike = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapContainsKeyLike = {description_mapContainsKeyLike, syntax_mapContainsKeyLike, arguments_mapContainsKeyLike, {}, returned_value_mapContainsKeyLike, examples_mapContainsKeyLike, introduced_in_mapContainsKeyLike, category_mapContainsKeyLike};
    factory.registerFunction<FunctionMapContainsKeyLike>(documentation_mapContainsKeyLike);

    /// mapContainsValueLike documentation
    FunctionDocumentation::Description description_mapContainsValueLike = R"(
Checks whether a map contains a value `LIKE` the specified pattern.
)";
    FunctionDocumentation::Syntax syntax_mapContainsValueLike = "mapContainsValueLike(map, pattern)";
    FunctionDocumentation::Arguments arguments_mapContainsValueLike = {
        {"map", "Map to search in.", {"Map(K, V)"}},
        {"pattern", "Pattern to match values against.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapContainsValueLike = {"Returns `1` if `map` contains a value matching `pattern`, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples_mapContainsValueLike = {
    {
        "Usage example",
        R"(
CREATE TABLE tab (a Map(String, String))
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab VALUES ({'abc':'abc','def':'def'}), ({'hij':'hij','klm':'klm'});

SELECT mapContainsValueLike(a, 'a%') FROM tab;
        )",
        R"(
┌─mapContainsV⋯ke(a, 'a%')─┐
│                        1 │
│                        0 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapContainsValueLike = {25, 5};
    FunctionDocumentation::Category category_mapContainsValueLike = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapContainsValueLike = {description_mapContainsValueLike, syntax_mapContainsValueLike, arguments_mapContainsValueLike, {}, returned_value_mapContainsValueLike, examples_mapContainsValueLike, introduced_in_mapContainsValueLike, category_mapContainsValueLike};
    factory.registerFunction<FunctionMapContainsValueLike>(documentation_mapContainsValueLike);

    /// mapExtractKeyLike documentation
    FunctionDocumentation::Description description_mapExtractKeyLike = R"(
Give a map with string keys and a `LIKE` pattern, this function returns a map with elements where the key matches the pattern.
)";
    FunctionDocumentation::Syntax syntax_mapExtractKeyLike = "mapExtractKeyLike(map, pattern)";
    FunctionDocumentation::Arguments arguments_mapExtractKeyLike = {
        {"map", "Map to extract from.", {"Map(K, V)"}},
        {"pattern", "Pattern to match keys against.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapExtractKeyLike = {"Returns a map containing elements the key matching the specified pattern. If no elements match the pattern, an empty map is returned.", {"Map(K, V)"}};
    FunctionDocumentation::Examples examples_mapExtractKeyLike = {
    {
        "Usage example",
        R"(
CREATE TABLE tab (a Map(String, String))
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab VALUES ({'abc':'abc','def':'def'}), ({'hij':'hij','klm':'klm'});

SELECT mapExtractKeyLike(a, 'a%') FROM tab;
        )",
        R"(
┌─mapExtractKeyLike(a, 'a%')─┐
│ {'abc':'abc'}              │
│ {}                         │
└────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapExtractKeyLike = {23, 4};
    FunctionDocumentation::Category category_mapExtractKeyLike = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapExtractKeyLike = {description_mapExtractKeyLike, syntax_mapExtractKeyLike, arguments_mapExtractKeyLike, {}, returned_value_mapExtractKeyLike, examples_mapExtractKeyLike, introduced_in_mapExtractKeyLike, category_mapExtractKeyLike};
    factory.registerFunction<FunctionMapExtractKeyLike>(documentation_mapExtractKeyLike);

    /// mapExtractValueLike documentation
    FunctionDocumentation::Description description_mapExtractValueLike = R"(
Given a map with string values and a `LIKE` pattern, this function returns a map with elements where the value matches the pattern.
)";
    FunctionDocumentation::Syntax syntax_mapExtractValueLike = "mapExtractValueLike(map, pattern)";
    FunctionDocumentation::Arguments arguments_mapExtractValueLike = {
        {"map", "Map to extract from.", {"Map(K, V)"}},
        {"pattern", "Pattern to match values against.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapExtractValueLike = {"Returns a map containing elements the value matching the specified pattern. If no elements match the pattern, an empty map is returned.", {"Map(K, V)"}};
    FunctionDocumentation::Examples examples_mapExtractValueLike = {
    {
        "Usage example",
        R"(
CREATE TABLE tab (a Map(String, String))
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab VALUES ({'abc':'abc','def':'def'}), ({'hij':'hij','klm':'klm'});

SELECT mapExtractValueLike(a, 'a%') FROM tab;
        )",
        R"(
┌─mapExtractValueLike(a, 'a%')─┐
│ {'abc':'abc'}                │
│ {}                           │
└──────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapExtractValueLike = {25, 5};
    FunctionDocumentation::Category category_mapExtractValueLike = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapExtractValueLike = {description_mapExtractValueLike, syntax_mapExtractValueLike, arguments_mapExtractValueLike, {}, returned_value_mapExtractValueLike, examples_mapExtractValueLike, introduced_in_mapExtractValueLike, category_mapExtractValueLike};
    factory.registerFunction<FunctionMapExtractValueLike>(documentation_mapExtractValueLike);
}

}
