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
    constexpr auto category_map = FunctionDocumentation::Category::Map;

    factory.registerFunction<FunctionMapConcat>(
        FunctionDocumentation{
            .description="The same as arrayConcat.",
            .examples{{"mapConcat", "SELECT mapConcat(map('k1', 'v1'), map('k2', 'v2'))", ""}},
            .introduced_in = {23, 4},
            .category = category_map,
        });

    factory.registerFunction<FunctionMapKeys>(
        FunctionDocumentation{
            .description="Returns an array with the keys of map.",
            .examples{{"mapKeys", "SELECT mapKeys(map('k1', 'v1', 'k2', 'v2'))", ""}},
            .introduced_in = {21, 2},
            .category = category_map,
        });

    factory.registerFunction<FunctionMapValues>(
        FunctionDocumentation{
            .description="Returns an array with the values of map.",
            .examples{{"mapValues", "SELECT mapValues(map('k1', 'v1', 'k2', 'v2'))", ""}},
            .introduced_in = {21, 2},
            .category = category_map,
        });

    factory.registerFunction<FunctionMapContainsKey>(
        FunctionDocumentation{
            .description="Checks whether the map has the specified key.",
            .examples{{"mapContainsKey", "SELECT mapContainsKey(map('k1', 'v1', 'k2', 'v2'), 'k1')", ""}},
            .introduced_in = {25, 5},
            .category = category_map,
        });

    factory.registerAlias("mapContains", "mapContainsKey", FunctionFactory::Case::Sensitive);

    factory.registerFunction<FunctionMapContainsValue>(
        FunctionDocumentation{
            .description="Checks whether the map has the specified value.",
            .examples{{"mapContainsValue", "SELECT mapContainsValue(map('k1', 'v1', 'k2', 'v2'), 'v1')", ""}},
            .introduced_in = {25, 5},
            .category = category_map,
        });

    factory.registerFunction<FunctionMapFilter>(
        FunctionDocumentation{
            .description="The same as arrayFilter.",
            .examples{{"mapFilter", "SELECT mapFilter((k, v) -> v > 1, map('k1', 1, 'k2', 2))", ""}},
            .introduced_in = {22, 3},
            .category = category_map,
        });

    factory.registerFunction<FunctionMapApply>(
        FunctionDocumentation{
            .description="The same as arrayMap.",
            .examples{{"mapApply", "SELECT mapApply((k, v) -> (k, v * 2), map('k1', 1, 'k2', 2))", ""}},
            .introduced_in = {22, 3},
            .category = category_map,
        });

    factory.registerFunction<FunctionMapExists>(
        FunctionDocumentation{
            .description="The same as arrayExists.",
            .examples{{"mapExists", "SELECT mapExists((k, v) -> v = 1, map('k1', 1, 'k2', 2))", ""}},
            .introduced_in = {23, 4},
            .category = category_map,
        });

    factory.registerFunction<FunctionMapAll>(
        FunctionDocumentation{
            .description="The same as arrayAll.",
            .examples{{"mapAll", "SELECT mapAll((k, v) -> v = 1, map('k1', 1, 'k2', 2))", ""}},
            .introduced_in = {23, 4},
            .category = category_map,
        });

    factory.registerFunction<FunctionMapSort>(
        FunctionDocumentation{
            .description="The same as arraySort.",
            .examples{{"mapSort", "SELECT mapSort((k, v) -> v, map('k1', 3, 'k2', 1, 'k3', 2))", ""}},
            .introduced_in = {23, 4},
            .category = category_map,
        });

    factory.registerFunction<FunctionMapReverseSort>(
        FunctionDocumentation{
            .description="The same as arrayReverseSort.",
            .examples{{"mapReverseSort", "SELECT mapReverseSort((k, v) -> v, map('k1', 3, 'k2', 1, 'k3', 2))", ""}},
            .introduced_in = {23, 4},
            .category = category_map,
        });

    factory.registerFunction<FunctionMapPartialSort>(
        FunctionDocumentation{
            .description="The same as arrayReverseSort.",
            .examples{{"mapPartialSort", "SELECT mapPartialSort((k, v) -> v, 2, map('k1', 3, 'k2', 1, 'k3', 2))", ""}},
            .introduced_in = {23, 4},
            .category = category_map,
        });

    factory.registerFunction<FunctionMapPartialReverseSort>(
        FunctionDocumentation{
            .description="The same as arrayPartialReverseSort.",
            .examples{{"mapPartialReverseSort", "SELECT mapPartialReverseSort((k, v) -> v, 2, map('k1', 3, 'k2', 1, 'k3', 2))", ""}},
            .introduced_in = {23, 4},
            .category = category_map,
        });

    factory.registerFunction<FunctionMapContainsKeyLike>(
        FunctionDocumentation{
            .description="Checks whether map contains key LIKE specified pattern.",
            .examples{{"mapContainsKeyLike", "SELECT mapContainsKeyLike(map('k1-1', 1, 'k2-1', 2), 'k1%')", ""}},
            .introduced_in = {23, 4},
            .category = category_map,
        });

    factory.registerFunction<FunctionMapContainsValueLike>(
        FunctionDocumentation{
            .description="Checks whether map contains value LIKE specified pattern.",
            .examples{{"mapContainsValueLike", "SELECT mapContainsValueLike(map(1, 'v1-1', '2, 'v2-2'), 'v1%')", ""}},
            .introduced_in = {25, 5},
            .category = category_map,
        });

    factory.registerFunction<FunctionMapExtractKeyLike>(
        FunctionDocumentation{
            .description="Returns a map with elements which key matches the specified pattern.",
            .examples{{"mapExtractKeyLike", "SELECT mapExtractKeyLike(map('k1-1', 1, 'k2-1', 2), 'k1%')", ""}},
            .introduced_in = {23, 4},
            .category = category_map,
        });

    factory.registerFunction<FunctionMapExtractValueLike>(
        FunctionDocumentation{
            .description="Returns a map with elements which value matches the specified pattern.",
            .examples{{"mapExtractValueLike", "SELECT mapExtractValueLike(map('k1-1', 'v1-1', 'k2-1', 'v2-1'), 'v1%')", ""}},
            .introduced_in = {25, 5},
            .category = category_map,
        });
}

}
