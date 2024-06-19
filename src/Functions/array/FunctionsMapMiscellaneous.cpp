#include <vector>
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

#include <base/map.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/IArraySource.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}

namespace
{

/** An adapter that allows to execute array* functions over Map types arguments.
  * E.g. transform mapConcat to arrayConcat.
  *
  * Impl - the implementation of function that is applied
  * to internal column of Map arguments (e.g. 'arrayConcat').
  *
  * Adapter - a struct that determines the way how to extract the internal
  * array columns from Map arguments and possibly modify other columns.
*/
template <typename Impl, typename Adapter>
class FunctionMapToArrayAdapter : public IFunction
{
public:
    static constexpr auto name = Adapter::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapToArrayAdapter>(); }
    String getName() const override { return name; }

    bool isVariadic() const override { return impl.isVariadic(); }
    size_t getNumberOfArguments() const override { return impl.getNumberOfArguments(); }
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
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument", getName());

        constexpr bool impl_has_get_return_type = requires
        {
            impl.getReturnTypeImpl(arguments);
        };

        auto nested_arguments = Adapter::extractArgumentsForShards(arguments).front();

        /// If method is not overloaded in the implementation call default implementation
        /// from IFunction. Here inheritance cannot be used for template parameterized field.
        if constexpr (impl_has_get_return_type)
            return Adapter::wrapType(impl.getReturnTypeImpl(nested_arguments));
        else
            return Adapter::wrapType(dynamic_cast<const IFunction &>(impl).getReturnTypeImpl(nested_arguments));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto impl_result_type = Adapter::extractResultType(result_type);
        auto arguments_for_shards = Adapter::extractArgumentsForShards(arguments);

        std::vector<ColumnPtr> result_shards;
        result_shards.reserve(arguments_for_shards.size());

        for (const auto & args : arguments_for_shards)
        {
            auto result = impl.executeImpl(args, impl_result_type, input_rows_count);
            result_shards.push_back(std::move(result));
        }

        return Adapter::gatherResult(std::move(result_shards));
    }

private:
    Impl impl;
};

template <typename Name, typename ArgumentExtractor, typename ResultGatherer>
struct MapAdapter
{
public:
    static constexpr auto name = Name::name;

    static DataTypePtr extractResultType(DataTypePtr type)
    {
        if constexpr (ResultGatherer::returns_map)
            return assert_cast<const DataTypeMap &>(*type).getNestedType();
        else
            return type;
    }

    static DataTypePtr wrapType(DataTypePtr type)
    {
        if constexpr (ResultGatherer::returns_map)
            return std::make_shared<DataTypeMap>(std::move(type));
        else
            return type;
    }

    static ColumnPtr gatherResult(std::vector<ColumnPtr> columns)
    {
        return ResultGatherer::gatherResult(std::move(columns));
    }

    static void extractNestedTypes(DataTypes & types)
    {
        bool has_map_column = false;
        for (auto & type : types)
        {
            const auto * type_map = typeid_cast<const DataTypeMap *>(type.get());
            if (!type_map)
                continue;

            type = ArgumentExtractor::extractType(*type_map);
            has_map_column = true;
        }

        if (!has_map_column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} requires at least one argument of type Map", Name::name);
    }

    static std::vector<ColumnsWithTypeAndName> extractArgumentsForShards(const ColumnsWithTypeAndName & arguments)
    {
        bool has_map_column = false;
        std::vector<size_t> map_column_indexes;
        auto nested_arguments = arguments;

        for (size_t i = 0; i < nested_arguments.size(); ++i)
        {
            auto & argument = nested_arguments[i];
            const auto * type_map = typeid_cast<const DataTypeMap *>(argument.type.get());
            if (!type_map)
                continue;

            argument.type = ArgumentExtractor::extractType(*type_map);
            has_map_column = true;

            if (argument.column)
            {
                map_column_indexes.push_back(i);
                if (ResultGatherer::need_finalize)
                    argument.column = argument.column->finalize();
            }
        }

        if (!has_map_column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} requires at least one argument of type Map", Name::name);

        if (map_column_indexes.empty())
            return {std::move(nested_arguments)};

        if (map_column_indexes.size() > 1 && !ResultGatherer::need_finalize)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Function {} requires several arguments of type Map and must use finalize", Name::name);

        size_t map_column_index = map_column_indexes.front();
        size_t num_shards = getNumShardsOfMapColumn(*nested_arguments[map_column_index].column);

        std::vector<ColumnsWithTypeAndName> shard_arguments(num_shards, nested_arguments);

        for (size_t shard = 0; shard < num_shards; ++shard)
        {
            for (auto idx : map_column_indexes)
            {
                auto & argument = shard_arguments[shard][idx];
                argument.column = extractColumnFromMap(*argument.column, shard);
            }
        }

        return shard_arguments;
    }

private:
    static ColumnPtr extractColumnFromMap(const IColumn & column, size_t shard_idx)
    {
        if (const auto * const_map = checkAndGetColumnConstData<ColumnMap>(&column))
        {
            auto res = ArgumentExtractor::extractColumn(*const_map, shard_idx);
            return ColumnConst::create(std::move(res), column.size());
        }

        return ArgumentExtractor::extractColumn(assert_cast<const ColumnMap &>(column), shard_idx);
    }

    static size_t getNumShardsOfMapColumn(const IColumn & column)
    {
        if (const auto * const_map = checkAndGetColumnConstData<ColumnMap>(&column))
            return const_map->getNumShards();

        return assert_cast<const ColumnMap &>(column).getNumShards();
    }
};

/// Extracts nested Array(Tuple(key, value)) from Map columns.
struct ArgumentExtractorMap
{
    static DataTypePtr extractType(const DataTypeMap & type_map)
    {
        return type_map.getNestedTypeWithUnnamedTuple();
    }

    static ColumnPtr extractColumn(const ColumnMap & column_map, size_t shard_idx)
    {
        return column_map.getShardPtr(shard_idx);
    }
};

/// Extracts array with keys or values from Map columns.
template <size_t position>
struct ArgumentExtractorSubcolumn
{
    static_assert(position <= 1);

    static DataTypePtr extractType(const DataTypeMap & type_map)
    {
        const auto & array_type = assert_cast<const DataTypeArray &>(*type_map.getNestedType());
        const auto & tuple_type = assert_cast<const DataTypeTuple &>(*array_type.getNestedType());
        return std::make_shared<DataTypeArray>(tuple_type.getElement(position));
    }

    static ColumnPtr extractColumn(const ColumnMap & column_map, size_t shard_idx)
    {
        const auto & array_column = column_map.getShard(shard_idx);
        const auto & tuple_column = assert_cast<const ColumnTuple &>(array_column.getData());
        return ColumnArray::create(tuple_column.getColumnPtr(position), array_column.getOffsetsPtr());
    }
};

struct ResultGathererMap
{
    static constexpr bool returns_map = true;
    static constexpr bool need_finalize = false;

    static ColumnPtr gatherResult(std::vector<ColumnPtr> columns)
    {
        return ColumnMap::create(std::move(columns));
    }
};

struct ResultGathererMapOneShard
{
    static constexpr bool returns_map = true;
    static constexpr bool need_finalize = true;

    static ColumnPtr gatherResult(std::vector<ColumnPtr> columns)
    {
        chassert(columns.size() == 1);
        return ColumnMap::create(std::move(columns));
    }
};

struct ResultGathererArray
{
    static constexpr bool returns_map = false;
    static constexpr bool need_finalize = false;

    static ColumnPtr gatherResult(std::vector<ColumnPtr> columns)
    {
        if (columns.size() == 1)
            return columns.front();

        std::vector<std::unique_ptr<GatherUtils::IArraySource>> sources(columns.size());
        for (size_t i = 0; i < columns.size(); ++i)
        {
            const auto & shard_array = assert_cast<const ColumnArray &>(*columns[i]);
            sources[i] = GatherUtils::createArraySource(shard_array, false, shard_array.size());
        }

        return GatherUtils::concat(sources);
    }
};

struct OrImpl { static UInt8 apply(UInt8 x, UInt8 y) { return x | y; } };
struct AndImpl { static UInt8 apply(UInt8 x, UInt8 y) { return x & y; } };

template <typename Op>
struct ResultGathererBool
{
    static constexpr bool returns_map = false;
    static constexpr bool need_finalize = false;

    static ColumnPtr gatherResult(std::vector<ColumnPtr> columns)
    {
        if (columns.size() == 1)
            return columns.front();

        auto res = columns.front()->assumeMutable();
        auto & res_data = assert_cast<ColumnUInt8 &>(*res).getData();
        size_t res_size = res_data.size();

        for (size_t i = 1; i < columns.size(); ++i)
        {
            const auto & data = assert_cast<const ColumnUInt8 &>(*columns[i]).getData();
            chassert(data.size() == res_size);

            for (size_t j = 0; j < res_size; ++j)
                res_data[j] = Op::apply(res_data[j], data[j]);
        }

        return res;
    }
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

/// Adapter for map*KeyLike functions.
/// It extracts nested Array(Tuple(key, value)) from Map columns
/// and prepares ColumnFunction as first argument which works
/// like lambda (k, v) -> k LIKE pattern to pass it to the nested
/// function derived from FunctionArrayMapped.
template <typename Name, typename ArgumentExtractor, typename ResultGatherer>
struct MapKeyLikeAdapter : MapAdapter<Name, ArgumentExtractor, ResultGatherer>
{
    using Base = MapAdapter<Name, ArgumentExtractor, ResultGatherer>;

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

        if (!isStringOrFixedString(map_type->getKeyType()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Key type of map for function {} must be String or FixedString", Name::name);
    }

    static void extractNestedTypes(DataTypes & types)
    {
        checkTypes(types);
        const auto & map_type = assert_cast<const DataTypeMap &>(*types[0]);

        DataTypes lambda_argument_types{types[1], map_type.getKeyType(), map_type.getValueType()};
        auto result_type = FunctionMapKeyLike().getReturnTypeImpl(lambda_argument_types);

        DataTypes argument_types{map_type.getKeyType(), map_type.getValueType()};
        auto function_type = std::make_shared<DataTypeFunction>(argument_types, result_type);

        types = {function_type, types[0]};
        Base::extractNestedTypes(types);
    }

    static std::vector<ColumnsWithTypeAndName> extractArgumentsForShards(const ColumnsWithTypeAndName & arguments)
    {
        auto new_arguments = arguments;
        checkTypes(collections::map<DataTypes>(new_arguments, [](const auto & elem) { return elem.type; }));

        const auto & map_type = assert_cast<const DataTypeMap &>(*new_arguments[0].type);
        const auto & pattern_arg = new_arguments[1];

        ColumnPtr function_column;
        auto function = std::make_shared<FunctionMapKeyLike>();

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

        ColumnWithTypeAndName function_arg{function_column, function_type, "__function_map_key_like"};
        new_arguments = {function_arg, new_arguments[0]};
        return Base::extractArgumentsForShards(new_arguments);
    }
};

struct NameMapConcat { static constexpr auto name = "mapConcat"; };
using FunctionMapConcat = FunctionMapToArrayAdapter<FunctionArrayConcat, MapAdapter<NameMapConcat, ArgumentExtractorMap, ResultGathererMapOneShard>>;

struct NameMapKeys { static constexpr auto name = "mapKeys"; };
using FunctionMapKeys = FunctionMapToArrayAdapter<FunctionIdentity, MapAdapter<NameMapKeys, ArgumentExtractorSubcolumn<0>, ResultGathererArray>>;

struct NameMapValues { static constexpr auto name = "mapValues"; };
using FunctionMapValues = FunctionMapToArrayAdapter<FunctionIdentity, MapAdapter<NameMapValues, ArgumentExtractorSubcolumn<1>, ResultGathererArray>>;

struct NameMapContains { static constexpr auto name = "mapContains"; };
using FunctionMapContains = FunctionMapToArrayAdapter<FunctionArrayIndex<HasAction, NameMapContains>, MapAdapter<NameMapContains, ArgumentExtractorSubcolumn<0>, ResultGathererBool<OrImpl>>>;

struct NameMapFilter { static constexpr auto name = "mapFilter"; };
using FunctionMapFilter = FunctionMapToArrayAdapter<FunctionArrayFilter, MapAdapter<NameMapFilter, ArgumentExtractorMap, ResultGathererMap>>;

struct NameMapApply { static constexpr auto name = "mapApply"; };
using FunctionMapApply = FunctionMapToArrayAdapter<FunctionArrayMap, MapAdapter<NameMapApply, ArgumentExtractorMap, ResultGathererMap>>;

struct NameMapExists { static constexpr auto name = "mapExists"; };
using FunctionMapExists = FunctionMapToArrayAdapter<FunctionArrayExists, MapAdapter<NameMapExists, ArgumentExtractorMap, ResultGathererBool<OrImpl>>>;

struct NameMapAll { static constexpr auto name = "mapAll"; };
using FunctionMapAll = FunctionMapToArrayAdapter<FunctionArrayAll, MapAdapter<NameMapAll, ArgumentExtractorMap, ResultGathererBool<AndImpl>>>;

struct NameMapContainsKeyLike { static constexpr auto name = "mapContainsKeyLike"; };
using FunctionMapContainsKeyLike = FunctionMapToArrayAdapter<FunctionArrayExists, MapKeyLikeAdapter<NameMapContainsKeyLike, ArgumentExtractorMap, ResultGathererBool<OrImpl>>>;

struct NameMapExtractKeyLike { static constexpr auto name = "mapExtractKeyLike"; };
using FunctionMapExtractKeyLike = FunctionMapToArrayAdapter<FunctionArrayFilter, MapKeyLikeAdapter<NameMapExtractKeyLike, ArgumentExtractorMap, ResultGathererMap>>;

struct NameMapSort { static constexpr auto name = "mapSort"; };
struct NameMapReverseSort { static constexpr auto name = "mapReverseSort"; };
struct NameMapPartialSort { static constexpr auto name = "mapPartialSort"; };
struct NameMapPartialReverseSort { static constexpr auto name = "mapPartialReverseSort"; };

using FunctionMapSort = FunctionMapToArrayAdapter<FunctionArraySort, MapAdapter<NameMapSort, ArgumentExtractorMap, ResultGathererMapOneShard>>;
using FunctionMapReverseSort = FunctionMapToArrayAdapter<FunctionArrayReverseSort, MapAdapter<NameMapReverseSort, ArgumentExtractorMap, ResultGathererMapOneShard>>;
using FunctionMapPartialSort = FunctionMapToArrayAdapter<FunctionArrayPartialSort, MapAdapter<NameMapPartialSort, ArgumentExtractorMap, ResultGathererMapOneShard>>;
using FunctionMapPartialReverseSort = FunctionMapToArrayAdapter<FunctionArrayPartialReverseSort, MapAdapter<NameMapPartialReverseSort, ArgumentExtractorMap, ResultGathererMapOneShard>>;

}

REGISTER_FUNCTION(MapMiscellaneous)
{
    factory.registerFunction<FunctionMapConcat>(
    FunctionDocumentation{
        .description="The same as arrayConcat.",
        .examples{{"mapConcat", "SELECT mapConcat(map('k1', 'v1'), map('k2', 'v2'))", ""}},
        .categories{"Map"},
    });

    factory.registerFunction<FunctionMapKeys>(
    FunctionDocumentation{
        .description="Returns an array with the keys of map.",
        .examples{{"mapKeys", "SELECT mapKeys(map('k1', 'v1', 'k2', 'v2'))", ""}},
        .categories{"Map"},
    });

    factory.registerFunction<FunctionMapValues>(
    FunctionDocumentation{
        .description="Returns an array with the values of map.",
        .examples{{"mapValues", "SELECT mapValues(map('k1', 'v1', 'k2', 'v2'))", ""}},
        .categories{"Map"},
    });

    factory.registerFunction<FunctionMapContains>(
    FunctionDocumentation{
        .description="Checks whether the map has the specified key.",
        .examples{{"mapContains", "SELECT mapContains(map('k1', 'v1', 'k2', 'v2'), 'k1')", ""}},
        .categories{"Map"},
    });

    factory.registerFunction<FunctionMapFilter>(
    FunctionDocumentation{
        .description="The same as arrayFilter.",
        .examples{{"mapFilter", "SELECT mapFilter((k, v) -> v > 1, map('k1', 1, 'k2', 2))", ""}},
        .categories{"Map"},
    });

    factory.registerFunction<FunctionMapApply>(
    FunctionDocumentation{
        .description="The same as arrayMap.",
        .examples{{"mapApply", "SELECT mapApply((k, v) -> (k, v * 2), map('k1', 1, 'k2', 2))", ""}},
        .categories{"Map"},
    });

    factory.registerFunction<FunctionMapExists>(
    FunctionDocumentation{
        .description="The same as arrayExists.",
        .examples{{"mapExists", "SELECT mapExists((k, v) -> v = 1, map('k1', 1, 'k2', 2))", ""}},
        .categories{"Map"},
    });

    factory.registerFunction<FunctionMapAll>(
    FunctionDocumentation{
        .description="The same as arrayAll.",
        .examples{{"mapAll", "SELECT mapAll((k, v) -> v = 1, map('k1', 1, 'k2', 2))", ""}},
        .categories{"Map"},
    });

    factory.registerFunction<FunctionMapSort>(
    FunctionDocumentation{
        .description="The same as arraySort.",
        .examples{{"mapSort", "SELECT mapSort((k, v) -> v, map('k1', 3, 'k2', 1, 'k3', 2))", ""}},
        .categories{"Map"},
    });

    factory.registerFunction<FunctionMapReverseSort>(
    FunctionDocumentation{
        .description="The same as arrayReverseSort.",
        .examples{{"mapReverseSort", "SELECT mapReverseSort((k, v) -> v, map('k1', 3, 'k2', 1, 'k3', 2))", ""}},
        .categories{"Map"},
    });

    factory.registerFunction<FunctionMapPartialSort>(
    FunctionDocumentation{
        .description="The same as arrayReverseSort.",
        .examples{{"mapPartialSort", "SELECT mapPartialSort((k, v) -> v, 2, map('k1', 3, 'k2', 1, 'k3', 2))", ""}},
        .categories{"Map"},
    });

    factory.registerFunction<FunctionMapPartialReverseSort>(
    FunctionDocumentation{
        .description="The same as arrayPartialReverseSort.",
        .examples{{"mapPartialReverseSort", "SELECT mapPartialReverseSort((k, v) -> v, 2, map('k1', 3, 'k2', 1, 'k3', 2))", ""}},
        .categories{"Map"},
    });

    factory.registerFunction<FunctionMapContainsKeyLike>(
    FunctionDocumentation{
        .description="Checks whether map contains key LIKE specified pattern.",
        .examples{{"mapContainsKeyLike", "SELECT mapContainsKeyLike(map('k1-1', 1, 'k2-1', 2), 'k1%')", ""}},
        .categories{"Map"},
    });

    factory.registerFunction<FunctionMapExtractKeyLike>(
    FunctionDocumentation{
        .description="Returns a map with elements which key matches the specified pattern.",
        .examples{{"mapExtractKeyLike", "SELECT mapExtractKeyLike(map('k1-1', 1, 'k2-1', 2), 'k1%')", ""}},
        .categories{"Map"},
    });
}

}
