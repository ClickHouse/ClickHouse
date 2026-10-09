#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Serializations/SerializationMapKeyColumns.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

bool hasOnlyStringKeyMaps(const IDataType & type)
{
    if (const auto * map = typeid_cast<const DataTypeMap *>(&type))
        return WhichDataType(map->getKeyType()).isString();
    if (const auto * array = typeid_cast<const DataTypeArray *>(&type))
        return hasOnlyStringKeyMaps(*array->getNestedType());
    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(&type))
        return std::ranges::all_of(tuple->getElements(), [](const auto & element) { return hasOnlyStringKeyMaps(*element); });
    return true;
}

/// Sorts the keys of the `Map(String, V)` values within each row, also inside `Array` and `Tuple`,
/// as they are stored with `map_serialization_version = 'with_key_columns'`. Used by mutations,
/// so that later statements see the Map as it is stored.
class FunctionSortMapKeys : public IFunction
{
public:
    static constexpr auto name = "__sortMapKeys";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSortMapKeys>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!hasOnlyStringKeyMaps(*arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} supports only Maps with String keys, got {}", getName(), arguments[0]->getName());
        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        if (!SerializationMapKeyColumns::hasMapsWithKeyColumns(*arguments[0].type))
            return arguments[0].column;
        return SerializationMapKeyColumns::sortMapKeys(arguments[0].column->convertToFullIfWrapped(), *arguments[0].type);
    }
};

}

REGISTER_FUNCTION(SortMapKeys)
{
    factory.registerFunction<FunctionSortMapKeys>(FunctionDocumentation::INTERNAL_FUNCTION_DOCS);
}

}
