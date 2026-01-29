#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/ObjectUtils.h>
#include <Columns/ColumnTuple.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class FunctionFlattenTuple : public IFunction
{
public:
    static constexpr auto name = "flattenTuple";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFlattenTuple>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto & type = arguments[0];
        const auto * type_tuple = checkAndGetDataType<DataTypeTuple>(type.get());
        if (!type_tuple || !type_tuple->haveExplicitNames())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument for function '{}' must be Named Tuple. Got '{}'",
                getName(), type->getName());

        auto [paths, types] = flattenTuple(type);
        Names names;
        names.reserve(paths.size());
        for (const auto & path : paths)
            names.push_back(path.getPath());

        return std::make_shared<DataTypeTuple>(types, names);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        auto column = arguments.at(0).column;
        if (!checkAndGetColumn<ColumnTuple>(column.get()))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}. Expected ColumnTuple",
                column->getName(), getName());

        return flattenTuple(column);
    }
};

}

REGISTER_FUNCTION(FlattenTuple)
{
    factory.registerFunction<FunctionFlattenTuple>();
}

}
