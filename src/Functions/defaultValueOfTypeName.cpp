#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Core/Field.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// Returns global default value for type name (example: 0 for numeric types, '' for String).
class FunctionDefaultValueOfTypeName : public IFunction
{
public:
    static constexpr auto name = "defaultValueOfTypeName";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionDefaultValueOfTypeName>();
    }

    String getName() const override
    {
        return name;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const ColumnConst * col_type_const = typeid_cast<const ColumnConst *>(arguments.front().column.get());
        if (!col_type_const || !isString(arguments.front().type))
            throw Exception("The argument of function " + getName() + " must be a constant string describing type.",
                ErrorCodes::ILLEGAL_COLUMN);

        return DataTypeFactory::instance().get(col_type_const->getValue<String>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType & type = *result_type;
        return type.createColumnConst(input_rows_count, type.getDefault());
    }
};

}

void registerFunctionDefaultValueOfTypeName(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDefaultValueOfTypeName>();
}

}
