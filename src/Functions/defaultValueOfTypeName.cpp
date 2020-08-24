#include <Functions/IFunctionImpl.h>
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

/// Returns global default value for type name (example: 0 for numeric types, '' for String).
class FunctionDefaultValueOfTypeName : public IFunction
{
public:
    static constexpr auto name = "defaultValueOfTypeName";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionDefaultValueOfTypeName>();
    }

    String getName() const override
    {
        return name;
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
            throw Exception("The argument of function " + getName() + " must be a constant string describing type. "
                    "Instead there is a column with the following structure: " + arguments.front().column->dumpStructure(),
                    ErrorCodes::ILLEGAL_COLUMN);

        return DataTypeFactory::instance().get(col_type_const->getValue<String>());
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) const override
    {
        const IDataType & type = *block.getByPosition(result).type;
        block.getByPosition(result).column = type.createColumnConst(input_rows_count, type.getDefault());
    }
};


void registerFunctionDefaultValueOfTypeName(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDefaultValueOfTypeName>();
}

}
