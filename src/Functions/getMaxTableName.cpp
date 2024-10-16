#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <Common/computeMaxTableNameLength.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int UNKNOWN_DATABASE;
}

class FunctionGetMaxTableName : public IFunction, WithContext
{
public:
    static constexpr auto name = "getMaxTableNameForDatabase";
    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionGetMaxTableName>(context_);
    }

    explicit FunctionGetMaxTableName(ContextPtr context_) : WithContext(context_)
    {
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Number of arguments for function {} can't be {}, should be 1", getName(), arguments.size());

        WhichDataType which(arguments[0]);

        if (!which.isStringOrFixedString())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}, expected String or FixedString",
                arguments[0]->getName(), getName());

        return std::make_shared<DataTypeUInt64>();
    }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t allowed_max_length;

        if (!isColumnConst(*arguments[0].column.get()))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The argument of function {} must be constant.", getName());

        const ColumnConst * col_const = checkAndGetColumnConstStringOrFixedString(arguments[0].column.get());
        if (!col_const)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected a constant string as argument for function {}", getName());

        String database_name = col_const->getValue<String>();

        if (!DatabaseCatalog::instance().isDatabaseExist(database_name))
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} doesn't exist.", database_name);

        allowed_max_length = computeMaxTableNameLength(database_name, getContext());
        return DataTypeUInt64().createColumnConst(input_rows_count, allowed_max_length);
    }

private:
    const ColumnConst * checkAndGetColumnConstStringOrFixedString(const IColumn * column) const
    {
        if (const auto * col = checkAndGetColumnConst<ColumnString>(column))
            return col;
        if (const auto * col = checkAndGetColumnConst<ColumnFixedString>(column))
            return col;
        return nullptr;
    }
};

REGISTER_FUNCTION(getMaxTableName)
{
    factory.registerFunction<FunctionGetMaxTableName>();
    factory.registerAlias("getMaxTableName", "getMaxTableNameForDatabase", FunctionFactory::Case::Insensitive);
}

}
