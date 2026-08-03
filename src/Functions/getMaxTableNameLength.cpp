#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
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
    extern const int INCORRECT_DATA;
}

class FunctionGetMaxTableNameLengthForDatabase : public IFunction
{
public:
    static constexpr auto name = "getMaxTableNameLengthForDatabase";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGetMaxTableNameLengthForDatabase>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t allowed_max_length;

        if (!isColumnConst(*arguments[0].column.get()))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The argument of function {} must be constant.", getName());

        const ColumnConst * col_const = checkAndGetColumnConstStringOrFixedString(arguments[0].column.get());
        if (!col_const)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected a constant string as argument for function {}", getName());

        String database_name = col_const->getValue<String>();

        if (database_name.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect name for a database. It shouldn't be empty");

        allowed_max_length = computeMaxTableNameLength(database_name, Context::getGlobalContextInstance());
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
    factory.registerFunction<FunctionGetMaxTableNameLengthForDatabase>(FunctionDocumentation{
        .description=R"(Returns the maximum table name length in a specified database.)",
        .syntax=R"(getMaxTableNameLengthForDatabase(database_name))",
        .arguments={{"database_name", "The name of the specified database.", {"String"}}},
        .returned_value={R"(Returns the length of the maximum table name, an Integer)"},
        .examples{
            {"typical",
            "SELECT getMaxTableNameLengthForDatabase('default');",
            R"(
            ┌─getMaxTableNameLengthForDatabase('default')─┐
            │                                         206 │
            └─────────────────────────────────────────────┘
            )"
        }},
        .introduced_in = {25, 1},
        .category = FunctionDocumentation::Category::Other
    });
}

}
