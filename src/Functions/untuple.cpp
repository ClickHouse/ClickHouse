#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/** untuple(named_tuple) is a function that allows you to unfold a named tuple. For example,
  * untuple(tuple(a, b, c)) will generate a column list: a, b, c.
  */
class FunctionUntuple : public IFunction
{
public:
    static constexpr auto name = "untuple";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionUntuple>(); }
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception("Function " + getName() + " requires one or two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() == 2)
        {
            if (!checkAndGetColumnConst<ColumnString>(arguments[1].column.get()))
                throw Exception(
                    "Second argument of function " + getName() + " must be a constant string", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        if (const auto * data_type_tuple = typeid_cast<const DataTypeTuple *>(arguments[0].type.get()))
        {
            if (data_type_tuple->haveExplicitNames())
                return arguments[0].type;
        }
        throw Exception("First argument of function " + getName() + " must be a named tuple", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    void executeImpl(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        columns[result].column = columns[arguments[0]].column;
    }
};

}

void registerFunctionUntuple(FunctionFactory & factory)
{
    factory.registerFunction<FunctionUntuple>();
}

}
