#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Interpreters/Context.h>
#include <Common/Macros.h>
#include <Core/Field.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/** Get the value of macro from configuration file.
  * For example, it may be used as a sophisticated replacement for the function 'hostName' if servers have complicated hostnames
  *  but you still need to distinguish them by some convenient names.
  */
class FunctionGetMacro : public IFunction
{
private:
    MultiVersion<Macros>::Version macros;
    bool is_distributed;

public:
    static constexpr auto name = "getMacro";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionGetMacro>(context->getMacros(), context->isDistributed());
    }

    explicit FunctionGetMacro(MultiVersion<Macros>::Version macros_, bool is_distributed_)
        : macros(std::move(macros_)), is_distributed(is_distributed_)
    {
    }

    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return true;
    }

    /// getMacro may return different values on different shards/replicas, so it's not constant for distributed query
    bool isSuitableForConstantFolding() const override { return !is_distributed; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception("The argument of function " + getName() + " must have String type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IColumn * arg_column = arguments[0].column.get();
        const ColumnString * arg_string = checkAndGetColumnConstData<ColumnString>(arg_column);

        if (!arg_string)
            throw Exception("The argument of function " + getName() + " must be constant String", ErrorCodes::ILLEGAL_COLUMN);

        return result_type->createColumnConst(input_rows_count, macros->getValue(arg_string->getDataAt(0).toString()));
    }
};

}

REGISTER_FUNCTION(GetMacro)
{
    factory.registerFunction<FunctionGetMacro>();
}

}
