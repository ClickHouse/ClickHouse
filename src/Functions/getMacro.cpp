#include <Functions/IFunctionImpl.h>
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

/** Get the value of macro from configuration file.
  * For example, it may be used as a sophisticated replacement for the function 'hostName' if servers have complicated hostnames
  *  but you still need to distinguish them by some convenient names.
  */
class FunctionGetMacro : public IFunction
{
private:
    MultiVersion<Macros>::Version macros;

public:
    static constexpr auto name = "getMacro";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionGetMacro>(context.getMacros());
    }

    explicit FunctionGetMacro(MultiVersion<Macros>::Version macros_) : macros(std::move(macros_)) {}

    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

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

    /** convertToFullColumn needed because in distributed query processing,
      *    each server returns its own value.
      */
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        const IColumn * arg_column = block.getByPosition(arguments[0]).column.get();
        const ColumnString * arg_string = checkAndGetColumnConstData<ColumnString>(arg_column);

        if (!arg_string)
            throw Exception("The argument of function " + getName() + " must be constant String", ErrorCodes::ILLEGAL_COLUMN);

        block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(
            input_rows_count, macros->getValue(arg_string->getDataAt(0).toString()))->convertToFullColumnIfConst();
    }
};


void registerFunctionGetMacro(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGetMacro>();
}

}
