#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


template <typename Impl, typename Name, bool is_injective = false>
class FunctionStringToString : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionStringToString>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isInjective(const Block &) const override
    {
        return is_injective;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();
            Impl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets());
            block.getByPosition(result).column = std::move(col_res);
        }
        else if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            auto col_res = ColumnFixedString::create(col_fixed->getN());
            Impl::vectorFixed(col_fixed->getChars(), col_fixed->getN(), col_res->getChars());
            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

}
