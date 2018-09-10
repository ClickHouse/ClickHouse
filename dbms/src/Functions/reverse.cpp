#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <ext/map.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/** Reverse the string as a sequence of bytes.
  */
struct ReverseImpl
{
    static void vector(const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.resize(data.size());
        res_offsets.assign(offsets);
        size_t size = offsets.size();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (size_t j = prev_offset; j < offsets[i] - 1; ++j)
                res_data[j] = data[offsets[i] + prev_offset - 2 - j];
            res_data[offsets[i] - 1] = 0;
            prev_offset = offsets[i];
        }
    }

    static void vector_fixed(const ColumnString::Chars_t & data, size_t n, ColumnString::Chars_t & res_data)
    {
        res_data.resize(data.size());
        size_t size = data.size() / n;

        for (size_t i = 0; i < size; ++i)
            for (size_t j = i * n; j < (i + 1) * n; ++j)
                res_data[j] = data[(i * 2 + 1) * n - j - 1];
    }
};


class FunctionReverse : public IFunction
{
public:
    static constexpr auto name = "reverse";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionReverse>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isInjective(const Block &) override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0])
            && !isArray(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();
            ReverseImpl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets());
            block.getByPosition(result).column = std::move(col_res);
        }
        else if (const ColumnFixedString * col = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            auto col_res = ColumnFixedString::create(col->getN());
            ReverseImpl::vector_fixed(col->getChars(), col->getN(), col_res->getChars());
            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Also works with arrays.
class FunctionBuilderReverse : public FunctionBuilderImpl
{
public:
    static constexpr auto name = "reverse";
    static FunctionBuilderPtr create(const Context & context) { return std::make_shared<FunctionBuilderReverse>(context); }

    FunctionBuilderReverse(const Context & context) : context(context) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

protected:
    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        if (isArray(arguments.at(0).type))
            return FunctionFactory::instance().get("arrayReverse", context)->build(arguments);
        else
            return std::make_shared<DefaultFunction>(
                FunctionReverse::create(context),
                ext::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
                return_type);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return arguments.at(0);
    }

private:
    const Context & context;
};


void registerFunctionReverse(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBuilderReverse>();
}

}
