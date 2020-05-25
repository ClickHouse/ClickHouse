#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <ext/map.h>
#include <../contrib/hyperscan/src/hs.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct extractOSFromUserAgentImpl
{
    static void vector(const ColumnString::Chars & data,
                       const ColumnString::Offsets & offsets,
                       ColumnString::Chars & res_data,
                       ColumnString::Offsets & res_offsets, const Context & context)
    {
        res_data.reserve(data.size());
        res_offsets.resize(offsets.size());

        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t cur_offset = offsets[i];

            const char * user_agent_string = reinterpret_cast<const char *>(&data[prev_offset]);
            const auto operating_system = context.getUserAgent()->detect(user_agent_string).getOperatingSystem().getName();
            const auto operating_system_version = toString(operating_system);

            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], operating_system_version.c_str(), operating_system_version.length());
            res_offset += operating_system_version.length() + 1;
            res_offsets[i] = res_offset;

            prev_offset = cur_offset;
        }
    }

    static void vectorFixed(const ColumnString::Chars & data, size_t n, ColumnString::Chars & res_data)
    {
        res_data.resize(data.size());
        size_t size = data.size() / n;

        for (size_t i = 0; i < size; ++i)
            for (size_t j = i * n; j < (i + 1) * n; ++j)
                res_data[j] = data[(i * 2 + 1) * n - j - 1];
    }
};


class extractOSFromUserAgent : public IFunction
{
public:
    static constexpr auto name = "extractOSFromUserAgent";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<extractOSFromUserAgent>(context);
    }

    explicit extractOSFromUserAgent(const Context & context_) : context(context_) {}

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
        return true;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]))
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
            extractOSFromUserAgentImpl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets(), context);
            block.getByPosition(result).column = std::move(col_res);
        }
        else if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
            // auto col_res = ColumnFixedString::create(col_fixed->getN());
            // extractBrowserFromUserAgentImpl::vectorFixed(col_fixed->getChars(), col_fixed->getN(), col_res->getChars());
            // block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

private:
    const Context & context;
};


void registerFunctionExtractOSFromUserAgent(FunctionFactory & factory)
{
    factory.registerFunction<extractOSFromUserAgent>(FunctionFactory::CaseInsensitive);
}

}
