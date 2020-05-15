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


/** Reverse the string as a sequence of bytes.
  */
struct extractBrowserFromUserAgentImpl
{
    static void vector(const ColumnString::Chars & data,
                       const ColumnString::Offsets & offsets,
                       ColumnString::Chars & res_data,
                       ColumnString::Offsets & res_offsets, const Context & context)
    {
        res_data.resize(data.size());
        res_offsets.assign(offsets);
        size_t size = offsets.size();

        hs_scratch_t * scratch = nullptr;
        hs_error_t err = hs_clone_scratch(context.getHyperscanBrowserBase()->getScratch(), &scratch);

        if (err != HS_SUCCESS)
            throw Exception("Could not clone scratch space for hyperscan", ErrorCodes::CANNOT_ALLOCATE_MEMORY);

        MultiRegexps::ScratchPtr smart_scratch(scratch);

        auto on_match = []([[maybe_unused]] unsigned int id,
                           unsigned long long /* from */, // NOLINT
                           unsigned long long /* to */, // NOLINT
                           unsigned int /* flags */,
                           void * ctx) -> int
        {
            *reinterpret_cast<UInt8 *>(ctx) = id;
            return 1;
        };
        const size_t haystack_offsets_size = offsets.size();
        UInt64 offset = 0;
        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < haystack_offsets_size; ++i)
        {
            UInt64 length = offsets[i] - offset - 1;
            /// Hyperscan restriction.
            if (length > std::numeric_limits<UInt32>::max())
                throw Exception("Too long string to search", 1);
            /// Zero the result, scan, check, update the offset.
            int a = 0;
            err = hs_scan(
                context.getHyperscanBrowserBase()->getDB(),
                reinterpret_cast<const char *>(offsets.data()) + offset,
                length,
                0,
                smart_scratch.get(),
                on_match,
                &a);
            if (err != HS_SUCCESS && err != HS_SCAN_TERMINATED)
                throw Exception("Failed to scan with hyperscan", 2);
            offset = offsets[i];
            if (a == 1)
            {
                const char * url_begin = "mobile";
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], url_begin, 6);
                res_offset += 6;
                res_offsets[i] = res_offset;
            }
            else if (a == 2)
            {
                const char * url_begin = "chrome";
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], url_begin, 6);
                res_offset += 6;
                res_offsets[i] = res_offset;
            }
            else
            {
                const char * url_begin = "hz";
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], url_begin, 2);
                res_offset += 2;
                res_offsets[i] = res_offset;
            }
        }

        for (size_t i = 0; i < size; ++i)
        {
            for (size_t j = prev_offset; j < offsets[i] - 1; ++j)
                res_data[j] = data[offsets[i] + prev_offset - 2 - j];
            res_data[offsets[i] - 1] = 0;
            prev_offset = offsets[i];
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


class extractBrowserFromUserAgent : public IFunction
{
public:
    static constexpr auto name = "extractBrowserFromUserAgent";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<extractBrowserFromUserAgent>(context);
    }

    explicit extractBrowserFromUserAgent(const Context & context_) : context(context_) {}

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
            extractBrowserFromUserAgentImpl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets());
            block.getByPosition(result).column = std::move(col_res);
        }
        else if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            auto col_res = ColumnFixedString::create(col_fixed->getN());
            extractBrowserFromUserAgentImpl::vectorFixed(col_fixed->getChars(), col_fixed->getN(), col_res->getChars());
            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

private:
    const Context & context;
};


void registerFunctionExtractBrowserFromUserAgent(FunctionFactory & factory)
{
    factory.registerFunction<extractBrowserFromUserAgent>(FunctionFactory::CaseInsensitive);
}

}
