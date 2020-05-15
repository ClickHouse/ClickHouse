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
        // size_t size = offsets.size();
        // std::vector<StringRef> regex = {"aa", "bb", "cc"};
        // auto * hyperscan_browser_base = MultiRegexps::get<true, false>(regex, 0);

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
            return 0;
        };
        size_t prev_offset = 0;
        size_t res_offset = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t cur_offset = offsets[i];

            const char * cur_str = reinterpret_cast<const char *>(&data[prev_offset]);
            UInt64 length = 2;
            /// Hyperscan restriction.
            if (length > std::numeric_limits<UInt32>::max())
                throw Exception("Too long string to search", 1);
            /// Zero the result, scan, check, update the offset.
            UInt8 a = 2;
            err = hs_scan(
                context.getHyperscanBrowserBase()->getDB(),
                cur_str,
                length,
                0,
                smart_scratch.get(),
                on_match,
                &a);
            if (err != HS_SUCCESS && err != HS_SCAN_TERMINATED)
                throw Exception("Failed to scan with hyperscan", 2);
            char ret[1];
            if (a == 0)
            {
                ret[0] = 'a'; 
            }
            else if (a == 1)
            {
                ret[0] = 'b'; 
            }
            else if (a == 2)
            {
                ret[0] = 'c'; 
            }
            else if (a == 3)
            {
                ret[0] = 'd'; 
            }
            else
            {
                ret[0] = 'x';
            }

            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], ret, 1);
            res_offset += 2;
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
            extractBrowserFromUserAgentImpl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets(), context);
            block.getByPosition(result).column = std::move(col_res);
        }
        else if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
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
