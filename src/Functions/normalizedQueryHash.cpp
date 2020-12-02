#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>
#include <Parsers/Lexer.h>
#include <common/find_symbols.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/SipHash.h>


/** The function returns 64bit hash value that is identical for similar queries.
  * See also 'normalizeQuery'. This function is only slightly more efficient.
  */

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

struct Impl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        PaddedPODArray<UInt64> & res_data)
    {
        size_t size = offsets.size();
        res_data.resize(size);

        ColumnString::Offset prev_src_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            SipHash hash;

            ColumnString::Offset curr_src_offset = offsets[i];
            Lexer lexer(reinterpret_cast<const char *>(&data[prev_src_offset]), reinterpret_cast<const char *>(&data[curr_src_offset - 1]));
            prev_src_offset = offsets[i];

            /// Coalesce a list of comma separated literals.
            size_t num_literals_in_sequence = 0;
            bool prev_comma = false;

            while (true)
            {
                Token token = lexer.nextToken();

                if (!token.isSignificant())
                    continue;

                /// Literals.
                if (token.type == TokenType::Number || token.type == TokenType::StringLiteral)
                {
                    if (0 == num_literals_in_sequence)
                        hash.update("\x00", 1);
                    ++num_literals_in_sequence;
                    prev_comma = false;
                    continue;
                }
                else if (token.type == TokenType::Comma)
                {
                    if (num_literals_in_sequence)
                    {
                        prev_comma = true;
                        continue;
                    }
                }
                else
                {
                    if (num_literals_in_sequence > 1)
                        hash.update("\x00", 1);

                    if (prev_comma)
                        hash.update(",", 1);

                    num_literals_in_sequence = 0;
                    prev_comma = false;
                }

                /// Slightly normalize something that look like aliases - if they are complex, replace them to `?` placeholders.
                if (token.type == TokenType::QuotedIdentifier
                    /// Differentiate identifier from function (example: SHA224(x)).
                    /// By the way, there is padding in columns and pointer dereference is Ok.
                    || (token.type == TokenType::BareWord && *token.end != '('))
                {
                    /// Identifier is complex if it contains whitespace or more than two digits
                    /// or it's at least 36 bytes long (UUID for example).
                    size_t num_digits = 0;

                    const char * pos = token.begin;
                    if (token.size() < 36)
                    {
                        for (; pos != token.end; ++pos)
                        {
                            if (isWhitespaceASCII(*pos))
                                break;

                            if (isNumericASCII(*pos))
                            {
                                ++num_digits;
                                if (num_digits > 2)
                                    break;
                            }
                        }
                    }

                    if (pos == token.end)
                        hash.update(token.begin, token.size());
                    else
                        hash.update("\x01", 1);

                    continue;
                }

                if (token.isEnd() || token.isError())
                    break;

                hash.update(token.begin, token.size());
            }

            res_data[i] = hash.get64();
        }
    }
};

class FunctionNormalizedQueryHash : public IFunction
{
public:
    static constexpr auto name = "normalizedQueryHash";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionNormalizedQueryHash>();
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
        if (!isString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr column = arguments[0].column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnUInt64::create();
            typename ColumnUInt64::Container & vec_res = col_res->getData();
            vec_res.resize(col->size());
            Impl::vector(col->getChars(), col->getOffsets(), vec_res);
            return col_res;
        }
        else
            throw Exception("Illegal column " + arguments[0].column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

}

void registerFunctionNormalizedQueryHash(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNormalizedQueryHash>();
}

}

