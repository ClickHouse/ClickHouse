#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/PositionImpl.h>
#include <Interpreters/Context_fwd.h>
#include <base/find_symbols.h>
#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Common/register_objects.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

    template <bool is_utf8>
    class FunctionSubstringIndex : public IFunction
    {
    public:
        static constexpr auto name = is_utf8 ? "substringIndexUTF8" : "substringIndex";


        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSubstringIndex>(); }

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 3; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        bool useDefaultImplementationForConstants() const override { return true; }
        ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (!isString(arguments[0]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of first argument of function {}, String expected",
                    arguments[0]->getName(),
                    getName());

            if (!isString(arguments[1]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of second argument of function {}, String expected",
                    arguments[1]->getName(),
                    getName());

            if (!isNativeInteger(arguments[2]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of third argument of function {}, Integer expected",
                    arguments[2]->getName(),
                    getName());

            return std::make_shared<DataTypeString>();
        }

        DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
        {
            return std::make_shared<DataTypeString>();
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            ColumnPtr column_string = arguments[0].column;
            ColumnPtr column_delim = arguments[1].column;
            ColumnPtr column_count = arguments[2].column;

            const ColumnConst * column_delim_const = checkAndGetColumnConst<ColumnString>(column_delim.get());
            if (!column_delim_const)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument to {} must be a constant String", getName());

            String delim = column_delim_const->getValue<String>();
            if constexpr (!is_utf8)
            {
                if (delim.size() != 1)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second argument to {} must be a single character", getName());
            }
            else
            {
                if (UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(delim.data()), delim.size()) != 1)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second argument to {} must be a single UTF-8 character", getName());
            }

            auto column_res = ColumnString::create();
            ColumnString::Chars & vec_res = column_res->getChars();
            ColumnString::Offsets & offsets_res = column_res->getOffsets();

            const ColumnConst * column_string_const = checkAndGetColumnConst<ColumnString>(column_string.get());
            if (column_string_const)
            {
                String str = column_string_const->getValue<String>();
                constantVector(str, delim, column_count.get(), vec_res, offsets_res);
            }
            else
            {
                const auto * col_str = checkAndGetColumn<ColumnString>(column_string.get());
                if (!col_str)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument to {} must be a String", getName());

                bool is_count_const = isColumnConst(*column_count);
                if (is_count_const)
                {
                    Int64 count = column_count->getInt(0);
                    vectorConstant(col_str, delim, count, vec_res, offsets_res, input_rows_count);
                }
                else
                    vectorVector(col_str, delim, column_count.get(), vec_res, offsets_res, input_rows_count);
            }
            return column_res;
        }

    protected:
        static void vectorVector(
            const ColumnString * str_column,
            const String & delim,
            const IColumn * count_column,
            ColumnString::Chars & res_data,
            ColumnString::Offsets & res_offsets,
            size_t input_rows_count)
        {
            res_data.reserve(str_column->getChars().size() / 2);
            res_offsets.reserve(input_rows_count);

            bool all_ascii = isAllASCII(str_column->getChars().data(), str_column->getChars().size())
                && isAllASCII(reinterpret_cast<const UInt8 *>(delim.data()), delim.size());
            std::unique_ptr<PositionCaseSensitiveUTF8::SearcherInBigHaystack> searcher
                = !is_utf8 || all_ascii ? nullptr : std::make_unique<PositionCaseSensitiveUTF8::SearcherInBigHaystack>(delim.data(), delim.size());

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                StringRef str_ref = str_column->getDataAt(i);
                Int64 count = count_column->getInt(i);

                StringRef res_ref;
                if constexpr (!is_utf8)
                    res_ref = substringIndex(str_ref, delim[0], count);
                else if (all_ascii)
                    res_ref = substringIndex(str_ref, delim[0], count);
                else
                    res_ref = substringIndexUTF8(searcher.get(), str_ref, delim, count);

                appendToResultColumn<true>(res_ref, res_data, res_offsets);
            }
        }

        static void vectorConstant(
            const ColumnString * str_column,
            const String & delim,
            Int64 count,
            ColumnString::Chars & res_data,
            ColumnString::Offsets & res_offsets,
            size_t input_rows_count)
        {
            res_data.reserve(str_column->getChars().size() / 2);
            res_offsets.reserve(input_rows_count);

            bool all_ascii = isAllASCII(str_column->getChars().data(), str_column->getChars().size())
                && isAllASCII(reinterpret_cast<const UInt8 *>(delim.data()), delim.size());
            std::unique_ptr<PositionCaseSensitiveUTF8::SearcherInBigHaystack> searcher
                = !is_utf8 || all_ascii ? nullptr : std::make_unique<PositionCaseSensitiveUTF8::SearcherInBigHaystack>(delim.data(), delim.size());

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                StringRef str_ref = str_column->getDataAt(i);

                StringRef res_ref;
                if constexpr (!is_utf8)
                    res_ref = substringIndex(str_ref, delim[0], count);
                else if (all_ascii)
                    res_ref = substringIndex(str_ref, delim[0], count);
                else
                    res_ref = substringIndexUTF8(searcher.get(), str_ref, delim, count);

                appendToResultColumn<true>(res_ref, res_data, res_offsets);
            }
        }

        static void constantVector(
            const String & str,
            const String & delim,
            const IColumn * count_column,
            ColumnString::Chars & res_data,
            ColumnString::Offsets & res_offsets)
        {
            size_t rows = count_column->size();
            res_data.reserve(str.size() * rows / 2);
            res_offsets.reserve(rows);

            bool all_ascii = isAllASCII(reinterpret_cast<const UInt8 *>(str.data()), str.size())
                && isAllASCII(reinterpret_cast<const UInt8 *>(delim.data()), delim.size());
            std::unique_ptr<PositionCaseSensitiveUTF8::SearcherInBigHaystack> searcher
                = !is_utf8 || all_ascii ? nullptr : std::make_unique<PositionCaseSensitiveUTF8::SearcherInBigHaystack>(delim.data(), delim.size());

            StringRef str_ref{str.data(), str.size()};
            for (size_t i = 0; i < rows; ++i)
            {
                Int64 count = count_column->getInt(i);

                StringRef res_ref;
                if constexpr (!is_utf8)
                    res_ref = substringIndex(str_ref, delim[0], count);
                else if (all_ascii)
                    res_ref = substringIndex(str_ref, delim[0], count);
                else
                    res_ref = substringIndexUTF8(searcher.get(), str_ref, delim, count);

                appendToResultColumn<false>(res_ref, res_data, res_offsets);
            }
        }

        template <bool padded>
        static void appendToResultColumn(const StringRef & res_ref, ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets)
        {
            size_t res_offset = res_data.size();
            res_data.resize(res_offset + res_ref.size + 1);

            if constexpr (padded)
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], res_ref.data, res_ref.size);
            else
                memcpy(&res_data[res_offset], res_ref.data, res_ref.size);

            res_offset += res_ref.size;
            res_data[res_offset] = 0;
            ++res_offset;

            res_offsets.emplace_back(res_offset);
        }

        static StringRef substringIndexUTF8(
            const PositionCaseSensitiveUTF8::SearcherInBigHaystack * searcher, const StringRef & str_ref, const String & delim, Int64 count)
        {
            if (count == 0)
                return {str_ref.data, 0};

            const auto * begin = reinterpret_cast<const UInt8 *>(str_ref.data);
            const auto * end = reinterpret_cast<const UInt8 *>(str_ref.data + str_ref.size);
            const auto * pos = begin;
            if (count > 0)
            {
                Int64 i = 0;
                while (i < count)
                {
                    pos = searcher->search(pos, end - pos);

                    if (pos != end)
                    {
                        pos += delim.size();
                        ++i;
                    }
                    else
                        return str_ref;
                }
                return {begin, static_cast<size_t>(pos - begin - delim.size())};
            }

            Int64 total = 0;
            while (pos < end && end != (pos = searcher->search(pos, end - pos)))
            {
                pos += delim.size();
                ++total;
            }

            if (total + count < 0)
                return str_ref;

            pos = begin;
            Int64 i = 0;
            Int64 count_from_left = total + 1 + count;
            while (i < count_from_left && pos < end && end != (pos = searcher->search(pos, end - pos)))
            {
                pos += delim.size();
                ++i;
            }
            return {pos, static_cast<size_t>(end - pos)};
        }

        static StringRef substringIndex(const StringRef & str_ref, char delim, Int64 count)
        {
            if (count == 0)
                return {str_ref.data, 0};

            const auto * pos = count > 0 ? str_ref.data : str_ref.data + str_ref.size - 1;
            const auto * end = count > 0 ? str_ref.data + str_ref.size : str_ref.data - 1;
            int d = count > 0 ? 1 : -1;

            for (; count; pos += d)
            {
                if (pos == end)
                    return str_ref;
                if (*pos == delim)
                    count -= d;
            }
            pos -= d;
            return {
                d > 0 ? str_ref.data : pos + 1, static_cast<size_t>(d > 0 ? pos - str_ref.data : str_ref.data + str_ref.size - pos - 1)};
        }
    };
}


REGISTER_FUNCTION(SubstringIndex)
{
    factory.registerFunction<FunctionSubstringIndex<false>>(); /// substringIndex
    factory.registerFunction<FunctionSubstringIndex<true>>(); /// substringIndexUTF8

    factory.registerAlias("SUBSTRING_INDEX", "substringIndex", FunctionFactory::Case::Insensitive);
}


}
