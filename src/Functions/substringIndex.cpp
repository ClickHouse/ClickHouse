#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Columns/ColumnString.h>
#include "base/find_symbols.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ZERO_ARRAY_OR_TUPLE_INDEX;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

template <bool is_utf8>
class FunctionSubstringIndex : public IFunction
{
public:
    static constexpr auto name = is_utf8 ? "substringIndexUTF8" : "substringIndex";


    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionSubstringIndex>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 3; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}",
                arguments[0]->getName(),
                getName());

        if (!isString(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}",
                arguments[1]->getName(),
                getName());

        if (!isNativeNumber(arguments[2]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of third argument of function {}",
                            arguments[2]->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr column_string = arguments[0].column;
        ColumnPtr column_delim = arguments[1].column;
        ColumnPtr column_index = arguments[2].column;

        const ColumnConst * column_delim_const = checkAndGetColumnConst<ColumnString>(column_delim.get());
        if (!column_delim_const)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN , "Second argument to {} must be a constant String", getName());

        String delim = column_delim_const->getValue<String>();
        if constexpr (!is_utf8)
        {
            if (delim.size() != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second argument to {} must be a single character", getName());
        }
        else
        {
            // TODO
        }

        auto column_res = ColumnString::create();
        ColumnString::Chars & vec_res = column_res->getChars();
        ColumnString::Offsets & offsets_res = column_res->getOffsets();

        const ColumnConst * column_string_const = checkAndGetColumnConst<ColumnString>(column_string.get());
        if (column_string_const)
        {
            String str = column_string_const->getValue<String>();
            constantVector(str, delim[0], column_index.get(), vec_res, offsets_res);
        }
        else
        {
            const auto * col_str = checkAndGetColumn<ColumnString>(column_string.get());
            if (!col_str)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument to {} must be a String", getName());

            bool is_index_const = isColumnConst(*column_index);
            if (is_index_const)
            {
                Int64 index = column_index->getInt(0);
                vectorConstant(col_str->getChars(), col_str->getOffsets(), delim[0], index, vec_res, offsets_res);
            }
            else
                vectorVector(col_str->getChars(), col_str->getOffsets(), delim[0], column_index.get(), vec_res, offsets_res);
        }
    }

protected:
    static void vectorVector(
        const ColumnString::Chars & str_data,
        const ColumnString::Offsets & str_offsets,
        char delim,
        const IColumn * index_column,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        size_t rows = str_offsets.size();
        res_data.reserve(str_data.size() / 2);
        res_offsets.reserve(rows);

        for (size_t i=0; i<rows; ++i)
        {
            StringRef str_ref{&str_data[str_offsets[i]], str_offsets[i] - str_offsets[i - 1] - 1};
            Int64 index = index_column->getInt(i);
            StringRef res_ref = substringIndex<delim>(str_ref, index);
            appendToResultColumn(res_ref, res_data, res_offsets);
        }
    }

    static void vectorConstant(
        const ColumnString::Chars & str_data,
        const ColumnString::Offsets & str_offsets,
        char delim,
        Int64 index,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        size_t rows = str_offsets.size();
        res_data.reserve(str_data.size() / 2);
        res_offsets.reserve(rows);

        for (size_t i = 0; i<rows; ++i)
        {
            StringRef str_ref{&str_data[str_offsets[i]], str_offsets[i] - str_offsets[i - 1] - 1};
            StringRef res_ref = substringIndex<delim>(str_ref, index);
            appendToResultColumn(res_ref, res_data, res_offsets);
        }
    }

    static void constantVector(
        const String & str,
        char delim,
        const IColumn * index_column,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        size_t rows = index_column->size();
        res_data.reserve(str.size() * rows / 2);
        res_offsets.reserve(rows);

        StringRef str_ref{str.data(), str.size()};
        for (size_t i=0; i<rows; ++i)
        {
            Int64 index = index_column->getInt(i);
            StringRef res_ref = substringIndex<delim>(str_ref, index);
            appendToResultColumn(res_ref, res_data, res_offsets);
        }
    }

    static void appendToResultColumn(
        const StringRef & res_ref, ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets)
    {
        size_t res_offset = res_data.size();
        res_data.resize(res_offset + res_ref.size + 1);
        memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], res_ref.data, res_ref.size);
        res_offset += res_ref.size;
        res_data[res_offset] = 0;
        ++res_offset;

        res_offsets.emplace_back(res_offset);
    }

    template <char delim>
    static StringRef substringIndex(
        const StringRef & str,
        Int64 index)
    {
        if (index == 0)
            return {str.data, 0};

        if (index > 0)
        {
            const auto * end = str.data + str.size;
            const auto * pos = str.data;
            Int64 i = 0;
            while (i < index)
            {
                pos = find_first_symbols<delim>(pos, end);

                if (pos != end)
                {
                    ++pos;
                    ++i;
                }
                else
                    return str;
            }
            return {str.data, static_cast<size_t>(pos - str.data)};
        }
        else
        {
            const auto * begin = str.data;
            const auto * pos = str.data + str.size;
            Int64 i = 0;
            while (i < index)
            {
                const auto * next_pos = detail::find_last_symbols_sse2<true, detail::ReturnMode::End, delim>(begin, pos);

                if (next_pos != pos)
                {
                    pos = next_pos;
                    ++i;
                }
                else
                    return str;
            }

            return {pos + 1, static_cast<size_t>(str.data + str.size - pos - 1)};
        }
    }
};
}

}

