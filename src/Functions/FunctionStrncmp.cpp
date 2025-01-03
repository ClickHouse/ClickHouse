#include <algorithm>
#include <limits>
#include <type_traits>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Common/memcmpSmall.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
}

// usage:
// strncmp(s, offset_s, t, offset_t, n)
//  Compare s from offset_s to offset_s + n with t from offset_t to offset_t + n.
class FunctionStrncmp : public IFunction
{
public:
    static constexpr auto name = "strncmp";
    static FunctionPtr create(const ContextPtr /*context*/) { return std::make_shared<FunctionStrncmp>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 5; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeInt32>(); }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override { return std::make_shared<DataTypeInt32>(); }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        checkArguments(arguments);
        auto compare_result_column = ColumnInt32::create();
        compare_result_column->getData().resize(arguments[0].column->size());

        auto a_col = arguments[0].column;
        a_col = recursiveRemoveSparse(a_col->convertToFullColumnIfLowCardinality());
        const auto * a_string = checkAndGetColumn<ColumnString>(a_col.get());
        const auto * a_fixed_string = checkAndGetColumn<ColumnFixedString>(a_col.get());
        const auto * a_const_string = checkAndGetColumn<ColumnConst>(a_col.get());

        auto b_col = arguments[2].column;
        b_col = recursiveRemoveSparse(b_col->convertToFullColumnIfLowCardinality());
        const auto * b_string = checkAndGetColumn<ColumnString>(b_col.get());
        const auto * b_fixed_string = checkAndGetColumn<ColumnFixedString>(b_col.get());
        const auto * b_const_string = checkAndGetColumn<ColumnConst>(b_col.get());

        auto offset_a = arguments[1].column->getUInt(0);
        auto offset_b = arguments[3].column->getUInt(0);
        auto n = arguments[4].column->getUInt(0);

        auto & result = compare_result_column->getData();

        if (a_string && b_const_string)
        {
            executeStringConst<true>(*a_string, offset_a, String(b_const_string->getDataAt(0)), offset_b, n, result);
        }
        else if (a_string && b_string)
        {
            executeStringString(*a_string, offset_a, *b_string, offset_b, n, result);
        }
        else if (a_fixed_string && b_fixed_string)
        {
            executeFixedStringFixedString(*a_fixed_string, offset_a, *b_fixed_string, offset_b, n, result);
        }
        else if (a_fixed_string && b_string)
        {
            executeFixedStringString<true>(*a_fixed_string, offset_a, *b_string, offset_b, n, result);
        }
        else if (a_string && b_fixed_string)
        {
            executeFixedStringString<false>(*b_fixed_string, offset_b, *a_string, offset_a, n, result);
        }
        else if (a_const_string && b_string)
        {
            executeStringConst<false>(*b_string, offset_b, String(a_const_string->getDataAt(0)), offset_a, n, result);
        }
        else if (a_fixed_string && b_const_string)
        {
            executeFixedStringConst<true>(*a_fixed_string, offset_a, String(b_const_string->getDataAt(0)), offset_b, n, result);
        }
        else if (b_fixed_string && a_const_string)
        {
            executeFixedStringConst<false>(*b_fixed_string, offset_b, String(a_const_string->getDataAt(0)), offset_a, n, result);
        }
        else if (a_const_string && b_const_string)
        {
            executeConstConst(
                String(a_const_string->getDataAt(0)),
                offset_a,
                String(b_const_string->getDataAt(0)),
                offset_b,
                n,
                arguments[0].column->size(),
                result);
        }
        else
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal columns {} and {} of arguments of function {}",
                a_col->getName(),
                b_col->getName(),
                getName());
        }
        return std::move(compare_result_column);
    }

private:
    void checkArguments(const ColumnsWithTypeAndName & args) const
    {
        if (args.size() != 5) [[unlikely]]
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires 5 arguments, but got {}.", getName(), args.size());

        if (!WhichDataType(args[0].type).isStringOrFixedString()) [[unlikely]]
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Function {}'s 1st argument must be a string, but got {}.", getName(), args[0].type->getName());

        if (!WhichDataType(args[1].type).isNativeUInt() || !args[1].column->isConst()) [[unlikely]]
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function {}'s 2nd argument must be a constant integer, but got {}.",
                getName(),
                args[1].column->getName());

        if (!WhichDataType(args[2].type).isStringOrFixedString()) [[unlikely]]
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function {}'s 3rd argument must be a string, but got {}.",
                getName(),
                args[2].column->getName());

        if (!WhichDataType(args[3].type).isNativeUInt() || !args[3].column->isConst()) [[unlikely]]
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function {}'s 4th argument must be a constant integer, but got {}.",
                getName(),
                args[3].column->getName());

        if (!WhichDataType(args[4].type).isNativeUInt() || !args[4].column->isConst()) [[unlikely]]
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function {}'s 5th argument must be a constant integer, but got {}.",
                getName(),
                args[4].column->getName());
    }

    template <bool positive = true>
    void executeStringConst(
        const ColumnString & a_strs, size_t offset_a, const String & b_str, size_t offset_b, size_t n, PaddedPODArray<Int32> & result) const
    {
        const auto & a_strs_data = a_strs.getChars();
        const auto & a_offsets = a_strs.getOffsets();
        size_t prev_a_offset = 0;
        auto b_n = offset_b >= b_str.size() ? 0 : std::min(n, b_str.size() - offset_b);
        for (size_t i = 0, size = a_strs.size(); i < size; ++i)
        {
            const auto * a_str = a_strs_data.data() + prev_a_offset + offset_a;
            size_t a_str_len = a_offsets[i] - prev_a_offset - 1;
            if (offset_a >= a_str_len && offset_b >= b_str.size()) [[unlikely]]
            {
                result[i] = 0;
            }
            else if (offset_b >= b_str.size()) [[unlikely]]
            {
                if constexpr (positive)
                    result[i] = 1;
                else
                    result[i] = -1;
            }
            else if (offset_a >= a_str_len) [[unlikely]]
            {
                if constexpr (positive)
                    result[i] = -1;
                else
                    result[i] = 1;
            }
            else
            {
                auto a_n = std::min(n, a_str_len - offset_a);
                if constexpr (positive)
                    result[i] = memcmpSmallLikeZeroPaddedAllowOverflow15(
                        reinterpret_cast<const char *>(a_str), a_n, b_str.data() + offset_b, b_n);
                else
                    result[i] = memcmpSmallLikeZeroPaddedAllowOverflow15(
                        b_str.data() + offset_b, b_n, reinterpret_cast<const char *>(a_str), a_n);
            }

            prev_a_offset = a_offsets[i];
        }
    }

    template <bool positive = true>
    void executeFixedStringConst(
        const ColumnFixedString & a_strs, size_t offset_a, const String & b_str, size_t offset_b, size_t n, PaddedPODArray<Int32> & result)
        const
    {
        size_t a_str_len = a_strs.getN();
        if (a_str_len <= offset_a && b_str.size() <= offset_b) [[unlikely]]
        {
            for (size_t i = 0; i < a_strs.size(); ++i)
                result[i] = 0;
        }
        else if (a_str_len <= offset_a) [[unlikely]]
        {
            for (size_t i = 0; i < a_strs.size(); ++i)
            {
                if constexpr (positive)
                    result[i] = -1;
                else
                    result[i] = 1;
            }
        }
        else if (b_str.size() <= offset_b) [[unlikely]]
        {
            for (size_t i = 0; i < a_strs.size(); ++i)
            {
                if constexpr (positive)
                    result[i] = 1;
                else
                    result[i] = -1;
            }
        }
        else
        {
            const auto & a_strs_data = a_strs.getChars();
            auto a_n = std::min(n, a_str_len - offset_a);
            auto b_n = std::min(n, b_str.size() - offset_b);
            const char * b_str_p = b_str.data() + offset_b;
            for (size_t i = 0; i < a_strs.size(); ++i)
            {
                const auto * a_str = a_strs_data.data() + i * a_str_len + offset_a;

                if constexpr (positive)
                    result[i] = memcmpSmallLikeZeroPaddedAllowOverflow15(reinterpret_cast<const char *>(a_str), a_n, b_str_p, b_n);
                else
                    result[i] = memcmpSmallLikeZeroPaddedAllowOverflow15(b_str_p, b_n, reinterpret_cast<const char *>(a_str), a_n);
            }
        }
    }

    template <bool positive = true>
    void executeFixedStringString(
        const ColumnFixedString & a_strs,
        size_t offset_a,
        const ColumnString & b_strs,
        size_t offset_b,
        size_t n,
        PaddedPODArray<Int32> & result) const
    {
        auto a_str_len = a_strs.getN();
        const auto & a_strs_data = a_strs.getChars();
        const auto & b_strs_data = b_strs.getChars();
        const auto & b_offsets = b_strs.getOffsets();
        ColumnString::Offset prev_b_offset = 0;
        for (size_t i = 0, size = a_strs.size(); i < size; ++i)
        {
            const auto * a_str = a_strs_data.data() + i * a_str_len + offset_a;
            auto a_n = std::min(n, a_str_len - offset_a);
            const auto * b_str = b_strs_data.data() + prev_b_offset + offset_b;
            size_t b_str_len = b_offsets[i] - prev_b_offset - 1;
            if (offset_a >= a_str_len && offset_b >= b_str_len) [[unlikely]]
            {
                result[i] = 0;
            }
            else if (offset_b >= b_str_len) [[unlikely]]
            {
                if constexpr (positive)
                    result[i] = 1;
                else
                    result[i] = -1;
            }
            else if (offset_a >= a_str_len) [[unlikely]]
            {
                if constexpr (positive)
                    result[i] = -1;
                else
                    result[i] = 1;
            }
            else
            {
                auto b_n = offset_b >= b_str_len ? 0 : std::min(n, b_str_len - offset_b);
                if constexpr (positive)
                    result[i] = memcmpSmallLikeZeroPaddedAllowOverflow15(
                        reinterpret_cast<const char *>(a_str), a_n, reinterpret_cast<const char *>(b_str), b_n);
                else
                    result[i] = memcmpSmallLikeZeroPaddedAllowOverflow15(
                        reinterpret_cast<const char *>(b_str), b_n, reinterpret_cast<const char *>(a_str), a_n);
            }
            prev_b_offset = b_offsets[i];
        }
    }

    void executeStringString(
        const ColumnString & a_strs,
        size_t offset_a,
        const ColumnString & b_strs,
        size_t offset_b,
        size_t n,
        PaddedPODArray<Int32> & result) const
    {
        const auto & a_strs_data = a_strs.getChars();
        const auto & a_offsets = a_strs.getOffsets();
        const auto & b_strs_data = b_strs.getChars();
        const auto & b_offsets = b_strs.getOffsets();
        ColumnString::Offset prev_a_offset = 0;
        ColumnString::Offset prev_b_offset = 0;
        for (size_t i = 0, size = a_strs.size(); i < size; ++i)
        {
            const auto * a_str = a_strs_data.data() + prev_a_offset + offset_a;
            size_t a_str_len = a_offsets[i] - prev_a_offset - 1;
            const auto * b_str = b_strs_data.data() + prev_b_offset + offset_b;
            size_t b_str_len = b_offsets[i] - prev_b_offset - 1;

            if (offset_a >= a_str_len && offset_b >= b_str_len) [[unlikely]]
            {
                result[i] = 0;
            }
            else if (offset_a >= a_str_len) [[unlikely]]
            {
                result[i] = -1;
            }
            else if (offset_b >= b_str_len) [[unlikely]]
            {
                result[i] = 1;
            }
            else
            {
                auto a_n = std::min(n, a_str_len - offset_a);
                auto b_n = std::min(n, b_str_len - offset_b);
                result[i] = memcmpSmallLikeZeroPaddedAllowOverflow15(
                    reinterpret_cast<const char *>(a_str), a_n, reinterpret_cast<const char *>(b_str), b_n);
            }

            prev_a_offset = a_offsets[i];
            prev_b_offset = b_offsets[i];
        }
    }

    void executeFixedStringFixedString(
        const ColumnFixedString & a_strs,
        size_t offset_a,
        const ColumnFixedString & b_strs,
        size_t offset_b,
        size_t n,
        PaddedPODArray<Int32> & result) const
    {
        size_t a_str_len = a_strs.getN();
        size_t b_str_len = b_strs.getN();
        if (offset_a >= a_str_len && offset_b >= b_str_len) [[unlikely]]
        {
            for (size_t i = 0; i < a_strs.size(); ++i)
                result[i] = 0;
        }
        else if (offset_a >= a_str_len) [[unlikely]]
        {
            for (size_t i = 0; i < a_strs.size(); ++i)
                result[i] = -1;
        }
        else if (offset_b >= b_str_len) [[unlikely]]
        {
            for (size_t i = 0; i < a_strs.size(); ++i)
                result[i] = 1;
        }
        else
        {
            const auto & a_strs_data = a_strs.getChars();
            const auto & b_strs_data = b_strs.getChars();
            auto a_n = std::min(n, a_str_len - offset_a);
            auto b_n = std::min(n, b_str_len - offset_b);
            for (size_t i = 0, size = a_strs.size(); i < size; ++i)
            {
                const auto * a_str = a_strs_data.data() + i * a_str_len + offset_a;
                const auto * b_str = b_strs_data.data() + i * b_str_len + offset_b;
                result[i] = memcmpSmallLikeZeroPaddedAllowOverflow15(
                    reinterpret_cast<const char *>(a_str), a_n, reinterpret_cast<const char *>(b_str), b_n);
            }
        }
    }

    void executeConstConst(
        const String & a_str, size_t offset_a, const String & b_str, size_t offset_b, size_t n, size_t rows, PaddedPODArray<Int32> & result)
        const
    {
        size_t a_str_len = a_str.size();
        size_t b_str_len = b_str.size();
        Int32 res = 0;
        if (offset_a >= a_str_len && offset_b >= b_str_len) [[unlikely]]
        {
            res = 0;
        }
        else if (offset_a >= a_str_len) [[unlikely]]
        {
            res = -1;
        }
        else if (offset_b >= b_str_len) [[unlikely]]
        {
            res = 1;
        }
        else
        {
            auto a_n = std::min(n, a_str_len - offset_a);
            auto b_n = std::min(n, b_str_len - offset_b);
            res = memcmpSmallLikeZeroPaddedAllowOverflow15(a_str.data() + offset_a, a_n, b_str.data() + offset_b, b_n);
        }
        for (size_t i = 0; i < rows; ++i)
            result[i] = res;
    }
};

REGISTER_FUNCTION(Strncmp)
{
    factory.registerFunction<FunctionStrncmp>(FunctionDocumentation{
        .description = R"(
            This function could compare parts of two strings directly, without the need to copy the parts of the string into new columns.
            )",
        .examples{{"strncmp", "select strncmp(s, 0, 'abc', 0, 3)", ""}},
        .categories{"String"}});
}
}
