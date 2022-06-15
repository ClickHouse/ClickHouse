#pragma once
#include "config_functions.h"

#if USE_BASE58
#    include <Columns/ColumnConst.h>
#    include <Common/MemorySanitizer.h>
#    include <Columns/ColumnString.h>
#    include <DataTypes/DataTypeString.h>
#    include <Functions/FunctionFactory.h>
#    include <Functions/FunctionHelpers.h>
#    include <Functions/GatherUtils/Algorithms.h>
#    include <IO/WriteHelpers.h>
#    include <base_x.hh>


namespace DB
{
using namespace GatherUtils;

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
    extern const int BAD_ARGUMENTS;
}

struct Base58Encode
{
    static constexpr auto name = "base58Encode";
    static size_t getBufferSize(size_t string_length, size_t string_count)
    {
        return ((string_length - string_count) / 3 + string_count) * 4 + string_count;
    }

    void process(ColumnString source, ColumnString result, std::string alphabet)
    {

    }
};

struct Base58Decode
{
    static constexpr auto name = "base58Decode";

    static size_t getBufferSize(size_t string_length, size_t string_count)
    {
        return ((string_length - string_count) / 4 + string_count) * 3 + string_count;
    }
};

struct TryBase58Decode
{
    static constexpr auto name = "tryBase58Decode";

    static size_t getBufferSize(size_t string_length, size_t string_count)
    {
        return Base58Decode::getBufferSize(string_length, string_count);
    }
};

template <typename Func>
class FunctionBase58Conversion : public IFunction
{
public:
    static constexpr auto name = Func::name;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionBase58Conversion>();
    }

    String getName() const override
    {
        return Func::name;
    }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1 || arguments.size() != 2)
            throw Exception(
                "Wrong number of arguments for function " + getName() + ":  " + arguments.size() + " provided, 1 or 2 expected.",
                ErrorCodes::BAD_ARGUMENTS);

        if (!isString(arguments[0].type))
            throw Exception(
                "Illegal type " + arguments[0].type->getName() + " of 1st argument of function " + getName() + ". Must be String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isString(arguments[1].type))
            throw Exception(
                "Illegal type " + arguments[1].type->getName() + " of 2nd argument of function " + getName() + ". Must be String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr column_string = arguments[0].column;
        const ColumnString * input = checkAndGetColumn<ColumnString>(column_string.get());
        if (!input)
            throw Exception(
                "Illegal column " + arguments[0].column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        std::string alphabet = "bitcoin";

        if (arguments.size() == 2)
        {
            const auto * alphabet_column = checkAndGetColumn<ColumnConst>(arguments[1].column.get());

            if (!alphabet_column)
                throw Exception("Second argument for function " + getName() + " must be constant String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if (alphabet = alphabet_column->getValue<DB::String>(); alphabet != "bitcoin" && alphabet != "ripple" && alphabet != "flickr" && alphabet != "gmp")
                throw Exception("Second argument for function " + getName() + " must be 'bitcoin', 'ripple', 'flickr' or 'gmp'", ErrorCodes::ILLEGAL_COLUMN);

        }

        auto dst_column = ColumnString::create();
        auto & dst_data = dst_column->getChars();
        auto & dst_offsets = dst_column->getOffsets();

        size_t reserve = Func::getBufferSize(input->getChars().size(), input->size());
        dst_data.resize(reserve);
        dst_offsets.resize(input_rows_count);

        const ColumnString::Offsets & src_offsets = input->getOffsets();

        const auto * source = input->getChars().data();
        auto * dst = dst_data.data();
        auto * dst_pos = dst;

        size_t src_offset_prev = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t srclen = src_offsets[row] - src_offset_prev - 1;
            size_t outlen = 0;

            if constexpr (std::is_same_v<Func, Base58Encode>)
            {
                Base58::
            }
            else if constexpr (std::is_same_v<Func, Base58Decode>)
            {
            }
            else
            {
            }

            source += srclen + 1;
            dst_pos += outlen;
            *dst_pos = '\0';
            dst_pos += 1;

            dst_offsets[row] = dst_pos - dst;
            src_offset_prev = src_offsets[row];
        }

        dst_data.resize(dst_pos - dst);

        return dst_column;
    }
};
}

#endif
