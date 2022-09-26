#pragma once
#include "config_functions.h"

#if USE_BASEX
#    include <Columns/ColumnConst.h>
#    include <Common/MemorySanitizer.h>
#    include <Columns/ColumnString.h>
#    include <DataTypes/DataTypeString.h>
#    include <Functions/FunctionFactory.h>
#    include <Functions/FunctionHelpers.h>
#    include <IO/WriteHelpers.h>
#    include <base_x.hh>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

struct Base58Encode
{
    static constexpr auto name = "base58Encode";

    static void process(const ColumnString & input, ColumnString::MutablePtr & dst_column, const std::string & alphabet, size_t input_rows_count)
    {
        auto & dst_data = dst_column->getChars();
        auto & dst_offsets = dst_column->getOffsets();

        /// Wikipedia states Base58 has efficiency of 73%, and we take 1.5 scale to avoid reallocation in most cases
        size_t current_allocated_size = ceil(1.5 * input.getChars().size());

        dst_data.resize(current_allocated_size);
        dst_offsets.resize(input_rows_count);

        const ColumnString::Offsets & src_offsets = input.getOffsets();

        const auto * source = input.getChars().raw_data();
        auto * dst = dst_data.data();
        auto * dst_pos = dst;

        size_t src_offset_prev = 0;
        size_t processed_size = 0;

        const auto& encoder = (alphabet == "bitcoin") ? Base58::bitcoin() :
                             ((alphabet == "flickr") ? Base58::flickr() :
                             ((alphabet == "ripple") ? Base58::ripple() :
                                                       Base58::base58())); //GMP

        std::string encoded;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t srclen = src_offsets[row] - src_offset_prev - 1;
            /// Why we didn't use char* here?
            /// We don't know the size of the result string beforehand (it's not byte-to-byte encoding),
            /// so we may need to do many resizes (the worst case -- we'll do it for each row)
            /// This way we do exponential resizes and one final resize after whole operation is complete
            encoded.clear();
            if (srclen)
                try
                {
                    encoder.encode(encoded, source, srclen);
                }
                catch (const std::invalid_argument& e)
                {
                    throw Exception(e.what(), ErrorCodes::BAD_ARGUMENTS);
                }
                catch (const std::domain_error& e)
                {
                    throw Exception(e.what(), ErrorCodes::BAD_ARGUMENTS);
                }
            size_t outlen = encoded.size();

            if (processed_size + outlen >= current_allocated_size)
            {
                current_allocated_size += current_allocated_size;
                dst_data.resize(current_allocated_size);
                auto processed_offset = dst_pos - dst;
                dst = dst_data.data();
                dst_pos = dst;
                dst_pos += processed_offset;
            }
            std::memcpy(dst_pos, encoded.c_str(), ++outlen);

            source += srclen + 1;
            dst_pos += outlen;

            dst_offsets[row] = dst_pos - dst;
            src_offset_prev = src_offsets[row];
            processed_size += outlen;
        }

        dst_data.resize(dst_pos - dst);
    }
};

struct Base58Decode
{
    static constexpr auto name = "base58Decode";

    static void process(const ColumnString & input, ColumnString::MutablePtr & dst_column, const std::string & alphabet, size_t input_rows_count)
    {
        auto & dst_data = dst_column->getChars();
        auto & dst_offsets = dst_column->getOffsets();

        /// We allocate probably even more then needed to avoid many resizes
        size_t current_allocated_size = input.getChars().size();

        dst_data.resize(current_allocated_size);
        dst_offsets.resize(input_rows_count);

        const ColumnString::Offsets & src_offsets = input.getOffsets();

        const auto * source = input.getChars().raw_data();
        auto * dst = dst_data.data();
        auto * dst_pos = dst;

        size_t src_offset_prev = 0;
        size_t processed_size = 0;

        const auto& decoder = (alphabet == "bitcoin") ? Base58::bitcoin() :
                             ((alphabet == "flickr") ? Base58::flickr() :
                             ((alphabet == "ripple") ? Base58::ripple() :
                                                       Base58::base58()));

        std::string decoded;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t srclen = src_offsets[row] - src_offset_prev - 1;
            /// Why we didn't use char* here?
            /// We don't know the size of the result string beforehand (it's not byte-to-byte encoding),
            /// so we may need to do many resizes (the worst case -- we'll do it for each row)
            /// This way we do exponential resizes and one final resize after whole operation is complete
            decoded.clear();
            if (srclen)
                try
                {
                    decoder.decode(decoded, source, srclen);
                }
                catch (const std::invalid_argument& e)
                {
                    throw Exception(e.what(), ErrorCodes::BAD_ARGUMENTS);
                }
                catch (const std::domain_error& e)
                {
                    throw Exception(e.what(), ErrorCodes::BAD_ARGUMENTS);
                }
            size_t outlen = decoded.size();

            if (processed_size + outlen >= current_allocated_size)
            {
                current_allocated_size += current_allocated_size;
                dst_data.resize(current_allocated_size);
                auto processed_offset = dst_pos - dst;
                dst = dst_data.data();
                dst_pos = dst;
                dst_pos += processed_offset;
            }
            std::memcpy(dst_pos, decoded.c_str(), ++outlen);

            source += srclen + 1;
            dst_pos += outlen;

            dst_offsets[row] = dst_pos - dst;
            src_offset_prev = src_offsets[row];
            processed_size += outlen;
        }

        dst_data.resize(dst_pos - dst);
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
        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception(
                "Wrong number of arguments for function " + getName() + ":  1 or 2 expected.",
                ErrorCodes::BAD_ARGUMENTS);

        if (!isString(arguments[0].type))
            throw Exception(
                "Illegal type " + arguments[0].type->getName() + " of 1st argument of function " + getName() + ". Must be String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2 && !isString(arguments[1].type))
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
                "Illegal column " + arguments[0].column->getName() + " of first argument of function " + getName() + ", must be String",
                ErrorCodes::ILLEGAL_COLUMN);

        std::string alphabet = "bitcoin";

        if (arguments.size() == 2)
        {
            const auto * alphabet_column = checkAndGetColumn<ColumnConst>(arguments[1].column.get());

            if (!alphabet_column)
                throw Exception("Second argument for function " + getName() + " must be constant String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            alphabet = alphabet_column->getValue<DB::String>();
            if (alphabet != "bitcoin" && alphabet != "ripple" && alphabet != "flickr" && alphabet != "gmp")
                throw Exception("Second argument for function " + getName() + " must be 'bitcoin', 'ripple', 'gmp' or 'flickr'", ErrorCodes::ILLEGAL_COLUMN);

        }

        auto dst_column = ColumnString::create();

        Func::process(*input, dst_column, alphabet, input_rows_count);

        return dst_column;
    }
};
}

#endif
