#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/BitHelpers.h>
#include <Common/hex.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/castColumn.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

constexpr size_t uuid_bytes_length = 16;
constexpr size_t uuid_text_length = 36;

class FunctionUUIDNumToString : public IFunction
{

public:
    static constexpr auto name = "UUIDNumToString";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionUUIDNumToString>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * ptr = checkAndGetDataType<DataTypeFixedString>(arguments[0].get());
        if (!ptr || ptr->getN() != uuid_bytes_length)
            throw Exception("Illegal type " + arguments[0]->getName() +
                            " of argument of function " + getName() +
                            ", expected FixedString(" + toString(uuid_bytes_length) + ")",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnWithTypeAndName & col_type_name = arguments[0];
        const ColumnPtr & column = col_type_name.column;

        if (const auto * col_in = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            if (col_in->getN() != uuid_bytes_length)
                throw Exception("Illegal type " + col_type_name.type->getName() +
                                " of column " + col_in->getName() +
                                " argument of function " + getName() +
                                ", expected FixedString(" + toString(uuid_bytes_length) + ")",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto size = col_in->size();
            const auto & vec_in = col_in->getChars();

            auto col_res = ColumnString::create();

            ColumnString::Chars & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();
            vec_res.resize(size * (uuid_text_length + 1));
            offsets_res.resize(size);

            size_t src_offset = 0;
            size_t dst_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                formatUUID(&vec_in[src_offset], &vec_res[dst_offset]);
                src_offset += uuid_bytes_length;
                dst_offset += uuid_text_length;
                vec_res[dst_offset] = 0;
                ++dst_offset;
                offsets_res[i] = dst_offset;
            }

            return col_res;
        }
        else
            throw Exception("Illegal column " + arguments[0].column->getName()
                            + " of argument of function " + getName(),
                            ErrorCodes::ILLEGAL_COLUMN);
    }
};


class FunctionUUIDStringToNum : public IFunction
{
private:
    static void parseHex(const UInt8 * __restrict src, UInt8 * __restrict dst, const size_t num_bytes)
    {
        size_t src_pos = 0;
        size_t dst_pos = 0;
        for (; dst_pos < num_bytes; ++dst_pos)
        {
            dst[dst_pos] = unhex2(reinterpret_cast<const char *>(&src[src_pos]));
            src_pos += 2;
        }
    }

    static void parseUUID(const UInt8 * src36, UInt8 * dst16)
    {
        /// If string is not like UUID - implementation specific behaviour.

        parseHex(&src36[0], &dst16[0], 4);
        parseHex(&src36[9], &dst16[4], 2);
        parseHex(&src36[14], &dst16[6], 2);
        parseHex(&src36[19], &dst16[8], 2);
        parseHex(&src36[24], &dst16[10], 6);
    }

public:
    static constexpr auto name = "UUIDStringToNum";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionUUIDStringToNum>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        /// String or FixedString(36)
        if (!isString(arguments[0]))
        {
            const auto * ptr = checkAndGetDataType<DataTypeFixedString>(arguments[0].get());
            if (!ptr || ptr->getN() != uuid_text_length)
                throw Exception("Illegal type " + arguments[0]->getName() +
                                " of argument of function " + getName() +
                                ", expected FixedString(" + toString(uuid_text_length) + ")",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeFixedString>(uuid_bytes_length);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnWithTypeAndName & col_type_name = arguments[0];
        const ColumnPtr & column = col_type_name.column;

        if (const auto * col_in = checkAndGetColumn<ColumnString>(column.get()))
        {
            const auto & vec_in = col_in->getChars();
            const auto & offsets_in = col_in->getOffsets();
            const size_t size = offsets_in.size();

            auto col_res = ColumnFixedString::create(uuid_bytes_length);

            ColumnString::Chars & vec_res = col_res->getChars();
            vec_res.resize(size * uuid_bytes_length);

            size_t src_offset = 0;
            size_t dst_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                /// If string has incorrect length - then return zero UUID.
                /// If string has correct length but contains something not like UUID - implementation specific behaviour.

                size_t string_size = offsets_in[i] - src_offset;
                if (string_size == uuid_text_length + 1)
                    parseUUID(&vec_in[src_offset], &vec_res[dst_offset]);
                else
                    memset(&vec_res[dst_offset], 0, uuid_bytes_length);

                dst_offset += uuid_bytes_length;
                src_offset += string_size;
            }

            return col_res;
        }
        else if (const auto * col_in_fixed = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            if (col_in_fixed->getN() != uuid_text_length)
                throw Exception("Illegal type " + col_type_name.type->getName() +
                                " of column " + col_in_fixed->getName() +
                                " argument of function " + getName() +
                                ", expected FixedString(" + toString(uuid_text_length) + ")",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto size = col_in_fixed->size();
            const auto & vec_in = col_in_fixed->getChars();

            auto col_res = ColumnFixedString::create(uuid_bytes_length);

            ColumnString::Chars & vec_res = col_res->getChars();
            vec_res.resize(size * uuid_bytes_length);

            size_t src_offset = 0;
            size_t dst_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                parseUUID(&vec_in[src_offset], &vec_res[dst_offset]);
                src_offset += uuid_text_length;
                dst_offset += uuid_bytes_length;
            }

            return col_res;
        }
        else
            throw Exception("Illegal column " + arguments[0].column->getName()
                            + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
    }
};

void registerFunctionsCodingUUID(FunctionFactory & factory)
{
    factory.registerFunction<FunctionUUIDNumToString>();
    factory.registerFunction<FunctionUUIDStringToNum>();
}

}
