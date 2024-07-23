#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Common/StringUtils.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ZERO_ARRAY_OR_TUPLE_INDEX;
}

namespace
{

/// If 'is_utf8' - measure offset and length in code points instead of bytes.
/// Syntax: overlay(input, replace, offset[, length])
template <bool is_utf8>
class FunctionOverlay : public IFunction
{
public:
    static constexpr auto name = is_utf8 ? "OverlayUTF8" : "Overlay";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionOverlay>(); }
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const size_t number_of_arguments = arguments.size();
        if (number_of_arguments < 3 || number_of_arguments > 4)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: "
                "passed {}, should be 3 or 4",
                getName(),
                number_of_arguments);

        /// first argument is string
        if (!isString(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected String",
                arguments[0]->getName(),
                getName());

        /// second argument is string
        if (!isString(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected String",
                arguments[1]->getName(),
                getName());

        if (!isNativeNumber(arguments[2]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of third argument of function {}, expected (U)Int8|16|32|64",
                arguments[2]->getName(),
                getName());

        if (number_of_arguments == 4 && !isNativeNumber(arguments[3]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected (U)Int8|16|32|64",
                arguments[3]->getName(),
                getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const size_t number_of_arguments = arguments.size();

        ColumnPtr column_string = arguments[0].column;
        ColumnPtr column_offset = arguments[1].column;
        ColumnPtr column_length;
        if (number_of_arguments == 3)
            column_length = arguments[2].column;

        const ColumnConst * column_offset_const = checkAndGetColumn<ColumnConst>(column_offset.get());
        const ColumnConst * column_length_const = nullptr;
        if (number_of_arguments == 3)
            column_length_const = checkAndGetColumn<ColumnConst>(column_length.get());

        Int64 offset = 0;
        Int64 length = 0;
        if (column_offset_const)
            offset = column_offset_const->getInt(0);
        if (column_length_const)
            length = column_length_const->getInt(0);

        auto res_col = ColumnString::create();
        auto & res_data = res_col->getChars();
        auto & res_offsets = res_col->getOffsets();
    }

private:
template <bool three_args = false, bool offset_is_const = false, bool length_is_const = false>
    void constantConstant(
        size_t rows,
        const StringRef & input,
        const StringRef & replace,
        const ColumnPtr & column_offset,
        const ColumnPtr & column_length,
        Int64 const_offset,
        Int64 const_length,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        if (!three_args && length_is_const && const_length < 0)
        {
            constantConstant(input, replace, column_offset, column_length, const_offset, -1, res_data, res_offsets);
            return;
        }

        Int64 offset = 0; // start from 1, maybe negative
        size_t valid_offset = 0; // start from 0, not negative
        if constexpr (offset_is_const)
        {
            offset = const_offset;
            valid_offset = offset > 0 ? (offset - 1) : (-offset);
        }

        size_t replace_size = replace.size;
        Int64 length = 0; // maybe negative
        size_t valid_length = 0; // not negative
        if constexpr (!three_args && length_is_const)
        {
            assert(const_length >= 0);
            valid_length = const_length;
        }
        else if constexpr (three_args)
        {
            valid_length = replace_size;
        }

        size_t res_offset = 0;
        size_t input_size = input.size;
        for (size_t i = 0; i < rows; ++i)
        {
            if constexpr (!offset_is_const)
            {
                offset = column_offset->getInt(i);
                valid_offset = offset > 0 ? (offset - 1) : (-offset);
            }

            if constexpr (!three_args && !length_is_const)
            {
                length = column_length->getInt(i);
                valid_length = length >= 0 ? length : replace_size;
            }

            size_t prefix_size = valid_offset > input_size ? input_size : valid_offset;
            size_t suffix_size = prefix_size + valid_length > input_size ? 0 : input_size - prefix_size - valid_length;
            size_t new_res_size = res_data.size() + prefix_size + replace_size + suffix_size + 1; /// +1 for zero terminator
            res_data.resize(new_res_size);

            /// copy prefix before replaced region
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], input.data, prefix_size);
            res_offset += prefix_size;

            /// copy replace
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], replace.data, replace_size);
            res_offset += replace_size;

            /// copy suffix after replaced region. It is not necessary to copy if suffix_size is zero.
            if (suffix_size)
            {
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], input.data + prefix_size + valid_length, suffix_size);
                res_offset += suffix_size;
            }

            /// add zero terminator
            res_data[res_offset] = 0;
            ++res_offset;

            res_offsets[i] = res_offset;
        }
    }

    template <bool three_args = false, bool offset_is_const = false, bool length_is_const = false>
    void vectorConstant(
        const ColumnString::Chars & input_data,
        const ColumnString::Offsets & input_offsets,
        const StringRef & replace,
        const ColumnPtr & column_offset,
        const ColumnPtr & column_length,
        Int64 const_offset,
        Int64 const_length,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        if (!three_args && length_is_const && const_length < 0)
        {
            vectorConstant(input_data, input_offsets, replace, column_offset, column_length, const_offset, -1, res_data, res_offsets);
            return;
        }

        Int64 offset = 0; // start from 1, maybe negative
        size_t valid_offset = 0; // start from 0, not negative
        if constexpr (offset_is_const)
        {
            offset = const_offset;
            valid_offset = offset > 0 ? (offset - 1) : (-offset);
        }

        size_t replace_size = replace.size;
        Int64 length = 0; // maybe negative
        size_t valid_length = 0; // not negative
        if constexpr (!three_args && length_is_const)
        {
            assert(const_length >= 0);
            valid_length = const_length;
        }
        else if constexpr (three_args)
        {
            valid_length = replace_size;
        }

        size_t rows = input_offsets.size();
        size_t res_offset = 0;
        for (size_t i = 0; i < rows; ++i)
        {
            size_t input_offset = input_offsets[i - 1];
            size_t input_size = input_offsets[i] - input_offsets[i - 1] - 1;

            if constexpr (!offset_is_const)
            {
                offset = column_offset->getInt(i);
                valid_offset = offset > 0 ? (offset - 1) : (-offset);
            }

            if constexpr (!three_args && !length_is_const)
            {
                length = column_length->getInt(i);
                valid_length = length >= 0 ? length : replace_size;
            }

            size_t prefix_size = valid_offset > input_size ? input_size : valid_offset;
            size_t suffix_size = prefix_size + valid_length > input_size ? 0 : input_size - prefix_size - valid_length;
            size_t new_res_size = res_data.size() + prefix_size + replace_size + suffix_size + 1; /// +1 for zero terminator
            res_data.resize(new_res_size);

            /// copy prefix before replaced region
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &input_data[input_offset], prefix_size);
            res_offset += prefix_size;

            /// copy replace
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], replace.data, replace_size);
            res_offset += replace_size;

            /// copy suffix after replaced region. It is not necessary to copy if suffix_size is zero.
            if (suffix_size)
            {
                memcpySmallAllowReadWriteOverflow15(
                    &res_data[res_offset], &input_data[input_offset + prefix_size + valid_length], suffix_size);
                res_offset += suffix_size;
            }

            /// add zero terminator
            res_data[res_offset] = 0;
            ++res_offset;

            res_offsets[i] = res_offset;
        }
    }

    template <bool three_args = false, bool offset_is_const = false, bool length_is_const = false>
    void constantVector(
        const StringRef & input,
        const ColumnString::Chars & replace_data,
        const ColumnString::Offsets & replace_offsets,
        const ColumnPtr & column_offset,
        const ColumnPtr & column_length,
        Int64 const_offset,
        Int64 const_length,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        if (!three_args && length_is_const && const_length < 0)
        {
            constantVector(input, replace_data, replace_offsets, column_offset, column_length, const_offset, -1, res_data, res_offsets);
            return;
        }

        Int64 offset = 0; // start from 1, maybe negative
        size_t valid_offset = 0; // start from 0, not negative
        if constexpr (offset_is_const)
        {
            offset = const_offset;
            valid_offset = offset > 0 ? (offset - 1) : (-offset);
        }

        Int64 length = 0; // maybe negative
        size_t valid_length = 0; // not negative
        if constexpr (!three_args && length_is_const)
        {
            assert(const_length >= 0);
            valid_length = const_length;
        }

        size_t rows = replace_offsets.size();
        size_t input_size = input.size;
        size_t res_offset = 0;
        for (size_t i = 0; i < rows; ++i)
        {
            size_t replace_offset = replace_offsets[i - 1];
            size_t replace_size = replace_offsets[i] - replace_offsets[i - 1] - 1;

            if constexpr (!offset_is_const)
            {
                offset = column_offset->getInt(i);
                valid_offset = offset > 0 ? (offset - 1) : (-offset);
            }

            if constexpr (three_args)
            {
                // length = replace_size;
                valid_length = replace_size;
            }
            else if constexpr (!length_is_const)
            {
                length = column_length->getInt(i);
                valid_length = length >= 0 ? length : replace_size;
            }

            size_t prefix_size = valid_offset > input_size ? input_size : valid_offset;
            size_t suffix_size = prefix_size + valid_length > input_size ? 0 : input_size - prefix_size - valid_length;
            size_t new_res_size = res_data.size() + prefix_size + replace_size + suffix_size + 1; /// +1 for zero terminator
            res_data.resize(new_res_size);

            /// copy prefix before replaced region
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], input.data, prefix_size);
            res_offset += prefix_size;

            /// copy replace
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &replace_data[replace_offset], replace_size);
            res_offset += replace_size;

            /// copy suffix after replaced region. It is not necessary to copy if suffix_size is zero.
            if (suffix_size)
            {
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], input.data + prefix_size + valid_length, suffix_size);
                res_offset += suffix_size;
            }

            /// add zero terminator
            res_data[res_offset] = 0;
            ++res_offset;

            res_offsets[i] = res_offset;
        }
    }

    template <bool three_args, bool offset_is_const, bool length_is_const>
    void vectorVector(
        const ColumnString::Chars & input_data,
        const ColumnString::Offsets & input_offsets,
        const ColumnString::Chars & replace_data,
        const ColumnString::Offsets & replace_offsets,
        const ColumnPtr & column_offset,
        const ColumnPtr & column_length,
        Int64 const_offset,
        Int64 const_length,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        if (!three_args && length_is_const && const_length < 0)
        {
            vectorVector<true, offset_is_const, true>(
                input_data,
                input_offsets,
                replace_data,
                replace_offsets,
                column_offset,
                column_length,
                const_offset,
                -1,
                res_data,
                res_offsets);
            return;
        }


        Int64 offset = 0; // start from 1, maybe negative
        size_t valid_offset = 0; // start from 0, not negative
        if constexpr (offset_is_const)
        {
            offset = const_offset;
            valid_offset = offset > 0 ? (offset - 1) : (-offset);
        }

        Int64 length = 0; // maybe negative
        size_t valid_length = 0; // not negative
        if constexpr (!three_args && length_is_const)
        {
            assert(const_length >= 0);
            valid_length = const_length;
        }

        size_t rows = input_offsets.size();
        size_t res_offset = 0;
        for (size_t i = 0; i < rows; ++i)
        {
            size_t input_offset = input_offsets[i - 1];
            size_t input_size = input_offsets[i] - input_offsets[i - 1] - 1;
            size_t replace_offset = replace_offsets[i - 1];
            size_t replace_size = replace_offsets[i] - replace_offsets[i - 1] - 1;

            if constexpr (!offset_is_const)
            {
                offset = column_offset->getInt(i);
                valid_offset = offset > 0 ? (offset - 1) : (-offset);
            }

            if constexpr (three_args)
            {
                // length = replace_size;
                valid_length = replace_size;
            }
            else if constexpr (!length_is_const)
            {
                length = column_length->getInt(i);
                valid_length = length >= 0 ? length : replace_size;
            }

            size_t prefix_size = valid_offset > input_size ? input_size : valid_offset;
            size_t suffix_size = prefix_size + valid_length > input_size ? 0 : input_size - prefix_size - valid_length;
            size_t new_res_size = res_data.size() + prefix_size + replace_size + suffix_size + 1; /// +1 for zero terminator
            res_data.resize(new_res_size);

            /// copy prefix before replaced region
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &input_data[input_offset], prefix_size);
            res_offset += prefix_size;

            /// copy replace
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &replace_data[replace_offset], replace_size);
            res_offset += replace_size;

            /// copy suffix after replaced region. It is not necessary to copy if suffix_size is zero.
            if (suffix_size)
            {
                memcpySmallAllowReadWriteOverflow15(
                    &res_data[res_offset], &input_data[input_offset + prefix_size + valid_length], suffix_size);
                res_offset += suffix_size;
            }

            /// add zero terminator
            res_data[res_offset] = 0;
            ++res_offset;

            res_offsets[i] = res_offset;
        }
    }
};

}

REGISTER_FUNCTION(Overlay)
{
    factory.registerFunction<FunctionOverlay<false>>({}, FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionOverlay<true>>({}, FunctionFactory::CaseSensitive);
}

}
