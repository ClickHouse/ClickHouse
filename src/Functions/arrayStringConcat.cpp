#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/Regexps.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>


/** arrayStringConcat(arr)
  * arrayStringConcat(arr, delimiter)
  * - join an array of strings into one string via a separator.
  */
namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// Joins an array of type serializable to string into one string via a separator.
class FunctionArrayStringConcat : public IFunction
{
private:
    static void executeInternal(
        const ColumnString::Chars & src_chars,
        const ColumnString::Offsets & src_string_offsets,
        const ColumnArray::Offsets & src_array_offsets,
        const char * delimiter,
        const size_t delimiter_size,
        ColumnString::Chars & dst_chars,
        ColumnString::Offsets & dst_string_offsets,
        const char8_t * null_map)
    {
        size_t size = src_array_offsets.size();

        if (!size)
            return;

        /// With a small margin - as if the separator goes after the last string of the array.
        dst_chars.resize(
            src_chars.size()
            + delimiter_size * src_string_offsets.size()    /// Separators after each string...
            + src_array_offsets.size()                      /// Zero byte after each joined string
            - src_string_offsets.size());                   /// The former zero byte after each string of the array

        /// There will be as many strings as there were arrays.
        dst_string_offsets.resize(src_array_offsets.size());

        ColumnArray::Offset current_src_array_offset = 0;

        ColumnString::Offset current_dst_string_offset = 0;

        /// Loop through the array of strings.
        for (size_t i = 0; i < size; ++i)
        {
            bool first_non_null = true;
            /// Loop through the rows within the array. /// NOTE You can do everything in one copy, if the separator has a size of 1.
            for (auto next_src_array_offset = src_array_offsets[i]; current_src_array_offset < next_src_array_offset; ++current_src_array_offset)
            {
                if (null_map && null_map[current_src_array_offset]) [[unlikely]]
                    continue;

                if (!first_non_null)
                {
                    memcpy(&dst_chars[current_dst_string_offset], delimiter, delimiter_size);
                    current_dst_string_offset += delimiter_size;
                }
                first_non_null = false;

                const auto current_src_string_offset = current_src_array_offset ? src_string_offsets[current_src_array_offset - 1] : 0;
                size_t bytes_to_copy = src_string_offsets[current_src_array_offset] - current_src_string_offset - 1;

                memcpySmallAllowReadWriteOverflow15(
                    &dst_chars[current_dst_string_offset], &src_chars[current_src_string_offset], bytes_to_copy);

                current_dst_string_offset += bytes_to_copy;
            }

            dst_chars[current_dst_string_offset] = 0;
            ++current_dst_string_offset;

            dst_string_offsets[i] = current_dst_string_offset;
        }

        dst_chars.resize(dst_string_offsets.back());
    }

    static void executeInternal(
        const ColumnString & col_string,
        const ColumnArray & col_arr,
        const String & delimiter,
        ColumnString & col_res,
        const char8_t * null_map = nullptr)
    {
        executeInternal(
            col_string.getChars(),
            col_string.getOffsets(),
            col_arr.getOffsets(),
            delimiter.data(),
            delimiter.size(),
            col_res.getChars(),
            col_res.getOffsets(),
            null_map);
    }

    static ColumnPtr serializeNestedColumn(const ColumnArray & col_arr, const DataTypePtr & nested_type)
    {
        DataTypePtr type = nested_type;
        ColumnPtr column = col_arr.getDataPtr();

        if (type->isNullable())
        {
            type = removeNullable(type);
            column = assert_cast<const ColumnNullable &>(*column).getNestedColumnPtr();
        }

        return castColumn({column, type, "tmp"}, std::make_shared<DataTypeString>());
    }

public:
    static constexpr auto name = "arrayStringConcat";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayStringConcat>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args
        {
            {"arr", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
        };

        FunctionArgumentDescriptors optional_args
        {
            {"separator", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"},
        };

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        String delimiter;
        if (arguments.size() == 2)
        {
            const ColumnConst * col_delim = checkAndGetColumnConstStringOrFixedString(arguments[1].column.get());
            if (!col_delim)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function {} must be constant string.", getName());

            delimiter = col_delim->getValue<String>();
        }

        const auto & nested_type = assert_cast<const DataTypeArray &>(*arguments[0].type).getNestedType();
        const ColumnArray & col_arr = assert_cast<const ColumnArray &>(*arguments[0].column);

        ColumnPtr str_subcolumn = serializeNestedColumn(col_arr, nested_type);
        const ColumnString & col_string = assert_cast<const ColumnString &>(*str_subcolumn.get());

        auto col_res = ColumnString::create();
        if (const ColumnNullable * col_nullable = checkAndGetColumn<ColumnNullable>(&col_arr.getData()))
            executeInternal(col_string, col_arr, delimiter, *col_res, col_nullable->getNullMapData().data());
        else
            executeInternal(col_string, col_arr, delimiter, *col_res);

        return col_res;
    }
};

}

REGISTER_FUNCTION(ArrayStringConcat)
{
    factory.registerFunction<FunctionArrayStringConcat>();
}

}
