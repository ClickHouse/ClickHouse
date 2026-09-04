#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeQBit.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnQBit.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Common/VectorWithMemoryTracking.h>
#include <IO/WriteHelpers.h>

#include <limits>
#include <optional>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** arraySlice(arr, offset, length) - make slice of array. Offsets and length may be < 0 or Null
  *   - if offset < 0, indexation from right element
  *   - if length < 0, length = len(array) - (positive_index(offset) - 1) + length
  *   indexation:
  *     [ 1,  2,  3,  4,  5,  6]
  *     [-6, -5, -4, -3, -2, -1]
  *   examples:
  *     arraySlice([1, 2, 3, 4, 5, 6], -4, 2) -> [3, 4]
  *     arraySlice([1, 2, 3, 4, 5, 6], 2, -1) -> [2, 3, 4, 5] (6 - (2 - 1) + (-1) = 4)
  *     arraySlice([1, 2, 3, 4, 5, 6], -5, -1) = arraySlice([1, 2, 3, 4, 5, 6], 2, -1) -> [2, 3, 4, 5]
  *
  * The first argument may also be a QBit: the result is a QBit over the selected dimensions. Because the dimension of
  * a QBit is part of its type, the offset and length must then be constants and the slice must select at least one
  * element. A slice aligned to stride-group boundaries keeps the stride and reuses the stored bit-plane streams
  * without copying; any other slice is repacked into a non-strided QBit, reading only the bit planes of the stride
  * groups it overlaps.
  */
class FunctionArraySlice final : public IFunction
{
public:
    static constexpr auto name = "arraySlice";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArraySlice>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const size_t number_of_arguments = arguments.size();

        if (number_of_arguments < 2 || number_of_arguments > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Number of arguments for function {} doesn't match: passed {}, should be 2 or 3",
                            getName(), number_of_arguments);

        if (arguments[0].type->onlyNull())
            return arguments[0].type;

        if (const auto * qbit_type = checkAndGetDataType<DataTypeQBit>(removeNullable(arguments[0].type).get()))
        {
            const auto bounds = getQBitSliceBounds(*qbit_type, arguments, /*require_constants=*/true);
            const auto result_type = getQBitSliceReturnType(*qbit_type, bounds);
            return arguments[0].type->isNullable() ? makeNullable(result_type) : result_type;
        }

        const auto * array_type = typeid_cast<const DataTypeArray *>(arguments[0].type.get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "First argument for function {} must be an array but it has type {}.",
                            getName(), arguments[0].type->getName());

        for (size_t i = 1; i < number_of_arguments; ++i)
        {
            if (!isInteger(removeNullable(arguments[i].type)) && !arguments[i].type->onlyNull())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Argument {} for function {} must be integer but it has type {}.",
                                i, getName(), arguments[i].type->getName());
        }

        return arguments[0].type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t input_rows_count) const override
    {
        if (return_type->onlyNull())
            return return_type->createColumnConstWithDefaultValue(input_rows_count);

        if (checkAndGetDataType<DataTypeQBit>(removeNullable(arguments[0].type).get()))
            return executeQBitSlice(arguments, input_rows_count);

        auto array_column = arguments[0].column;
        /// The offset and length are per-row numbers
        /// The array argument is the one worth keeping lazy: it is consumed by a ReplicatedSource
        const auto offset_column = arguments[1].column->convertToFullColumnIfReplicated();
        const auto length_column = arguments.size() > 2 ? arguments[2].column->convertToFullColumnIfReplicated() : nullptr;

        std::unique_ptr<GatherUtils::IArraySource> source;

        size_t size = array_column->size();
        bool is_const = false;

        if (const auto * const_array_column = typeid_cast<const ColumnConst *>(array_column.get()))
        {
            is_const = true;
            array_column = const_array_column->getDataColumnPtr();
        }

        if (const auto * replicated_column = typeid_cast<const ColumnReplicated *>(array_column.get()))
            source = GatherUtils::createArraySourceFromReplicated(*replicated_column);
        else if (const auto * argument_column_array = typeid_cast<const ColumnArray *>(array_column.get()))
            source = GatherUtils::createArraySource(*argument_column_array, is_const, size);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "First arguments for function {} must be array.", getName());

        ColumnArray::MutablePtr sink;

        if (offset_column->onlyNull())
        {
            if (!length_column || length_column->onlyNull())
            {
                return arguments[0].column;
            }
            if (isColumnConst(*length_column))
                sink = GatherUtils::sliceFromLeftConstantOffsetBounded(*source, 0, length_column->getInt(0));
            else
            {
                auto const_offset_column = ColumnConst::create(ColumnInt8::create(1, static_cast<Int8>(1)), size);
                sink = GatherUtils::sliceDynamicOffsetBounded(*source, *const_offset_column, *length_column);
            }
        }
        else if (isColumnConst(*offset_column))
        {
            ssize_t offset = offset_column->getUInt(0);

            if (!length_column || length_column->onlyNull())
            {
                if (offset > 0)
                    sink = GatherUtils::sliceFromLeftConstantOffsetUnbounded(*source, static_cast<size_t>(offset - 1));
                else
                    sink = GatherUtils::sliceFromRightConstantOffsetUnbounded(*source, -static_cast<size_t>(offset));
            }
            else if (isColumnConst(*length_column))
            {
                ssize_t length = length_column->getInt(0);
                if (offset > 0)
                    sink = GatherUtils::sliceFromLeftConstantOffsetBounded(*source, static_cast<size_t>(offset - 1), length);
                else
                    sink = GatherUtils::sliceFromRightConstantOffsetBounded(*source, -static_cast<size_t>(offset), length);
            }
            else
                sink = GatherUtils::sliceDynamicOffsetBounded(*source, *offset_column, *length_column);
        }
        else
        {
            if (!length_column || length_column->onlyNull())
                sink = GatherUtils::sliceDynamicOffsetUnbounded(*source, *offset_column);
            else
                sink = GatherUtils::sliceDynamicOffsetBounded(*source, *offset_column, *length_column);
        }

        return sink;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    /// When set to true, materializes the columns whenever the offset or length is a full column.
    bool useDefaultImplementationForReplicatedColumns() const override { return false; }

private:
    struct QBitSliceBounds
    {
        size_t start; /// 0-based index of the first selected dimension
        size_t length; /// Number of selected dimensions, always >= 1
    };

    /// Slicing a QBit produces a QBit of a different dimension, and the dimension is part of the type. The result type
    /// must therefore be known from the argument types and constant values alone, so the offset and the length are
    /// required to be constants; this also allows validating the bounds during type inference. A NULL offset behaves as
    /// offset 1 and a NULL (or omitted) length means "to the end of the vector", mirroring the Array behaviour. Unlike
    /// Array slicing, a slice that selects no elements is an error, because a QBit of dimension 0 cannot exist.
    ///
    /// `require_constants` must be true during type inference but false during execution:
    /// useDefaultImplementationForConstants may have unwrapped the all-constant case into non-const single-row columns
    /// by the time executeImpl runs, while the values read via `(*column)[0]` are still correct.
    QBitSliceBounds getQBitSliceBounds(const DataTypeQBit & qbit_type, const ColumnsWithTypeAndName & arguments, bool require_constants) const
    {
        const size_t dimension = qbit_type.getDimension();

        auto get_constant = [&](size_t i, const char * argument_name) -> std::optional<Int64>
        {
            const auto & argument = arguments[i];
            if (argument.type->onlyNull())
                return std::nullopt;

            if (!isInteger(removeNullable(argument.type)))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Argument {} ({}) for function {} must be integer but it has type {}.",
                                i, argument_name, getName(), argument.type->getName());

            if (!argument.column || (require_constants && !isColumnConst(*argument.column)))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Argument {} ({}) for function {} must be a constant when slicing a QBit: "
                                "the dimension of the result type depends on its value.",
                                i, argument_name, getName());

            const Field value = (*argument.column)[0];
            if (value.isNull())
                return std::nullopt;

            if (value.getType() == Field::Types::UInt64)
            {
                const UInt64 unsigned_value = value.safeGet<UInt64>();
                if (unsigned_value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Argument {} ({}) of function {} is out of range for a QBit of dimension {}.",
                                    i, argument_name, getName(), dimension);
                return static_cast<Int64>(unsigned_value);
            }
            return value.safeGet<Int64>();
        };

        const std::optional<Int64> offset = get_constant(1, "offset");
        const std::optional<Int64> length = arguments.size() > 2 ? get_constant(2, "length") : std::nullopt;

        size_t start = 0;
        if (offset)
        {
            if (*offset > 0)
            {
                start = static_cast<size_t>(*offset - 1);
            }
            else
            {
                /// Compute |offset| in the unsigned domain: -INT64_MIN does not fit in Int64. Offset 0 falls here too.
                const UInt64 abs_offset = UInt64(0) - static_cast<UInt64>(*offset);
                if (abs_offset == 0)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Offset {} of function {} is out of range for a QBit of dimension {}: "
                                    "the slice must select at least one element.",
                                    *offset, getName(), dimension);
                start = abs_offset > dimension ? 0 : dimension - static_cast<size_t>(abs_offset);
            }
        }

        if (start >= dimension)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Offset {} of function {} is out of range for a QBit of dimension {}: "
                            "the slice must select at least one element.",
                            offset ? *offset : 1, getName(), dimension);

        size_t result_length = dimension - start;
        if (length)
        {
            if (*length >= 0)
            {
                const size_t requested_length = static_cast<size_t>(*length);
                const UInt64 abs_offset = offset && *offset < 0 ? UInt64(0) - static_cast<UInt64>(*offset) : 0;
                if (abs_offset > dimension)
                {
                    /// `getSliceFromRight` clamps an oversized negative offset to the left edge, but preserves the
                    /// original endpoint. A slice starting before the left edge must therefore lose the part before it.
                    const UInt64 clipped_prefix = abs_offset - dimension;
                    result_length = requested_length > clipped_prefix ? std::min(dimension, requested_length - static_cast<size_t>(clipped_prefix)) : 0;
                }
                else
                    result_length = std::min(result_length, requested_length);
            }
            else
            {
                /// A negative length leaves the last |length| elements of the vector unselected, as for Arrays.
                const UInt64 abs_length = UInt64(0) - static_cast<UInt64>(*length);
                result_length = abs_length < result_length ? result_length - static_cast<size_t>(abs_length) : 0;
            }
        }

        if (result_length == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "The slice of function {} with offset {} and length {} selects no elements of a QBit of dimension {}, "
                            "but a QBit of dimension 0 cannot exist.",
                            getName(), offset ? *offset : 1, length ? toString(*length) : "NULL", dimension);

        return {start, result_length};
    }

    static DataTypePtr getQBitSliceReturnType(const DataTypeQBit & qbit_type, const QBitSliceBounds & bounds)
    {
        /// A slice aligned to stride-group boundaries keeps the stride (its bit-plane streams are reused as-is);
        /// any other slice is repacked into a non-strided QBit.
        const size_t stride = qbit_type.getStride();
        if (qbit_type.getNumStrides() > 1 && bounds.start % stride == 0 && bounds.length % stride == 0)
            return std::make_shared<DataTypeQBit>(qbit_type.getElementType(), bounds.length, stride);
        return std::make_shared<DataTypeQBit>(qbit_type.getElementType(), bounds.length, bounds.length);
    }

    ColumnPtr executeQBitSlice(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        const auto & qbit_type = assert_cast<const DataTypeQBit &>(*removeNullable(arguments[0].type));
        const QBitSliceBounds bounds = getQBitSliceBounds(qbit_type, arguments, /*require_constants=*/false);

        const size_t dimension = qbit_type.getDimension();
        const size_t stride = qbit_type.getStride();
        const size_t element_size = qbit_type.getElementSize();

        /// The whole vector is selected: the result type equals the input type, so the input column is reused as-is.
        if (bounds.start == 0 && bounds.length == dimension)
            return arguments[0].column;

        ColumnPtr column = arguments[0].column;

        bool is_const = false;
        if (const auto * const_column = checkAndGetColumn<ColumnConst>(column.get()))
        {
            is_const = true;
            column = const_column->getDataColumnPtr();
        }

        /// Peel a Nullable wrapper off; NULL rows hold default (all-zero) QBit data whose slice is again all zeros,
        /// so the nested data can be sliced as-is and the null map reattached.
        ColumnPtr null_map;
        if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(column.get()))
        {
            null_map = nullable_column->getNullMapColumnPtr();
            column = nullable_column->getNestedColumnPtr();
        }

        const auto & qbit_column = assert_cast<const ColumnQBit &>(*column);
        const auto & tuple = qbit_column.getNestedData();

        ColumnPtr sliced;
        if (qbit_type.getNumStrides() > 1 && bounds.start % stride == 0 && bounds.length % stride == 0)
        {
            /// The slice covers whole stride groups: reference the needed groups' bit-plane columns without copying.
            const size_t first_group = bounds.start / stride;
            const size_t num_groups = bounds.length / stride;
            Columns planes(num_groups * element_size);
            for (size_t i = 0; i < planes.size(); ++i)
                planes[i] = tuple.getColumnPtr(first_group * element_size + i);
            sliced = ColumnQBit::create(ColumnTuple::create(planes), bounds.length, stride);
        }
        else
        {
            sliced = repackQBitSlice(qbit_column, qbit_type, bounds);
        }

        if (null_map)
            sliced = ColumnNullable::create(sliced, null_map);
        if (is_const)
            sliced = ColumnConst::create(sliced, input_rows_count);
        return sliced;
    }

    /// Repack the selected dimensions into a fresh non-strided QBit. Only the bit planes of the stride groups that
    /// overlap [start, start + length) are read. When the slice begins on an octet boundary (start % 8 == 0) every
    /// result byte is a plain copy of one source byte, because a byte of a bit plane holds the same octet of 8
    /// dimensions regardless of the stride; otherwise the bits are scattered one by one.
    static ColumnPtr repackQBitSlice(const ColumnQBit & src, const DataTypeQBit & src_type, const QBitSliceBounds & bounds)
    {
        const size_t stride = src_type.getStride();
        const size_t element_size = src_type.getElementSize();
        const size_t src_bytes = DataTypeQBit::bitsToBytes(stride);
        const size_t dst_bytes = DataTypeQBit::bitsToBytes(bounds.length);
        const size_t rows = src.size();
        const ColumnTuple & tuple = src.getNestedData();

        MutableColumns dst_columns(element_size);
        VectorWithMemoryTracking<UInt8 *> dst_data(element_size);
        for (size_t bit = 0; bit < element_size; ++bit)
        {
            auto column = ColumnFixedString::create(dst_bytes);
            column->getChars().resize_fill(rows * dst_bytes);
            dst_data[bit] = reinterpret_cast<UInt8 *>(column->getChars().data());
            dst_columns[bit] = std::move(column);
        }

        auto src_plane = [&](size_t group, size_t bit) -> const UInt8 *
        {
            return reinterpret_cast<const UInt8 *>(
                assert_cast<const ColumnFixedString &>(tuple.getColumn(group * element_size + bit)).getChars().data());
        };

        if (bounds.start % 8 == 0)
        {
            /// Octet-aligned slice: move whole bytes. Byte offsets run high-octet-first within a plane row.
            for (size_t octet = 0; octet < dst_bytes; ++octet)
            {
                const size_t src_octet = bounds.start / 8 + octet;
                const size_t src_group = src_octet / src_bytes;
                const size_t src_offset = src_bytes - 1 - (src_octet % src_bytes);
                const size_t dst_offset = dst_bytes - 1 - octet;

                for (size_t bit = 0; bit < element_size; ++bit)
                {
                    const UInt8 * s = src_plane(src_group, bit) + src_offset;
                    UInt8 * d = dst_data[bit] + dst_offset;
                    for (size_t row = 0; row < rows; ++row)
                        d[row * dst_bytes] = s[row * src_bytes];
                }
            }
        }
        else
        {
            /// The slice begins mid-octet, so every bit moves to a different position within its byte: scatter the
            /// bits one by one. Only reachable for slices not aligned to 8 dimensions.
            for (size_t i = 0; i < bounds.length; ++i)
            {
                const size_t element = bounds.start + i;
                const size_t src_group = element / stride;
                const size_t within_group = element % stride;
                const size_t src_offset = src_bytes - 1 - within_group / 8;
                const size_t src_bit = within_group % 8;
                const size_t dst_offset = dst_bytes - 1 - i / 8;
                const size_t dst_bit = i % 8;

                for (size_t bit = 0; bit < element_size; ++bit)
                {
                    const UInt8 * s = src_plane(src_group, bit) + src_offset;
                    UInt8 * d = dst_data[bit] + dst_offset;
                    for (size_t row = 0; row < rows; ++row)
                        d[row * dst_bytes] |= static_cast<UInt8>(((s[row * src_bytes] >> src_bit) & 1) << dst_bit);
                }
            }
        }

        /// When length % 8 != 0 the top byte of each result plane ends in padding bits that must stay zero for the
        /// result to be canonical (QBit equality compares the raw bytes). The octet-aligned path may have copied
        /// source bits beyond the slice (or non-canonical padding of a tuple-backed source) into them, so clear them.
        const size_t padding_bits = dst_bytes * 8 - bounds.length;
        if (padding_bits)
        {
            const UInt8 valid_mask = static_cast<UInt8>(0xFF >> padding_bits);
            for (size_t bit = 0; bit < element_size; ++bit)
            {
                UInt8 * d = dst_data[bit];
                for (size_t row = 0; row < rows; ++row)
                    d[row * dst_bytes] &= valid_mask;
            }
        }

        return ColumnQBit::create(IColumn::mutate(ColumnTuple::create(std::move(dst_columns))), bounds.length, bounds.length);
    }
};


REGISTER_FUNCTION(ArraySlice)
{
    FunctionDocumentation::Description description = R"(
Returns a slice of the array, with `NULL` elements included.

The first argument may also be a [QBit](/sql-reference/data-types/qbit): the result is a QBit over the selected dimensions (a projection to a subset of dimensions). In that case `offset` and `length` must be constants, because the dimension of a QBit is part of its type, and the slice must select at least one dimension. A slice aligned to the QBit's stride-group boundaries keeps the stride and reuses the stored bit-plane streams without copying.
    )";
    FunctionDocumentation::Syntax syntax = "arraySlice(arr, offset [, length])";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "Array to slice.", {"Array(T)", "QBit"}},
        {"offset", "Indent from the edge of the array. A positive value indicates an offset on the left, and a negative value is an indent on the right. Numbering of the array items begins with `1`.", {"(U)Int*"}},
        {"length", "The length of the required slice. If you specify a negative value, the function returns an open slice `[offset, array_length - length]`. If you omit the value, the function returns the slice `[offset, the_end_of_array]`.", {"(U)Int*"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a slice of the array with `length` elements from the specified `offset`", {"Array(T)"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT arraySlice([1, 2, NULL, 4, 5], 2, 3) AS res;", "[2,NULL,4]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArraySlice>(documentation);
}


}
