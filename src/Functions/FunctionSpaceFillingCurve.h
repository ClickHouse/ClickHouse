#pragma once
#include <Functions/IFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Functions/FunctionHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_COLUMN;
}

/// A read-only view of a native unsigned integer column.
/// `width` is loop-invariant, so the switch in `operator[]` is an inlined predicted jump, not a call.
/// `fallback` is the only correct accessor for a column whose values are not a contiguous
/// fixed-width array (sparse, low-cardinality, nullable).
struct UIntColumnSpan
{
    const void * data = nullptr;
    const IColumn * fallback = nullptr;
    size_t width = 0;

    ALWAYS_INLINE UInt64 operator[](size_t row) const
    {
        switch (width)
        {
            case 8: return static_cast<const UInt64 *>(data)[row];
            case 4: return static_cast<const UInt32 *>(data)[row];
            case 2: return static_cast<const UInt16 *>(data)[row];
            case 1: return static_cast<const UInt8 *>(data)[row];
            default: return fallback->getUInt(row);
        }
    }
};

inline UIntColumnSpan makeUIntColumnSpan(const IColumn & column)
{
    UIntColumnSpan span;
    span.fallback = &column;
    switch (column.getDataType())
    {
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
            /// A `ColumnConst` forwards `getDataType`, `isFixedAndContiguous` and `getRawData` to its
            /// single-row nested column, so a raw read would index one element `input_rows_count` times.
            if (!isColumnConst(column) && column.isFixedAndContiguous())
            {
                span.data = column.getRawData().data();
                span.width = column.sizeOfValueIfFixed();
            }
            break;
        default:
            break;
    }
    return span;
}

class FunctionSpaceFillingCurveEncode: public IFunction
{
public:
    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    /// Range-mask `Tuple` accessor for the expanded mode of `mortonEncode` / `hilbertEncode`.
    /// `is_const` selects between reading row 0 for every row or reading row `row_idx`.
    struct RangeMask
    {
        const ColumnTuple * tuple = nullptr;
        bool is_const = false;

        UInt64 read(size_t col_idx, size_t row_idx) const
        {
            return tuple->getColumn(col_idx).getUInt(is_const ? 0 : row_idx);
        }

        size_t tupleSize() const { return tuple->tupleSize(); }

        explicit operator bool() const { return tuple != nullptr; }
    };

    static RangeMask extractRangeMask(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.empty())
            return {};
        const auto * const_col = typeid_cast<const ColumnConst *>(arguments[0].column.get());
        if (const_col)
        {
            const auto * tuple = typeid_cast<const ColumnTuple *>(const_col->getDataColumnPtr().get());
            if (tuple)
                return RangeMask{tuple, true};
            return {};
        }
        const auto * tuple = typeid_cast<const ColumnTuple *>(arguments[0].column.get());
        if (tuple)
            return RangeMask{tuple, false};
        return {};
    }

    DataTypePtr getReturnTypeImpl(const DB::DataTypes & arguments) const override
    {
        size_t vector_start_index = 0;
        if (arguments.empty())
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                            "At least one UInt argument is required for function {}",
                            getName());
        if (WhichDataType(arguments[0]).isTuple())
        {
            vector_start_index = 1;
            const auto * type_tuple = typeid_cast<const DataTypeTuple *>(arguments[0].get());
            auto tuple_size = type_tuple->getElements().size();
            if (tuple_size == 0)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Empty tuple is not allowed for function {}",
                                getName());
            if (tuple_size != (arguments.size() - 1))
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                "Illegal argument {} for function {}, tuple size should be equal to number of UInt arguments",
                                arguments[0]->getName(), getName());
            for (size_t i = 0; i < tuple_size; i++)
            {
                if (!WhichDataType(type_tuple->getElement(i)).isNativeUInt())
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                    "Illegal type {} of argument in tuple for function {}, should be a native UInt",
                                    type_tuple->getElement(i)->getName(), getName());
            }
        }

        for (size_t i = vector_start_index; i < arguments.size(); i++)
        {
            const auto & arg = arguments[i];
            if (!WhichDataType(arg).isNativeUInt())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Illegal type {} of argument for function {}, should be a native UInt",
                                arg->getName(), getName());
        }
        return std::make_shared<DataTypeUInt64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }
};

template <UInt8 max_dimensions, UInt8 min_ratio, UInt8 max_ratio>
class FunctionSpaceFillingCurveDecode: public IFunction
{
public:
    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        UInt64 tuple_size = 0;
        const auto * col_const = typeid_cast<const ColumnConst *>(arguments[0].column.get());
        if (!col_const)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Illegal column type {} for function {}, should be a constant (UInt or Tuple)",
                            arguments[0].type->getName(), getName());
        if (!WhichDataType(arguments[1].type).isNativeUInt())
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Illegal column type {} for function {}, should be a native UInt",
                            arguments[1].type->getName(), getName());
        const auto * mask = typeid_cast<const ColumnTuple *>(col_const->getDataColumnPtr().get());
        if (mask)
        {
            tuple_size = mask->tupleSize();
        }
        else if (WhichDataType(arguments[0].type).isNativeUInt())
        {
            tuple_size = col_const->getUInt(0);
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Illegal column type {} for function {}, should be UInt or Tuple",
                            arguments[0].type->getName(), getName());
        if (tuple_size > max_dimensions || tuple_size < 1)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                            "Illegal first argument for function {}, should be a number in range 1-{} or a Tuple of such size",
                            getName(), String{max_dimensions});
        if (mask)
        {
            const auto * type_tuple = typeid_cast<const DataTypeTuple *>(arguments[0].type.get());
            for (size_t i = 0; i < tuple_size; i++)
            {
                if (!WhichDataType(type_tuple->getElement(i)).isNativeUInt())
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                    "Illegal type {} of argument in tuple for function {}, should be a native UInt",
                                    type_tuple->getElement(i)->getName(), getName());
                auto ratio = mask->getColumn(i).getUInt(0);
                if (ratio > max_ratio || ratio < min_ratio)
                    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                    "Illegal argument {} in tuple for function {}, should be a number in range {}-{}",
                                    ratio, getName(), String{min_ratio}, String{max_ratio});
            }
        }
        DataTypes types(tuple_size);
        for (size_t i = 0; i < tuple_size; i++)
            types[i] = std::make_shared<DataTypeUInt64>();
        return std::make_shared<DataTypeTuple>(types);
    }
};

}
