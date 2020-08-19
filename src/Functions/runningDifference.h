#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NumberTraits.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


template <bool is_first_line_zero>
struct FunctionRunningDifferenceName;

template <>
struct FunctionRunningDifferenceName<true>
{
    static constexpr auto name = "runningDifference";
};

template <>
struct FunctionRunningDifferenceName<false>
{
    static constexpr auto name = "runningDifferenceStartingWithFirstValue";
};

/** Calculate difference of consecutive values in block.
  * So, result of function depends on partition of data to blocks and on order of data in block.
  */
template <bool is_first_line_zero>
class FunctionRunningDifferenceImpl : public IFunction
{
private:
    /// It is possible to track value from previous block, to calculate continuously across all blocks. Not implemented.

    template <typename Src, typename Dst>
    static void process(const PaddedPODArray<Src> & src, PaddedPODArray<Dst> & dst, const NullMap * null_map)
    {
        size_t size = src.size();
        dst.resize(size);

        if (size == 0)
            return;

        /// It is possible to SIMD optimize this loop. By no need for that in practice.

        Src prev{};
        bool has_prev_value = false;

        for (size_t i = 0; i < size; ++i)
        {
            if (null_map && (*null_map)[i])
            {
                dst[i] = Dst{};
                continue;
            }

            if (!has_prev_value)
            {
                dst[i] = is_first_line_zero ? 0 : src[i];
                prev = src[i];
                has_prev_value = true;
            }
            else
            {
                auto cur = src[i];
                dst[i] = static_cast<Dst>(cur) - prev;
                prev = cur;
            }
        }
    }

    /// Result type is same as result of subtraction of argument types.
    template <typename SrcFieldType>
    using DstFieldType = typename NumberTraits::ResultOfSubtraction<SrcFieldType, SrcFieldType>::Type;

    /// Call polymorphic lambda with tag argument of concrete field type of src_type.
    template <typename F>
    void dispatchForSourceType(const IDataType & src_type, F && f) const
    {
        WhichDataType which(src_type);

        if (which.isUInt8())
            f(UInt8());
        else if (which.isUInt16())
            f(UInt16());
        else if (which.isUInt32())
            f(UInt32());
        else if (which.isUInt64())
            f(UInt64());
        else if (which.isInt8())
            f(Int8());
        else if (which.isInt16())
            f(Int16());
        else if (which.isInt32())
            f(Int32());
        else if (which.isInt64())
            f(Int64());
        else if (which.isFloat32())
            f(Float32());
        else if (which.isFloat64())
            f(Float64());
        else if (which.isDate())
            f(DataTypeDate::FieldType());
        else if (which.isDateTime())
            f(DataTypeDateTime::FieldType());
        else
            throw Exception("Argument for function " + getName() + " must have numeric type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

public:
    static constexpr auto name = FunctionRunningDifferenceName<is_first_line_zero>::name;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionRunningDifferenceImpl<is_first_line_zero>>();
    }

    String getName() const override
    {
        return name;
    }

    bool isStateful() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        DataTypePtr res;
        dispatchForSourceType(*removeNullable(arguments[0]), [&](auto field_type_tag)
        {
            res = std::make_shared<DataTypeNumber<DstFieldType<decltype(field_type_tag)>>>();
        });

        if (arguments[0]->isNullable())
            res = makeNullable(res);

        return res;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        auto & src = block.getByPosition(arguments.at(0));
        const auto & res_type = block.getByPosition(result).type;

        /// When column is constant, its difference is zero.
        if (isColumnConst(*src.column))
        {
            block.getByPosition(result).column = res_type->createColumnConstWithDefaultValue(input_rows_count);
            return;
        }

        auto res_column = removeNullable(res_type)->createColumn();
        auto * src_column = src.column.get();
        ColumnPtr null_map_column = nullptr;
        const NullMap * null_map = nullptr;
        if (auto * nullable_column = checkAndGetColumn<ColumnNullable>(src_column))
        {
            src_column = &nullable_column->getNestedColumn();
            null_map_column = nullable_column->getNullMapColumnPtr();
            null_map = &nullable_column->getNullMapData();
        }

        dispatchForSourceType(*removeNullable(src.type), [&](auto field_type_tag)
        {
            using SrcFieldType = decltype(field_type_tag);

            process(assert_cast<const ColumnVector<SrcFieldType> &>(*src_column).getData(),
                assert_cast<ColumnVector<DstFieldType<SrcFieldType>> &>(*res_column).getData(), null_map);
        });

        if (null_map_column)
            block.getByPosition(result).column = ColumnNullable::create(std::move(res_column), null_map_column);
        else
            block.getByPosition(result).column = std::move(res_column);
    }
};

}
