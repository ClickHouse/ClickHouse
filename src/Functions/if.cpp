#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnVector.h>
#include <Columns/MaskOperations.h>
#include <Core/Settings.h>
#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NumberTraits.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionIfBase.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <type_traits>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_variant_type;
    extern const SettingsBool use_variant_as_common_type;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

namespace
{

using namespace GatherUtils;

/** Selection function by condition: if(cond, then, else).
  * cond - UInt8
  * then, else - numeric types for which there is a general type, or dates, datetimes, or strings, or arrays of these types.
  * For better performance, try to use branch free code for numeric types(i.e. cond ? a : b --> !!cond * a + !cond * b)
*/

template <typename ResultType>
concept is_native_int_or_decimal_v
    = std::is_integral_v<ResultType> || (is_decimal<ResultType> && sizeof(ResultType) <= 8);

// This macro performs a branch-free conditional assignment for floating point types.
// It uses bitwise operations to avoid branching, which can be beneficial for performance.
#define BRANCHFREE_IF_FLOAT(TYPE, vc, va, vb, vr) \
    using UIntType = typename NumberTraits::Construct<false, false, sizeof(TYPE)>::Type; \
    using IntType = typename NumberTraits::Construct<true, false, sizeof(TYPE)>::Type; \
    auto mask = static_cast<UIntType>(static_cast<IntType>(vc) - 1); \
    auto new_a = static_cast<ResultType>(va); \
    auto new_b = static_cast<ResultType>(vb); \
    UIntType uint_a; \
    std::memcpy(&uint_a, &new_a, sizeof(UIntType)); \
    UIntType uint_b; \
    std::memcpy(&uint_b, &new_b, sizeof(UIntType)); \
    UIntType tmp = (~mask & uint_a) | (mask & uint_b); \
    (vr) = *(reinterpret_cast<ResultType *>(&tmp));

template <typename ArrayCond, typename ArrayA, typename ArrayB, typename ArrayResult, typename ResultType>
inline void fillVectorVector(const ArrayCond & cond, const ArrayA & a, const ArrayB & b, ArrayResult & res)
{

    size_t size = cond.size();
    for (size_t i = 0; i < size; ++i)
    {
        if constexpr (is_native_int_or_decimal_v<ResultType>)
            res[i] = !!cond[i] * static_cast<ResultType>(a[i]) + (!cond[i]) * static_cast<ResultType>(b[i]);
        else if constexpr (std::is_floating_point_v<ResultType>)
        {
            BRANCHFREE_IF_FLOAT(ResultType, cond[i], a[i], b[i], res[i])
        }
        else
        {
            res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b[i]);
        }
    }
}

template <typename ArrayCond, typename ArrayA, typename B, typename ArrayResult, typename ResultType>
inline void fillVectorConstant(const ArrayCond & cond, const ArrayA & a, B b, ArrayResult & res)
{
    size_t size = cond.size();
    for (size_t i = 0; i < size; ++i)
    {
        if constexpr (is_native_int_or_decimal_v<ResultType>)
            res[i] = !!cond[i] * static_cast<ResultType>(a[i]) + (!cond[i]) * static_cast<ResultType>(b);
        else if constexpr (std::is_floating_point_v<ResultType>)
        {
            BRANCHFREE_IF_FLOAT(ResultType, cond[i], a[i], b, res[i])
        }
        else
            res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b);
    }
}

template <typename ArrayCond, typename A, typename ArrayB, typename ArrayResult, typename ResultType>
inline void fillConstantVector(const ArrayCond & cond, A a, const ArrayB & b, ArrayResult & res)
{
    size_t size = cond.size();
    for (size_t i = 0; i < size; ++i)
    {
        if constexpr (is_native_int_or_decimal_v<ResultType>)
            res[i] = !!cond[i] * static_cast<ResultType>(a) + (!cond[i]) * static_cast<ResultType>(b[i]);
        else if constexpr (std::is_floating_point_v<ResultType>)
        {
            BRANCHFREE_IF_FLOAT(ResultType, cond[i], a, b[i], res[i])
        }
        else
            res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b[i]);
    }
}

template <typename ArrayCond, typename A, typename B, typename ArrayResult, typename ResultType>
inline void fillConstantConstant(const ArrayCond & cond, A a, B b, ArrayResult & res)
{
    size_t size = cond.size();

    /// We manually optimize the loop for types like (U)Int128|256 or Decimal128/256 to avoid branches
    if constexpr (is_over_big_int<ResultType>)
    {
        alignas(64) const ResultType ab[2] = {static_cast<ResultType>(a), static_cast<ResultType>(b)};
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = ab[!cond[i]];
        }
    }
    else if constexpr (std::is_same_v<ResultType, Decimal32> || std::is_same_v<ResultType, Decimal64>)
    {
        ResultType new_a = static_cast<ResultType>(a);
        ResultType new_b = static_cast<ResultType>(b);
        for (size_t i = 0; i < size; ++i)
        {
            /// Reuse new_a and new_b to achieve auto-vectorization
            res[i] = cond[i] ? new_a : new_b;
        }
    }
    else
    {
        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b);
    }
}

template <typename A, typename B, typename ResultType>
struct NumIfImpl
{
    using ArrayCond = PaddedPODArray<UInt8>;
    using ArrayA = typename ColumnVector<A>::Container;
    using ArrayB = typename ColumnVector<B>::Container;
    using ColVecResult = ColumnVector<ResultType>;
    using ArrayResult = typename ColVecResult::Container;

    static ColumnPtr vectorVector(const ArrayCond & cond, const ArrayA & a, const ArrayB & b, UInt32)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        ArrayResult & res = col_res->getData();
        fillVectorVector<ArrayCond, ArrayA, ArrayB, ArrayResult, ResultType>(cond, a, b, res);
        return col_res;
    }

    static ColumnPtr vectorConstant(const ArrayCond & cond, const ArrayA & a, B b, UInt32)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        ArrayResult & res = col_res->getData();
        fillVectorConstant<ArrayCond, ArrayA, B, ArrayResult, ResultType>(cond, a, b, res);
        return col_res;
    }

    static ColumnPtr constantVector(const ArrayCond & cond, A a, const ArrayB & b, UInt32)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        ArrayResult & res = col_res->getData();
        fillConstantVector<ArrayCond, A, ArrayB, ArrayResult, ResultType>(cond, a, b, res);
        return col_res;
    }

    static ColumnPtr constantConstant(const ArrayCond & cond, A a, B b, UInt32)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        ArrayResult & res = col_res->getData();

        fillConstantConstant<ArrayCond, A, B, ArrayResult, ResultType>(cond, a, b, res);
        return col_res;
    }
};

template <typename A, typename B, typename R>
struct NumIfImpl<Decimal<A>, Decimal<B>, Decimal<R>>
{
    using ResultType = Decimal<R>;
    using ArrayCond = PaddedPODArray<UInt8>;
    using ArrayA = typename ColumnDecimal<Decimal<A>>::Container;
    using ArrayB = typename ColumnDecimal<Decimal<B>>::Container;
    using ColVecResult = ColumnDecimal<ResultType>;
    using Block = ColumnsWithTypeAndName;
    using ArrayResult = typename ColVecResult::Container;

    static ColumnPtr vectorVector(const ArrayCond & cond, const ArrayA & a, const ArrayB & b, UInt32 scale)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size, scale);
        ArrayResult & res = col_res->getData();
        fillVectorVector<ArrayCond, ArrayA, ArrayB, ArrayResult, ResultType>(cond, a, b, res);
        return col_res;
    }

    static ColumnPtr vectorConstant(const ArrayCond & cond, const ArrayA & a, B b, UInt32 scale)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size, scale);
        ArrayResult & res = col_res->getData();
        fillVectorConstant<ArrayCond, ArrayA, B, ArrayResult, ResultType>(cond, a, b, res);
        return col_res;
    }

    static ColumnPtr constantVector(const ArrayCond & cond, A a, const ArrayB & b, UInt32 scale)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size, scale);
        ArrayResult & res = col_res->getData();
        fillConstantVector<ArrayCond, A, ArrayB, ArrayResult, ResultType>(cond, a, b, res);
        return col_res;
    }

    static ColumnPtr constantConstant(const ArrayCond & cond, A a, B b, UInt32 scale)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size, scale);
        ArrayResult & res = col_res->getData();

        fillConstantConstant<ArrayCond, A, B, ArrayResult, ResultType>(cond, a, b, res);
        return col_res;
    }
};


class FunctionIf : public FunctionIfBase
{
public:
    static constexpr auto name = "if";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionIf>(context->getSettingsRef()[Setting::allow_experimental_variant_type] && context->getSettingsRef()[Setting::use_variant_as_common_type]);
    }

    explicit FunctionIf(bool use_variant_when_no_common_type_ = false) : FunctionIfBase(), use_variant_when_no_common_type(use_variant_when_no_common_type_) {}

private:
    bool use_variant_when_no_common_type = false;

    template <typename T0, typename T1>
    static UInt32 decimalScale(const ColumnsWithTypeAndName & arguments [[maybe_unused]])
    {
        if constexpr (is_decimal<T0> && is_decimal<T1>)
        {
            UInt32 left_scale = getDecimalScale(*arguments[1].type);
            UInt32 right_scale = getDecimalScale(*arguments[2].type);
            if (left_scale != right_scale)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Conditional functions with different Decimal scales");
            return left_scale;
        }
        else
            return std::numeric_limits<UInt32>::max();
    }

    template <typename T0, typename T1, typename ColVecT0, typename ColVecT1>
    ColumnPtr executeRightType(
        [[maybe_unused]] const ColumnUInt8 * cond_col,
        [[maybe_unused]] const ColumnsWithTypeAndName & arguments,
        [[maybe_unused]] const ColVecT0 * col_left) const
    {
        using ResultType = typename NumberTraits::ResultOfIf<T0, T1>::Type;

        if constexpr (std::is_same_v<ResultType, NumberTraits::Error>)
        {
            return nullptr;
        }
        else
        {
            const IColumn * col_right_untyped = arguments[2].column.get();
            UInt32 scale = decimalScale<T0, T1>(arguments);

            if (const auto * col_right_vec = checkAndGetColumn<ColVecT1>(col_right_untyped))
            {
                return NumIfImpl<T0, T1, ResultType>::vectorVector(
                    cond_col->getData(), col_left->getData(), col_right_vec->getData(), scale);
            }
            if (const auto * col_right_const = checkAndGetColumnConst<ColVecT1>(col_right_untyped))
            {
                return NumIfImpl<T0, T1, ResultType>::vectorConstant(
                    cond_col->getData(), col_left->getData(), col_right_const->template getValue<T1>(), scale);
            }

            return nullptr;
        }
    }

    template <typename T0, typename T1, typename ColVecT0, typename ColVecT1>
    ColumnPtr executeConstRightType(
        [[maybe_unused]] const ColumnUInt8 * cond_col,
        [[maybe_unused]] const ColumnsWithTypeAndName & arguments,
        [[maybe_unused]] const ColumnConst * col_left) const
    {
        using ResultType = typename NumberTraits::ResultOfIf<T0, T1>::Type;

        if constexpr (std::is_same_v<ResultType, NumberTraits::Error>)
        {
            return nullptr;
        }
        else
        {
            const IColumn * col_right_untyped = arguments[2].column.get();
            UInt32 scale = decimalScale<T0, T1>(arguments);

            if (const auto * col_right_vec = checkAndGetColumn<ColVecT1>(col_right_untyped))
            {
                return NumIfImpl<T0, T1, ResultType>::constantVector(
                    cond_col->getData(), col_left->template getValue<T0>(), col_right_vec->getData(), scale);
            }
            if (const auto * col_right_const = checkAndGetColumnConst<ColVecT1>(col_right_untyped))
            {
                return NumIfImpl<T0, T1, ResultType>::constantConstant(
                    cond_col->getData(), col_left->template getValue<T0>(), col_right_const->template getValue<T1>(), scale);
            }

            return nullptr;
        }
    }

    template <typename T0, typename T1, typename ColVecT0, typename ColVecT1>
    ColumnPtr executeRightTypeArray(
        [[maybe_unused]] const ColumnUInt8 * cond_col,
        [[maybe_unused]] const ColumnsWithTypeAndName & arguments,
        [[maybe_unused]] const DataTypePtr result_type,
        [[maybe_unused]] const ColumnArray * col_left_array,
        [[maybe_unused]] size_t input_rows_count) const
    {
        using ResultType = typename NumberTraits::ResultOfIf<T0, T1>::Type;

        if constexpr (std::is_same_v<ResultType, NumberTraits::Error>)
        {
            return nullptr;
        }
        else
        {
            const IColumn * col_right_untyped = arguments[2].column.get();

            if (const auto * col_right_array = checkAndGetColumn<ColumnArray>(col_right_untyped))
            {
                const ColVecT1 * col_right_vec = checkAndGetColumn<ColVecT1>(&col_right_array->getData());
                if (!col_right_vec)
                    return nullptr;

                auto res = result_type->createColumn();
                auto & arr_res = assert_cast<ColumnArray &>(*res);

                conditional(
                    NumericArraySource<T0>(*col_left_array),
                    NumericArraySource<T1>(*col_right_array),
                    NumericArraySink<ResultType>(arr_res.getData(), arr_res.getOffsets(), input_rows_count),
                    cond_col->getData());

                return res;
            }
            if (const auto * col_right_const_array = checkAndGetColumnConst<ColumnArray>(col_right_untyped))
            {
                const ColumnArray * col_right_const_array_data = checkAndGetColumn<ColumnArray>(&col_right_const_array->getDataColumn());
                if (!checkColumn<ColVecT1>(&col_right_const_array_data->getData()))
                    return nullptr;

                auto res = result_type->createColumn();
                auto & arr_res = assert_cast<ColumnArray &>(*res);

                conditional(
                    NumericArraySource<T0>(*col_left_array),
                    ConstSource<NumericArraySource<T1>>(*col_right_const_array),
                    NumericArraySink<ResultType>(arr_res.getData(), arr_res.getOffsets(), input_rows_count),
                    cond_col->getData());

                return res;
            }

            return nullptr;
        }
    }

    template <typename T0, typename T1, typename ColVecT0, typename ColVecT1>
    ColumnPtr executeConstRightTypeArray(
        [[maybe_unused]] const ColumnUInt8 * cond_col,
        [[maybe_unused]] const ColumnsWithTypeAndName & arguments,
        [[maybe_unused]] const DataTypePtr & result_type,
        [[maybe_unused]] const ColumnConst * col_left_const_array,
        [[maybe_unused]] size_t input_rows_count) const
    {
        using ResultType = typename NumberTraits::ResultOfIf<T0, T1>::Type;

        if constexpr (std::is_same_v<ResultType, NumberTraits::Error>)
        {
            return nullptr;
        }
        else
        {
            const IColumn * col_right_untyped = arguments[2].column.get();

            if (const auto * col_right_array = checkAndGetColumn<ColumnArray>(col_right_untyped))
            {
                const ColVecT1 * col_right_vec = checkAndGetColumn<ColVecT1>(&col_right_array->getData());

                if (!col_right_vec)
                    return nullptr;

                auto res = result_type->createColumn();
                auto & arr_res = assert_cast<ColumnArray &>(*res);

                conditional(
                    ConstSource<NumericArraySource<T0>>(*col_left_const_array),
                    NumericArraySource<T1>(*col_right_array),
                    NumericArraySink<ResultType>(arr_res.getData(), arr_res.getOffsets(), input_rows_count),
                    cond_col->getData());

                return res;
            }
            if (const auto * col_right_const_array = checkAndGetColumnConst<ColumnArray>(col_right_untyped))
            {
                const ColumnArray * col_right_const_array_data = checkAndGetColumn<ColumnArray>(&col_right_const_array->getDataColumn());
                if (!checkColumn<ColVecT1>(&col_right_const_array_data->getData()))
                    return nullptr;

                auto res = result_type->createColumn();
                auto & arr_res = assert_cast<ColumnArray &>(*res);

                conditional(
                    ConstSource<NumericArraySource<T0>>(*col_left_const_array),
                    ConstSource<NumericArraySource<T1>>(*col_right_const_array),
                    NumericArraySink<ResultType>(arr_res.getData(), arr_res.getOffsets(), input_rows_count),
                    cond_col->getData());

                return res;
            }

            return nullptr;
        }
    }

    template <typename T0, typename T1>
    ColumnPtr executeTyped(
        const ColumnUInt8 * cond_col, const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        using ColVecT0 = ColumnVectorOrDecimal<T0>;
        using ColVecT1 = ColumnVectorOrDecimal<T1>;

        const IColumn * col_left_untyped = arguments[1].column.get();

        ColumnPtr right_column = nullptr;

        if (const auto * col_left = checkAndGetColumn<ColVecT0>(col_left_untyped))
        {
            right_column = executeRightType<T0, T1, ColVecT0, ColVecT1>(cond_col, arguments, col_left);
        }
        else if (const auto * col_const_left = checkAndGetColumnConst<ColVecT0>(col_left_untyped))
        {
            right_column = executeConstRightType<T0, T1, ColVecT0, ColVecT1>(cond_col, arguments, col_const_left);
        }
        else if (const auto * col_arr_left = checkAndGetColumn<ColumnArray>(col_left_untyped))
        {
            if (auto col_arr_left_elems = checkAndGetColumn<ColVecT0>(&col_arr_left->getData()))
            {
                right_column = executeRightTypeArray<T0, T1, ColVecT0, ColVecT1>(
                    cond_col, arguments, result_type, col_arr_left, input_rows_count);
            }
        }
        else if (const auto * col_const_arr_left = checkAndGetColumnConst<ColumnArray>(col_left_untyped))
        {
            if (checkColumn<ColVecT0>(&assert_cast<const ColumnArray &>(col_const_arr_left->getDataColumn()).getData()))
            {
                right_column = executeConstRightTypeArray<T0, T1, ColVecT0, ColVecT1>(
                    cond_col, arguments, result_type, col_const_arr_left, input_rows_count);
            }
        }

        return right_column;
    }

    static ColumnPtr executeString(const ColumnUInt8 * cond_col, const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type)
    {
        const IColumn * col_then_untyped = arguments[1].column.get();
        const IColumn * col_else_untyped = arguments[2].column.get();

        const ColumnString * col_then = checkAndGetColumn<ColumnString>(col_then_untyped);
        const ColumnString * col_else = checkAndGetColumn<ColumnString>(col_else_untyped);
        const ColumnFixedString * col_then_fixed = checkAndGetColumn<ColumnFixedString>(col_then_untyped);
        const ColumnFixedString * col_else_fixed = checkAndGetColumn<ColumnFixedString>(col_else_untyped);
        const ColumnConst * col_then_const = checkAndGetColumnConst<ColumnString>(col_then_untyped);
        const ColumnConst * col_else_const = checkAndGetColumnConst<ColumnString>(col_else_untyped);
        const ColumnConst * col_then_const_fixed = checkAndGetColumnConst<ColumnFixedString>(col_then_untyped);
        const ColumnConst * col_else_const_fixed = checkAndGetColumnConst<ColumnFixedString>(col_else_untyped);

        const PaddedPODArray<UInt8> & cond_data = cond_col->getData();
        size_t rows = cond_data.size();

        if (isFixedString(result_type))
        {
            /// The result is FixedString.

            auto col_res_untyped = result_type->createColumn();
            ColumnFixedString * col_res = assert_cast<ColumnFixedString *>(col_res_untyped.get());
            auto sink = FixedStringSink(*col_res, rows);

            if (col_then_fixed && col_else_fixed)
                conditional(FixedStringSource(*col_then_fixed), FixedStringSource(*col_else_fixed), sink, cond_data);
            else if (col_then_fixed && col_else_const_fixed)
                conditional(FixedStringSource(*col_then_fixed), ConstSource<FixedStringSource>(*col_else_const_fixed), sink, cond_data);
            else if (col_then_const_fixed && col_else_fixed)
                conditional(ConstSource<FixedStringSource>(*col_then_const_fixed), FixedStringSource(*col_else_fixed), sink, cond_data);
            else if (col_then_const_fixed && col_else_const_fixed)
                conditional(ConstSource<FixedStringSource>(*col_then_const_fixed),
                            ConstSource<FixedStringSource>(*col_else_const_fixed), sink, cond_data);
            else
                return nullptr;

            return col_res_untyped;
        }

        if (isString(result_type))
        {
            /// The result is String.

            auto col_res = ColumnString::create();
            auto sink = StringSink(*col_res, rows);

            if (col_then && col_else)
                conditional(StringSource(*col_then), StringSource(*col_else), sink, cond_data);
            else if (col_then && col_else_const)
                conditional(StringSource(*col_then), ConstSource<StringSource>(*col_else_const), sink, cond_data);
            else if (col_then_const && col_else)
                conditional(ConstSource<StringSource>(*col_then_const), StringSource(*col_else), sink, cond_data);
            else if (col_then_const && col_else_const)
                conditional(ConstSource<StringSource>(*col_then_const), ConstSource<StringSource>(*col_else_const), sink, cond_data);
            else if (col_then && col_else_fixed)
                conditional(StringSource(*col_then), FixedStringSource(*col_else_fixed), sink, cond_data);
            else if (col_then_fixed && col_else)
                conditional(FixedStringSource(*col_then_fixed), StringSource(*col_else), sink, cond_data);
            else if (col_then_const && col_else_fixed)
                conditional(ConstSource<StringSource>(*col_then_const), FixedStringSource(*col_else_fixed), sink, cond_data);
            else if (col_then_fixed && col_else_const)
                conditional(FixedStringSource(*col_then_fixed), ConstSource<StringSource>(*col_else_const), sink, cond_data);
            else if (col_then && col_else_const_fixed)
                conditional(StringSource(*col_then), ConstSource<FixedStringSource>(*col_else_const_fixed), sink, cond_data);
            else if (col_then_const_fixed && col_else)
                conditional(ConstSource<FixedStringSource>(*col_then_const_fixed), StringSource(*col_else), sink, cond_data);
            else if (col_then_const && col_else_const_fixed)
                conditional(ConstSource<StringSource>(*col_then_const), ConstSource<FixedStringSource>(*col_else_const_fixed), sink, cond_data);
            else if (col_then_const_fixed && col_else_const)
                conditional(ConstSource<FixedStringSource>(*col_then_const_fixed), ConstSource<StringSource>(*col_else_const), sink, cond_data);
            else if (col_then_fixed && col_else_fixed)
                conditional(FixedStringSource(*col_then_fixed), FixedStringSource(*col_else_fixed), sink, cond_data);
            else if (col_then_fixed && col_else_const_fixed)
                conditional(FixedStringSource(*col_then_fixed), ConstSource<FixedStringSource>(*col_else_const_fixed), sink, cond_data);
            else if (col_then_const_fixed && col_else_fixed)
                conditional(ConstSource<FixedStringSource>(*col_then_const_fixed), FixedStringSource(*col_else_fixed), sink, cond_data);
            else if (col_then_const_fixed && col_else_const_fixed)
                conditional(ConstSource<FixedStringSource>(*col_then_const_fixed),
                            ConstSource<FixedStringSource>(*col_else_const_fixed), sink, cond_data);
            else
                return nullptr;

            return col_res;
        }

        return nullptr;
    }

    static ColumnPtr executeGenericArray(const ColumnUInt8 * cond_col, const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type)
    {
        /// For generic implementation, arrays must be of same type.
        if (!arguments[1].type->equals(*arguments[2].type))
            return nullptr;

        const IColumn * col_then_untyped = arguments[1].column.get();
        const IColumn * col_else_untyped = arguments[2].column.get();

        const ColumnArray * col_arr_then = checkAndGetColumn<ColumnArray>(col_then_untyped);
        const ColumnArray * col_arr_else = checkAndGetColumn<ColumnArray>(col_else_untyped);
        const ColumnConst * col_arr_then_const = checkAndGetColumnConst<ColumnArray>(col_then_untyped);
        const ColumnConst * col_arr_else_const = checkAndGetColumnConst<ColumnArray>(col_else_untyped);

        const PaddedPODArray<UInt8> & cond_data = cond_col->getData();
        size_t rows = cond_data.size();

        if ((col_arr_then || col_arr_then_const)
            && (col_arr_else || col_arr_else_const))
        {
            auto res = result_type->createColumn();
            auto * col_res = assert_cast<ColumnArray *>(res.get());

            if (col_arr_then && col_arr_else)
                conditional(GenericArraySource(*col_arr_then), GenericArraySource(*col_arr_else), GenericArraySink(col_res->getData(), col_res->getOffsets(), rows), cond_data);
            else if (col_arr_then && col_arr_else_const)
                conditional(GenericArraySource(*col_arr_then), ConstSource<GenericArraySource>(*col_arr_else_const), GenericArraySink(col_res->getData(), col_res->getOffsets(), rows), cond_data);
            else if (col_arr_then_const && col_arr_else)
                conditional(ConstSource<GenericArraySource>(*col_arr_then_const), GenericArraySource(*col_arr_else), GenericArraySink(col_res->getData(), col_res->getOffsets(), rows), cond_data);
            else if (col_arr_then_const && col_arr_else_const)
                conditional(ConstSource<GenericArraySource>(*col_arr_then_const), ConstSource<GenericArraySource>(*col_arr_else_const), GenericArraySink(col_res->getData(), col_res->getOffsets(), rows), cond_data);
            else
                return nullptr;
            return res;
        }

        return nullptr;
    }

    ColumnPtr executeTuple(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        /// Calculate function for each corresponding elements of tuples.

        const ColumnWithTypeAndName & arg1 = arguments[1];
        const ColumnWithTypeAndName & arg2 = arguments[2];

        Columns col1_contents;
        Columns col2_contents;

        if (const ColumnTuple * tuple1 = typeid_cast<const ColumnTuple *>(arg1.column.get()))
            col1_contents = tuple1->getColumnsCopy();
        else if (const ColumnConst * const_tuple = checkAndGetColumnConst<ColumnTuple>(arg1.column.get()))
            col1_contents = convertConstTupleToConstantElements(*const_tuple);
        else
            return nullptr;

        if (const ColumnTuple * tuple2 = typeid_cast<const ColumnTuple *>(arg2.column.get()))
            col2_contents = tuple2->getColumnsCopy();
        else if (const ColumnConst * const_tuple = checkAndGetColumnConst<ColumnTuple>(arg2.column.get()))
            col2_contents = convertConstTupleToConstantElements(*const_tuple);
        else
            return nullptr;

        const DataTypeTuple & type1 = static_cast<const DataTypeTuple &>(*arg1.type);
        const DataTypeTuple & type2 = static_cast<const DataTypeTuple &>(*arg2.type);
        const DataTypeTuple & tuple_result = static_cast<const DataTypeTuple &>(*result_type);

        ColumnsWithTypeAndName temporary_columns(3);
        temporary_columns[0] = arguments[0];

        size_t tuple_size = type1.getElements().size();
        if (tuple_size == 0)
            return ColumnTuple::create(input_rows_count);

        Columns tuple_columns(tuple_size);

        for (size_t i = 0; i < tuple_size; ++i)
        {
            temporary_columns[1] = {col1_contents[i], type1.getElements()[i], {}};
            temporary_columns[2] = {col2_contents[i], type2.getElements()[i], {}};

            tuple_columns[i] = executeImpl(temporary_columns, tuple_result.getElements()[i], input_rows_count);
        }

        return ColumnTuple::create(tuple_columns);
    }

    ColumnPtr executeMap(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        auto extract_kv_from_map = [](const ColumnMap * map)
        {
            const ColumnTuple & tuple = map->getNestedData();
            const auto & keys = tuple.getColumnPtr(0);
            const auto & values = tuple.getColumnPtr(1);
            const auto & offsets = map->getNestedColumn().getOffsetsPtr();
            return std::make_pair(ColumnArray::create(keys, offsets), ColumnArray::create(values, offsets));
        };

        /// Extract keys and values from both arguments
        Columns key_cols(2);
        Columns value_cols(2);
        for (size_t i = 0; i < 2; ++i)
        {
            const auto & arg = arguments[i + 1];
            if (const ColumnMap * map = checkAndGetColumn<ColumnMap>(arg.column.get()))
            {
                auto [key_col, value_col] = extract_kv_from_map(map);
                key_cols[i] = std::move(key_col);
                value_cols[i] = std::move(value_col);
            }
            else if (const ColumnConst * const_map = checkAndGetColumnConst<ColumnMap>(arg.column.get()))
            {
                const ColumnMap * map_data = assert_cast<const ColumnMap *>(&const_map->getDataColumn());
                auto [key_col, value_col] = extract_kv_from_map(map_data);

                size_t size = const_map->size();
                key_cols[i] = ColumnConst::create(std::move(key_col), size);
                value_cols[i] = ColumnConst::create(std::move(value_col), size);
            }
            else
                return nullptr;
        }

        /// Compose temporary columns for keys and values
        ColumnsWithTypeAndName key_columns(3);
        key_columns[0] = arguments[0];
        ColumnsWithTypeAndName value_columns(3);
        value_columns[0] = arguments[0];
        for (size_t i = 0; i < 2; ++i)
        {
            const auto & arg = arguments[i + 1];
            const DataTypeMap & type = static_cast<const DataTypeMap &>(*arg.type);
            const auto & key_type = type.getKeyType();
            const auto & value_type = type.getValueType();
            key_columns[i + 1] = {key_cols[i], std::make_shared<DataTypeArray>(key_type), {}};
            value_columns[i + 1] = {value_cols[i], std::make_shared<DataTypeArray>(value_type), {}};
        }

        /// Calculate function corresponding keys and values in map
        const DataTypeMap & map_result_type = static_cast<const DataTypeMap &>(*result_type);
        auto key_result_type = std::make_shared<DataTypeArray>(map_result_type.getKeyType());
        auto value_result_type = std::make_shared<DataTypeArray>(map_result_type.getValueType());
        ColumnPtr key_result = executeImpl(key_columns, key_result_type, input_rows_count);
        ColumnPtr value_result = executeImpl(value_columns, value_result_type, input_rows_count);

        /// key_result and value_result are not constant columns otherwise we won't reach here in executeMap
        const auto * key_array = assert_cast<const ColumnArray *>(key_result.get());
        const auto * value_array = assert_cast<const ColumnArray *>(value_result.get());
        if (!key_array)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Key result column should be {} instead of {} in executeMap of function {}",
                key_result_type->getName(),
                key_result->getName(),
                getName());
        if (!value_array)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Value result column should be {} instead of {} in executeMap of function {}",
                key_result_type->getName(),
                value_result->getName(),
                getName());
        if (!key_array->hasEqualOffsets(*value_array))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Key array and value array must have equal sizes in executeMap of function {}", getName());

        auto nested_column = ColumnArray::create(
            ColumnTuple::create(Columns{key_array->getDataPtr(), value_array->getDataPtr()}), key_array->getOffsetsPtr());
        return ColumnMap::create(std::move(nested_column));
    }

    static ColumnPtr executeGeneric(
        const ColumnUInt8 * cond_col, const ColumnsWithTypeAndName & arguments, size_t input_rows_count, bool use_variant_when_no_common_type)
    {
        /// Convert both columns to the common type (if needed).
        const ColumnWithTypeAndName & arg1 = arguments[1];
        const ColumnWithTypeAndName & arg2 = arguments[2];

        DataTypePtr common_type;
        if (use_variant_when_no_common_type)
            common_type = getLeastSupertypeOrVariant(DataTypes{arg1.type, arg2.type});
        else
            common_type = getLeastSupertype(DataTypes{arg1.type, arg2.type});

        ColumnPtr col_then = castColumn(arg1, common_type);
        ColumnPtr col_else = castColumn(arg2, common_type);

        MutableColumnPtr result_column = common_type->createColumn();
        result_column->reserve(input_rows_count);

        bool then_is_const = isColumnConst(*col_then);
        bool else_is_const = isColumnConst(*col_else);

        const auto & cond_array = cond_col->getData();

        if (then_is_const && else_is_const)
        {
            const IColumn & then_nested_column = assert_cast<const ColumnConst &>(*col_then).getDataColumn();
            const IColumn & else_nested_column = assert_cast<const ColumnConst &>(*col_else).getDataColumn();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                if (cond_array[i])
                    result_column->insertFrom(then_nested_column, 0);
                else
                    result_column->insertFrom(else_nested_column, 0);
            }
        }
        else if (then_is_const)
        {
            const IColumn & then_nested_column = assert_cast<const ColumnConst &>(*col_then).getDataColumn();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                if (cond_array[i])
                    result_column->insertFrom(then_nested_column, 0);
                else
                    result_column->insertFrom(*col_else, i);
            }
        }
        else if (else_is_const)
        {
            const IColumn & else_nested_column = assert_cast<const ColumnConst &>(*col_else).getDataColumn();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                if (cond_array[i])
                    result_column->insertFrom(*col_then, i);
                else
                    result_column->insertFrom(else_nested_column, 0);
            }
        }
        else
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                if (cond_array[i])
                    result_column->insertFrom(*col_then, i);
                else
                    result_column->insertFrom(*col_else, i);
            }
        }

        return result_column;
    }

    ColumnPtr executeForConstAndNullableCondition(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const
    {
        const ColumnWithTypeAndName & arg_cond = arguments[0];
        bool cond_is_null = arg_cond.column->onlyNull();

        ColumnPtr not_const_condition = arg_cond.column;
        bool cond_is_const = false;
        bool cond_is_true = false;
        bool cond_is_false = false;
        if (const auto * const_arg = checkAndGetColumn<ColumnConst>(&*arg_cond.column))
        {
            cond_is_const = true;
            not_const_condition = const_arg->getDataColumnPtr();
            ColumnPtr data_column = const_arg->getDataColumnPtr();
            if (const auto * const_nullable_arg = checkAndGetColumn<ColumnNullable>(&*data_column))
            {
                data_column = const_nullable_arg->getNestedColumnPtr();
                if (!data_column->empty())
                    cond_is_null = const_nullable_arg->getNullMapData()[0];
            }

            if (!data_column->empty())
            {
                cond_is_true = !cond_is_null && checkAndGetColumn<ColumnUInt8>(*data_column).getBool(0);
                cond_is_false = !cond_is_null && !cond_is_true;
            }
        }

        const auto & column1 = arguments[1];
        const auto & column2 = arguments[2];

        if (cond_is_true)
            return castColumn(column1, result_type);
        if (cond_is_false || cond_is_null)
            return castColumn(column2, result_type);

        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&*not_const_condition))
        {
            ColumnPtr new_cond_column = nullable->getNestedColumnPtr();
            size_t column_size = arg_cond.column->size();

            if (checkAndGetColumn<ColumnUInt8>(&*new_cond_column))
            {
                auto nested_column_copy = new_cond_column->cloneResized(new_cond_column->size());
                typeid_cast<ColumnUInt8 *>(nested_column_copy.get())->applyZeroMap(nullable->getNullMapData());
                new_cond_column = std::move(nested_column_copy);

                if (cond_is_const)
                    new_cond_column = ColumnConst::create(new_cond_column, column_size);
            }
            else
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of {} condition", arg_cond.column->getName(), getName());

            ColumnsWithTypeAndName temporary_columns
            {
                { new_cond_column, removeNullable(arg_cond.type), arg_cond.name },
                column1,
                column2,
            };

            return executeImpl(temporary_columns, result_type, new_cond_column->size());
        }

        return nullptr;
    }

    template <typename AnyColumnPtr>
    static ColumnPtr materializeColumnIfConst(const AnyColumnPtr & column)
    {
        return column->convertToFullColumnIfConst();
    }

    static ColumnPtr makeNullableColumnIfNot(const ColumnPtr & column)
    {
        auto materialized = materializeColumnIfConst(column);

        if (isColumnNullable(*materialized))
            return materialized;

        return ColumnNullable::create(materialized, ColumnUInt8::create(column->size(), 0));
    }

    /// Return nested column recursively removing Nullable, examples:
    /// Nullable(size = 1, Int32(size = 1), UInt8(size = 1)) -> Int32(size = 1)
    /// Const(size = 0, Nullable(size = 1, Int32(size = 1), UInt8(size = 1))) ->
    /// Const(size = 0, Int32(size = 1))
    static ColumnPtr recursiveGetNestedColumnWithoutNullable(const ColumnPtr & column)
    {
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&*column))
        {
            /// Nullable cannot contain Nullable
            return nullable->getNestedColumnPtr();
        }
        if (const auto * column_const = checkAndGetColumn<ColumnConst>(&*column))
        {
            /// Save Constant, but remove Nullable
            return ColumnConst::create(recursiveGetNestedColumnWithoutNullable(column_const->getDataColumnPtr()), column->size());
        }

        return column;
    }

    ColumnPtr executeForNullableThenElse(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        /// If result type is Variant/Dynamic, we don't need to remove Nullable.
        if (isVariant(result_type) || isDynamic(result_type))
            return nullptr;

        const ColumnWithTypeAndName & arg_cond = arguments[0];
        const ColumnWithTypeAndName & arg_then = arguments[1];
        const ColumnWithTypeAndName & arg_else = arguments[2];

        const auto * then_is_nullable = checkAndGetColumn<ColumnNullable>(&*arg_then.column);
        const auto * else_is_nullable = checkAndGetColumn<ColumnNullable>(&*arg_else.column);

        if (!then_is_nullable && !else_is_nullable)
            return nullptr;

        /** Calculate null mask of result and nested column separately.
          */
        ColumnPtr result_null_mask;

        {
            ColumnsWithTypeAndName temporary_columns(
            {
                arg_cond,
                {
                    then_is_nullable
                        ? then_is_nullable->getNullMapColumnPtr()
                        : DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count),
                    std::make_shared<DataTypeUInt8>(),
                    ""
                },
                {
                    else_is_nullable
                        ? else_is_nullable->getNullMapColumnPtr()
                        : DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count),
                    std::make_shared<DataTypeUInt8>(),
                    ""
                }
            });

            result_null_mask = executeImpl(temporary_columns, std::make_shared<DataTypeUInt8>(), input_rows_count);
        }

        ColumnPtr result_nested_column;

        {
            ColumnsWithTypeAndName temporary_columns(
            {
                arg_cond,
                {
                    recursiveGetNestedColumnWithoutNullable(arg_then.column),
                    removeNullable(arg_then.type),
                    ""
                },
                {
                    recursiveGetNestedColumnWithoutNullable(arg_else.column),
                    removeNullable(arg_else.type),
                    ""
                }
            });

            result_nested_column = executeImpl(temporary_columns, removeNullable(result_type), temporary_columns.front().column->size());
        }

        return ColumnNullable::create(
            materializeColumnIfConst(result_nested_column), materializeColumnIfConst(result_null_mask));
    }

    ColumnPtr executeForNullThenElse(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        const ColumnWithTypeAndName & arg_cond = arguments[0];
        const ColumnWithTypeAndName & arg_then = arguments[1];
        const ColumnWithTypeAndName & arg_else = arguments[2];

        bool then_is_null = arg_then.column->onlyNull();
        bool else_is_null = arg_else.column->onlyNull();

        if (!then_is_null && !else_is_null)
            return nullptr;

        if (then_is_null && else_is_null)
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        const ColumnUInt8 * cond_col = typeid_cast<const ColumnUInt8 *>(arg_cond.column.get());
        const ColumnConst * cond_const_col = checkAndGetColumnConst<ColumnVector<UInt8>>(arg_cond.column.get());

        /// If then is NULL, we create Nullable column with null mask OR-ed with condition.
        if (then_is_null)
        {
            ColumnPtr arg_else_column;
            /// In case when arg_else column type differs with result
            /// column type we should cast it to result type.
            if (removeNullable(arg_else.type)->getName() != removeNullable(result_type)->getName())
                arg_else_column = castColumn(arg_else, result_type);
            else
                arg_else_column = arg_else.column;

            if (cond_col)
            {
                arg_else_column = arg_else_column->convertToFullColumnIfConst();
                auto result_column = IColumn::mutate(std::move(arg_else_column));
                if (isColumnNullable(*result_column))
                {
                    assert_cast<ColumnNullable &>(*result_column).applyNullMap(assert_cast<const ColumnUInt8 &>(*arg_cond.column));
                    return result_column;
                }
                if (auto * variant_column = typeid_cast<ColumnVariant *>(result_column.get()))
                {
                    variant_column->applyNullMap(assert_cast<const ColumnUInt8 &>(*arg_cond.column).getData());
                    return result_column;
                }
                if (auto * dynamic_column = typeid_cast<ColumnDynamic *>(result_column.get()))
                {
                    dynamic_column->applyNullMap(assert_cast<const ColumnUInt8 &>(*arg_cond.column).getData());
                    return result_column;
                }
                return ColumnNullable::create(materializeColumnIfConst(result_column), arg_cond.column);
            }
            if (cond_const_col)
            {
                if (cond_const_col->getValue<UInt8>())
                    return result_type->createColumn()->cloneResized(input_rows_count);
                return makeNullableColumnIfNot(arg_else_column);
            }
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}. "
                "Must be ColumnUInt8 or ColumnConstUInt8.",
                arg_cond.column->getName(),
                getName());
        }

        /// If else is NULL, we create Nullable column with null mask OR-ed with negated condition.
        if (else_is_null)
        {
            ColumnPtr arg_then_column;
            /// In case when arg_then column type differs with result
            /// column type we should cast it to result type.
            if (removeNullable(arg_then.type)->getName() != removeNullable(result_type)->getName())
                arg_then_column = castColumn(arg_then, result_type);
            else
                arg_then_column = arg_then.column;

            if (cond_col)
            {
                arg_then_column = arg_then_column->convertToFullColumnIfConst();
                auto result_column = IColumn::mutate(std::move(arg_then_column));

                if (isColumnNullable(*result_column))
                {
                    assert_cast<ColumnNullable &>(*result_column).applyNegatedNullMap(assert_cast<const ColumnUInt8 &>(*arg_cond.column));
                    return result_column;
                }
                if (auto * variant_column = typeid_cast<ColumnVariant *>(result_column.get()))
                {
                    variant_column->applyNegatedNullMap(assert_cast<const ColumnUInt8 &>(*arg_cond.column).getData());
                    return result_column;
                }
                if (auto * dynamic_column = typeid_cast<ColumnDynamic *>(result_column.get()))
                {
                    dynamic_column->applyNegatedNullMap(assert_cast<const ColumnUInt8 &>(*arg_cond.column).getData());
                    return result_column;
                }

                size_t size = input_rows_count;
                const auto & null_map_data = cond_col->getData();

                auto negated_null_map = ColumnUInt8::create();
                auto & negated_null_map_data = negated_null_map->getData();
                negated_null_map_data.resize(size);

                for (size_t i = 0; i < size; ++i)
                    negated_null_map_data[i] = !null_map_data[i];

                return ColumnNullable::create(materializeColumnIfConst(result_column), std::move(negated_null_map));
            }
            if (cond_const_col)
            {
                if (cond_const_col->getValue<UInt8>())
                    return makeNullableColumnIfNot(arg_then_column);
                return result_type->createColumn()->cloneResized(input_rows_count);
            }
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}. "
                "Must be ColumnUInt8 or ColumnConstUInt8.",
                arg_cond.column->getName(),
                getName());
        }

        return nullptr;
    }

    static void executeShortCircuitArguments(ColumnsWithTypeAndName & arguments)
    {
        int last_short_circuit_argument_index = checkShortCircuitArguments(arguments);
        if (last_short_circuit_argument_index == -1)
            return;

        executeColumnIfNeeded(arguments[0]);

        /// Check if condition is const or null to not create full mask from it.
        if ((isColumnConst(*arguments[0].column) || arguments[0].column->onlyNull()) && !arguments[0].column->empty())
        {
            bool value = arguments[0].column->getBool(0);
            executeColumnIfNeeded(arguments[1], !value);
            executeColumnIfNeeded(arguments[2], value);
            return;
        }

        IColumn::Filter mask(arguments[0].column->size(), 1);
        auto mask_info = extractMask(mask, arguments[0].column);
        maskedExecute(arguments[1], mask, mask_info);
        inverseMask(mask, mask_info);
        maskedExecute(arguments[2], mask, mask_info);
    }

public:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 3; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool isShortCircuit(ShortCircuitSettings & settings, size_t /*number_of_arguments*/) const override
    {
        settings.arguments_with_disabled_lazy_execution.insert(0);
        settings.enable_lazy_execution_for_common_descendants_of_arguments = false;
        settings.force_enable_lazy_execution = false;
        return true;
    }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }
    bool canBeExecutedOnLowCardinalityDictionary() const override { return false; }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->onlyNull())
        {
            if (arguments[0]->isNullable())
                return getReturnTypeImpl({
                    removeNullable(arguments[0]), arguments[1], arguments[2]});

            if (!WhichDataType(arguments[0]).isUInt8())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of first argument (condition) of function if. "
                    "Must be UInt8.", arguments[0]->getName());
        }

        if (use_variant_when_no_common_type)
            return getLeastSupertypeOrVariant(DataTypes{arguments[1], arguments[2]});

        return getLeastSupertype(DataTypes{arguments[1], arguments[2]});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnsWithTypeAndName arguments = args;
        executeShortCircuitArguments(arguments);
        ColumnPtr res;
        if (   (res = executeForConstAndNullableCondition(arguments, result_type, input_rows_count))
            || (res = executeForNullThenElse(arguments, result_type, input_rows_count))
            || (res = executeForNullableThenElse(arguments, result_type, input_rows_count)))
            return res;

        const ColumnWithTypeAndName & arg_cond = arguments[0];
        const ColumnWithTypeAndName & arg_then = arguments[1];
        const ColumnWithTypeAndName & arg_else = arguments[2];

        /// A case for identical then and else (pointers are the same).
        if (arg_then.column.get() == arg_else.column.get())
        {
            /// Just point result to them.
            return arg_then.column;
        }

        const ColumnUInt8 * cond_col = typeid_cast<const ColumnUInt8 *>(arg_cond.column.get());
        const ColumnConst * cond_const_col = checkAndGetColumnConst<ColumnVector<UInt8>>(arg_cond.column.get());
        ColumnPtr materialized_cond_col;

        if (cond_const_col)
        {
            UInt8 value = cond_const_col->getValue<UInt8>();
            const ColumnWithTypeAndName & arg = value ? arg_then : arg_else;
            if (arg.type->equals(*result_type))
                return arg.column;
            return castColumn(arg, result_type);
        }

        if (!cond_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}. "
                "Must be ColumnUInt8 or ColumnConstUInt8.", arg_cond.column->getName(), getName());

        /// If result is Variant, always use generic implementation.
        /// Using typed implementations may lead to incorrect result column type when
        /// resulting Variant is created by use_variant_when_no_common_type.
        if (isVariant(result_type))
            return executeGeneric(cond_col, arguments, input_rows_count, use_variant_when_no_common_type);

        auto call = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using T0 = typename Types::LeftType;
            using T1 = typename Types::RightType;

            res = executeTyped<T0, T1>(cond_col, arguments, result_type, input_rows_count);
            return res != nullptr;
        };

        DataTypePtr left_type = arg_then.type;
        DataTypePtr right_type = arg_else.type;

        if (const auto * left_array = checkAndGetDataType<DataTypeArray>(arg_then.type.get()))
            left_type = left_array->getNestedType();

        if (const auto * right_array = checkAndGetDataType<DataTypeArray>(arg_else.type.get()))
            right_type = right_array->getNestedType();

        /// Special case when one column is Integer and another is UInt64 that can be actually Int64.
        /// The result type for this case is Int64 and we need to change UInt64 type to Int64
        /// so the NumberTraits::ResultOfIf will return Int64 instead if Int128.
        if (isNativeInteger(left_type) && isUInt64ThatCanBeInt64(right_type))
            right_type = std::make_shared<DataTypeInt64>();
        else if (isNativeInteger(right_type) && isUInt64ThatCanBeInt64(left_type))
            left_type = std::make_shared<DataTypeInt64>();

        TypeIndex left_id = left_type->getTypeId();
        TypeIndex right_id = right_type->getTypeId();

        /// TODO optimize for map type
        /// TODO optimize for nullable type
        if (!(callOnBasicTypes<true, true, true, false>(left_id, right_id, call)
            || (res = executeTyped<UUID, UUID>(cond_col, arguments, result_type, input_rows_count))
            || (res = executeString(cond_col, arguments, result_type))
            || (res = executeGenericArray(cond_col, arguments, result_type))
            || (res = executeTuple(arguments, result_type, input_rows_count))
            || (res = executeMap(arguments, result_type, input_rows_count))))
        {
            return executeGeneric(cond_col, arguments, input_rows_count, use_variant_when_no_common_type);
        }

        return res;
    }

    ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        const ColumnWithTypeAndName & arg_cond = arguments[0];
        if (!arg_cond.column || !isColumnConst(*arg_cond.column))
            return {};

        const ColumnConst * cond_const_col = checkAndGetColumnConst<ColumnVector<UInt8>>(arg_cond.column.get());
        if (!cond_const_col)
            return {};

        bool condition_value = cond_const_col->getValue<UInt8>();

        const ColumnWithTypeAndName & arg_then = arguments[1];
        const ColumnWithTypeAndName & arg_else = arguments[2];
        const ColumnWithTypeAndName & potential_const_column = condition_value ? arg_then : arg_else;

        if (!potential_const_column.column || !isColumnConst(*potential_const_column.column))
            return {};

        auto result = castColumn(potential_const_column, result_type);
        if (!isColumnConst(*result))
            return {};

        return result;
    }
};

}

REGISTER_FUNCTION(If)
{
    FunctionDocumentation::Description description = R"(
Performs conditional branching.

- If the condition `cond` evaluates to a non-zero value, the function returns the result of the expression `then`.
- If `cond` evaluates to zero or NULL, the result of the `else` expression is returned.

The setting [`short_circuit_function_evaluation`](/operations/settings/settings#short_circuit_function_evaluation) controls whether short-circuit evaluation is used.

If enabled, the `then` expression is evaluated only on rows where `cond` is true and the `else` expression where `cond` is false.

For example, with short-circuit evaluation, no division-by-zero exception is thrown when executing the following query:

```sql
SELECT if(number = 0, 0, intDiv(42, number)) FROM numbers(10)
```

`then` and `else` must be of a similar type.
)";
    FunctionDocumentation::Syntax syntax = "if(cond, then, else)";
    FunctionDocumentation::Arguments arguments = {
        {"cond", "The evaluated condition. [`UInt8`](/sql-reference/data-types/int-uint), `Nullable(UInt8)` or `NULL`."},
        {"then", "The expression returned if `cond` is true."},
        {"else", "The expression returned if `cond` is false or `NULL`."}
    };
    FunctionDocumentation::ReturnedValue returned_value = "The result of either the `then` or `else` expressions, depending on condition `cond`.";
    FunctionDocumentation::Examples examples = {
        {"Example usage", R"(
SELECT if(1, 2 + 2, 2 + 6) AS res;
)",
    R"(
┌─res─┐
│   4 │
└─────┘
)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Conditional;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIf>(documentation, FunctionFactory::Case::Insensitive);
}

FunctionOverloadResolverPtr createInternalFunctionIfOverloadResolver(bool allow_experimental_variant_type, bool use_variant_as_common_type)
{
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionIf>(allow_experimental_variant_type && use_variant_as_common_type));
}

}
