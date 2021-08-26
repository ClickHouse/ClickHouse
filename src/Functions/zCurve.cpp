#include <Functions/zCurve.h>


namespace DB
{

    struct NameZCurve { static constexpr auto name = "zCurve"; };

    ZCurveOpImpl::ResultType ZCurveOpImpl::decode(
            ZCurveOpImpl::ResultType num,
            const DB::DataTypePtr & type,
            bool is_left,
            size_t significant_digits)
    {
        size_t bit_size = type->getSizeOfValueInMemory() << 3;
        num >>= ((sizeof(ResultType) << 3) - bit_size);

        /* If some bits were lost and we are handling the right end of the range,
         * then sadly set those bits to one. :(
         */
        if (significant_digits < bit_size && !is_left)
        {
            num |= (static_cast<ResultType>(1) << (bit_size - significant_digits)) - 1;
        }

        auto type_id = type->getTypeId();
        if (type->isValueRepresentedByUnsignedInteger())
        {
            return num;
        }
        else if (type_id == TypeIndex::Int8)
        {
            num ^= static_cast<UInt8>(std::numeric_limits<Int8>::min());
        }
        else if (type_id == TypeIndex::Int16)
        {
            num ^= static_cast<UInt16>(std::numeric_limits<Int16>::min());
        }
        else if (type_id == TypeIndex::Int32)
        {
            num ^= static_cast<UInt32>(std::numeric_limits<Int32>::min());
        }
        else if (type_id == TypeIndex::Int64)
        {
            num ^= static_cast<UInt64>(std::numeric_limits<Int64>::min());
        }
        else if (type_id == TypeIndex::Float32)
        {
            /* Deals with the fact that when
             * the number is too big or too small it is transformed to "nan" and not +inf or -inf.
             */
            constexpr int exp = 9, sz = (sizeof(UInt32) << 3);
            constexpr UInt32 inf = ((static_cast<UInt32>(1) << exp) - 1) << (sz - exp);
            constexpr UInt32 neg_inf = (static_cast<UInt32>(1) << (sz - exp)) - 1;
            if (num > inf)
            {
                num = inf;
            }
            else if (num < neg_inf)
            {
                num = neg_inf;
            }
            num = RadixSortFloatTransform<UInt32>::backward(static_cast<UInt32>(num));
        }
        else if (type_id == TypeIndex::Float64)
        {
            /* Deals with the fact that when
             * the number is too big or too small it is transformed to "nan" and not +inf or -inf.
             */
            constexpr int exp = 11, sz = (sizeof(UInt64) << 3);
            constexpr UInt64 inf = ((static_cast<UInt64>(1) << exp) - 1) << (sz - exp);
            constexpr UInt64 neg_inf = (static_cast<UInt64>(1) << (sz - exp)) - 1;
            if (num > inf)
            {
                num = inf;
            }
            else if (num < neg_inf)
            {
                num = neg_inf;
            }
            num = RadixSortFloatTransform<UInt64>::backward(num);
        }
        return num;
    }

    void ZCurveOpImpl::encode(ZCurveOpImpl::ResultType & num, const DataTypePtr & type)
    {
        auto type_id = type->getTypeId();
        // Flip the sign bit of signed integers
        if (type_id == TypeIndex::Int8)
        {
            num ^= static_cast<UInt8>(std::numeric_limits<Int8>::min());
        }
        if (type_id == TypeIndex::Int16)
        {
            num ^= static_cast<UInt16>(std::numeric_limits<Int16>::min());
        }
        if (type_id == TypeIndex::Int32)
        {
            num ^= static_cast<UInt32>(std::numeric_limits<Int32>::min());
        }
        if (type_id == TypeIndex::Int64)
        {
            num ^= static_cast<UInt64>(std::numeric_limits<Int64>::min());
        }
        // Uses a transformation from Common/RadixSort.h,
        // it inverts the sign bit for positive floats and inverts the whole number for negative floats.
        if (type_id == TypeIndex::Float32)
        {
            num = RadixSortFloatTransform<UInt32>::forward(static_cast<UInt32>(num));
        }
        if (type_id == TypeIndex::Float64)
        {
            num = RadixSortFloatTransform<UInt64>::forward(num);
        }
        // Shift the significant bits upwards
        num <<= ((sizeof(ResultType) - type->getSizeOfValueInMemory()) << 3);
    }

    std::vector<std::pair<ZCurveOpImpl::ResultType, ZCurveOpImpl::ResultType>> ZCurveOpImpl::decodeRange(
            ResultType left,
            ResultType right,
            const DataTypePtr & type,
            size_t significant_digits)
    {
        return {{decode(left, type, true, significant_digits), decode(right, type, false, significant_digits)}};
    }


    using FunctionZCurve = FunctionZCurveBase<ZCurveOpImpl, NameZCurve>;
    void registerFunctionZCurve(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionZCurve>(FunctionFactory::CaseInsensitive);
    }
}
