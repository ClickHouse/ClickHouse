#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>
#include <iostream>
#include <Core/iostream_debug_helpers.h>
#include <Functions/zCurveBase.h>
#include <Functions/zCurve.h>
#include <Common/RadixSort.h>


namespace DB
{

    struct NameZCurveNormalized { static constexpr auto name = "zCurveN"; };

    struct ZCurveNormalizedOpImpl
    {
        using ResultType = UInt64;
        static void encode(ResultType & num, const DataTypePtr & type)
        {
            ZCurveOpImpl::encode(num, type);
            if (num == 0)
            {
                return;
            }
            size_t leading_zeroes = __builtin_clzll(num);
            num <<= leading_zeroes;
            size_t bit_diff = (sizeof(ResultType) << 3) - (type->getSizeOfValueInMemory() << 3);
            leading_zeroes -= bit_diff;
            num += (leading_zeroes << bit_diff);
        }

        using Segment = std::pair<ResultType, ResultType>;
        static inline void append(std::vector<Segment> & v, ResultType L, ResultType R, size_t shift)
        {
            L >>= shift;
            R >>= shift;
            if (!v.empty() && R + 1 >= v.back().first)
            {
                v.back().first = L;
            }
            else
            {
                v.emplace_back(L, R);
            }
        }

        static std::vector<Segment> decodeRange(
                ResultType left,
                ResultType right,
                const DataTypePtr & type,
                size_t significant_digits)
        {
            size_t bits = sizeof(ResultType) << 3;
            size_t type_bits = (type->getSizeOfValueInMemory() << 3);
            size_t diff_bits = bits - type_bits;

            left >>= diff_bits;
            right >>= diff_bits;

            std::vector<Segment> ranges;
            ranges.emplace_back(left, right);

            size_t lcp = __builtin_clzll(left ^ right);
            ResultType clear_suf = (std::numeric_limits<ResultType>::max() << 1);

            for (ResultType nbits = 1; nbits < type_bits; ++nbits, clear_suf <<= 1)
            {
                if (lcp + nbits >= bits)
                {
                    ResultType suf_val = (left & clear_suf) | nbits;
                    if (left <= suf_val && suf_val <= right)
                    {
                        append(ranges, suf_val, suf_val, nbits);
                    }
                }
                else if (lcp + nbits + 1 == bits)
                {
                    ResultType suf_min = (left & clear_suf) | nbits;
                    ResultType suf_max = (right & clear_suf) | nbits;
                    if (left <= suf_max && suf_max <= right)
                    {
                        append(ranges, suf_max, suf_max, nbits);
                    }
                    if (left <= suf_min && suf_min <= right)
                    {
                        append(ranges, suf_min, suf_min, nbits);
                    }
                }
                else
                {
                    ResultType middle = (((clear_suf) << (lcp + 1)) >> (lcp + 1));
                    ResultType left_middle = left & middle, right_middle = right & middle;
                    ResultType left_suf = left & (~clear_suf), right_suf = right & (~clear_suf);
                    ResultType range_left, range_right;
                    ResultType inc = (static_cast<ResultType>(1) << nbits);
                    if (left_suf <= nbits)
                    {
                        range_left = (left & clear_suf) | nbits;
                    }
                    else
                    {
                        auto new_middle = left_middle + inc;
                        range_left = (((left & (~middle)) | new_middle) & clear_suf) | nbits;
                        if (range_left > right)
                        {
                            continue;
                        }
                    }
                    if (nbits <= right_suf)
                    {
                        range_right = (right & clear_suf) | nbits;
                    }
                    else
                    {
                        auto new_middle = right_middle - inc;
                        range_right = (((right & (~middle)) | new_middle) & clear_suf) | nbits;
                        if (range_right < left)
                        {
                            continue;
                        }
                    }
                    append(ranges, range_left, range_right, nbits);
                }
            }
            std::reverse(ranges.begin(), ranges.end());
            for (auto & range : ranges)
            {
                range.first <<= diff_bits;
                range.second <<= diff_bits;
                range.first = ZCurveOpImpl::decode(range.first, type, true, significant_digits);
                range.second = ZCurveOpImpl::decode(range.second, type, false, significant_digits);
            }
            return ranges;
        }
    };

    using FunctionZCurveNormalized = FunctionZCurveBase<ZCurveNormalizedOpImpl, NameZCurveNormalized>;

    void registerFunctionZCurveNormalized(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionZCurveNormalized>();
    }
}
