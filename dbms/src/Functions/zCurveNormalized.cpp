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

    struct ZCurveNormalizedOpImpl {
        using ResultType = UInt64;
        static void encode(ResultType& num, const DataTypePtr & type)
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

        template <typename T>
        static std::string binary(T x) {
            std::string res;
            for (int i = (sizeof(T) << 3) - 1; i >= 0; --i) {
                if (x & (1ull << i)) {
                    res += "1";
                } else {
                    res += "0";
                }
            }
            return res;
        }

        static std::vector<std::pair<ResultType, ResultType>> decodeRange(
                ResultType left,
                ResultType right,
                const DataTypePtr & type,
                size_t significant_digits) {
            std::tie(left, right) = ZCurveOpImpl::decodeRange(left, right, type, significant_digits)[0];
            /*std::cerr << "L: " << binary(left) << "\n";
            std::cerr << "R: " << binary(right) << "\n";*/
            size_t bitsz = sizeof(ResultType) << 3;
            std::vector<std::pair<ResultType, ResultType>> ranges;
            ranges.emplace_back(left, right);
            size_t lcp = __builtin_clzll(left ^ right);
            const ResultType MX = std::numeric_limits<ResultType>::max();
            ResultType clear_suf = (MX << 1);
            size_t previous_size = 0;
            for (size_t nbits = 1; nbits < (type->getSizeOfValueInMemory() << 3); ++nbits, clear_suf <<= 1)
            {
                ResultType suffix = nbits;
                if (lcp + nbits >= bitsz)
                {
                    ResultType suf_val = (left & clear_suf) | suffix;
                    if (left <= suf_val && suf_val <= right)
                    {
                        ranges.emplace_back(suf_val, suf_val);
                    }
                }
                else if (lcp + nbits + 1 == bitsz)
                {
                    ResultType suf_min = (left & clear_suf) | suffix;
                    ResultType suf_max = (right & clear_suf) | suffix;
                    if (left <= suf_min && suf_min <= right)
                    {
                        ranges.emplace_back(suf_min, suf_min);
                    }
                    if (left <= suf_max && suf_max <= right)
                    {
                        ranges.emplace_back(suf_max, suf_max);
                    }
                }
                else
                {
                    ResultType middle = (((clear_suf) << (lcp + 1)) >> (lcp + 1));
                    ResultType left_middle = left & middle;
                    ResultType right_middle = right & middle;
                    ResultType left_suf = left & (~clear_suf);
                    ResultType right_suf = right & (~clear_suf);
                    std::pair<ResultType, ResultType> range;
                    ResultType inc = (static_cast<ResultType>(1) << nbits);
                    if (left_suf <= suffix)
                    {
                        range.first = (left & clear_suf) | suffix;
                    }
                    else
                    {
                        auto new_middle = left_middle + inc;
                        range.first = (((left & (~middle)) | new_middle) & clear_suf) | suffix;
                        if (range.first > right) {
                            continue;
                        }
                    }
                    if (suffix <= right_suf)
                    {
                        range.second = (right & clear_suf) | suffix;
                    }
                    else
                    {
                        auto new_middle = right_middle - inc;
                        range.second = (((right & (~middle)) | new_middle) & clear_suf) | suffix;
                        if (range.second < left)
                        {
                            continue;
                        }
                    }
                    ranges.push_back(range);
                }
                while (previous_size < ranges.size()) {
                    ranges[previous_size].first >>= nbits;
                    ranges[previous_size].second >>= nbits;
                    ++previous_size;
                }
            }
            std::reverse(ranges.begin(), ranges.end());
            return ranges;
        }
    };

    using FunctionZCurveNormalized = FunctionZCurveBase<ZCurveNormalizedOpImpl, NameZCurveNormalized>;

    void registerFunctionZCurveNormalized(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionZCurveNormalized>();
    }
}
