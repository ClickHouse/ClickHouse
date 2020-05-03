//#include <algorithm>
#include <DataTypes/DataTypesNumber.h>
#include <Common/HashTable/ClearableHashMap.h>
#include <Common/ColumnsHashing.h>
#include <Functions/array/arrayStatTest.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

enum class Alternative {
    TwoSided,
    Greater,
    Less
};

template <typename Name, Alternative alternative>
class ArrayMannWhitneyUTestImpl
{
private:
    /// Initially allocate a piece of memory for 512 elements. NOTE: This is just a guess.
    static constexpr size_t INITIAL_SIZE_DEGREE = 9;

public:
    static constexpr auto name = Name::name;
    using ResultType = Float64;

    static String getName()
    {
        return name;
    }

    static DataTypePtr getReturnType(const DataTypePtr &v1, const DataTypePtr &v2)
    {
        if (!(isNumber(v1) && isNumber(v2)))
            throw Exception(std::string(getName()) + " label must have numeric type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<ResultType>>();
    }


    template <class ForwardIt, class T>
    static void iota(
        ForwardIt first,
        ForwardIt last,
        T value)
    {
        while(first != last)
        {
            *first++ = value;
            ++value;
        }
    }


    template <typename T>
    static PODArrayWithStackMemory<size_t, 1ULL << INITIAL_SIZE_DEGREE> sortIndexes(
        const PODArrayWithStackMemory<T, 1ULL << INITIAL_SIZE_DEGREE> &v)
    {
        PODArrayWithStackMemory<size_t, 1ULL << INITIAL_SIZE_DEGREE> idx(v.size());
        iota(idx.begin(), idx.end(), 0);

        std::sort(idx.begin(), idx.end(),
                  [&v](size_t i1, size_t i2) { return v[i1] < v[i2]; });

        return idx;
    }


    template <typename T>
    static PODArrayWithStackMemory<Float64, 1ULL << INITIAL_SIZE_DEGREE> rankData(
        const PODArrayWithStackMemory<T, 1ULL << INITIAL_SIZE_DEGREE> &v)
    {
        PODArrayWithStackMemory<size_t, 1ULL << INITIAL_SIZE_DEGREE> idx = sortIndexes(v);
        PODArrayWithStackMemory<Float64, 1ULL << INITIAL_SIZE_DEGREE> ranks(v.size());

        for (size_t i = 0; i < v.size(); ++i)
            ranks[idx[i]] = i + 1;

        struct RankPair
        {
            size_t first;
            Int64 second;
        };

        ClearableHashMap<T, RankPair, DefaultHash<T>, HashTableGrower<INITIAL_SIZE_DEGREE>,
            HashTableAllocatorWithStackMemory<(1ULL << INITIAL_SIZE_DEGREE) * sizeof(T)>> ht;

        for (auto i: idx)
        {
            if (ht.has(v[i]))
            {
                ht[v[i]].first += ranks[i];
                ht[v[i]].second += 1;
            }
            else
            {
                ht[v[i]].first = ranks[i];
                ht[v[i]].second = 1;
            }
        }

        size_t j = 0;
        for (auto i: v)
        {
            ranks[j] = ht[i].first / Float64(ht[i].second);
            ++j;
        }

        return ranks;
    }


    template <typename T, typename U>
    static ResultType apply(
        const T * v1,
        const U * v2,
        size_t n1,
        size_t n2)
    {
        PODArrayWithStackMemory<T, 1ULL << INITIAL_SIZE_DEGREE> concatenated;
        concatenated.insert(concatenated.end(), v1, v1 + n1);
        concatenated.insert(concatenated.end(), v2, v2 + n2);

        PODArrayWithStackMemory<Float64, 1ULL << INITIAL_SIZE_DEGREE> ranked = rankData(concatenated);

        Float64 s1 = 0;
        for (size_t i = 0; i < n1; ++i)
            s1 += ranked[i];

        Float64 u1 = n1 * n2 + (n1 * (n1 + 1)) / 2. - s1;
        Float64 u2 = n1 * n2 - u1;
        Float64 u_big;

        switch (alternative)
        {
            case Alternative::TwoSided:
            {
                u_big = std::max(u1, u2);
                break;
            }
            case Alternative::Greater:
            {
                u_big = u1;
                break;
            }
            case Alternative::Less:
            {
                u_big = u2;
                break;
            }
        }

        Float64 mu = (n1 * n2) / 2.;
        Float64 std = std::sqrt((n1 * n2) * (n1 + n2 + 1.) / 6.);
        Float64 z = (u_big - mu) / std;

        Float64 p_value;
        if (alternative == Alternative::TwoSided)
            p_value = 2. - erfc(-abs(z));
        else
            p_value = 1 - erfc(z) / 2.;

//        LOG_INFO(
//            &Log  ger::get("executeQuery"),
//            "temp_str: " << temp_str << " s1: " << s1 << " n1: " << n1 << " n2: " << n2 << " u1: " << u1 << " u2: " << u2 << " u_big: " << u_big << " mu: " << mu << " std: " << std << " z: " << z << " p_value: " << p_value;
//        );

        return ResultType(p_value);
    }
};


struct NameArrayMannWhitneyUTestTwoSided { static constexpr auto name = "arrayMannWhitneyUTestTwoSided"; };
struct NameArrayMannWhitneyUTestLess { static constexpr auto name = "arrayMannWhitneyUTestLess"; };
struct NameArrayMannWhitneyUTestGreater { static constexpr auto name = "arrayMannWhitneyUTestGreater"; };

using ArrayMannWhitneyUTestImplTwoSided = ArrayMannWhitneyUTestImpl<NameArrayMannWhitneyUTestTwoSided, Alternative::TwoSided>;
using ArrayMannWhitneyUTestImplLess = ArrayMannWhitneyUTestImpl<NameArrayMannWhitneyUTestLess, Alternative::Less>;
using ArrayMannWhitneyUTestImplGreater = ArrayMannWhitneyUTestImpl<NameArrayMannWhitneyUTestGreater, Alternative::Greater>;

using FunctionArrayMannWhitneyUTestTwoSided = FunctionArrayStatTest<ArrayMannWhitneyUTestImplTwoSided>;
using FunctionArrayMannWhitneyUTestLess = FunctionArrayStatTest<ArrayMannWhitneyUTestImplLess>;
using FunctionArrayMannWhitneyUTestGreater = FunctionArrayStatTest<ArrayMannWhitneyUTestImplGreater>;


}
