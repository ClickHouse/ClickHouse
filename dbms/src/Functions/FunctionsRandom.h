#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/HashTable/Hash.h>
#include <Common/randomSeed.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Pseudo-random number generation functions.
  * The function can be called without arguments or with one argument.
  * The argument is ignored and only serves to ensure that several calls to one function are considered different and do not stick together.
  *
  * Example:
  * SELECT rand(), rand() - will output two identical columns.
  * SELECT rand(1), rand(2) - will output two different columns.
  *
  * Non-cryptographic generators:
  *
  * rand   - linear congruental generator 0 .. 2^32 - 1.
  * rand64 - combines several rand values to get values from the range 0 .. 2^64 - 1.
  *
  * randConstant - service function, produces a constant column with a random value.
  *
  * The time is used as the seed.
  * Note: it is reinitialized for each block.
  * This means that the timer must be of sufficient resolution to give different values to each block.
  */

namespace detail
{
    /// NOTE Probably
    ///    http://www.pcg-random.org/
    /// or http://www.math.sci.hiroshima-u.ac.jp/~m-mat/MT/SFMT/
    /// or http://docs.yeppp.info/c/group__yep_random___w_e_l_l1024a.html
    /// could go better.

    struct LinearCongruentialGenerator
    {
        /// Constants from `man lrand48_r`.
        static constexpr UInt64 a = 0x5DEECE66D;
        static constexpr UInt64 c = 0xB;

        /// And this is from `head -c8 /dev/urandom | xxd -p`
        UInt64 current = 0x09826f4a081cee35ULL;

        LinearCongruentialGenerator() {}
        LinearCongruentialGenerator(UInt64 value) : current(value) {}

        void seed(UInt64 value)
        {
            current = value;
        }

        UInt32 next()
        {
            current = current * a + c;
            return current >> 16;
        }
    };

    void seed(LinearCongruentialGenerator & generator, intptr_t additional_seed);
}

struct RandImpl
{
    using ReturnType = UInt32;

    static void execute(ReturnType * output, size_t size)
    {
        detail::LinearCongruentialGenerator generator0;
        detail::LinearCongruentialGenerator generator1;
        detail::LinearCongruentialGenerator generator2;
        detail::LinearCongruentialGenerator generator3;

        detail::seed(generator0, 0xfb4121280b2ab902ULL + reinterpret_cast<intptr_t>(output));
        detail::seed(generator1, 0x0121cf76df39c673ULL + reinterpret_cast<intptr_t>(output));
        detail::seed(generator2, 0x17ae86e3a19a602fULL + reinterpret_cast<intptr_t>(output));
        detail::seed(generator3, 0x8b6e16da7e06d622ULL + reinterpret_cast<intptr_t>(output));

        ReturnType * pos = output;
        ReturnType * end = pos + size;
        ReturnType * end4 = pos + size / 4 * 4;

        while (pos < end4)
        {
            pos[0] = generator0.next();
            pos[1] = generator1.next();
            pos[2] = generator2.next();
            pos[3] = generator3.next();
            pos += 4;
        }

        while (pos < end)
        {
            pos[0] = generator0.next();
            ++pos;
        }
    }
};

struct Rand64Impl
{
    using ReturnType = UInt64;

    static void execute(ReturnType * output, size_t size)
    {
        RandImpl::execute(reinterpret_cast<RandImpl::ReturnType *>(output), size * 2);
    }
};


template <typename Impl, typename Name>
class FunctionRandom : public IFunction
{
private:
    using ToType = typename Impl::ReturnType;

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionRandom>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isDeterministicInScopeOfQuery() override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 0 or 1.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<DataTypeNumber<typename Impl::ReturnType>>();
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        auto col_to = ColumnVector<ToType>::create();
        typename ColumnVector<ToType>::Container & vec_to = col_to->getData();

        size_t size = input_rows_count;
        vec_to.resize(size);
        Impl::execute(&vec_to[0], vec_to.size());

        block.getByPosition(result).column = std::move(col_to);
    }
};


template <typename Impl, typename Name>
class FunctionRandomConstant : public IFunction
{
private:
    using ToType = typename Impl::ReturnType;

    /// The value is one for different blocks.
    bool is_initialized = false;
    ToType value;

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionRandomConstant>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 0 or 1.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<DataTypeNumber<typename Impl::ReturnType>>();
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        if (!is_initialized)
        {
            is_initialized = true;
            typename ColumnVector<ToType>::Container vec_to(1);
            Impl::execute(&vec_to[0], vec_to.size());
            value = vec_to[0];
        }

        block.getByPosition(result).column = DataTypeNumber<ToType>().createColumnConst(input_rows_count, toField(value));
    }
};


struct NameRand         { static constexpr auto name = "rand"; };
struct NameRand64       { static constexpr auto name = "rand64"; };
struct NameRandConstant { static constexpr auto name = "randConstant"; };

using FunctionRand = FunctionRandom<RandImpl, NameRand> ;
using FunctionRand64 = FunctionRandom<Rand64Impl, NameRand64>;
using FunctionRandConstant = FunctionRandomConstant<RandImpl, NameRandConstant>;


}
