#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>
#include <iostream>
#include <Core/iostream_debug_helpers.h>


namespace DB
{
    template <typename Op, typename Name>
    class FunctionZCurveBase : public IFunction
    {
    public:
        static constexpr auto name = Name::name;
        static FunctionPtr create(const Context & context)
        {
            return std::make_shared<FunctionZCurveBase>(context);
        }
        FunctionZCurveBase(const Context & /*context*/) {}


        bool isVariadic() const override { return true; }
        size_t getNumberOfArguments() const override { return 0; }

        using ResultType = typename Op::ResultType;
        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            for (const auto & type : arguments)
            {
                if (!type->isValueRepresentedByInteger() &&
                    type->getTypeId() != TypeIndex::Float32 &&
                    type->getTypeId() != TypeIndex::Float64)
                {
                    throw Exception("Function " + getName() + " cannot take arguments of type " + type->getName(), ErrorCodes::LOGICAL_ERROR);
                }
            }
            return std::make_shared<DataTypeNumber<ResultType>>();
        }

        void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
        {
            size_t number_of_elements = arguments.size();

            using return_type = ResultType;
            auto out = ColumnVector<return_type>::create();
            auto & out_data = out->getData();
            out_data.assign(input_rows_count, static_cast<ResultType>(0));
            for (size_t i = 0; i < number_of_elements; ++i)
            {
                const auto & arg = block.getByPosition(arguments[i]);
                auto column_data = arg.column.get()->getRawData();
                auto size_per_element = arg.column.get()->sizeOfValueIfFixed();
                for (size_t j = 0; j < input_rows_count; ++j)
                {
                    out_data[j] >>= 1;
                    out_data[j] |= zShiftElement(
                            column_data.data + j * size_per_element,
                            size_per_element,
                            number_of_elements,
                            arg.type);
                }

            }
            block.getByPosition(result).column = std::move(out);
        }

        bool isInvertible() const override
        {
            return true;
        }

        bool invertRange(const Range& value_range, size_t arg_index, const DataTypes & arg_types, RangeSet & result) const override
        {
            Range copy = value_range;
            copy.shrinkToIncludedIfPossible(); // always possible if not unbounded, since the result type is UInt64
            ResultType left, right;
            if (!copy.left_bounded)
            {
                left = std::numeric_limits<ResultType>::min();
            }
            else
            {
                left = copy.left.get<ResultType>();
            }
            if (!copy.right_bounded)
            {
                right = std::numeric_limits<ResultType>::max();
            }
            else
            {
                right = copy.right.get<ResultType>();
            }
            auto minmax = getMinMaxPossibleBitValueOfArgument(left, right, arg_index, arg_types.size());
            auto type = arg_types[arg_index];
            size_t significant_digits = (sizeof(ResultType) << 3) / arg_types.size();
            if (arg_types.size() - arg_index <= sizeof(ResultType) % arg_types.size())
            {
                ++significant_digits;
            }
            auto plain_ranges = Op::decodeRange(minmax.first, minmax.second, type, significant_digits);
            std::vector<Range> ranges;
            //auto type_size = type->getSizeOfValueInMemory();
            for (auto & [left_point, right_point] : plain_ranges)
            {
                Field left_field, right_field;
                writeField(type, left_point, left_field);
                writeField(type, right_point, right_field);
                auto range = Range(left_field, true, Field(right_point), true);
                if (!range.empty())
                {
                    ranges.push_back(range);
                }
            }
            /* The segments are already sorted and not intersecting in the current implementations of decode Range.
             * If this is changed, you should put result = RangeSet(ranges) here
             */
            result.data = ranges;
            return true;
        }


    private:
        /// Write binary data of certain type to field.
        void writeField(const DataTypePtr & type, ResultType src, Field & field) const
        {
            auto type_id = type->getTypeId();
            if (type->isValueRepresentedByUnsignedInteger())
            {
                field = Field(src);
            }
            else if (type_id == TypeIndex::Float32)
            {
                Float32 val;
                memcpy(&val, &src, sizeof(val));
                field = Field(val);
            }
            else if (type_id == TypeIndex::Float64)
            {
                Float64 val;
                memcpy(&val, &src, sizeof(val));
                field = Field(val);
            }
            else if (type_id == TypeIndex::Int8)
            {
                Int8 val;
                memcpy(&val, &src, sizeof(val));
                field = Field(val);
            }
            else if (type_id == TypeIndex::Int16)
            {
                Int16 val;
                memcpy(&val, &src, sizeof(val));
                field = Field(val);
            }
            else if (type_id == TypeIndex::Int32)
            {
                Int32 val;
                memcpy(&val, &src, sizeof(val));
                field = Field(val);
            }
            else if (type_id == TypeIndex::Int64)
            {
                Int64 val;
                memcpy(&val, &src, sizeof(val));
                field = Field(val);
            }
        }
        /// Extract the i-th argument from given z value,
        /// which is just taking every arity-th bit starting from some position
        ResultType extractArgument(ResultType z_value, size_t argument_index, size_t arity) const
        {
            static constexpr size_t number_of_bits = (sizeof(ResultType) << 3);
            ResultType result = 0;
            ResultType res_bit = 1ull << (number_of_bits - 1);
            for (ResultType bit = 1ull << (number_of_bits - (arity - argument_index)); bit; bit >>= arity)
            {
                if (z_value & bit)
                {
                    result |= res_bit;
                }
                res_bit >>= 1;
            }
            return result;
        }
        /// Get maximal and minimal possible bit representation of a given argument
        /// when the values fall in a given range of z values.
        /* For the i-th argument of a function with arity k in the range [L, R] we do roughly the following:
         * Consider L and R as blocks of k bits, such that the bit corresponding to out argument
         * is the last bit of every block. The first block (highest-order bits) can be smaller than k.
         * We try to find the maximum value for the last bit of each block, going from the highest bits.
         * If the current blocks are L_b and R_b we are basically choosing between L_b and L_b + 1, since
         * it's useless to pick a higher value. Out of those two we want the one, that is odd
         * (and thus ends with a set bit, which would increase the value of our argument),
         * while bearing in mind that the value shouldn't be greater than R_b
         * (this means only that we can't choose L_b + 1 when L_b = R_b)
         * If the value L_x we choose is strictly (!) less than R_b, then we should set the remainder of R to ones,
         * since we can put any bits in the next blocks, without R being a constraint.
         * We proceed similarly when calculating the minimum possible value.
         */
        std::pair<ResultType, ResultType> getMinMaxPossibleBitValueOfArgument(
                ResultType left,
                ResultType right,
                size_t argument_index,
                size_t arity) const
        {
            auto left_min = left, left_max = left;
            auto right_min = right, right_max = right;
            static constexpr size_t number_of_bits = sizeof(ResultType) << 3;

            ResultType max_first_block_value = (1ull << (arity - argument_index)) - 1;
            ResultType get_first_block = max_first_block_value << (number_of_bits - arity + argument_index);
            ResultType first_plus_one = 1ull << (number_of_bits - arity + argument_index);


            ResultType left_block_min, left_block_max, right_block_min, right_block_max;

            // Handling the first block for MAX
            left_block_max = left_max & get_first_block;
            right_block_max = right_max & get_first_block;
            bool is_left_best = static_cast<bool>(left_block_max & first_plus_one);
            if (left_block_max < right_block_max)
            {
                if (!is_left_best)
                {
                    left_max += first_plus_one;
                    left_max &= ~(first_plus_one - 1);
                }
                ResultType tmp;
                if (is_left_best || (!__builtin_add_overflow(left_block_max, first_plus_one, &tmp) && tmp < right_block_max))
                {
                    right_max |= first_plus_one - 1;
                }
            }

            // Handling the first block for MIN
            left_block_min = left_min & get_first_block;
            right_block_min = right_min & get_first_block;
            bool is_right_worse = static_cast<bool>(right_block_min & first_plus_one);
            if (left_block_min < right_block_min)
            {
                if (is_right_worse)
                {
                    right_min -= first_plus_one;
                    right_min |= (first_plus_one - 1);
                }
                ResultType tmp;
                if (!is_right_worse || (!__builtin_add_overflow(left_block_min, first_plus_one, &tmp) && tmp < right_block_min))
                {
                    left_min &= ~(first_plus_one - 1);
                }
            }

            if (arity - argument_index + arity <= number_of_bits)
            {
                ResultType max_block_value = (1ull << arity) - 1;
                size_t initial_shift = number_of_bits - (arity << 1) + argument_index;
                ResultType get_block = max_block_value << initial_shift;
                ResultType plus_one = 1ull << initial_shift;

                for (size_t i = 0; i <= initial_shift / arity; ++i)
                {
                    // Handling the next block for MAX
                    left_block_max = left_max & get_block;
                    right_block_max = right_max & get_block;
                    is_left_best = static_cast<bool>(left_block_max & plus_one);
                    if (left_block_max < right_block_max)
                    {
                        if (!is_left_best)
                        {
                            left_max += plus_one;
                            left_max &= ~(plus_one - 1);
                        }
                        if (is_left_best || left_block_max + plus_one < right_block_max)
                        {
                            right_max |= plus_one - 1;
                        }
                    }
                    else
                    {
                        left_max |= get_block;
                        left_max ^= get_block;
                        left_max |= right_block_max;
                    }

                    // Handling the next block for MIN
                    left_block_min = left_min & get_block;
                    right_block_min = right_min & get_block;
                    is_right_worse = static_cast<bool>(right_block_min & plus_one);
                    if (left_block_min < right_block_min)
                    {
                        if (is_right_worse)
                        {
                            right_min -= plus_one;
                            right_min |= (plus_one - 1);
                        }
                        if (!is_right_worse || left_block_min + plus_one < right_block_min)
                        {
                            left_min &= ~(plus_one - 1);
                        }
                    }
                    else
                    {
                        right_min |= get_block;
                        right_min ^= get_block;
                        right_min |= left_block_min;
                    }
                    get_block >>= arity;
                    plus_one >>= arity;
                }
            }

            return {extractArgument(right_min, argument_index, arity),
                    extractArgument(left_max, argument_index, arity)};
        }
        /// Put the bits of argument at every arity-th position in the resulting number
        ResultType zShiftElement(
                const char * argument,
                size_t argument_size,
                size_t arity,
                const DataTypePtr & type) const
        {
            static constexpr int byte_length = sizeof(ResultType), bit_length = byte_length << 3;
            ResultType tmp = 0;
            memcpy(&tmp, argument, argument_size);
            Op::encode(tmp, type);
            ResultType result = 0;
            for (int bit = bit_length - 1, curr = bit_length - 1; curr >= 0; --bit, curr -= arity)
            {
                result |= ((tmp & (1ull << bit)) >> (bit - curr));
            }
            return result;
        }
        String getName() const override
        {
            return name;
        }
    };
}
