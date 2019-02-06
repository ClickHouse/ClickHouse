#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>
#include <iostream>
#include <Core/iostream_debug_helpers.h>


namespace DB
{

    class FunctionZCurve : public IFunction
    {
    public:
        static constexpr auto name = "ZCurve";
        static FunctionPtr create(const Context & context)
        {
            return std::make_shared<FunctionZCurve>(context);
        }
        FunctionZCurve(const Context& context): context(context) {}

        bool isVariadic() const override { return true; }
        size_t getNumberOfArguments() const override { return 0; }

        using ResultType = UInt64;
        DataTypePtr getReturnTypeImpl(const DataTypes& arguments) const override
        {
            // TODO: check for improper data types
            if (arguments.empty()) {
                // just a placeholder
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
                    out_data[j] |= zShiftElement(column_data.data + j * size_per_element, size_per_element, number_of_elements);
                }

            }
            block.getByPosition(result).column = std::move(out);
        }

        bool canSatisfyRangeOnParallelogram(const std::vector<Range> & parallelogram, Range & value_range) const override
        {
            value_range.shrinkToIncludedIfPossible(); // always possible if not unbounded, since the result type is UInt64
            ResultType left, right;
            if (!value_range.left_bounded)
            {
                left = std::numeric_limits<ResultType>::min();
            }
            else
            {
                left = value_range.left.get<ResultType>();
            }
            if (!value_range.right_bounded)
            {
                right = std::numeric_limits<ResultType>::max();
            }
            else
            {
                right = value_range.right.get<ResultType>();
            }
            for (size_t argument = 0; argument < parallelogram.size(); ++argument)
            {
                auto min_max = getMinMaxPossibleBitValueOfArgument(left, right, argument);

            }
        }


    private:
        ResultType extractArgument(ResultType z_value, size_t argument_index, size_t arity) const
        {
            size_t number_of_bits = (sizeof(ResultType) << 3);
            ResultType result = 0;
            ResultType res_bit = 1ull << (number_of_bits - 1);
            for (ResultType bit = 1ull << (number_of_bits - argument_index); bit; bit >>= arity)
            {
                if (z_value & bit)
                {
                    result |= res_bit;
                }
                res_bit >>= 1;
            }
            return result;
        }
        // Get maximal and minimal possible bit representation of a given argument the values fall in a given range.
        std::pair<ResultType, ResultType> getMinMaxPossibleBitValueOfArgument(
                ResultType left,
                ResultType right,
                size_t argument_index,
                size_t arity) const
        {
            auto left_min = left, left_max = left;
            auto right_min = right, right_max = right;
            size_t number_of_bits = sizeof(ResultType) << 3;

            ResultType max_first_block_value = (1ull << (arity - argument_index)) - 1;
            ResultType get_first_block = max_first_block_value << (number_of_bits - arity + argument_index);
            ResultType first_plus_one = 1ull << (number_of_bits - arity + argument_index);


            ResultType left_block_min, left_block_max, right_block_min, right_block_max;

            // First step for max
            left_block_max = left_max & get_first_block;
            right_block_max = right_max & get_first_block;
            bool is_left_best = static_cast<bool>(left_block_max & first_plus_one);
            if (left_block_max < right_block_max)
            {
                if (!is_left_best) {
                    left_max += first_plus_one;
                }
                if (is_left_best || left_block_max + 1 < right_block_max) {
                    right_max |= first_plus_one - 1;
                }
            }

            // First step for min
            left_block_min = left_min & get_first_block;
            right_block_min = right_min & get_first_block;
            bool is_right_worse = static_cast<bool>(right_block_min & first_plus_one);
            if (left_block_min < right_block_min)
            {
                if (is_right_worse) {
                    right_min -= first_plus_one;
                }
                if (!is_right_worse || left_block_max + 1 < right_block_max) {
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
                    // Step for max
                    left_block_max = left_max & get_block;
                    right_block_max = right_max & get_block;
                    is_left_best = static_cast<bool>(left_block_max & plus_one);
                    if (left_block_max < right_block_max)
                    {
                        if (!is_left_best) {
                            left_max += plus_one;
                        }
                        if (is_left_best || left_block_max + 1 < right_block_max) {
                            right_max |= plus_one - 1;
                        }
                    }

                    // Step for min
                    left_block_min = left_min & get_first_block;
                    right_block_min = right_min & get_first_block;
                    is_right_worse = static_cast<bool>(right_block_min & first_plus_one);
                    if (left_block_min < right_block_min)
                    {
                        if (is_right_worse) {
                            right_min -= plus_one;
                        }
                        if (!is_right_worse || left_block_max + 1 < right_block_max) {
                            left_min &= ~(plus_one - 1);
                        }
                    }
                    get_block >>= arity;
                    plus_one >>= arity;
                }
            }

            return {extractArgument(left_max, argument_index, arity),
                    extractArgument(right_min, argument_index, arity)};
        }
        ResultType zShiftElement(
                const char * argument,
                size_t argument_size,
                size_t arity) const
        {
            int byte_length = sizeof(ResultType), bit_length = byte_length << 3;
            ResultType tmp = 0;
            memcpy(&tmp, argument, argument_size);
            tmp <<= ((byte_length - argument_size) << 3);
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
        const Context & context;
    };

    void registerFunctionZCurve(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionZCurve>();
    }
}
