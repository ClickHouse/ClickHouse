#include <Columns/ColumnArray.h>
#include <Columns/ColumnFunction.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int TYPE_MISMATCH;
}

/**
 * arrayFold( acc,a1,...,aN->expr, arr1, ..., arrN, acc_initial)
 */
class FunctionArrayFold : public IFunction
{
public:
    static constexpr auto name = "arrayFold";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayFold>(); }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /// Avoid the default adaptors since they modify the inputs and that makes knowing the lambda argument types
    /// (getLambdaArgumentTypes) more complex, as it requires knowing what the adaptors will do
    /// It's much simpler to avoid the adapters
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    void getLambdaArgumentTypes(DataTypes & arguments) const override
    {
        if (arguments.size() < 3)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} requires as arguments a lambda function, at least one array and an accumulator", getName());

        DataTypes accumulator_and_array_types(arguments.size() - 1);
        accumulator_and_array_types[0] = arguments.back();
        for (size_t i = 1; i < accumulator_and_array_types.size(); ++i)
        {
            const auto * array_type = checkAndGetDataType<DataTypeArray>(&*arguments[i]);
            if (!array_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument {} of function {} must be of type Array, found {} instead", i + 1, getName(), arguments[i]->getName());
            accumulator_and_array_types[i] = recursiveRemoveLowCardinality(array_type->getNestedType());
        }

        const auto * lambda_function_type = checkAndGetDataType<DataTypeFunction>(arguments[0].get());
        if (!lambda_function_type || lambda_function_type->getArgumentTypes().size() != accumulator_and_array_types.size())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must be a lambda function with {} arguments, found {} instead.",
                            getName(), accumulator_and_array_types.size(), arguments[0]->getName());

        arguments[0] = std::make_shared<DataTypeFunction>(accumulator_and_array_types);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 3)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} requires as arguments a lambda function, at least one array and an accumulator", getName());

        const auto * lambda_function_type = checkAndGetDataType<DataTypeFunction>(arguments[0].type.get());
        if (!lambda_function_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a function", getName());

        auto accumulator_type = arguments.back().type;
        auto lambda_type = lambda_function_type->getReturnType();
        if (!accumulator_type->equals(*lambda_type))
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "Return type of lambda function must be the same as the accumulator type, inferred return type of lambda: {}, inferred type of accumulator: {}",
                    lambda_type->getName(), accumulator_type->getName());

        return accumulator_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & lambda_function_with_type_and_name = arguments[0];

        if (!lambda_function_with_type_and_name.column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a function", getName());

        const auto * lambda_function = typeid_cast<const ColumnFunction *>(lambda_function_with_type_and_name.column.get());
        if (!lambda_function)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a function", getName());

        ColumnPtr first_array_col;
        const ColumnArray * first_array_col_concrete = nullptr;
        ColumnPtr first_array_col_offsets;

        ColumnsWithTypeAndName arrays_data_with_type_and_name; /// for all arrays, the pointers to the internal data column, type and name
        arrays_data_with_type_and_name.reserve(arguments.size() - 1);

        /// Validate array arguments and set pointers so we can access them more conveniently
        for (size_t i = 1; i < arguments.size() - 1; ++i)
        {
            const auto & array_with_type_and_name = arguments[i];
            ColumnPtr array_col = array_with_type_and_name.column;
            const auto * array_col_concrete = checkAndGetColumn<ColumnArray>(array_col.get());
            if (!array_col_concrete)
            {
                const ColumnConst * aray_col_concrete_const = checkAndGetColumnConst<ColumnArray>(array_col.get());
                if (!aray_col_concrete_const)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected array column, found {}", array_col->getName());
                array_col = recursiveRemoveLowCardinality(aray_col_concrete_const->convertToFullColumn());
                array_col_concrete = checkAndGetColumn<ColumnArray>(array_col.get());
            }

            const DataTypePtr & array_type = array_with_type_and_name.type;
            const auto * array_type_concrete = checkAndGetDataType<DataTypeArray>(array_type.get());
            if (!array_type_concrete)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Expected array type, found {}", array_type->getName());

            /// Check that the cardinality of the arrays across a row is the same for all array arguments.
            /// This simplifies later calculations which can work only with the offsets of the first column.
            if (!first_array_col_offsets)
                first_array_col_offsets = array_col_concrete->getOffsetsPtr();
            else
            {
                /// It suffices to check that the internal offset columns are equal.
                /// The first condition is optimization: skip comparison if the offset pointers are equal.
                if (array_col_concrete->getOffsetsPtr() != first_array_col_offsets
                    && array_col_concrete->getOffsets() != typeid_cast<const ColumnArray::ColumnOffsets &>(*first_array_col_offsets).getData())
                    throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "arrays_data_with_type_and_name passed to {} must have equal size", getName());
            }

            if (i == 1)
            {
                first_array_col = array_col;
                first_array_col_concrete = array_col_concrete;
            }

            ColumnWithTypeAndName data_type_name(array_col_concrete->getDataPtr(), recursiveRemoveLowCardinality(array_type_concrete->getNestedType()), array_with_type_and_name.name);
            arrays_data_with_type_and_name.push_back(data_type_name);
        }

        const ssize_t num_rows = input_rows_count; /// how many rows are processed
        const size_t num_array_cols = arrays_data_with_type_and_name.size(); /// number of given array arguments
        const ssize_t num_elements_in_array_col = arrays_data_with_type_and_name[0].column->size(); /// total number of array elements in the 1st array argument (the value is the same for other array arguments)

        if (num_rows == 0)
            return arguments.back().column->convertToFullColumnIfConst()->cloneEmpty();

        const auto & offsets = first_array_col_concrete->getOffsets(); /// the internal offsets column of the first array argument (other array arguments have the same offsets)

        /// Find the first row which contains a non-empty array
        ssize_t first_row_with_non_empty_array = 0;
        if (num_elements_in_array_col)
            while (offsets[first_row_with_non_empty_array] == 0)
                ++first_row_with_non_empty_array;

        /// Build a selector which stores for every array element in the first array argument if the array element is the 0th, 1st, ... (horizontal) array element in the current row
        /// Better explained by an example:
        ///             0        1      <-- horizontal position
        ///   row0: ['elem1']
        ///   row1: ['elem2', 'elem3']
        ///   row2: ['elem4']
        ///   --> Selector will contain [0, 0, 1, 0].
        IColumn::Selector selector(num_elements_in_array_col);
        size_t max_array_size = 0; /// cardinality of the array with the most elements in the first array argument
        size_t cur_element_in_cur_array = 0;
        for (ssize_t i = 0; i < num_elements_in_array_col; ++i)
        {
            selector[i] = cur_element_in_cur_array;
            ++cur_element_in_cur_array;
            max_array_size = std::max(cur_element_in_cur_array, max_array_size);
            while (first_row_with_non_empty_array < num_rows && cur_element_in_cur_array >= offsets[first_row_with_non_empty_array] - offsets[first_row_with_non_empty_array - 1])
            {
                ++first_row_with_non_empty_array;
                cur_element_in_cur_array = 0;
            }
        }

        /// Based on the selector, scatter elements of the arrays on all rows into vertical slices
        /// Example:
        ///   row0: ['elem1']
        ///   row1: ['elem2', 'elem3']
        ///   row2: ['elem4']
        ///   --> create two slices based on selector [0, 0, 1, 0]
        ///       - slice0: 'elem1', 'elem2', 'elem4''
        ///       - slice1: 'elem3'
        std::vector<MutableColumns> vertical_slices; /// contains for every array argument, a vertical slice for the 0th array element, a vertical slice for the 1st array element, ...
        vertical_slices.resize(num_array_cols);
        if (max_array_size > 0)
            for (size_t i = 0; i < num_array_cols; ++i)
                vertical_slices[i] = arrays_data_with_type_and_name[i].column->scatter(max_array_size, selector);

        ColumnPtr accumulator_col = arguments.back().column->convertToFullColumnIfConst();
        MutableColumnPtr result_col = accumulator_col->cloneEmpty();
        ColumnPtr lambda_col = lambda_function->cloneResized(num_rows);

        IColumn::Permutation inverse_permutation(num_rows);
        size_t num_inverse_permutations = 0;

        /// Iterate the slices. The accumulator value of a row is updated iff the array in the row has at least slice_i-many elements. Since
        /// slices become incrementally smaller, fewer and fewer accumulator values are updated in each iteration. Once the calculation for
        /// a row is finished (i.e. there are no more slices to process), it is added to the result. Since that happens in random order,
        /// we also maintain a mapping to reconstruct the right result order at the end.
        size_t unfinished_rows = num_rows; /// number of rows to consider in the current iteration
        for (size_t slice = 0; slice < max_array_size; ++slice)
        {
            IColumn::Selector prev_selector(unfinished_rows); /// 1 for rows which have slice_i-many elements, otherwise 0
            size_t prev_index = 0;
            for (ssize_t row = 0; row < num_rows; ++row)
            {
                size_t num_elements = offsets[row] - offsets[row - 1]; /// cardinality of array on the row
                if (num_elements > slice)
                {
                    prev_selector[prev_index] = 1;
                    ++prev_index;
                }
                else if (num_elements == slice)
                {
                    prev_selector[prev_index] = 0;
                    ++prev_index;
                    inverse_permutation[num_inverse_permutations] = row;
                    ++num_inverse_permutations;
                }
            }

            /// Scatter the accumulator into two columns
            /// - one column with accumulator values for rows less than slice-many elements, no further calculation is performed on them
            /// - one column with accumulator values for rows with slice-many or more elements, these are updated in this or following iteration
            std::vector<IColumn::MutablePtr> finished_unfinished_accumulator_values = accumulator_col->scatter(2, prev_selector);
            IColumn::MutablePtr & finished_accumulator_values = finished_unfinished_accumulator_values[0];
            IColumn::MutablePtr & unfinished_accumulator_values = finished_unfinished_accumulator_values[1];

            /// Copy finished accumulator values into the result
            result_col->insertRangeFrom(*finished_accumulator_values, 0, finished_accumulator_values->size());

            /// The lambda function can contain statically bound arguments, in particular their row values. We need to filter for the rows
            /// we care about.
            IColumn::Filter filter(unfinished_rows);
            for (size_t i = 0; i < prev_selector.size(); ++i)
                filter[i] = prev_selector[i];
            ColumnPtr lambda_col_filtered = lambda_col->filter(filter, lambda_col->size());
            IColumn::MutablePtr lambda_col_filtered_cloned = lambda_col_filtered->cloneResized(lambda_col_filtered->size()); /// clone so we can bind more arguments
            auto * lambda = typeid_cast<ColumnFunction *>(lambda_col_filtered_cloned.get());

            /// Bind arguments to lambda function (accumulator + array arguments)
            lambda->appendArguments(std::vector({ColumnWithTypeAndName(std::move(unfinished_accumulator_values), arguments.back().type, arguments.back().name)}));
            for (size_t array_col = 0; array_col < num_array_cols; ++array_col)
                lambda->appendArguments(std::vector({ColumnWithTypeAndName(std::move(vertical_slices[array_col][slice]), arrays_data_with_type_and_name[array_col].type, arrays_data_with_type_and_name[array_col].name)}));

            /// Perform the actual calculation and copy the result into the accumulator
            ColumnWithTypeAndName res_with_type_and_name = lambda->reduce();
            accumulator_col = res_with_type_and_name.column->convertToFullColumnIfConst();

            unfinished_rows = accumulator_col->size();
            lambda_col = lambda_col_filtered;
        }

        /// Copy accumulator values of last iteration into result.
        result_col->insertRangeFrom(*accumulator_col, 0, accumulator_col->size());

        for (ssize_t row = 0; row < num_rows; ++row)
        {
            size_t num_elements = offsets[row] - offsets[row - 1]; /// cardinality of array on the row
            if (num_elements == max_array_size)
            {
                inverse_permutation[num_inverse_permutations] = row;
                ++num_inverse_permutations;
            }
        }

        /// We have result_col containing result for every row and inverse_permutation which contains indexes of rows in input it corresponds to.
        /// Now we need to invert inverse_permuation and apply it to result_col to get rows in right order.
        IColumn::Permutation perm(num_rows);
        for (ssize_t row = 0; row < num_rows; ++row)
            perm[inverse_permutation[row]] = row;
        return result_col->permute(perm, 0);
    }

private:
    String getName() const override
    {
        return name;
    }
};

REGISTER_FUNCTION(ArrayFold)
{
    factory.registerFunction<FunctionArrayFold>(FunctionDocumentation{.description=R"(
        Function arrayFold(acc,a1,...,aN->expr, arr1, ..., arrN, acc_initial) applies a lambda function to each element
        in each (equally-sized) array and collects the result in an accumulator.
        )", .examples{{"sum", "SELECT arrayFold(acc,x->acc+x, [1,2,3,4], toInt64(1));", "11"}}, .categories{"Array"}});
}
}
