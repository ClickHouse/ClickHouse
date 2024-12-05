#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/Context_fwd.h>

#include <numeric>

namespace DB
{
static constexpr size_t MAX_ARRAY_SIZE = 1 << 20;

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

struct NormalizedGiniImpl
{
    template <typename T1, typename T2>
    static void vectorArrayConstArrayNormalizedGini(
        const PaddedPODArray<T1> & array_datas1,
        const ColumnArray::Offsets & offsets1,
        const PaddedPODArray<T2> & const_array,
        PaddedPODArray<Float64> & pred_gini_col,
        PaddedPODArray<Float64> & label_gini_col,
        PaddedPODArray<Float64> & norm_gini_col)
    {
        size_t size = pred_gini_col.size();
        size_t array_size = const_array.size();

        if (array_size > MAX_ARRAY_SIZE)
            throw Exception(
                ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size in normalizedGini: {}, maximum: {}", array_size, MAX_ARRAY_SIZE);

        for (size_t i = 0; i < size; ++i)
        {
            size_t array1_size = offsets1[i] - offsets1[i - 1];
            if (array1_size != array_size)
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "All array in function normalizedGini should have same size");
            }

            PODArrayWithStackMemory<T1, 1024> array1(array_datas1.data() + offsets1[i - 1], array_datas1.data() + offsets1[i]);
            // Why we need to create a new array here every loop, because array2 will be sorted in normalizedGiniImpl.
            PODArrayWithStackMemory<T2, 1024> array2(const_array.begin(), const_array.end());

            auto [pred_gini, label_gini, norm_gini] = normalizedGiniImpl(array1, array2, array_size);

            pred_gini_col[i] = pred_gini;
            label_gini_col[i] = label_gini;
            norm_gini_col[i] = norm_gini;
        }
    }

    template <typename T1, typename T2>
    static void vectorArrayVectorArrayNormalizedGini(
        const PaddedPODArray<T1> & array_datas1,
        const ColumnArray::Offsets & offsets1,
        const PaddedPODArray<T2> & array_datas2,
        const ColumnArray::Offsets & offsets2,
        PaddedPODArray<Float64> & pred_gini_col,
        PaddedPODArray<Float64> & label_gini_col,
        PaddedPODArray<Float64> & norm_gini_col)
    {
        size_t size = pred_gini_col.size();
        size_t array_size = size > 0 ? offsets1[0] - offsets1[-1] : 0;

        if (array_size > MAX_ARRAY_SIZE)
            throw Exception(
                ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size in normalizedGini: {}, maximum: {}", array_size, MAX_ARRAY_SIZE);

        for (size_t i = 0; i < size; ++i)
        {
            size_t array1_size = offsets1[i] - offsets1[i - 1];
            size_t array2_size = offsets2[i] - offsets2[i - 1];
            if (array1_size != array_size || array2_size != array_size)
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "All array in function normalizedGini should have same size");
            }

            PODArrayWithStackMemory<T1, 1024> array1(array_datas1.data() + offsets1[i - 1], array_datas1.data() + offsets1[i]);
            PODArrayWithStackMemory<T2, 1024> array2(array_datas2.data() + offsets2[i - 1], array_datas2.data() + offsets2[i]);

            auto [pred_gini, label_gini, norm_gini] = normalizedGiniImpl(array1, array2, array_size);

            pred_gini_col[i] = pred_gini;
            label_gini_col[i] = label_gini;
            norm_gini_col[i] = norm_gini;
        }
    }

    template <typename T1, typename T2>
    static void constArrayVectorArrayNormalizedGini(
        const PaddedPODArray<T1> & const_array,
        const PaddedPODArray<T2> & array_datas1,
        const ColumnArray::Offsets & offsets1,
        PaddedPODArray<Float64> & pred_gini_col,
        PaddedPODArray<Float64> & label_gini_col,
        PaddedPODArray<Float64> & norm_gini_col)
    {
        size_t size = pred_gini_col.size();
        size_t array_size = const_array.size();

        if (array_size > MAX_ARRAY_SIZE)
            throw Exception(
                ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size in normalizedGini: {}, maximum: {}", array_size, MAX_ARRAY_SIZE);

        PODArrayWithStackMemory<T1, 1024> array1(const_array.begin(), const_array.end());
        for (size_t i = 0; i < size; ++i)
        {
            size_t array1_size = offsets1[i] - offsets1[i - 1];
            if (array1_size != array_size)
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "All array in function normalizedGini should have same size");
            }

            PODArrayWithStackMemory<T2, 1024> array2(array_datas1.data() + offsets1[i - 1], array_datas1.data() + offsets1[i]);

            auto [pred_gini, label_gini, norm_gini] = normalizedGiniImpl(array1, array2, array_size);

            pred_gini_col[i] = pred_gini;
            label_gini_col[i] = label_gini;
            norm_gini_col[i] = norm_gini;
        }
    }

private:
    template <typename T1, typename T2>
    static std::tuple<Float64, Float64, Float64>
    normalizedGiniImpl(const PODArrayWithStackMemory<T1, 1024> & array1, PODArrayWithStackMemory<T2, 1024> & array2, size_t array_size)
    {
        auto sort_idx = sortIndexes(array1);

        PODArrayWithStackMemory<T2, 1024> sorted_array2(array_size);
        for (size_t i = 0; i < array_size; ++i)
        {
            sorted_array2[i] = array2[sort_idx[i]];
        }

        Float64 total_sum = std::accumulate(array2.begin(), array2.end(), 0.0);

        PODArrayWithStackMemory<Float64, 1024> pred_cumsum_ratio(array_size);

        for (size_t i = 0; i < array_size; ++i)
        {
            pred_cumsum_ratio[i] = pred_cumsum_ratio[i - 1] + sorted_array2[i] / total_sum;
        }

        std::stable_sort(array2.begin(), array2.end());
        PODArrayWithStackMemory<Float64, 1024> ltv_cumsum_ratio(array_size);
        for (size_t i = 0; i < array_size; ++i)
        {
            ltv_cumsum_ratio[i] = ltv_cumsum_ratio[i - 1] + array2[i] / total_sum;
        }

        Float64 random_gain_cumsum_ratio = 0.5 * (array_size + 1);
        Float64 accumulate_pred_ratio = std::accumulate(pred_cumsum_ratio.begin(), pred_cumsum_ratio.end(), 0.0);
        Float64 accumulate_ltv_ratio = std::accumulate(ltv_cumsum_ratio.begin(), ltv_cumsum_ratio.end(), 0.0);

        Float64 pred_gini = (random_gain_cumsum_ratio - accumulate_pred_ratio) / array_size;
        Float64 label_gini = (random_gain_cumsum_ratio - accumulate_ltv_ratio) / array_size;

        return std::make_tuple(pred_gini, label_gini, pred_gini / label_gini);
    }

    template <typename T>
    static PODArrayWithStackMemory<size_t, 1024> sortIndexes(const PODArrayWithStackMemory<T, 1024> & array)
    {
        PODArrayWithStackMemory<size_t, 1024> idx(array.size());
        std::iota(idx.begin(), idx.end(), 0);

        std::stable_sort(idx.begin(), idx.end(), [&array](size_t i1, size_t i2) { return array[i1] < array[i2]; });
        return idx;
    }
};

/**
 * Calculate the normalized Gini coefficient. See https://arxiv.org/pdf/1912.07753
 */
class FunctionNormalizedGini : public IFunction
{
    template <typename F>
    static bool castType(const IDataType * type, F && f)
    {
        return castTypeToEither<
            DataTypeInt8,
            DataTypeInt16,
            DataTypeInt32,
            DataTypeInt64,
            DataTypeUInt8,
            DataTypeUInt16,
            DataTypeUInt32,
            DataTypeUInt64,
            DataTypeFloat32,
            DataTypeFloat64>(type, std::forward<F>(f));
    }

    template <typename F>
    static bool castBothTypes(const IDataType * left, const IDataType * right, F && f)
    {
        return castType(left, [&](const auto & left_)
        {
            return castType(right, [&](const auto & right_)
            {
                return f(left_, right_);
            });
        });
    }

public:
    static constexpr auto name = "normalizedGini";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionNormalizedGini>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2",
                getName(), arguments.size());

        const DataTypeArray * arg1_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (arg1_type == nullptr || !isNumber(arg1_type->getNestedType()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be numeric array, got {}",
                getName(),
                arguments[0]->getName());

        const DataTypeArray * arg2_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());
        if (arg2_type == nullptr || !isNumber(arg2_type->getNestedType()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be numeric array, not {}",
                getName(),
                arguments[1]->getName());

        return std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & array_col1 = arguments[0].column;
        const auto & array_col2 = arguments[1].column;

        Columns tuple_columns(3);
        for (size_t i = 0; i < 3; ++i)
        {
            tuple_columns[i] = DataTypeFloat64{}.createColumn();
        }

        if (const ColumnArray * array_col = checkAndGetColumn<ColumnArray>(array_col1.get()))
        {
            const auto & offsets1 = array_col->getOffsets();
            const auto & array_arg_type1 = typeid_cast<const DataTypeArray *>(arguments[0].type.get())->getNestedType();

            if (const ColumnConst * array_column_const = checkAndGetColumn<ColumnConst>(array_col2.get()))
            {
                const ColumnArray * column_array_const = checkAndGetColumn<ColumnArray>(array_column_const->getDataColumnPtr().get());
                const auto & array_arg_type2 = typeid_cast<const DataTypeArray *>(arguments[1].type.get())->getNestedType();

                if (castBothTypes(
                        array_arg_type1.get(),
                        array_arg_type2.get(),
                        [&](const auto & array_column_type1, const auto & array_column_type2)
                        {
                            using ArrayDataType1 = std::decay_t<decltype(array_column_type1)>;
                            using T1 = typename ArrayDataType1::FieldType;
                            const ColumnVector<T1> * array_column_vector1
                                = checkAndGetColumn<ColumnVector<T1>>(array_col->getDataPtr().get());

                            using ArrayDataType2 = std::decay_t<decltype(array_column_type2)>;
                            using T2 = typename ArrayDataType2::FieldType;
                            const ColumnVector<T2> * array_column_vector2
                                = checkAndGetColumn<ColumnVector<T2>>(column_array_const->getDataPtr().get());

                            auto pred_gini_col = ColumnFloat64::create(input_rows_count);
                            auto label_gini_col = ColumnFloat64::create(input_rows_count);
                            auto norm_gini_col = ColumnFloat64::create(input_rows_count);


                            NormalizedGiniImpl::vectorArrayConstArrayNormalizedGini(
                                array_column_vector1->getData(),
                                offsets1,
                                array_column_vector2->getData(),
                                pred_gini_col->getData(),
                                label_gini_col->getData(),
                                norm_gini_col->getData());

                            tuple_columns[0] = std::move(pred_gini_col);
                            tuple_columns[1] = std::move(label_gini_col);
                            tuple_columns[2] = std::move(norm_gini_col);

                            return true;
                        }))
                {
                    return ColumnTuple::create(tuple_columns);
                }
            }
            else
            {
                const ColumnArray * column_array2 = checkAndGetColumn<ColumnArray>(array_col2.get());
                const auto & offsets2 = column_array2->getOffsets();
                const auto & array_arg_type2 = typeid_cast<const DataTypeArray *>(arguments[1].type.get())->getNestedType();

                if (castBothTypes(
                        array_arg_type1.get(),
                        array_arg_type2.get(),
                        [&](const auto & array_column_type1, const auto & array_column_type2)
                        {
                            using ArrayDataType1 = std::decay_t<decltype(array_column_type1)>;
                            using T1 = typename ArrayDataType1::FieldType;
                            const ColumnVector<T1> * array_column_vector1
                                = checkAndGetColumn<ColumnVector<T1>>(array_col->getDataPtr().get());

                            using ArrayDataType2 = std::decay_t<decltype(array_column_type2)>;
                            using T2 = typename ArrayDataType2::FieldType;
                            const ColumnVector<T2> * array_column_vector2
                                = checkAndGetColumn<ColumnVector<T2>>(column_array2->getDataPtr().get());

                            auto pred_gini_col = ColumnFloat64::create(input_rows_count);
                            auto label_gini_col = ColumnFloat64::create(input_rows_count);
                            auto norm_gini_col = ColumnFloat64::create(input_rows_count);


                            NormalizedGiniImpl::vectorArrayVectorArrayNormalizedGini(
                                array_column_vector1->getData(),
                                offsets1,
                                array_column_vector2->getData(),
                                offsets2,
                                pred_gini_col->getData(),
                                label_gini_col->getData(),
                                norm_gini_col->getData());

                            tuple_columns[0] = std::move(pred_gini_col);
                            tuple_columns[1] = std::move(label_gini_col);
                            tuple_columns[2] = std::move(norm_gini_col);

                            return true;
                        }))
                {
                    return ColumnTuple::create(tuple_columns);
                }
            }
        }
        else if (const ColumnConst * array_column_const = checkAndGetColumn<ColumnConst>(array_col1.get()))
        {
            /// Note that const-const case is handled by useDefaultImplementationForConstants.

            const ColumnArray * column_array_const = checkAndGetColumn<ColumnArray>(array_column_const->getDataColumnPtr().get());
            const auto & array_arg_type1 = typeid_cast<const DataTypeArray *>(arguments[0].type.get())->getNestedType();

            const ColumnArray * column_array2 = checkAndGetColumn<ColumnArray>(array_col2.get());
            const auto & offsets2 = column_array2->getOffsets();
            const auto & array_arg_type2 = typeid_cast<const DataTypeArray *>(arguments[1].type.get())->getNestedType();

            if (castBothTypes(
                    array_arg_type1.get(),
                    array_arg_type2.get(),
                    [&](const auto & array_column_type1, const auto & array_column_type2)
                    {
                        using ArrayDataType1 = std::decay_t<decltype(array_column_type1)>;
                        using T1 = typename ArrayDataType1::FieldType;
                        const ColumnVector<T1> * array_column_vector1
                            = checkAndGetColumn<ColumnVector<T1>>(column_array_const->getDataPtr().get());

                        using ArrayDataType2 = std::decay_t<decltype(array_column_type2)>;
                        using T2 = typename ArrayDataType2::FieldType;
                        const ColumnVector<T2> * array_column_vector2
                            = checkAndGetColumn<ColumnVector<T2>>(column_array2->getDataPtr().get());

                        auto pred_gini_col = ColumnFloat64::create(input_rows_count);
                        auto label_gini_col = ColumnFloat64::create(input_rows_count);
                        auto norm_gini_col = ColumnFloat64::create(input_rows_count);


                        NormalizedGiniImpl::constArrayVectorArrayNormalizedGini(
                            array_column_vector1->getData(),
                            array_column_vector2->getData(),
                            offsets2,
                            pred_gini_col->getData(),
                            label_gini_col->getData(),
                            norm_gini_col->getData());

                        tuple_columns[0] = std::move(pred_gini_col);
                        tuple_columns[1] = std::move(label_gini_col);
                        tuple_columns[2] = std::move(norm_gini_col);

                        return true;
                    }))
            {
                return ColumnTuple::create(tuple_columns);
            }
        }

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column of argument of function {}", getName());
    }

};

REGISTER_FUNCTION(NormalizedGini)
{
    FunctionDocumentation::Description doc_description = "The function is used to calculate the normalized Gini coefficient.";
    FunctionDocumentation::Syntax doc_syntax = "normalizedGini(pltv, ltv)";
    FunctionDocumentation::Arguments doc_arguments
        = {{"pltv", "Predicted liefetime value (Array(T))."}, {"ltv", "Actual lifetime value (Array(T))."}};
    FunctionDocumentation::ReturnedValue doc_returned_value = "A tuple contains Gini coefficient for the predicted LTV, Gini coefficient "
                                                              "for the actual  LTV and the normalized Gini coefficient .";
    FunctionDocumentation::Examples doc_examples
        = {{"Example",
            "SELECT normalizedGini([0.9, 0.3, 0.8, 0.7],[6, 1, 0, 2]);",
            "(0.18055555555555558,0.2638888888888889,0.6842105263157896)"}};
    FunctionDocumentation::Categories doc_categories = {"Array"};

    factory.registerFunction<FunctionNormalizedGini>(
        {doc_description, doc_syntax, doc_arguments, doc_returned_value, doc_examples, doc_categories}, FunctionFactory::Case::Insensitive);
}

}
