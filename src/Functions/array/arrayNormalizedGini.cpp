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
#include <pdqsort.h>

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

struct Impl
{
    template <typename T1, typename T2>
    static void vectorConst(
        const PaddedPODArray<T1> & array_predicted_data,
        const ColumnArray::Offsets & array_predicted_offsets,
        const PaddedPODArray<T2> & array_labels_const,
        PaddedPODArray<Float64> & col_gini_predicted,
        PaddedPODArray<Float64> & col_gini_labels,
        PaddedPODArray<Float64> & col_gini_normalized)
    {
        size_t size = col_gini_predicted.size();
        size_t array_size = array_labels_const.size();

        if (array_size > MAX_ARRAY_SIZE)
            throw Exception(
                ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                "Too large array size in function arrayNormalizedGini: {}, maximum: {}",
                array_size,
                MAX_ARRAY_SIZE);

        for (size_t i = 0; i < size; ++i)
        {
            size_t array1_size = array_predicted_offsets[i] - array_predicted_offsets[i - 1];
            if (array1_size != array_size)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "All arrays in function arrayNormalizedGini should have same size");

            // Why we need to create a new array here every loop, because array2 will be sorted in calculateNormalizedGini.
            PODArrayWithStackMemory<T2, 1024> array2(array_labels_const.begin(), array_labels_const.end());

            auto [gini_predicted, gini_labels, gini_normalized] = calculateNormalizedGini(array_predicted_data, array_predicted_offsets[i - 1], array2, array_size);

            col_gini_predicted[i] = gini_predicted;
            col_gini_labels[i] = gini_labels;
            col_gini_normalized[i] = gini_normalized;
        }
    }

    template <typename T1, typename T2>
    static void vectorVector(
        const PaddedPODArray<T1> & array_predicted_data,
        const ColumnArray::Offsets & array_predicted_offsets,
        const PaddedPODArray<T2> & array_labels_data,
        const ColumnArray::Offsets & array_labels_offsets,
        PaddedPODArray<Float64> & col_gini_predicted,
        PaddedPODArray<Float64> & col_gini_labels,
        PaddedPODArray<Float64> & col_gini_normalized)
    {
        size_t size = col_gini_predicted.size();
        size_t array_size = size > 0 ? array_predicted_offsets[0] - array_predicted_offsets[-1] : 0;

        if (array_size > MAX_ARRAY_SIZE)
            throw Exception(
                ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size in arrayNormalizedGini: {}, maximum: {}", array_size, MAX_ARRAY_SIZE);

        for (size_t i = 0; i < size; ++i)
        {
            size_t array1_size = array_predicted_offsets[i] - array_predicted_offsets[i - 1];
            size_t array2_size = array_labels_offsets[i] - array_labels_offsets[i - 1];
            if (array1_size != array_size || array2_size != array_size)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "All arrays in function arrayNormalizedGini should have same size");

            PODArrayWithStackMemory<T2, 1024> array2(array_labels_data.data() + array_labels_offsets[i - 1], array_labels_data.data() + array_labels_offsets[i]);

            auto [gini_predicted, gini_labels, gini_normalized] = calculateNormalizedGini(array_predicted_data, array_predicted_offsets[i - 1], array2, array_size);

            col_gini_predicted[i] = gini_predicted;
            col_gini_labels[i] = gini_labels;
            col_gini_normalized[i] = gini_normalized;
        }
    }

    template <typename T1, typename T2>
    static void constVector(
        const PaddedPODArray<T1> & array_predicted_const,
        const PaddedPODArray<T2> & array_labels_data,
        const ColumnArray::Offsets & array_labels_offsets,
        PaddedPODArray<Float64> & col_gini_predicted,
        PaddedPODArray<Float64> & col_gini_labels,
        PaddedPODArray<Float64> & col_gini_normalized)
    {
        size_t size = col_gini_predicted.size();
        size_t array_size = array_predicted_const.size();

        if (array_size > MAX_ARRAY_SIZE)
            throw Exception(
                ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size in arrayNormalizedGini: {}, maximum: {}", array_size, MAX_ARRAY_SIZE);

        for (size_t i = 0; i < size; ++i)
        {
            size_t array1_size = array_labels_offsets[i] - array_labels_offsets[i - 1];
            if (array1_size != array_size)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "All arrays in function arrayNormalizedGini should have same size");

            PODArrayWithStackMemory<T2, 1024> array2(array_labels_data.data() + array_labels_offsets[i - 1], array_labels_data.data() + array_labels_offsets[i]);

            auto [gini_predicted, gini_labels, gini_normalized] = calculateNormalizedGini(array_predicted_const, 0, array2, array_size);

            col_gini_predicted[i] = gini_predicted;
            col_gini_labels[i] = gini_labels;
            col_gini_normalized[i] = gini_normalized;
        }
    }

private:
    template <typename T1, typename T2>
    static std::tuple<Float64, Float64, Float64> calculateNormalizedGini(
        const PaddedPODArray<T1> & array1, size_t offset,
        PODArrayWithStackMemory<T2, 1024> & array2, size_t array_size)
    {
        auto sort_idx = sortIndexes(array1, offset, array_size);

        PODArrayWithStackMemory<T2, 1024> sorted_array2(array_size);
        for (size_t i = 0; i < array_size; ++i)
            sorted_array2[i] = array2[sort_idx[i]];

        Float64 total_sum = std::accumulate(array2.begin(), array2.end(), 0.0);

        PODArrayWithStackMemory<Float64, 1024> pred_cumsum_ratio(array_size);

        Float64 pred_cumsum = 0;
        for (size_t i = 0; i < array_size; ++i)
        {
            pred_cumsum += sorted_array2[i] / total_sum;
            pred_cumsum_ratio[i] = pred_cumsum;
        }

        pdqsort(array2.begin(), array2.end());
        PODArrayWithStackMemory<Float64, 1024> ltv_cumsum_ratio(array_size);

        Float64 ltv_cumsum = 0;
        for (size_t i = 0; i < array_size; ++i)
        {
            ltv_cumsum += array2[i] / total_sum;
            ltv_cumsum_ratio[i] = ltv_cumsum;
        }

        Float64 random_gain_cumsum_ratio = 0.5 * (array_size + 1);
        Float64 accumulate_pred_ratio = std::accumulate(pred_cumsum_ratio.begin(), pred_cumsum_ratio.end(), 0.0);
        Float64 accumulate_ltv_ratio = std::accumulate(ltv_cumsum_ratio.begin(), ltv_cumsum_ratio.end(), 0.0);

        Float64 pred_gini = (random_gain_cumsum_ratio - accumulate_pred_ratio) / array_size;
        Float64 gini_labels = (random_gain_cumsum_ratio - accumulate_ltv_ratio) / array_size;

        return std::make_tuple(pred_gini, gini_labels, pred_gini / gini_labels);
    }

    template <typename T>
    static PODArrayWithStackMemory<size_t, 1024> sortIndexes(const PaddedPODArray<T> & array, size_t offset, size_t array_size)
    {
        PODArrayWithStackMemory<size_t, 1024> idx(array_size);
        std::iota(idx.begin(), idx.end(), 0);
        pdqsort(idx.begin(), idx.end(), [&array, offset](size_t i1, size_t i2) { return array[i1 + offset] < array[i2 + offset]; });
        return idx;
    }
};

/**
 * Calculate the normalized Gini coefficient. See https://arxiv.org/pdf/1912.07753
 */
class FunctionArrayNormalizedGini : public IFunction
{
public:
    static constexpr auto name = "arrayNormalizedGini";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayNormalizedGini>(); }

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
                "First argument for function {} must be an Array of numeric type, got {}",
                getName(),
                arguments[0]->getName());

        const DataTypeArray * arg2_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());
        if (arg2_type == nullptr || !isNumber(arg2_type->getNestedType()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function {} must be an Array of numeric typegot {}",
                getName(),
                arguments[1]->getName());

        return std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & col_predicted = arguments[0].column;
        const auto & col_labels = arguments[1].column;

        Columns result(3);
        for (size_t i = 0; i < 3; ++i)
            result[i] = DataTypeFloat64().createColumn();

        if (const ColumnArray * array_predicted = checkAndGetColumn<ColumnArray>(col_predicted.get()))
        {
            const auto & array_predicted_offsets = array_predicted->getOffsets();
            const auto & array_predicted_type = typeid_cast<const DataTypeArray *>(arguments[0].type.get())->getNestedType();

            if (const ColumnConst * array_labels_const = checkAndGetColumn<ColumnConst>(col_labels.get()))
            {
                const ColumnArray * column_array_const_internal_array = checkAndGetColumn<ColumnArray>(array_labels_const->getDataColumnPtr().get());
                const auto & array_labels_type = typeid_cast<const DataTypeArray *>(arguments[1].type.get())->getNestedType();

                if (castBothTypes(
                        array_predicted_type.get(),
                        array_labels_type.get(),
                        [&](const auto & type_predicted, const auto & type_labels)
                        {
                            using TypePredicted = typename std::decay_t<decltype(type_predicted)>::FieldType;
                            const ColumnVector<TypePredicted> * array_predicted_data = checkAndGetColumn<ColumnVector<TypePredicted>>(array_predicted->getDataPtr().get());

                            using TypeLabels = typename std::decay_t<decltype(type_labels)>::FieldType;
                            const ColumnVector<TypeLabels> * col_labels_data = checkAndGetColumn<ColumnVector<TypeLabels>>(column_array_const_internal_array->getDataPtr().get());

                            auto col_gini_predicted = ColumnFloat64::create(input_rows_count);
                            auto col_gini_labels = ColumnFloat64::create(input_rows_count);
                            auto col_gini_normalized = ColumnFloat64::create(input_rows_count);

                            Impl::vectorConst(
                                array_predicted_data->getData(),
                                array_predicted_offsets,
                                col_labels_data->getData(),
                                col_gini_predicted->getData(),
                                col_gini_labels->getData(),
                                col_gini_normalized->getData());

                            result[0] = std::move(col_gini_predicted);
                            result[1] = std::move(col_gini_labels);
                            result[2] = std::move(col_gini_normalized);

                            return true;
                        }))
                {
                    return ColumnTuple::create(result);
                }
            }
            else
            {
                const ColumnArray * array_labels = checkAndGetColumn<ColumnArray>(col_labels.get());
                const auto & array_label_offsets = array_labels->getOffsets();
                const auto & array_labels_type = typeid_cast<const DataTypeArray *>(arguments[1].type.get())->getNestedType();

                if (castBothTypes(
                        array_predicted_type.get(),
                        array_labels_type.get(),
                        [&](const auto & type_predicted, const auto & type_labels)
                        {
                            using TypePredicted = typename std::decay_t<decltype(type_predicted)>::FieldType;
                            const ColumnVector<TypePredicted> * array_predicted_data = checkAndGetColumn<ColumnVector<TypePredicted>>(array_predicted->getDataPtr().get());

                            using TypeLabels = typename std::decay_t<decltype(type_labels)>::FieldType;
                            const ColumnVector<TypeLabels> * col_labels_data = checkAndGetColumn<ColumnVector<TypeLabels>>(array_labels->getDataPtr().get());

                            auto col_gini_predicted = ColumnFloat64::create(input_rows_count);
                            auto col_gini_labels = ColumnFloat64::create(input_rows_count);
                            auto col_gini_normalized = ColumnFloat64::create(input_rows_count);

                            Impl::vectorVector(
                                array_predicted_data->getData(),
                                array_predicted_offsets,
                                col_labels_data->getData(),
                                array_label_offsets,
                                col_gini_predicted->getData(),
                                col_gini_labels->getData(),
                                col_gini_normalized->getData());

                            result[0] = std::move(col_gini_predicted);
                            result[1] = std::move(col_gini_labels);
                            result[2] = std::move(col_gini_normalized);

                            return true;
                        }))
                {
                    return ColumnTuple::create(result);
                }
            }
        }
        else if (const ColumnConst * array_predicted_const = checkAndGetColumn<ColumnConst>(col_predicted.get()))
        {
            /// Note that const-const case is handled by useDefaultImplementationForConstants.

            const ColumnArray * column_array_const = checkAndGetColumn<ColumnArray>(array_predicted_const->getDataColumnPtr().get());
            const auto & array_predicted_type = typeid_cast<const DataTypeArray *>(arguments[0].type.get())->getNestedType();

            const ColumnArray * array_labels = checkAndGetColumn<ColumnArray>(col_labels.get());
            const auto & array_label_offsets = array_labels->getOffsets();
            const auto & array_labels_type = typeid_cast<const DataTypeArray *>(arguments[1].type.get())->getNestedType();

            if (castBothTypes(
                    array_predicted_type.get(),
                    array_labels_type.get(),
                    [&](const auto & type_predicted, const auto & type_labels)
                    {
                        using TypePredicted = typename std::decay_t<decltype(type_predicted)>::FieldType;
                        const ColumnVector<TypePredicted> * array_predicted_data = checkAndGetColumn<ColumnVector<TypePredicted>>(column_array_const->getDataPtr().get());

                        using TypeLabels = typename std::decay_t<decltype(type_labels)>::FieldType;
                        const ColumnVector<TypeLabels> * col_labels_data = checkAndGetColumn<ColumnVector<TypeLabels>>(array_labels->getDataPtr().get());

                        auto col_gini_predicted = ColumnFloat64::create(input_rows_count);
                        auto col_gini_labels = ColumnFloat64::create(input_rows_count);
                        auto col_gini_normalized = ColumnFloat64::create(input_rows_count);

                        Impl::constVector(
                            array_predicted_data->getData(),
                            col_labels_data->getData(),
                            array_label_offsets,
                            col_gini_predicted->getData(),
                            col_gini_labels->getData(),
                            col_gini_normalized->getData());

                        result[0] = std::move(col_gini_predicted);
                        result[1] = std::move(col_gini_labels);
                        result[2] = std::move(col_gini_normalized);

                        return true;
                    }))
            {
                return ColumnTuple::create(result);
            }
        }

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column of argument of function {}", getName());
    }

private:
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

};

REGISTER_FUNCTION(NormalizedGini)
{
    FunctionDocumentation::Description doc_description = "Calculates the normalized Gini coefficient.";
    FunctionDocumentation::Syntax doc_syntax = "arrayNormalizedGini(predicted, label)";
    FunctionDocumentation::Arguments doc_arguments = {
        {"predicted", "The predicted value.", {"Array(T)"}},
        {"label", "The actual value.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue doc_returned_value = {"A tuple containing the Gini coefficients of the predicted values, the Gini coefficient of the normalized values, and the normalized Gini coefficient (= the ratio of the former two Gini coefficients)", {"Tuple(Float64, Float64, Float64)"}};
    FunctionDocumentation::Examples doc_examples = {
        {"Usage example", "SELECT arrayNormalizedGini([0.9, 0.3, 0.8, 0.7],[6, 1, 0, 2]);", "(0.18055555555555558, 0.2638888888888889, 0.6842105263157896)"}
    };
    FunctionDocumentation::IntroducedIn doc_introduced_in = {25, 1};
    FunctionDocumentation::Category doc_category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {doc_description, doc_syntax, doc_arguments, doc_returned_value, doc_examples, doc_introduced_in, doc_category};

    factory.registerFunction<FunctionArrayNormalizedGini>(documentation, FunctionFactory::Case::Sensitive);
}

}
