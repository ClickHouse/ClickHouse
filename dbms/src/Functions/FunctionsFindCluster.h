#pragma once

#include <cmath>

#include <Common/FieldVisitors.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>

#include <Common/Arena.h>
#include <Common/typeid_cast.h>
#include <common/StringRef.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

enum ClusterOperation
{
    FindClusterIndex = 0,
    FindCentroidValue = 1
};

/// The centroid values are converted to Float64 for easier coding of
/// distance calculations.
///
/// We assume to have 10th to 100th centroids, usually of type Float64, as a typical use case.
/// While it is possible to sort centroids and use a modification of a binary search to find the
/// nearest centroid, we think for arrays of 10th to 100th this might be an overkill.
///
/// Also, even though centroids of other types are feasible, this first implementation
/// lacks support of them for simplicity. Date, DateTime and Strings (eg. with the
/// Levenshtein distance) could be theoretically supported, as well as custom distance
/// functions (eg. Hamming distance) using Clickhouse lambdas.

// Centroids array has the same size as number of clusters.
size_t find_centroid(Float64 x, std::vector<Float64> & centroids)
{
    // Centroids array has to have at least one element, and if it has only one element,
    // it is also the result of this Function.
    Float64 distance = std::abs(centroids[0] - x);
    size_t index = 0;

    // Check if we have more clusters and if we have, whether some is closer to src[i]
    for (size_t j = 1; j < centroids.size(); ++j)
    {
        Float64 next_distance = std::abs(centroids[j] - x);

        if (next_distance < distance)
        {
            distance = next_distance;
            index = j;
        }
    }

    // Index of the closest cluster, or 0 in case of just one cluster
    return index;
}

/** findClusterIndex(x, centroids_array) - find index of element in centroids_array with the value nearest to x
 * findClusterValue(x, centroids_array) - find value of element in centroids_array with the value nearest to x
 *
 * Types:
 * findClusterIndex(T, Array(T)) -> UInt64
 * findClusterValue(T, Array(T)) -> T
 *
 * T can be any numeric type.
 * centroids_array must be constant
 */
class FunctionFindClusterIndex : public IFunction
{
public:
    static constexpr auto name = "findClusterIndex";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionFindClusterIndex>();
    }

    String getName() const override
    {
        return FunctionFindClusterIndex::name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto args_size = arguments.size();
        if (args_size != 2)
            throw Exception{"Number of arguments for function " + getName() + " doesn't match: passed " + toString(args_size) + ", should be 2",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        const auto type_x = arguments[0];

        if (!type_x->isNumber())
            throw Exception{"Unsupported type " + type_x->getName() + " of first argument of function " + getName() + " must be a numeric type",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        const DataTypeArray * type_arr_from = checkAndGetDataType<DataTypeArray>(arguments[1].get());

        if (!type_arr_from)
            throw Exception{"Second argument of function " + getName() + " must be literal array", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        const auto in_untyped = block.getByPosition(arguments[0]).column.get();
        const auto centroids_array_untyped = block.getByPosition(arguments[1]).column.get();
        auto column_result = block.getByPosition(result).type->createColumn();
        auto out_untyped = column_result.get();

        if (!centroids_array_untyped->isColumnConst())
            throw Exception{"Second argument of function " + getName() + " must be literal array", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        executeImplTyped(in_untyped, out_untyped, centroids_array_untyped);

        block.getByPosition(result).column = std::move(column_result);
    }

protected:
    virtual ClusterOperation getOperation()
    {
        return ClusterOperation::FindClusterIndex;
    }

    virtual void executeImplTyped(const IColumn* in_untyped, IColumn* out_untyped, const IColumn* centroids_array_untyped)
    {
        if (!executeOperation<UInt8, UInt64>(in_untyped, out_untyped, centroids_array_untyped)
                && !executeOperation<UInt16, UInt64>(in_untyped, out_untyped, centroids_array_untyped)
                && !executeOperation<UInt32, UInt64>(in_untyped, out_untyped, centroids_array_untyped)
                && !executeOperation<UInt64, UInt64>(in_untyped, out_untyped, centroids_array_untyped)
                && !executeOperation<Int8, UInt64>(in_untyped, out_untyped, centroids_array_untyped)
                && !executeOperation<Int16, UInt64>(in_untyped, out_untyped, centroids_array_untyped)
                && !executeOperation<Int32, UInt64>(in_untyped, out_untyped, centroids_array_untyped)
                && !executeOperation<Int64, UInt64>(in_untyped, out_untyped, centroids_array_untyped)
                && !executeOperation<Float32, UInt64>(in_untyped, out_untyped, centroids_array_untyped)
                && !executeOperation<Float64, UInt64>(in_untyped, out_untyped, centroids_array_untyped))
        {
            throw Exception{"Function " + getName() + " expects both x and centroids_array of a numeric type."
                    " Passed arguments are " + in_untyped->getName() + " and " + centroids_array_untyped->getName(), ErrorCodes::ILLEGAL_COLUMN};

        }
    }

    // Match the type of the centrods array and convert them to Float64, because we
    // don't want to have problems calculating negative distances of UInts
    template <typename CentroidsType>
    bool fillCentroids(const IColumn * centroids_array_untyped, std::vector<Float64> & centroids)
    {
        const ColumnConst * const_centroids_array = checkAndGetColumnConst<ColumnVector<Array>>(centroids_array_untyped);

        if (!const_centroids_array)
            return false;

        Array array = const_centroids_array->getValue<Array>();
        if (array.empty())
            throw Exception{"Centroids array must be not empty", ErrorCodes::ILLEGAL_COLUMN};

        for (size_t k = 0; k < array.size(); ++k)
        {
            const Field & tmp_field = array[k];
            typename NearestFieldType<CentroidsType>::Type value;
            if (!tmp_field.tryGet(value))
                return false;

            centroids.push_back(Float64(value));
        }
        return true;
    }

    template <typename CentroidsType, typename OutputType>
    bool executeOperation(const IColumn * in_untyped, IColumn * out_untyped, const IColumn * centroids_array_untyped)
    {
        // Match the type of the output
        auto out = typeid_cast<ColumnVector<OutputType> *>(out_untyped);

        if (!out)
            return false;

        PaddedPODArray<OutputType> & dst = out->getData();

        // try to match the type of the input column
        if (!executeOperationTyped<UInt8, OutputType, CentroidsType>(in_untyped, dst, centroids_array_untyped)
            && !executeOperationTyped<UInt16, OutputType, CentroidsType>(in_untyped, dst, centroids_array_untyped)
            && !executeOperationTyped<UInt32, OutputType, CentroidsType>(in_untyped, dst, centroids_array_untyped)
            && !executeOperationTyped<UInt64, OutputType, CentroidsType>(in_untyped, dst, centroids_array_untyped)
            && !executeOperationTyped<Int8, OutputType, CentroidsType>(in_untyped, dst, centroids_array_untyped)
            && !executeOperationTyped<Int16, OutputType, CentroidsType>(in_untyped, dst, centroids_array_untyped)
            && !executeOperationTyped<Int32, OutputType, CentroidsType>(in_untyped, dst, centroids_array_untyped)
            && !executeOperationTyped<Int64, OutputType, CentroidsType>(in_untyped, dst, centroids_array_untyped)
            && !executeOperationTyped<Float32, OutputType, CentroidsType>(in_untyped, dst, centroids_array_untyped)
            && !executeOperationTyped<Float64, OutputType, CentroidsType>(in_untyped, dst, centroids_array_untyped))
        {
            return false;
        }

        return true;
    }

    template <typename InputType, typename OutputType, typename CentroidsType>
    bool executeOperationTyped(const IColumn * in_untyped, PaddedPODArray<OutputType> & dst, const IColumn * centroids_array_untyped)
    {
        const auto maybe_const = in_untyped->convertToFullColumnIfConst();
        if (maybe_const)
            in_untyped = maybe_const.get();

        const auto in_vector = checkAndGetColumn<ColumnVector<InputType>>(in_untyped);
        if (in_vector)
        {
            const PaddedPODArray<InputType> & src = in_vector->getData();

            std::vector<Float64> centroids;
            if (!fillCentroids<CentroidsType>(centroids_array_untyped, centroids))
                return false;

            for (size_t i = 0; i < src.size(); ++i)
            {
                size_t index = find_centroid(Float64(src[i]), centroids);
                if (getOperation() == ClusterOperation::FindClusterIndex)
                    // Note that array indexes start with 1 in Clickhouse
                    dst.push_back(UInt64(index + 1));
                else if (getOperation() == ClusterOperation::FindCentroidValue)
                    dst.push_back(centroids[index]);
                else
                    throw Exception{"Unexpected error in findCluster* function", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
            }

            return true;
        }
        return false;
    }

};

class FunctionFindClusterValue : public FunctionFindClusterIndex
{
public:
    static constexpr auto name = "findClusterValue";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionFindClusterValue>();
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        FunctionFindClusterIndex::getReturnTypeImpl(arguments);
        const DataTypeArray * type_arr_from = checkAndGetDataType<DataTypeArray>(arguments[1].get());
        return type_arr_from->getNestedType();
    }

    String getName() const override
    {
        return FunctionFindClusterValue::name;
    }

protected:
    ClusterOperation getOperation() override
    {
        return ClusterOperation::FindCentroidValue;
    }

    void executeImplTyped(const IColumn* in_untyped, IColumn* out_untyped, const IColumn* centroids_array_untyped) override
    {
        if (!executeOperation<UInt8, UInt8>(in_untyped, out_untyped, centroids_array_untyped)
            && !executeOperation<UInt16, UInt16>(in_untyped, out_untyped, centroids_array_untyped)
            && !executeOperation<UInt32, UInt32>(in_untyped, out_untyped, centroids_array_untyped)
            && !executeOperation<UInt64, UInt64>(in_untyped, out_untyped, centroids_array_untyped)
            && !executeOperation<Int8, Int8>(in_untyped, out_untyped, centroids_array_untyped)
            && !executeOperation<Int16, Int16>(in_untyped, out_untyped, centroids_array_untyped)
            && !executeOperation<Int32, Int32>(in_untyped, out_untyped, centroids_array_untyped)
            && !executeOperation<Int64, Int64>(in_untyped, out_untyped, centroids_array_untyped)
            && !executeOperation<Float32, Float32>(in_untyped, out_untyped, centroids_array_untyped)
            && !executeOperation<Float64, Float64>(in_untyped, out_untyped, centroids_array_untyped))
        {
            throw Exception{"Function " + getName() + " expects both x and centroids_array of a numeric type."
                    "Passed arguments are " + in_untyped->getName() + " and " + centroids_array_untyped->getName(), ErrorCodes::ILLEGAL_COLUMN};
        }
    }
};

}
