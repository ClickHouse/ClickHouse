#pragma once

#include <cmath>

#include <Core/FieldVisitors.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/IFunction.h>
#include <Common/Arena.h>
#include <common/StringRef.h>
#include <Common/HashTable/HashMap.h>
#include <DataTypes/EnrichedDataTypePtr.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

enum ClusterOperation
{
    FindClusterIndex   = 0,
    FindCentroidValue  = 1
};

/// Centroids of the clusters to match, as well as the cluster finding logic.
///
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
template <typename CentroidsType>
class Centroids
{
public:
    bool fill(const IColumn* centroids_array_untyped)
     {
        const ColumnArray * centroids_array = typeid_cast<const ColumnArray *>(centroids_array_untyped);

        if (centroids_array)
        {
            if (centroids_array->empty())
                throw Exception{"Centroids array must be not empty", ErrorCodes::ILLEGAL_COLUMN};

            for (size_t k = 0; k < centroids_array->size(); k++)
            {
                const Field& tmp_field = (*centroids_array)[k];
                CentroidsType value;
                if (!tmp_field.tryGet(value))
                    return false;
                centroids.push_back(Float64(value));
            }
        }
        else
        {
            const ColumnConst<Array> * const_centroids_array = typeid_cast<const ColumnConst<Array> *>(centroids_array_untyped);

            if (!const_centroids_array)
                return false;

            if (const_centroids_array->getData().empty())
                throw Exception{"Centroids array must be not empty", ErrorCodes::ILLEGAL_COLUMN};

            for (size_t k = 0; k < const_centroids_array->getData().size(); ++k)
            {
                const Field& tmp_field = (const_centroids_array->getData())[k];
                CentroidsType value;
                if (!tmp_field.tryGet(value))
                    return false;
                centroids.push_back(Float64(value));
            }
        }
        return true;
    }


    template <typename InputType>
    bool findCluster(
        const IColumn* in_untyped,
        IColumn* out_untyped,
        ClusterOperation operation)
    {
        if (operation == ClusterOperation::FindClusterIndex)
            return findClusterTyped<InputType, UInt64>(in_untyped, out_untyped, operation);
        else if (operation == ClusterOperation::FindCentroidValue)
            return findClusterTyped<InputType, CentroidsType>(in_untyped, out_untyped, operation);

        throw Exception{"Unexpected error in findCluster* function", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }

private:
    std::vector<Float64> centroids;

    // Centroids array has the same size as number of clusters. We expect it
    // to be small, maybe 10s or 100s in most real life situation, so we
    // choose the naive implementation
    size_t find_centroid(CentroidsType x)
    {
        Float64 y = Float64(x);

        // Centroids array has to have at least one element, and if it has only one element,
        // it is also the result of this Function.
        Float64 distance = abs(centroids[0]-y);
        size_t index = 0;

        // Check if we have more clusters and if we have, whether some is closer to src[i]
        for (size_t j = 1; j < centroids.size(); ++j)
        {
            Float64 next_distance = abs(centroids[j]-y);

            if (next_distance < distance)
            {
                distance = next_distance;
                index = j;
            }
        }

        // Index of the closest cluster, or 0 in case of just one cluster
        return index;
    }

    template <typename InputType, typename OutputType>
    bool findClusterTyped(
            const IColumn* in_untyped,
            IColumn* out_untyped,
            ClusterOperation operation)
    {
        ColumnVector<OutputType> * out = typeid_cast<ColumnVector<OutputType> *>(out_untyped);

        if (!out)
            return false;

        PaddedPODArray<OutputType> & dst = out->getData();


        const auto in_vector = typeid_cast<const ColumnVector<InputType> *>(in_untyped);
        if (in_vector)
        {
            const PaddedPODArray<InputType> & src = in_vector->getData();

            if (operation == ClusterOperation::FindClusterIndex)
                for (size_t i = 0; i < src.size(); ++i)
                    // Note that array indexes start with 1 in Clickhouse
                    dst.push_back(UInt64(find_centroid(CentroidsType(src[i]))+1));
            else if (operation == ClusterOperation::FindCentroidValue)
                for (size_t i = 0; i < src.size(); ++i)
                    dst.push_back(centroids[find_centroid(CentroidsType(src[i]))]);
            else
                throw Exception{"Unexpected error in findCluster* function", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

            return true;
        }
        else
        {
            const auto in_const = typeid_cast<const ColumnConst<InputType> *>(in_untyped);

            if (!in_const)
                return false;

            if (operation == ClusterOperation::FindClusterIndex)
                // Note that array indexes start with 1 in Clickhouse
                dst.push_back(UInt64(find_centroid(CentroidsType(in_const->getData()))+1));
            else if (operation == ClusterOperation::FindCentroidValue)
                dst.push_back(centroids[find_centroid(CentroidsType(in_const->getData()))]);
            else
                throw Exception{"Unexpected error in findCluster* function", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return true;
    }
};


/** findClusterIndex(x, centroids_array) - find index of element in centroids_array with the value nearest to x
  * findClusterValue(x, centroids_array) - find value of element in centroids_array with the value nearest to x
  *
  * Types:
  * findClusterIndex(T, Array(T)) -> UInt64
  * findClusterValue(T, Array(T)) -> T
  *
  * T can be any numeric type.
  *
  */
class FunctionFindClusterIndex : public IFunction
{
public:
    static constexpr auto name = "findClusterIndex";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionFindClusterIndex>(); }

    String getName() const override
    {
        return FunctionFindClusterIndex::name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }


    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto args_size = arguments.size();
        if (args_size != 2)
            throw Exception{
                "Number of arguments for function " + getName() + " doesn't match: passed " +
                    toString(args_size) + ", should be 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        const auto type_x = arguments[0];

        if (!type_x->isNumeric())
            throw Exception{"Unsupported type " + type_x->getName()
                + " of first argument of function " + getName()
                + ", must be a numeric type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        const DataTypeArray * type_arr_from = typeid_cast<const DataTypeArray *>(arguments[1].get());

        if (!type_arr_from)
            throw Exception{"Second argument of function " + getName()
                + ", must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeUInt64>();
    }


    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        const auto in_untyped = block.safeGetByPosition(arguments[0]).column.get();
        const auto centroids_array_untyped = block.safeGetByPosition(arguments[1]).column.get();
        auto column_result = block.safeGetByPosition(result).type->createColumn();
        auto out_untyped = column_result.get();

        if (    !executeByCentroidsType<UInt8>(in_untyped, out_untyped, centroids_array_untyped)
            &&     !executeByCentroidsType<UInt16>(in_untyped, out_untyped, centroids_array_untyped)
            &&     !executeByCentroidsType<UInt32>(in_untyped, out_untyped, centroids_array_untyped)
            &&     !executeByCentroidsType<UInt64>(in_untyped, out_untyped, centroids_array_untyped)
            &&     !executeByCentroidsType<Int8>(in_untyped, out_untyped, centroids_array_untyped)
            &&     !executeByCentroidsType<Int16>(in_untyped, out_untyped, centroids_array_untyped)
            &&     !executeByCentroidsType<Int32>(in_untyped, out_untyped, centroids_array_untyped)
            &&     !executeByCentroidsType<Int64>(in_untyped, out_untyped, centroids_array_untyped)
            &&     !executeByCentroidsType<Float32>(in_untyped, out_untyped, centroids_array_untyped)
            &&     !executeByCentroidsType<Float64>(in_untyped, out_untyped, centroids_array_untyped)
            )
        {
            throw Exception{
                "Function " + getName() + " expects centroids_array of a numeric type",
                ErrorCodes::ILLEGAL_COLUMN};
        }

        block.safeGetByPosition(result).column = column_result;
    }

protected:
    virtual ClusterOperation getOperation()
    {
        return ClusterOperation::FindClusterIndex;
    }

    template <typename CentroidsType>
    bool executeByCentroidsType(
            const IColumn* in_untyped,
            IColumn* out_untyped,
            const IColumn* centroids_array_untyped)
    {
        Centroids<typename NearestFieldType<CentroidsType>::Type> centroids;

        if (!centroids.fill(centroids_array_untyped))
            return false;

        if (    !centroids.template findCluster<UInt8>(in_untyped, out_untyped, getOperation())
            &&     !centroids.template findCluster<UInt16>(in_untyped, out_untyped, getOperation())
            &&     !centroids.template findCluster<UInt32>(in_untyped, out_untyped, getOperation())
            &&     !centroids.template findCluster<UInt64>(in_untyped, out_untyped, getOperation())
            &&     !centroids.template findCluster<Int8>(in_untyped, out_untyped, getOperation())
            &&     !centroids.template findCluster<Int16>(in_untyped, out_untyped, getOperation())
            &&     !centroids.template findCluster<Int32>(in_untyped, out_untyped, getOperation())
            &&     !centroids.template findCluster<Int64>(in_untyped, out_untyped, getOperation())
            &&     !centroids.template findCluster<Float32>(in_untyped, out_untyped, getOperation())
            &&     !centroids.template findCluster<Float64>(in_untyped, out_untyped, getOperation())
            )
        {
            throw Exception{
                "Illegal column " + in_untyped->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};
        }

        return true;
    }

};


class FunctionFindClusterValue : public FunctionFindClusterIndex
{
public:
    static constexpr auto name = "findClusterValue";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionFindClusterValue>(); }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        FunctionFindClusterIndex::getReturnTypeImpl(arguments);
        const DataTypeArray * type_arr_from = typeid_cast<const DataTypeArray *>(arguments[1].get());
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
};

}
