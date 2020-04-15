#pragma once

#include <type_traits>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <Functions/FunctionHelpers.h>
#include <Common/FieldVisitors.h>
#include <Interpreters/castColumn.h>

#include <cmath>
#include <exception>
#include <algorithm>
#include <numeric>
#include <random>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

using Point = std::vector<Float64>;
using Points = std::vector<Point>;

class KMeansCluster
{
public:
    KMeansCluster() = default;
    KMeansCluster(const KMeansCluster& other) = default;

    explicit KMeansCluster(Point coordinates_) : coordinates(std::move(coordinates_)), num_points(1) {}

    void swap(KMeansCluster& other)
    {
        std::swap(coordinates, other.coordinates);
        std::swap(num_points, other.num_points);
    }

    KMeansCluster(const IColumn ** columns, size_t row_num, size_t dimensions) : num_points(1)
    {
        coordinates.resize(dimensions);

        for (size_t i = 0; i < dimensions; ++i)
            coordinates[i] = columns[i]->getFloat64(row_num);
    }

    void appendPoint(const IColumn ** columns, size_t row_num)
    {
        if (num_points == 0)
            throw Exception("Append to empty cluster", ErrorCodes::LOGICAL_ERROR);

        Float64 point_weight = appendPointWeight(++num_points);
        Float64 cluster_weight = 1 - point_weight;

        for (size_t i = 0; i < coordinates.size(); ++i)
            coordinates[i] = cluster_weight * coordinates[i] + point_weight * columns[i]->getFloat64(row_num);
    }

    void appendPointFromVector(const Point & new_coordinates)
    {
        if (num_points == 0)
            throw Exception("Append to empty cluster", ErrorCodes::LOGICAL_ERROR);

        if (coordinates.size() != new_coordinates.size())
            throw Exception("Cannot append point of other size", ErrorCodes::LOGICAL_ERROR);

        Float64 point_weight = appendPointWeight(++num_points);
        Float64 cluster_weight = 1 - point_weight;

        for (size_t i = 0; i < coordinates.size(); ++i)
            coordinates[i] = cluster_weight * coordinates[i] + point_weight * new_coordinates[i];
    }

    void mergeCluster(const KMeansCluster & other)
    {
        if (size() == 0 || other.size() == 0)
            throw Exception("Merge empty cluster", ErrorCodes::LOGICAL_ERROR);

        Float64 this_weight = mergeWeight(size(), other.size());
        Float64 other_weight = 1 - this_weight;

        for (size_t i = 0; i < coordinates.size(); ++i)
            coordinates[i] = this_weight * coordinates[i] + other_weight * other.coordinates[i];

        num_points += other.num_points;
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(coordinates, buf);
        writeBinary(num_points, buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinary(coordinates, buf);
        readBinary(num_points, buf);
    }

    UInt32 size() const
    {
        return num_points;
    }

    const Point & center() const
    {
        return coordinates;
    }

private:
    static Float64 appendPointWeight(UInt32 size)
    {
        return Float64{1.0} / size;
    }

    static Float64 mergeWeight(UInt32 size1, UInt32 size2)
    {
        return static_cast<Float64>(size1) / (size1 + size2);
    }

    Point coordinates;
    UInt32 num_points = 0;
};

using KMeansClusters = std::vector<KMeansCluster>;

}

class ClusteringData
{
public:
    ClusteringData() = delete;

    ClusteringData(UInt32 clusters_num_, UInt32 dimensions_) : num_clusters(clusters_num_), num_dimensions(dimensions_)
    {
        if (num_clusters == 0)
            throw Exception("Number of cluster must be greater than zero", ErrorCodes::LOGICAL_ERROR);

        clusters.reserve(multiplication_factor * num_clusters);
        predicted_clusters.reserve(num_clusters);
    }

    void add(const IColumn ** columns, size_t row_num)
    {
        predicted_clusters.clear();

        /// Add point as new cluster if there is not enough clusters.
        if (clusters.size() < multiplication_factor * num_clusters)
        {
            clusters.emplace_back(columns, row_num, num_dimensions);
            return;
        }

        size_t closest_cluster = findNearestClusterForPointWithPenalty(columns, row_num, clusters);
        clusters[closest_cluster].appendPoint(columns, row_num);
    }

    void addFromVector(const Point & coordinates)
    {
        predicted_clusters.clear();
        if (clusters.size() < multiplication_factor * num_clusters)
        {
            clusters.emplace_back(coordinates);
            return;
        }

        size_t closest_cluster = findNearestClusterForPointWithPenalty(coordinates, clusters);
        clusters[closest_cluster].appendPointFromVector(coordinates);
    }

    void merge(const ClusteringData & rhs)
    {
        predicted_clusters.clear();
        /// If one of cluster sets are not filled in, put its points to another cluster set
        if (rhs.clusters.size() < rhs.multiplication_factor * rhs.num_clusters)
        {
            for (auto & cluster : rhs.clusters)
                addFromVector(cluster.center());

            return;
        }

        if (clusters.size() < multiplication_factor * num_clusters)
        {
            KMeansClusters old_clusters = std::move(clusters);
            clusters = rhs.clusters;

            for (auto & old_cluster : old_clusters)
                addFromVector(old_cluster.center());

            return;
        }

        /// Merge closest pairs of clusters greedily
        for (size_t i = 0; i < rhs.clusters.size(); ++i)
        {
            size_t closest_cluster = findNearestCluster(rhs.clusters[i].center(), clusters, i);
            std::swap(clusters[closest_cluster], clusters[i]);
            clusters[i].mergeCluster(rhs.clusters[i]);
        }
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(num_clusters, buf);
        writeBinary(num_dimensions, buf);

        UInt64 clusters_size = clusters.size();
        writeBinary(clusters_size, buf);

        for (auto & cluster : clusters)
            cluster.write(buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinary(num_clusters, buf);
        readBinary(num_dimensions, buf);

        UInt64 clusters_size;
        readBinary(clusters_size, buf);
        clusters.resize(clusters_size);

        for (auto & cluster : clusters)
            cluster.read(buf);
    }

    // TODO consider adding new points (from prediction) into clusters
    void predict(ColumnVector<UInt32>::Container & container,
                 Block & block,
                 size_t offset,
                 size_t limit,
                 const ColumnNumbers & arguments) const
    {
        if (num_dimensions + 1 != arguments.size())
            throw Exception("In predict function number of arguments differs from the size of weights vector",
                            ErrorCodes::LOGICAL_ERROR);

        size_t num_rows = block.rows();

        if (offset > num_rows || offset + limit > num_rows)
            throw Exception("Invalid offset and limit for incrementalClustering::predict. "
                            "Block has " + toString(num_rows) + " rows, but offset is " + toString(offset) +
                            " and limit is " + toString(limit), ErrorCodes::LOGICAL_ERROR);

        makePrediction();

        Points distances(
            limit,
            Point(predicted_clusters.size(), 0)
        );

        for (size_t i = 0; i < num_dimensions; ++i)
        {
            const ColumnWithTypeAndName & cur_col = block.getByPosition(arguments[i + 1]);
            if (!isNativeNumber(cur_col.type))
                throw Exception("Prediction arguments must have numeric type", ErrorCodes::BAD_ARGUMENTS);

            auto features_column = cur_col.column;
            if (!features_column)
                throw Exception("Unexpectedly cannot dynamically cast features column " + std::to_string(i),
                                ErrorCodes::LOGICAL_ERROR);

            for (size_t row = 0; row < limit; ++row)
            {
                Float64 value = features_column->getFloat64(offset + row);
                for (size_t cluster_idx = 0; cluster_idx < predicted_clusters.size(); ++cluster_idx)
                {
                    Float64 diff = predicted_clusters[cluster_idx][i] - value;
                    distances[row][cluster_idx] += diff * diff;
                }
            }
        }

        container.reserve(container.size() + limit);
        for (size_t row = 0; row < limit; ++row)
        {
            size_t closest_cluster = 0;

            for (size_t cluster_idx = 1; cluster_idx < distances[row].size(); ++cluster_idx)
                if (distances[row][cluster_idx] < distances[row][closest_cluster])
                    closest_cluster = cluster_idx;

            container.emplace_back(closest_cluster);
        }
    }

    void returnClusters(IColumn & to) const
    {
        makePrediction();

        size_t size = num_clusters * num_dimensions;

        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        size_t old_size = offsets_to.back();
        offsets_to.push_back(old_size + size);

        typename ColumnFloat64::Container & val_to
                = static_cast<ColumnFloat64 &>(arr_to.getData()).getData();

        val_to.reserve(old_size + size);
        for (size_t cluster_idx = 0; cluster_idx != num_clusters; ++cluster_idx)
            for (size_t i = 0; i != num_dimensions; ++i)
                val_to.push_back(predicted_clusters[cluster_idx][i]);
    }

private:

    static Float64 computeDistanceBetweenPoints(const Point & point1, const Point & point2)
    {
        Float64 distance = 0;
        for (size_t i = 0; i != point1.size(); ++i)
        {
            Float64 coordinate_diff = point1[i] - point2[i];
            distance += coordinate_diff * coordinate_diff;
        }
        return std::sqrt(distance);
    }

    static Float64 computeDistanceBetweenPoints(const Point & point, const IColumn ** columns, size_t row_num)
    {
        Float64 distance = 0;
        for (size_t i = 0; i != point.size(); ++i)
        {
            Float64 coordinate_diff = point[i] - columns[i]->getFloat64(row_num);
            distance += coordinate_diff * coordinate_diff;
        }
        return std::sqrt(distance);
    }

    static size_t findNearestClusterForPointWithPenalty(
        const IColumn ** columns, size_t row_num, const KMeansClusters & clusters)
    {
        Float64 min_distance = 0;
        UInt32 closest_point = 0;

        for (size_t i = 0; i < clusters.size(); ++i)
        {
            Float64 distance = getClusterPenalty(clusters[i].size()) *
                               computeDistanceBetweenPoints(clusters[i].center(), columns, row_num);

            if (i == 0 || distance < min_distance)
            {
                min_distance = distance;
                closest_point = i;
            }
        }

        return closest_point;
    }

    static size_t findNearestClusterForPointWithPenalty(const Point & point, const KMeansClusters & clusters)
    {
        Float64 min_distance = 0;
        UInt32 closest_point = 0;

        for (size_t i = 0; i < clusters.size(); ++i)
        {
            Float64 distance = getClusterPenalty(clusters[i].size()) *
                               computeDistanceBetweenPoints(clusters[i].center(), point);

            if (i == 0 || distance < min_distance)
            {
                min_distance = distance;
                closest_point = i;
            }
        }
        return closest_point;
    }

    static size_t findNearestCluster(const Point & point, const Points & clusters)
    {
        Float64 min_distance = 0;
        UInt32 closest_point = 0;

        for (size_t i = 0; i < clusters.size(); ++i)
        {
            Float64 distance = computeDistanceBetweenPoints(point, clusters[i]);
            if (distance < min_distance)
            {
                min_distance = distance;
                closest_point = i;
            }
        }
        return closest_point;
    }

    static size_t findNearestCluster(const Point & point, const KMeansClusters & clusters, size_t from = 0)
    {
        Float64 min_distance = 0;
        UInt32 closest_point = 0;

        for (size_t i = from ; i < clusters.size(); ++i)
        {
            Float64 distance = computeDistanceBetweenPoints(point, clusters[i].center());
            if (i == from || distance < min_distance)
            {
                min_distance = distance;
                closest_point = i;
            }
        }
        return closest_point;
    }

    static Float64 getClusterPenalty(UInt32 cluster_size)
    {
        return std::sqrt(cluster_size);
    }

    static Float64 minSquaredDistance(const Point & point, const Points & clusters)
    {
        Float64 min_squared_dist = 0;

        for (size_t cluster_idx = 0; cluster_idx < clusters.size(); ++cluster_idx)
        {
            Float64 squared_dist = 0;
            for (size_t i = 0; i < point.size(); ++i)
            {
                Float64 distance = point[i] - clusters[cluster_idx][i];
                squared_dist += distance * distance;
            }

            if (cluster_idx == 0)
                min_squared_dist = squared_dist;
            else
                min_squared_dist = std::min(min_squared_dist, squared_dist);
        }

        return min_squared_dist;
    }

    void kMeansInitializeClusters() const
    {
        /// Pick first cluster from the first point.
        size_t first_cluster = 0;
        predicted_clusters.reserve(num_clusters);
        predicted_clusters.emplace_back(clusters[first_cluster].center());

        for (size_t i = 1; i < num_clusters; ++i)
        {
            /// Simply pick the point that has the maximum squared distance from it's nearest cluster
            Float64 max_min_squared_dist = 0;
            size_t farthest_point = 0;
            for (size_t point_idx = 0; point_idx < clusters.size(); ++point_idx)
            {
                Float64 min_squared_dist = minSquaredDistance(clusters[point_idx].center(), predicted_clusters);
                if (min_squared_dist >= max_min_squared_dist)
                {
                    max_min_squared_dist = min_squared_dist;
                    farthest_point = point_idx;
                }
            }
            predicted_clusters.emplace_back(clusters[farthest_point].center());
        }
    }

    /// Returns pairwise squared distance between new and old clusters
    Float64 kMeansUpdateClusters(const std::vector<size_t> & closest_cluster) const
    {
        Float64 diff = 0;
        Points new_clusters(
            predicted_clusters.size(),
            Point(predicted_clusters[0].size(), 0)
        );
        std::vector<UInt64> cluster_weights(predicted_clusters.size());
        for (size_t point_idx = 0; point_idx < clusters.size(); ++point_idx)
        {
            size_t cluster_idx = closest_cluster[point_idx];
            ++cluster_weights[cluster_idx];
            for (size_t i = 0; i < new_clusters[cluster_idx].size(); ++i)
                new_clusters[cluster_idx][i] += clusters[point_idx].center()[i];
        }
        for (size_t cluster_idx = 0; cluster_idx < new_clusters.size(); ++cluster_idx)
        {
            for (size_t i = 0; i < new_clusters[cluster_idx].size(); ++i)
            {
                new_clusters[cluster_idx][i] /= cluster_weights[cluster_idx];
                Float64 d = new_clusters[cluster_idx][i] - predicted_clusters[cluster_idx][i];
                diff += d * d;
            }
        }
        predicted_clusters = std::move(new_clusters);

        return diff;
    }

    Float64 kMeans() const
    {
        predicted_clusters.clear();

        kMeansInitializeClusters();

        std::vector<size_t> closest_cluster(clusters.size());
        for (size_t iter = 0; iter < kmeans_max_iter; ++iter)
        {
            /// For each point find the cluster with closest center
            for (size_t point_idx = 0; point_idx < clusters.size(); ++point_idx)
                closest_cluster[point_idx] = findNearestCluster(clusters[point_idx].center(), predicted_clusters);

            /// Change predicted cluster centers
            Float64 diff = kMeansUpdateClusters(closest_cluster);
            /// Stop if diff is too small
            if (diff < eps_diff_)
                break;
        }

        Float64 mean_distance = 0;
        for (size_t point_idx = 0; point_idx < clusters.size(); ++point_idx)
            mean_distance += computeDistanceBetweenPoints(predicted_clusters[closest_cluster[point_idx]],
                                                          clusters[point_idx].center());
        if (!clusters.empty())
            mean_distance /= clusters.size();

        return mean_distance;
    }

    void makePrediction() const
    {
        if (predicted_clusters.empty())
            kMeans();
    }

    // TODO consider letting user change some of those parameters
    /// Factor on how larger the number of stored clusters should be compared to user-defined number of clusters.
    /// Big enough factor is essential for good prediction.
    static const UInt32 multiplication_factor = 30;
    static const UInt32 kmeans_max_iter = 1000;         /// Number of kmeans iteration steps
    static constexpr Float64 eps_diff_ = 0.00001;       /// Kmeans stops if difference between clusters is smaller than this value

    UInt32 num_clusters = 0;
    UInt32 num_dimensions = 0;

    KMeansClusters clusters;
    mutable Points predicted_clusters;
};


class AggregateFunctionIncrementalClustering final : public IAggregateFunctionDataHelper<ClusteringData, AggregateFunctionIncrementalClustering>
{
public:
    String getName() const override { return "incrementalClustering"; }

    explicit AggregateFunctionIncrementalClustering(UInt32 clusters_num_,
                                                    UInt32 dimensions_,
                                                    const DataTypes & argument_types_,
                                                    const Array & params_)
        : IAggregateFunctionDataHelper<ClusteringData, AggregateFunctionIncrementalClustering>(argument_types_, params_)
        , clusters_num(clusters_num_), dimensions(dimensions_)
    {}

    /// This function is called when SELECT incrementalClustering(...) is called
    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
    }

    /// This function is called from evalMLMethod function for correct predictValues call
    DataTypePtr getReturnTypeToPredict() const override
    {
        return std::make_shared<DataTypeNumber<UInt32>>();
    }

    void create(AggregateDataPtr place) const override
    {
        new (place) Data(clusters_num, dimensions);
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).add(columns, row_num);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void predictValues(
            ConstAggregateDataPtr place,
            IColumn & to,
            Block & block,
            size_t offset,
            size_t limit,
            const ColumnNumbers & arguments) const override
    {
        if (arguments.size() != dimensions + 1)
            throw Exception("Predict got incorrect number of arguments. Got: " +
                            std::to_string(arguments.size()) + ". Required: " + std::to_string(dimensions + 1),
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto * column = typeid_cast<ColumnVector<UInt32> *>(&to);
        if (!column)
            throw Exception("Cast of column of predictions is incorrect. getReturnTypeToPredict must return same value as it is casted to",
                            ErrorCodes::BAD_CAST);

        this->data(place).predict(column->getData(), block, offset, limit, arguments);
    }

    /// Return all clusters in one array (of size clusters_num * dimensions)
    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        this->data(place).returnClusters(to);
    }

    const char * getHeaderFilePath() const override { return __FILE__; }

private:
    UInt32 clusters_num;
    UInt32 dimensions;
};

}
