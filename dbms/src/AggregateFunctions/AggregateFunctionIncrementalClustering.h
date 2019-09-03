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

class ClusteringData
{
public:
    ClusteringData()
    {}

    ClusteringData(UInt32 clusters_num, UInt32 dimensions)
    : clusters_num(clusters_num), dimensions(dimensions)
    {
        if (clusters_num == 0)
            throw Exception("Number of cluster must be greater than zero", ErrorCodes::LOGICAL_ERROR);

        clusters.reserve(multiplication_factor * clusters_num);
        predicted_clusters.reserve(clusters_num);
    }

    void add(const IColumn ** columns, size_t row_num) {
        predicted_clusters.clear();
        if (clusters.size() < multiplication_factor * clusters_num)
        {
            clusters.emplace_back(columns, row_num, dimensions);
            return;
        }

        size_t closest_cluster = find_closest_cluster_for_point_with_penalty(columns, row_num, clusters);
        clusters[closest_cluster].append_point(columns, row_num);
    }

    void add_from_vector(const std::vector<Float64> & coordinates)
    {
        predicted_clusters.clear();
        if (clusters.size() < multiplication_factor * clusters_num)
        {
            clusters.emplace_back(coordinates);
            return;
        }

        size_t closest_cluster = find_closest_cluster_with_penalty(coordinates, clusters);
        clusters[closest_cluster].append_point_from_vector(coordinates);
    }

    void merge(const ClusteringData & rhs)
    {
        predicted_clusters.clear();
        /// If one of cluster sets are not filled in, put its points to another cluster set
        if (rhs.clusters.size() < rhs.multiplication_factor * rhs.clusters_num)
        {
            for (size_t i = 0; i != rhs.clusters.size(); ++i)
            {
                add_from_vector(rhs.clusters[i].center());
            }
            return;
        }
        if (clusters.size() < multiplication_factor * clusters_num)
        {
            std::vector<Cluster> old_clusters = std::move(clusters);
            clusters = rhs.clusters;
            for (size_t i = 0; i != old_clusters.size(); ++i)
            {
                add_from_vector(old_clusters[i].center());
            }
            return;
        }

        /// Merge closest pairs of clusters greedily
        for (size_t i = 0; i != rhs.clusters.size(); ++i)
        {
            size_t closest_cluster = find_closest_cluster(rhs.clusters[i].center(), clusters, i);
            std::swap(clusters[closest_cluster], clusters[i]);
            clusters[i].merge_cluster(rhs.clusters[i]);
        }
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(clusters_num, buf);
        writeBinary(dimensions, buf);
        writeBinary(clusters.size(), buf);
        for (auto & cluster : clusters)
        {
            cluster.write(buf);
        }
    }

    void read(ReadBuffer & buf)
    {
        readBinary(clusters_num, buf);
        readBinary(dimensions, buf);
        decltype(clusters.size()) clusters_size;
        readBinary(clusters_size, buf);
        clusters.resize(clusters_size);
        for (auto & cluster : clusters)
        {
            cluster.read(buf);
        }
    }

    // TODO consider adding new points (from prediction) into clusters
    void predict(ColumnVector<UInt32>::Container & container,
                 Block & block,
                 size_t offset,
                 size_t limit,
                 const ColumnNumbers & arguments) const
    {
        if (dimensions + 1 != arguments.size())
            throw Exception("In predict function number of arguments differs from the size of weights vector", ErrorCodes::LOGICAL_ERROR);

        size_t rows_num = block.rows();

        if (offset > rows_num || offset + limit > rows_num)
            throw Exception("Invalid offset and limit for incrementalClustering::predict. "
                            "Block has " + toString(rows_num) + " rows, but offset is " + toString(offset) +
                            " and limit is " + toString(limit), ErrorCodes::LOGICAL_ERROR);

        make_prediction();
        std::vector<std::vector<Float64>> distances(
            limit,
            std::vector<Float64>(predicted_clusters.size(), 0.0)
        );

        for (size_t i = 0; i != dimensions; ++i)
        {
            const ColumnWithTypeAndName & cur_col = block.getByPosition(arguments[i + 1]);
            if (!isNativeNumber(cur_col.type))
                throw Exception("Prediction arguments must have numeric type", ErrorCodes::BAD_ARGUMENTS);

            auto features_column = cur_col.column;
            if (!features_column)
                throw Exception("Unexpectedly cannot dynamically cast features column " + std::to_string(i),
                                ErrorCodes::LOGICAL_ERROR);

            for (size_t row = 0; row != limit; ++row)
            {
                Float64 value = features_column->getFloat64(offset + row);
                for (size_t cluster_idx = 0; cluster_idx != predicted_clusters.size(); ++cluster_idx)
                {
                    Float64 diff = predicted_clusters[cluster_idx][i] - value;
                    distances[row][cluster_idx] += diff * diff;
                }
            }
        }

        container.reserve(container.size() + limit);
        for (size_t row = 0; row != limit; ++row)
        {
            size_t closest = 0;
            for (size_t cluster_idx = 1; cluster_idx != distances[row].size(); ++cluster_idx)
            {
                if (distances[row][cluster_idx] < distances[row][closest])
                    closest = cluster_idx;
            }
            container.emplace_back(closest);
        }
    }

    void returnClusters(IColumn & to) const
    {
        make_prediction();

        size_t size = clusters_num * dimensions;

        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        size_t old_size = offsets_to.back();
        offsets_to.push_back(old_size + size);

        typename ColumnFloat64::Container & val_to
                = static_cast<ColumnFloat64 &>(arr_to.getData()).getData();

        val_to.reserve(old_size + size);
        for (size_t cluster_idx = 0; cluster_idx != clusters_num; ++cluster_idx)
            for (size_t i = 0; i != dimensions; ++i)
                val_to.push_back(predicted_clusters[cluster_idx][i]);
    }

private:

    class Cluster
    {
    public:
        Cluster() : coordinates(), points_num(0)
        {}

        explicit Cluster(std::vector<Float64> coordinates)
            : coordinates(std::move(coordinates)),
              points_num(1)
        {}

        Cluster(const Cluster& other)
            : coordinates(other.coordinates),
              points_num(other.points_num)
        {}

        void swap(Cluster& other)
        {
            std::swap(coordinates, other.coordinates);
            std::swap(points_num, other.points_num);
        }

        Cluster(const IColumn ** columns, size_t row_num, size_t dimensions)
                : points_num(1)
        {
            coordinates.resize(dimensions);
            for (size_t i = 0; i != dimensions; ++i)
            {
                coordinates[i] = columns[i]->getFloat64(row_num);
            }
        }

        void append_point(const IColumn ** columns, size_t row_num)
        {
            if (points_num == 0)
                throw Exception("Append to empty cluster", ErrorCodes::LOGICAL_ERROR);

            Float64 point_weight = append_point_weight(++points_num);
            Float64 cluster_weight = 1 - point_weight;
            for (size_t i = 0; i != coordinates.size(); ++i)
            {
                coordinates[i] = cluster_weight * coordinates[i] + point_weight * columns[i]->getFloat64(row_num);
            }
        }

        void append_point_from_vector(const std::vector<Float64> & new_coordinates)
        {
            if (points_num == 0)
                throw Exception("Append to empty cluster", ErrorCodes::LOGICAL_ERROR);
            if (coordinates.size() != new_coordinates.size())
                throw Exception("Cannot append point of other size", ErrorCodes::LOGICAL_ERROR);

            Float64 point_weight = append_point_weight(++points_num);
            Float64 cluster_weight = 1 - point_weight;
            for (size_t i = 0; i != coordinates.size(); ++i)
            {
                coordinates[i] = cluster_weight * coordinates[i] + point_weight * new_coordinates[i];
            }
        }

        void merge_cluster(const Cluster & other)
        {
            if (size() == 0 || other.size() == 0)
                throw Exception("Merge empty cluster", ErrorCodes::LOGICAL_ERROR);

            Float64 this_weight = merge_weight(size(), other.size());
            Float64 other_weight = 1 - this_weight;
            for (size_t i = 0; i != coordinates.size(); ++i)
            {
                coordinates[i] = this_weight * coordinates[i] + other_weight * other.coordinates[i];
            }
            points_num += other.points_num;
        }

        void write(WriteBuffer & buf) const
        {
            writeBinary(coordinates, buf);
            writeBinary(points_num, buf);
        }

        void read(ReadBuffer & buf)
        {
            readBinary(coordinates, buf);
            readBinary(points_num, buf);
        }

        UInt32 size() const
        {
            return points_num;
        }

        const std::vector<Float64> & center() const
        {
            return coordinates;
        }

    private:
        static Float64 append_point_weight(UInt32 size)
        {
            return Float64{1.0} / size;
        }

        static Float64 merge_weight(UInt32 size1, UInt32 size2)
        {
            return static_cast<Float64>(size1) / (size1 + size2);
        }

        std::vector<Float64> coordinates;
        UInt32 points_num = 0;
    };

    static Float64 compute_distance(const std::vector<Float64> & point1, const std::vector<Float64> & point2)
    {
        Float64 distance = 0.0;
        for (size_t i = 0; i != point1.size(); ++i)
        {
            Float64 coordinate_diff = point1[i] - point2[i];
            distance += coordinate_diff * coordinate_diff;
        }
        return std::sqrt(distance);
    }

    static Float64 compute_distance_for_point(const std::vector<Float64> & point, const IColumn ** columns, size_t row_num)
    {
        Float64 distance = 0;
        for (size_t i = 0; i != point.size(); ++i)
        {
            Float64 coordinate_diff = point[i] - columns[i]->getFloat64(row_num);
            distance += coordinate_diff * coordinate_diff;
        }
        return std::sqrt(distance);
    }

    static size_t find_closest_cluster_for_point_with_penalty(const IColumn ** columns, size_t row_num, const std::vector<Cluster> & clusters)
    {
        Float64 min_dist = cluster_penalty(clusters[0].size()) * compute_distance_for_point(clusters[0].center(), columns, row_num);
        UInt32 closest = 0;
        for (size_t i = 1; i != clusters.size(); ++i)
        {
            Float64 dist = cluster_penalty(clusters[i].size()) * compute_distance_for_point(clusters[i].center(), columns, row_num);
            if (dist < min_dist)
            {
                min_dist = dist;
                closest = i;
            }
        }
        return closest;
    }

    static size_t find_closest_cluster_with_penalty(const std::vector<Float64> & point, const std::vector<Cluster> & clusters)
    {
        Float64 min_dist = cluster_penalty(clusters[0].size()) * compute_distance(clusters[0].center(), point);
        UInt32 closest = 0;
        for (size_t i = 1; i != clusters.size(); ++i)
        {
            Float64 dist = cluster_penalty(clusters[i].size()) * compute_distance(clusters[i].center(), point);
            if (dist < min_dist)
            {
                min_dist = dist;
                closest = i;
            }
        }
        return closest;
    }


    static size_t find_closest_cluster(const std::vector<Float64> & point, const std::vector<std::vector<Float64>> & clusters)
    {
        Float64 min_dist = compute_distance(point, clusters[0]);
        UInt32 closest = 0;
        for (size_t i = 1; i != clusters.size(); ++i)
        {
            Float64 dist = compute_distance(point, clusters[i]);
            if (dist < min_dist)
            {
                min_dist = dist;
                closest = i;
            }
        }
        return closest;
    }

    static size_t find_closest_cluster(const std::vector<Float64> & point, const std::vector<Cluster> & clusters, size_t from = 0)
    {
        Float64 min_dist = compute_distance(point, clusters[from].center());
        UInt32 closest = from;
        for (size_t i = from + 1; i != clusters.size(); ++i)
        {
            Float64 dist = compute_distance(point, clusters[i].center());
            if (dist < min_dist)
            {
                min_dist = dist;
                closest = i;
            }
        }
        return closest;
    }

    static Float64 cluster_penalty(UInt32 cluster_size)
    {
        return std::sqrt(cluster_size);
    }

    static Float64 min_squared_distance(const std::vector<Float64> & point, const std::vector<std::vector<Float64>> & clusters)
    {
        Float64 min_squared_dist = 0.0;
        for (size_t i = 0; i != point.size(); ++i)
        {
            Float64 d = point[i] - clusters[0][i];
            min_squared_dist += d * d;
        }
        for (size_t cluster_idx = 1; cluster_idx != clusters.size(); ++cluster_idx)
        {
            Float64 squared_dist = 0.0;
            for (size_t i = 0; i != point.size(); ++i)
            {
                Float64 d = point[i] - clusters[cluster_idx][i];
                squared_dist += d * d;
            }
            min_squared_dist = std::min(min_squared_dist, squared_dist);
        }

        return min_squared_dist;
    }

    void k_means_initialize_clusters() const
    {
        std::uniform_int_distribution<int> distribution(0, clusters.size() - 1);
        size_t first_cluster = distribution(random_generator_);

        predicted_clusters.reserve(clusters_num);
        predicted_clusters.emplace_back(clusters[first_cluster].center());
        for (size_t i = 1; i != clusters_num; ++i)
        {
            /// Simply pick the point that has the maximum squared distance from it's nearest cluster
            Float64 max_min_squared_dist = -1.0;
            size_t farthest_point = 0;
            for (size_t point_idx = 0; point_idx != clusters.size(); ++point_idx)
            {
                Float64 min_squared_dist = min_squared_distance(clusters[point_idx].center(), predicted_clusters);
                if (min_squared_dist > max_min_squared_dist)
                {
                    max_min_squared_dist = min_squared_dist;
                    farthest_point = point_idx;
                }
            }
            predicted_clusters.emplace_back(clusters[farthest_point].center());
        }
    }

    /// Returns pairwise squared distance between new and old clusters
    Float64 k_means_update_clusters(const std::vector<size_t> & closest_cluster) const
    {
        Float64 diff = 0.0;
        std::vector<std::vector<Float64>> new_clusters(
            predicted_clusters.size(),
            std::vector<Float64>(predicted_clusters[0].size(), 0.0)
        );
        std::vector<UInt64> cluster_weights(predicted_clusters.size());
        for (size_t point_idx = 0; point_idx != clusters.size(); ++point_idx)
        {
            size_t cluster_idx = closest_cluster[point_idx];
            ++cluster_weights[cluster_idx];
            for (size_t i = 0; i != new_clusters[cluster_idx].size(); ++i)
                new_clusters[cluster_idx][i] += clusters[point_idx].center()[i];
        }
        for (size_t cluster_idx = 0; cluster_idx != new_clusters.size(); ++cluster_idx)
        {
            for (size_t i = 0; i != new_clusters[cluster_idx].size(); ++i)
            {
                new_clusters[cluster_idx][i] /= cluster_weights[cluster_idx];
                Float64 d = new_clusters[cluster_idx][i] - predicted_clusters[cluster_idx][i];
                diff += d * d;
            }
        }
        predicted_clusters = std::move(new_clusters);

        return diff;
    }

    Float64 k_means() const
    {
        predicted_clusters.clear();

        k_means_initialize_clusters();
        std::vector<size_t> closest_cluster(clusters.size());
        for (size_t iter = 0; iter != kmeans_max_iter; ++iter)
        {
            /// For each point find the cluster with closest center
            for (size_t point_idx = 0; point_idx != clusters.size(); ++point_idx)
            {
                closest_cluster[point_idx] = find_closest_cluster(clusters[point_idx].center(), predicted_clusters);
            }
            /// Change predicted cluster centers
            Float64 diff = k_means_update_clusters(closest_cluster);
            /// Stop if diff is too small
            if (diff < eps_diff_)
                break;
        }

        Float64 mean_distance{0.0};
        for (size_t point_idx = 0; point_idx != clusters.size(); ++point_idx)
            mean_distance += compute_distance(predicted_clusters[closest_cluster[point_idx]], clusters[point_idx].center());
        if (!clusters.empty())
            mean_distance /= clusters.size();

        return mean_distance;
    }

    void make_prediction() const
    {
        if (predicted_clusters.empty())
        {
            /// Searching for the best result over max_tries_to_predict tries
            /// and save it into predict_clusters
            Float64 min_mean_distance = k_means();
            auto best_prediction = predicted_clusters;
            for (size_t i = 1; i != max_tries_to_predict + 1; ++i)
            {
                Float64 cur_mean_distance = k_means();
                if (cur_mean_distance < min_mean_distance)
                {
                    min_mean_distance = cur_mean_distance;
                    best_prediction = predicted_clusters;
                }
            }

            predicted_clusters = std::move(best_prediction);
        }
    }

    // TODO consider letting user change some of those parameters
    /// Factor on how larger the number of stored clusters should be compared to user-defined number of clusters. Big enough factor is essential for good prediction
    static const UInt32 multiplication_factor = 30;
    static const UInt32 kmeans_max_iter = 1000;         /// Number of kmeans iteration steps
    static const UInt32 max_tries_to_predict = 1;       /// Number of tries to find the best kmeans prediction
    static constexpr Float64 eps_diff_ = 0.00001;       /// Kmeans stops if difference between clusters is smaller than this value

    mutable std::mt19937 random_generator_;

    UInt32 clusters_num;
    UInt32 dimensions;

    std::vector<Cluster> clusters;
    mutable std::vector<std::vector<Float64>> predicted_clusters;
};


class AggregateFunctionIncrementalClustering final : public IAggregateFunctionDataHelper<ClusteringData, AggregateFunctionIncrementalClustering>
{
public:
    String getName() const override { return "incrementalClustering"; }

    explicit AggregateFunctionIncrementalClustering(UInt32 clusters_num,
                                                    UInt32 dimensions,
                                                    const DataTypes & argument_types,
                                                    const Array & params)
    : IAggregateFunctionDataHelper<ClusteringData, AggregateFunctionIncrementalClustering>(argument_types, params),
    clusters_num(clusters_num), dimensions(dimensions)
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
