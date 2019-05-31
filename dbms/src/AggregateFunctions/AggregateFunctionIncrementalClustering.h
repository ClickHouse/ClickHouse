#pragma once

#include <type_traits>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <cmath>
#include <exception>
#include <algorithm>
#include <numeric>

#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Common/FieldVisitors.h>
#include <Interpreters/castColumn.h>


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
        /// if one of cluster sets are not filled in, just put it points to another cluster set
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

        /// Merge closest pairs of clusters by greedy algorithm
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

    void predict(ColumnVector<UInt32>::Container &container,
                 Block &block,
                 const ColumnNumbers &arguments,
                 const Context & context) const
    {
        if (dimensions + 1 != arguments.size())
        {
            throw Exception("In predict function number of arguments differs from the size of weights vector", ErrorCodes::LOGICAL_ERROR);
        }
        make_prediction();
        std::vector<std::vector<Float64>> distances(
            block.rows(),
            std::vector<Float64>(predicted_clusters.size(), 0.0)
        );
        for (size_t i = 0; i != dimensions; ++i)
        {
            const ColumnWithTypeAndName & cur_col = block.getByPosition(arguments[i + 1]);
            if (!isNumber(cur_col.type))
            {
                throw Exception("Prediction arguments must have numeric type", ErrorCodes::BAD_ARGUMENTS);
            }

            /// If column type is already Float64 then castColumn simply returns it
            auto features_col_ptr = castColumn(cur_col, std::make_shared<DataTypeFloat64>(), context);
            auto features_column = typeid_cast<const ColumnFloat64 *>(features_col_ptr.get());

            if (!features_column)
            {
                throw Exception("Unexpectedly cannot dynamically cast features column " + std::to_string(i), ErrorCodes::LOGICAL_ERROR);
            }

            for (size_t row = 0; row != distances.size(); ++row)
            {
                Float64 value = features_column->getElement(row);
                for (size_t cluster_idx = 0; cluster_idx != predicted_clusters.size(); ++cluster_idx)
                {
                    Float64 diff = predicted_clusters[cluster_idx][i] - value;
                    distances[row][cluster_idx] += diff * diff;
                }
            }
        }
        container.reserve(block.rows());
        for (size_t row = 0; row != distances.size(); ++row)
        {
            size_t closest = 0;
            for (size_t cluster_idx = 1; cluster_idx != distances[row].size(); ++cluster_idx)
            {
                if (distances[row][cluster_idx] < distances[row][closest])
                {
                    closest = cluster_idx;
                }
            }
            container.emplace_back(closest);
        }
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
                coordinates[i] = static_cast<const ColumnVector<Float64> &>(*columns[i]).getData()[row_num];
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
                coordinates[i] = cluster_weight * coordinates[i] +
                                 point_weight * static_cast<const ColumnVector<Float64> &>(*columns[i]).getData()[row_num];
            }
        }

        void append_point_from_vector(const std::vector<Float64> & new_coordinates)
        {
            if (points_num == 0)
                throw Exception("Append to empty cluster", ErrorCodes::LOGICAL_ERROR);

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

        const std::vector<Float64>& center() const
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
            Float64 coordinate_diff = point[i] - static_cast<const ColumnVector<Float64> &>(*columns[i]).getData()[row_num];
            distance += coordinate_diff * coordinate_diff;
        }
        return std::sqrt(distance);
    }

    static size_t find_closest_cluster_for_point_with_penalty(const IColumn ** columns, size_t row_num, const std::vector<Cluster>& clusters)
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

    static size_t find_closest_cluster_with_penalty(const std::vector<Float64>& point, const std::vector<Cluster>& clusters)
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


    static size_t find_closest_cluster(const std::vector<Float64>& point, const std::vector<std::vector<Float64>>& clusters)
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

    static size_t find_closest_cluster(const std::vector<Float64>& point, const std::vector<Cluster>& clusters, size_t from = 0)
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

    static Float64 min_squared_distance(const std::vector<Float64>& point, const std::vector<std::vector<Float64>>& clusters)
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

    static std::vector<std::vector<Float64>> k_means_initialize_clusters(const std::vector<Cluster>& points, UInt32 clusters_num)
    {
        std::vector<std::vector<Float64>> clusters;
        clusters.reserve(clusters_num);
        // TODO: change std::rand
        size_t first_cluster = std::rand() % points.size();
        clusters.emplace_back(points[first_cluster].center());
        for (size_t i = 1; i != clusters_num; ++i)
        {
            /// just search point that has the maximum squared distance from it's nearest cluster
            Float64 max_min_squared_dist = -1.0;
            size_t farest_point = 0;
            for (size_t point_idx = 0; point_idx != points.size(); ++point_idx)
            {
                Float64 min_squared_dist = min_squared_distance(points[point_idx].center(), clusters);
                if (min_squared_dist > max_min_squared_dist)
                {
                    max_min_squared_dist = min_squared_dist;
                    farest_point = point_idx;
                }
            }
            clusters.emplace_back(points[farest_point].center());
        }
        return clusters;
    }

    /// Returns squared distance between new and old clusters
    static Float64 k_means_update_clusters(std::vector<std::vector<Float64>>& clusters, const std::vector<Cluster>& points, const std::vector<size_t>& closest_cluster)
    {
        Float64 diff = 0.0;
        std::vector<std::vector<Float64>> new_clusters(
            clusters.size(),
            std::vector<Float64>(clusters[0].size(), 0.0)
        );
        std::vector<Float64> cluster_weights(clusters.size());
        for (size_t point_idx = 0; point_idx != points.size(); ++point_idx)
        {
            size_t cluster_idx = closest_cluster[point_idx];
            Float64 weight = 1.0;
            cluster_weights[cluster_idx] += weight;
            for (size_t i = 0; i != new_clusters[cluster_idx].size(); ++i)
            {
                new_clusters[cluster_idx][i] += weight * points[point_idx].center()[i];
            }
        }
        for (size_t cluster_idx = 0; cluster_idx != new_clusters.size(); ++cluster_idx)
        {
            for (size_t i = 0; i != new_clusters[cluster_idx].size(); ++i)
            {
                new_clusters[cluster_idx][i] /= cluster_weights[cluster_idx];
                Float64 d = new_clusters[cluster_idx][i] - clusters[cluster_idx][i];
                diff += d * d;
            }
        }
        clusters = std::move(new_clusters);
        return diff;
    }

    static std::vector<std::vector<Float64>> k_means(const std::vector<Cluster>& points, UInt32 clusters_num, Float64& mean_distance)
    {
        std::vector<std::vector<Float64>> clusters = k_means_initialize_clusters(points, clusters_num);
        std::vector<size_t> closest_cluster(points.size());
        for (size_t iter = 0; iter != max_iter; ++iter)
        {
            /// Search closest cluster from predicted_clusters for each point from clusters
            for (size_t point_idx = 0; point_idx != points.size(); ++point_idx)
            {
                closest_cluster[point_idx] = find_closest_cluster(points[point_idx].center(), clusters);
            }
            /// Change predicted_clusters
            Float64 diff = k_means_update_clusters(clusters, points, closest_cluster);
            /// stop if didn't change
            if (diff == 0)
            {
                break;
            }
        }
        mean_distance = 0.0;
        for (size_t point_idx = 0; point_idx != points.size(); ++point_idx)
        {
            mean_distance += compute_distance(clusters[closest_cluster[point_idx]], points[point_idx].center()) / points.size();
        }
        return clusters;
    }

    const std::vector<std::vector<Float64>>& make_prediction() const
    {
        if (predicted_clusters.empty())
        {
            /// Searching for the best result over max_tries tries
            Float64 min_mean_distance;
            predicted_clusters = k_means(clusters, clusters_num, min_mean_distance);
            for (size_t i = 1; i != max_tries; ++i)
            {
                Float64 mean_distance;
                auto new_predicted_clusters = k_means(clusters, clusters_num, mean_distance);
                if (mean_distance < min_mean_distance)
                {
                    min_mean_distance = mean_distance;
                    predicted_clusters = std::move(new_predicted_clusters);
                }
            }
        }
        return predicted_clusters;
    }

    static const UInt32 multiplication_factor = 30;
    static const UInt32 max_iter = 1000;
    static const UInt32 max_tries = 1;

    UInt32 clusters_num;
    UInt32 dimensions;

    std::vector<Cluster> clusters;
    mutable std::vector<std::vector<Float64>> predicted_clusters;
};


class AggregateFunctionIncrementalClustering final : public IAggregateFunctionDataHelper<ClusteringData, AggregateFunctionIncrementalClustering>
{
public:
    String getName() const override { return "IncrementalClustering"; }

    explicit AggregateFunctionIncrementalClustering(UInt32 clusters_num, UInt32 dimensions,
                                                    const DataTypes & argument_types,
                                                    const Array & params)
    : IAggregateFunctionDataHelper<ClusteringData, AggregateFunctionIncrementalClustering>(argument_types, params),
    clusters_num(clusters_num), dimensions(dimensions)
    {}

    DataTypePtr getReturnType() const override
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

    void predictValues(ConstAggregateDataPtr place, IColumn & to,
                               Block & block, const ColumnNumbers & arguments, const Context & context) const override
    {
        if (arguments.size() != dimensions + 1)
            throw Exception("Predict got incorrect number of arguments. Got: " +
                            std::to_string(arguments.size()) + ". Required: " + std::to_string(dimensions + 1),
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto &column = dynamic_cast<ColumnVector<UInt32> &>(to);

        this->data(place).predict(column.getData(), block, arguments, context);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        std::ignore = place;
        std::ignore = to;
        throw std::runtime_error("not implemented");
    }

    const char * getHeaderFilePath() const override { return __FILE__; }

private:
    UInt32 clusters_num;
    UInt32 dimensions;
};

}
