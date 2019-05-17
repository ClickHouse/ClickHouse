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
    }

    void add(const IColumn ** columns, size_t row_num) {
        predicted_clusters.clear();
        if (clusters.size() < multiplication_factor * clusters_num)
        {
            clusters.emplace_back(columns, row_num, dimensions);
            return;
        }

        /// simply searching for the closest cluster over all clusters
        size_t closest_cluster = 0;
        Float64 min_distance = compute_distance_from_point(clusters[0].center(), columns, row_num) * cluster_penalty(clusters[0].size());
        Float64 cur_distance;
        for (size_t i = 1; i != clusters.size(); ++i)
        {
            /// Штрафуем, если в кластере слишком много элементов, чтобы они заполнялись равномерно
            /// Можно попробовать штрафовать на корень? от числа элементов
            cur_distance = compute_distance_from_point(clusters[i].center(), columns, row_num) * cluster_penalty(clusters[i].size());
            if (cur_distance < min_distance)
            {
                min_distance = cur_distance;
                closest_cluster = i;
            }
        }

        clusters[closest_cluster].append_point(columns, row_num, min_distance);
    }

    void add_from_vector(const std::vector<Float64> & coordinates)
    {
        predicted_clusters.clear();
        if (clusters.size() < multiplication_factor * clusters_num)
        {
            clusters.emplace_back(coordinates);
            return;
        }

        /// simply searching for the closest cluster over all clusters
        size_t closest_cluster = 0;
        Float64 min_distance = compute_distance(clusters[0].center(), coordinates) * cluster_penalty(clusters[0].size());
        Float64 cur_distance;
        for (size_t i = 1; i != clusters.size(); ++i)
        {
            /// Штрафуем, если в кластере слишком много элементов, чтобы они заполнялись равномерно
            /// Можно попробовать штрафовать на корень? от числа элементов
            cur_distance = compute_distance(clusters[i].center(), coordinates) * cluster_penalty(clusters[i].size());
            if (cur_distance < min_distance)
            {
                min_distance = cur_distance;
                closest_cluster = i;
            }
        }

        clusters[closest_cluster].append_point_from_vector(coordinates);
    }

    void merge(const ClusteringData & rhs)
    {
        predicted_clusters.clear();
        /// По-простому объединяем попарно элементы (чтобы предположение об одинаковом числе элеметов
        /// в каждом класетер сохранялось (надо бы проверить, насколько это существенно))
        /// Для этого пока что за квадрат ищем ближайших друг к другу соседей
        std::cout << "\n\nSTART TO MERGE\n\n";
        if (rhs.clusters.size() < rhs.multiplication_factor * rhs.clusters_num)
        {
            /// Тут мы передаем все точки от rhs к *this
            /// Т.к. rhs константен, то точки будут продублированы еще и там
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

        /// merge ближайших пар кластеров жадным алгоритмом
        for (size_t i = 0; i != rhs.clusters.size(); ++i)
        {
            size_t closest_cluster = 0;
            Float64 min_dist = compute_distance(clusters[i].center(), rhs.clusters[i].center());
            for (size_t j = i + 1; j != clusters.size(); ++j)
            {
                Float64 cur_dist = compute_distance(clusters[j].center(), rhs.clusters[i].center());
                if (cur_dist < min_dist)
                {
                    min_dist = cur_dist;
                    closest_cluster = j;
                }
            }

            clusters[closest_cluster].merge_cluster(rhs.clusters[i]);
            std::swap(clusters[closest_cluster], clusters[i]);
        }
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(clusters_num, buf);
        writeBinary(dimensions, buf);
        // writeBinary(initialized_clusters, buf);
        for (auto & cluster : clusters)
        {
            cluster.write(buf);
        }
    }

    void read(ReadBuffer & buf)
    {
        readBinary(clusters_num, buf);
        readBinary(dimensions, buf);
        // readBinary(initialized_clusters, buf);
        for (auto & cluster : clusters)
        {
            cluster.read(buf);
        }
    }

    Float64 predict(const std::vector<Float64> & predict_feature) const
    {
        /// Можно запилить обычную кластеризацию на малом числе элементов

        size_t closest_cluster = 0;
        Float64 min_dist = compute_distance(clusters[0].center(), predict_feature);
        for (size_t i = 1; i != clusters.size(); ++i)
        {
            Float64 cur_dist = compute_distance(clusters[i].center(), predict_feature);
            if (cur_dist < min_dist)
            {
                min_dist = cur_dist;
                closest_cluster = i;
            }
        }

        std::cout << "\n\nmy prediction: " << closest_cluster << "\n";
        std::cout << "with min dist: " << min_dist << "\n";
        for (size_t j = 0; j != clusters[closest_cluster].center().size(); ++j)
        {
            std::cout << clusters[closest_cluster].center()[j] << " ";
        }
        std::cout << "\n\n";

        // TODO
        std::ignore = predict_feature;
        return 0;
    }

    void print_clusters(ColumnVector<Float64>& column) const
    {
        std::cout << "\n\nIM GOING TO PRINT CLUSTERS\n\n";
        make_prediction();
        std::cout << "PRINTER\n\n";
        column.getData().push_back(predicted_clusters.size());
        for (auto& cluster : clusters)
        {
            column.getData().push_back(cluster.size());
            for (auto&& coord : cluster.center())
            {
                column.getData().push_back(coord);
            }
        }
        for (auto& cluster : predicted_clusters)
        {
            // column.getData().push_back(cluster.size());
            for (auto&& coord : cluster)
            {
                column.getData().push_back(coord);
            }
        }
    }

private:
    static Float64 compute_distance(const std::vector<Float64> & point1, const std::vector<Float64> & point2)
    {
        Float64 distance = 0;
        for (size_t i = 0; i != point1.size(); ++i)
        {
            Float64 coordinate_diff = point1[i] - point2[i];
            distance += coordinate_diff * coordinate_diff;
        }
        return std::sqrt(distance);
    }

    static Float64 compute_distance_from_point(const std::vector<Float64> & point1, const IColumn ** columns, size_t row_num)
    {
        Float64 distance = 0;
        for (size_t i = 0; i != point1.size(); ++i)
        {
            Float64 coordinate_diff = point1[i] - static_cast<const ColumnVector<Float64> &>(*columns[i]).getData()[row_num];
            distance += coordinate_diff * coordinate_diff;
        }
        return std::sqrt(distance);
    }

    static size_t find_closest_cluster(const std::vector<Float64>& point, const std::vector<std::vector<Float64>>& clusters) {
        Float64 min_dist = compute_distance(point, clusters[0]);
        UInt32 closest = 0;
        for (size_t i = 1; i != clusters.size(); ++i) {
            Float64 dist = compute_distance(point, clusters[i]);
            if (dist < min_dist) {
                min_dist = dist;
                closest = i;
            }
        }
        return closest;
    }

    static Float64 cluster_penalty(UInt32 cluster_size)
    {
        std::ignore = cluster_size;
        return 1.0;
    }

    class Cluster
    {
    public:
        explicit Cluster(std::vector<Float64> coordinates)
        : coordinates(std::move(coordinates)), points_num(1), total_center_dist(0.0)
        {}

        Cluster(const Cluster& other)
        : coordinates(other.coordinates),
        points_num(other.points_num),
        total_center_dist(other.total_center_dist)
        {}

        void swap(Cluster& other)
        {
            std::swap(coordinates, other.coordinates);
            std::swap(points_num, other.points_num);
            std::swap(total_center_dist, other.total_center_dist);
        }

        Cluster(const IColumn ** columns, size_t row_num, size_t dimensions)
        : points_num(1), total_center_dist(0.0)
        {
            coordinates.resize(dimensions);
            for (size_t i = 0; i != dimensions; ++i)
            {
                coordinates[i] = static_cast<const ColumnVector<Float64> &>(*columns[i]).getData()[row_num];
            }
        }

        void append_point(const IColumn ** columns, size_t row_num, Float64 distance)
        {
            if (points_num == 0)
                throw Exception("Append to empty cluster", ErrorCodes::LOGICAL_ERROR);

            Float64 point_weight = append_point_weight(points_num + 1);
            Float64 cluster_weight = 1 - point_weight;
            for (size_t i = 0; i != coordinates.size(); ++i)
            {
                coordinates[i] = coordinates[i] * cluster_weight +
                        static_cast<const ColumnVector<Float64> &>(*columns[i]).getData()[row_num] * point_weight;
            }
            total_center_dist += distance;
            ++points_num;
        }

        void append_point_from_vector(const std::vector<Float64> & new_coordinates)
        {
            if (points_num == 0)
                return;
//                throw Exception("This should not be reached 2", ErrorCodes::LOGICAL_ERROR);

            Float64 point_weight = append_point_weight(points_num + 1);
            Float64 cluster_weight = 1 - point_weight;
            Float64 distance = 0;
            for (size_t i = 0; i != coordinates.size(); ++i)
            {
                distance += (coordinates[i] - new_coordinates[i]) * (coordinates[i] - new_coordinates[i]);
                coordinates[i] = coordinates[i] * cluster_weight + new_coordinates[i] * point_weight;
            }
            total_center_dist += std::sqrt(distance);
            ++points_num;
        }

        void merge_cluster(const Cluster & other)
        {
            if (size() == 0 || other.size() == 0)
                return;
//                throw Exception("This should not be reached 3", ErrorCodes::LOGICAL_ERROR);

            Float64 this_weight = merge_weight(size(), other.size());
            Float64 other_weight = 1 - this_weight;
            for (size_t i = 0; i != coordinates.size(); ++i)
            {
                coordinates[i] = coordinates[i] * this_weight + other.coordinates[i] * other_weight;
            }
            points_num += other.points_num;

            /// Поменять
            total_center_dist += other.total_center_dist;
        }

        void write(WriteBuffer & buf) const
        {
            writeBinary(coordinates, buf);
            writeBinary(points_num, buf);
            writeBinary(total_center_dist, buf);
        }
        void read(ReadBuffer & buf)
        {
            readBinary(coordinates, buf);
            readBinary(points_num, buf);
            readBinary(total_center_dist, buf);
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
        Float64 total_center_dist = 0.0;
    };

    static std::vector<std::vector<Float64>> k_means_initialize_clusters(const std::vector<Cluster>& points, UInt32 clusters_num) {
        /// Для начала проинициализируем кластера случайными точками
        std::vector<std::vector<Float64>> clusters;
        clusters.reserve(clusters_num);
        std::vector<size_t> idxs(points.size());
        std::iota(std::begin(idxs), std::end(idxs), 0);
        for (size_t i = 0; i != clusters_num; ++i) {
            size_t j = std::rand() % (idxs.size() - i);
            std::swap(idxs[i], idxs[i + j]);
            clusters.emplace_back(points[idxs[i]].center());
        }
        return clusters;
    }

    static Float64 k_means_update_clusters(std::vector<std::vector<Float64>>& clusters, const std::vector<Cluster>& points, const std::vector<size_t>& closest_cluster) {
        Float64 diff = 0.0;
        for (size_t cluster_idx = 0; cluster_idx != clusters.size(); ++cluster_idx) {
            Float64 sum_weight = 0.0;
            std::vector<Float64> new_cluster(clusters[cluster_idx].size(), 0.0);
            for (size_t point_idx = 0; point_idx != points.size(); ++point_idx) {
                if (closest_cluster[point_idx] != cluster_idx) {
                    continue;
                }
                Float64 weight = points[point_idx].size();
                sum_weight += weight;
                for (size_t i = 0; i != new_cluster.size(); ++i) {
                    new_cluster[i] += weight * points[point_idx].center()[i];
                }
            }
            for (size_t i = 0; i != new_cluster.size(); ++i) {
                new_cluster[i] /= sum_weight;
                Float64 d = new_cluster[i] - clusters[cluster_idx][i];
                diff += d * d;
            }
            clusters[cluster_idx] = std::move(new_cluster);
        }
        return diff;
    }

    static std::vector<std::vector<Float64>> k_means(const std::vector<Cluster>& points, UInt32 clusters_num) {
        std::vector<std::vector<Float64>> clusters = k_means_initialize_clusters(points, clusters_num);
        std::vector<size_t> closest_cluster(points.size());
        for (size_t step = 0; step != 1000; ++step) {
            /// Ищем ближайший кластер из predicted_clusters для каждого кластера из clusters
            for (size_t point_idx = 0; point_idx != points.size(); ++point_idx) {
                closest_cluster[point_idx] = find_closest_cluster(points[point_idx].center(), clusters);
            }
            /// Меняем центры кластеров
            Float64 diff = k_means_update_clusters(clusters, points, closest_cluster);
            if (diff == 0) {
                break;
            }
        }
        return clusters;
    }

    const std::vector<std::vector<Float64>>& make_prediction() const
    {
        if (predicted_clusters.empty()) {
            predicted_clusters = k_means(clusters, clusters_num);
        }
        return predicted_clusters;
    }

    static const UInt32 multiplication_factor = 30;
    UInt32 clusters_num;
    UInt32 dimensions;
    mutable UInt64 steps;

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
        return std::make_shared<DataTypeNumber<Float64>>();
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

    /*
    void predictValues(ConstAggregateDataPtr place, IColumn & to, Block & block, size_t row_num, const ColumnNumbers & arguments) const override
    {
        if (arguments.size() != dimensions)
            throw Exception("Predict got incorrect number of arguments. Got: " + std::to_string(arguments.size()) + ". Required: " + std::to_string(clusters_num),
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto &column = dynamic_cast<ColumnVector<Float64> &>(to);

        std::vector<Float64> predict_features(arguments.size());
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            const auto& element = (*block.getByPosition(arguments[i]).column)[row_num];
            if (element.getType() != Field::Types::Float64)
                throw Exception("Prediction arguments must be values of type Float",
                                ErrorCodes::BAD_ARGUMENTS);

            predict_features[i - 1] = element.get<Float64>();
        }
        column.getData().push_back(this->data(place).predict(predict_features));
    }
    */
    /*
    void predictValues(ConstAggregateDataPtr place, IColumn & to,
                               Block & block, const ColumnNumbers & arguments, const Context & context) const override
    {
        // throw Exception("Method predictValues is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
        if (arguments.size() != param_num + 1)
            throw Exception("Predict got incorrect number of arguments. Got: " +
                            std::to_string(arguments.size()) + ". Required: " + std::to_string(param_num + 1),
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto &column = dynamic_cast<ColumnVector<Float64> &>(to);

        this->data(place).predict(column.getData(), block, arguments, context);
    }
    */

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        std::ignore = place;
        std::ignore = to;

        auto &column = dynamic_cast<ColumnVector<Float64> &>(to);
        this->data(place).print_clusters(column);
    }

    const char * getHeaderFilePath() const override { return __FILE__; }

private:
    UInt32 clusters_num;
    UInt32 dimensions;
};

}
