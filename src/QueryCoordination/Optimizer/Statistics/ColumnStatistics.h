#pragma once

#include <math.h>
#include <memory>
#include <base/types.h>
#include <DataTypes/IDataType.h>
#include <QueryCoordination/Optimizer/Statistics/Histogram.h>

namespace DB
{

class ColumnStatistics;

using ColumnStatisticsPtr = std::shared_ptr<ColumnStatistics>;
using ColumnStatisticsMap = std::unordered_map<String, ColumnStatisticsPtr>;

class ColumnStatistics
{
public:
    enum OP_TYPE
    {
        EQUAL,
        NOT_EQUAL,
        GREATER,
        GREATER_OR_EQUAL,
        LESS,
        LESS_OR_EQUAL
    };

    ColumnStatistics(
        Float64 min_value_,
        Float64 max_value_,
        Float64 ndv_,
        Float64 avg_row_size_,
        DataTypePtr data_type_,
        bool is_unknown_,
        std::optional<Histogram> histogram_)
        : min_value(min_value_)
        , max_value(max_value_)
        , ndv(ndv_)
        , avg_row_size(avg_row_size_)
        , data_type(data_type_)
        , is_unknown(is_unknown_)
        , histogram(histogram_)
    {
    }

    ColumnStatistics() : ColumnStatistics(0.0, 0.0, 1.0, 1.0, {}, false, {}) { }

    ColumnStatistics(Float64 value)
        : ColumnStatistics(value, value, 1.0, 1.0, {}, false, {})
    {
    }

    ColumnStatistics(Float64 min_value_, Float64 max_value_, Float64 ndv_, Float64 avg_row_size_)
        : ColumnStatistics(min_value_, max_value_, ndv_, avg_row_size_, {}, false, {})
    {
    }

    ColumnStatistics(Float64 min_value_, Float64 max_value_, Float64 ndv_, Float64 avg_row_size_, DataTypePtr data_type_)
        : ColumnStatistics(min_value_, max_value_, ndv_, avg_row_size_, data_type_, data_type_ == nullptr, {})
    {
    }

    static ColumnStatisticsPtr unknown();
    static ColumnStatisticsPtr create(Float64 value);

    /// Calculate selectivity
    Float64 calculateSelectivity(OP_TYPE, Float64 value);
    /// Adjust min_value or max_value by value.
    void updateValues(OP_TYPE, Float64 value);

    void mergeColumnByUnion(ColumnStatisticsPtr other);
    /// Intersect min_value/max_value with others
    void mergeColumnByIntersect(ColumnStatisticsPtr other);

    ColumnStatisticsPtr clone();
    bool isUnKnown() const;

    Float64 getNdv() const;
    void setNdv(Float64 new_value);
private:
    Float64 min_value;

public:
    Float64 getMinValue() const;
    void setMinValue(Float64 minValue);
    Float64 getMaxValue() const;
    void setMaxValue(Float64 maxValue);
    Float64 getAvgRowSize() const;
    void setAvgRowSize(Float64 avgRowSize);

private:
    Float64 max_value;

    /// number of distinct value
    Float64 ndv;
    Float64 avg_row_size;

    DataTypePtr data_type;
    bool is_unknown;

    std::optional<Histogram> histogram;
};

}
