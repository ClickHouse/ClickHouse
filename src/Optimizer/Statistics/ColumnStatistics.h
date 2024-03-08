#pragma once

#include <memory>
#include <math.h>
#include <DataTypes/IDataType.h>
#include <Optimizer/Statistics/Histogram.h>
#include <base/types.h>

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
    ColumnStatistics(Float64 value) : ColumnStatistics(value, value, 1.0, 1.0, {}, false, {}) { }

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

    /// Calculate selectivity by value and adjust min_value, max_value and ndv.
    Float64 calculateByValue(OP_TYPE, Float64 value);

    /// Union min_value/max_value with others
    void mergeColumnValueByUnion(ColumnStatisticsPtr other);

    /// Intersect min_value/max_value with others
    void mergeColumnValueByIntersect(ColumnStatisticsPtr other);

    /// Revert min_value/max_value with others
    void revertColumnValue();

    ColumnStatisticsPtr clone();
    bool isUnKnown() const;

    Float64 getNdv() const;
    void setNdv(Float64 new_value);

    Float64 getMinValue() const;
    void setMinValue(Float64 minValue);

    Float64 getMaxValue() const;
    void setMaxValue(Float64 maxValue);

    Float64 getAvgRowSize() const;
    void setAvgRowSize(Float64 avgRowSize);

    const DataTypePtr & getDataType() const;
    void setDataType(const DataTypePtr & dataType);

    /// Whether value is in column.
    /// For numeric column check whether it is in min_value/max_value range.
    /// For non numeric column is always true.
    bool inRange(Float64 value);

private:
    Float64 calculateForNumber(OP_TYPE op_type, Float64 value);
    Float64 calculateForNaN(OP_TYPE op_type);

    Float64 min_value;
    Float64 max_value;

    /// number of distinct value
    Float64 ndv;
    Float64 avg_row_size;

    /// used to calculate avg_row_size
    DataTypePtr data_type;
    bool is_unknown;

    std::optional<Histogram> histogram;
};

}
