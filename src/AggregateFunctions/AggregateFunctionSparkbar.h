#pragma once

#include <array>
#include <string_view>
#include <DataTypes/DataTypeString.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <base/range.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnString.h>
#include <Common/PODArray.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromString.h>
#include <Common/HashTable/HashMap.h>
#include <Columns/IColumn.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

template<typename X, typename Y>
struct AggregateFunctionSparkbarData
{
    /// TODO: calculate histogram instead of storing all points
    using Points = HashMap<X, Y>;
    Points points;

    X min_x = std::numeric_limits<X>::max();
    X max_x = std::numeric_limits<X>::lowest();

    Y min_y = std::numeric_limits<Y>::max();
    Y max_y = std::numeric_limits<Y>::lowest();

    Y insert(const X & x, const Y & y)
    {
        if (isNaN(y) || y <= 0)
            return 0;

        auto [it, inserted] = points.insert({x, y});
        if (!inserted)
            it->getMapped() += y;
        return it->getMapped();
    }

    void add(X x, Y y)
    {
        auto new_y = insert(x, y);

        min_x = std::min(x, min_x);
        max_x = std::max(x, max_x);

        min_y = std::min(y, min_y);
        max_y = std::max(new_y, max_y);
    }

    void merge(const AggregateFunctionSparkbarData & other)
    {
        if (other.points.empty())
            return;

        for (auto & point : other.points)
        {
            auto new_y = insert(point.getKey(), point.getMapped());
            max_y = std::max(new_y, max_y);
        }

        min_x = std::min(other.min_x, min_x);
        max_x = std::max(other.max_x, max_x);

        min_y = std::min(other.min_y, min_y);
        max_y = std::max(other.max_y, max_y);
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(min_x, buf);
        writeBinary(max_x, buf);
        writeBinary(min_y, buf);
        writeBinary(max_y, buf);
        writeVarUInt(points.size(), buf);

        for (const auto & elem : points)
        {
            writeBinary(elem.getKey(), buf);
            writeBinary(elem.getMapped(), buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(min_x, buf);
        readBinary(max_x, buf);
        readBinary(min_y, buf);
        readBinary(max_y, buf);
        size_t size;
        readVarUInt(size, buf);

        X x;
        Y y;
        for (size_t i = 0; i < size; ++i)
        {
            readBinary(x, buf);
            readBinary(y, buf);
            insert(x, y);
        }
    }
};

template<typename X, typename Y>
class AggregateFunctionSparkbar final
    : public IAggregateFunctionDataHelper<AggregateFunctionSparkbarData<X, Y>, AggregateFunctionSparkbar<X, Y>>
{

private:
    const size_t width = 0;

    /// Range for x specified in parameters.
    const bool is_specified_range_x = false;
    const X begin_x = std::numeric_limits<X>::min();
    const X end_x = std::numeric_limits<X>::max();

    size_t updateFrame(ColumnString::Chars & frame, Y value) const
    {
        static constexpr std::array<std::string_view, 9> bars{" ", "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"};
        const auto & bar = (isNaN(value) || value < 1 || 8 < value) ? bars[0] : bars[static_cast<UInt8>(value)];
        frame.insert(bar.begin(), bar.end());
        return bar.size();
    }

    /**
     *  The minimum value of y is rendered as the lowest height "▁",
     *  the maximum value of y is rendered as the highest height "█", and the middle value will be rendered proportionally.
     *  If a bucket has no y value, it will be rendered as " ".
     */
    void render(ColumnString & to_column, const AggregateFunctionSparkbarData<X, Y> & data) const
    {
        auto & values = to_column.getChars();
        auto & offsets = to_column.getOffsets();

        if (data.points.empty())
        {
            values.push_back('\0');
            offsets.push_back(offsets.empty() ? 1 : offsets.back() + 1);
            return;
        }

        auto from_x = is_specified_range_x ? begin_x : data.min_x;
        auto to_x = is_specified_range_x ? end_x : data.max_x;

        if (from_x >= to_x)
        {
            size_t sz = updateFrame(values, 8);
            values.push_back('\0');
            offsets.push_back(offsets.empty() ? sz + 1 : offsets.back() + sz + 1);
            return;
        }

        PaddedPODArray<Y> histogram(width, 0);
        PaddedPODArray<UInt64> fhistogram(width, 0);

        for (const auto & point : data.points)
        {
            if (point.getKey() < from_x || to_x < point.getKey())
                continue;

            X delta = to_x - from_x;
            if (delta < std::numeric_limits<X>::max())
                delta = delta + 1;

            X value = point.getKey() - from_x;
            Float64 w = histogram.size();
            size_t index = std::min<size_t>(static_cast<size_t>(w / delta * value), histogram.size() - 1);

            if (std::numeric_limits<Y>::max() - histogram[index] > point.getMapped())
            {
                histogram[index] += point.getMapped();
                fhistogram[index] += 1;
            }
            else
            {
                /// In case of overflow, just saturate
                histogram[index] = std::numeric_limits<Y>::max();
            }
        }

        for (size_t i = 0; i < histogram.size(); ++i)
        {
            if (fhistogram[i] > 0)
                histogram[i] /= fhistogram[i];
        }

        Y y_max = 0;
        for (auto & y : histogram)
        {
            if (isNaN(y) || y <= 0)
                continue;
            y_max = std::max(y_max, y);
        }

        if (y_max == 0)
        {
            values.push_back('\0');
            offsets.push_back(offsets.empty() ? 1 : offsets.back() + 1);
            return;
        }

        for (auto & y : histogram)
        {
            if (isNaN(y) || y <= 0)
                y = 0;
            else
                y = y * 7 / y_max + 1;
        }

        size_t sz = 0;
        for (const auto & y : histogram)
            sz += updateFrame(values, y);

        values.push_back('\0');
        offsets.push_back(offsets.empty() ? sz + 1 : offsets.back() + sz + 1);
    }

public:
    AggregateFunctionSparkbar(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionSparkbarData<X, Y>, AggregateFunctionSparkbar>(arguments, params, std::make_shared<DataTypeString>())
        , width(params.empty() ? 0 : params.at(0).safeGet<UInt64>())
        , is_specified_range_x(params.size() >= 3)
        , begin_x(is_specified_range_x ? static_cast<X>(params.at(1).safeGet<X>()) : std::numeric_limits<X>::min())
        , end_x(is_specified_range_x ? static_cast<X>(params.at(2).safeGet<X>()) : std::numeric_limits<X>::max())
    {
        if (width < 2 || 1024 < width)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter width must be in range [2, 1024]");

        if (begin_x >= end_x)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter `min_x` must be less than `max_x`");
    }

    String getName() const override
    {
        return "sparkbar";
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * /*arena*/) const override
    {
        X x = assert_cast<const ColumnVector<X> *>(columns[0])->getData()[row_num];
        if (begin_x <= x && x <= end_x)
        {
            Y y = assert_cast<const ColumnVector<Y> *>(columns[1])->getData()[row_num];
            this->data(place).add(x, y);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr __restrict rhs, Arena * /*arena*/) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    bool allocatesMemoryInArena() const override { return false; }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * /*arena*/) const override
    {
        auto & to_column = assert_cast<ColumnString &>(to);
        const auto & data = this->data(place);
        render(to_column, data);
    }
};

}
