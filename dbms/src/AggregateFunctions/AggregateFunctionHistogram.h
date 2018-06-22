#pragma once

#include <Common/PODArray.h>
#include <Common/RadixSort.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>

#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/VarInt.h>

#include <AggregateFunctions/IAggregateFunction.h>

namespace DB {

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
}

/**
 * distance compression algorigthm implementation
 * http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf
 */
class AggregateFunctionHistogramData
{
public:
    using Mean = Float32;
    using Weight = Float32;

private:
    struct WeightedValue {
        Mean mean;
        Weight weight;

        WeightedValue operator + (const WeightedValue& other)
        {
            return {(other.mean * other.weight + mean * weight) / (other.weight + weight), other.weight + weight};
        }
    };

    struct RadixSortTraits
    {
        using Element = WeightedValue;
        using Key = Mean;
        using CountType = UInt32;
        using KeyBits = UInt32;

        static constexpr size_t PART_SIZE_BITS = 8;

        using Transform = RadixSortFloatTransform<KeyBits>;
        using Allocator = RadixSortMallocAllocator;

        static Key & extractKey(Element & elem) { return elem.mean; }
    };

private:
    UInt32 max_bins;
    Mean lower_bound;
    Mean upper_bound;

    static constexpr UInt32 preallocated_bins = 16;
    static constexpr Float32 epsilon = 1e-8;

    // Weighted values representation of histogram.
    // We allow up to max_bins * 2 values stored in intermediate states
    PODArray<WeightedValue, preallocated_bins * sizeof(WeightedValue), Allocator<false>> points;

private:
    void sort()
    {
        RadixSort<RadixSortTraits>::execute(&points[0], points.size());
    }

    /**
     * Repeatedly fuse until max_bins bins left
     */
    void compress()
    {
        sort();
        while (points.size() > max_bins)
        {
            size_t min_index = 0;
            auto quality = [&](size_t i)
            {
                return points[i + 1].mean - points[i].mean;
            };
            for (size_t i = 0; i + 1 != points.size(); ++i)
            {
                if (quality(min_index) > quality(i))
                {
                    min_index = i;
                }
            }

            points[min_index] = points[min_index] + points[min_index + 1];
            for (size_t i = min_index + 1; i + 1 < points.size(); ++i)
            {
                points[i] = points[i + 1];
            }
            points.pop_back();
        }
    }

    /***
     * Delete too close points from histogram
     */
    void unique()
    {
        sort();
        auto l = points.begin();
        for (auto r = std::next(l); r != points.end(); r++)
        {
            if (abs(l->mean - r->mean) < epsilon)
            {
                *l = *l + *r;
            }
            else
            {
                l++;
                *l = *r;
            }
        }
        points.resize(l - points.begin() + 1);
    }

public:
    AggregateFunctionHistogramData(UInt32 max_bins)
        : max_bins(max_bins)
    {
    }

    void insertResultInto(ColumnVector<Mean>& to_lower, ColumnVector<Mean>& to_upper, ColumnVector<Weight>& to_weights) {
        compress();

        points.push_back({lower_bound, 0});
        points.push_back({upper_bound, 0});
        unique();

        Weight prev_weight = points[0].weight;
        Mean prev_value = points[0].mean;

        // here we assume that pt.weight/2 points are before pt.mean  and pt.weight/2 are after
        for (size_t i = 1; i < points.size(); i++)
        {
            auto cur_value = points[i].mean;
            auto bin_weight = points[i].weight / 2 + prev_weight;

            // all points are before
            if (i + 1 == points.size())
            {
                bin_weight += points[i].weight / 2;
            }

            to_lower.insert(prev_value);
            to_upper.insert(cur_value);
            to_weights.insert(bin_weight / (cur_value - prev_value));

            prev_weight = points[i].weight / 2;
            prev_value = points[i].mean;
        }
    }

    void add(Mean value, Weight weight)
    {
        points.push_back({value, weight});
        lower_bound = std::min(lower_bound, value);
        upper_bound = std::max(upper_bound, value);

        if (points.size() >= max_bins * 2)
        {
            compress();
        }
    }

    void merge(const AggregateFunctionHistogramData& other)
    {
        lower_bound = std::min(lower_bound, other.lower_bound);
        upper_bound = std::max(lower_bound, other.upper_bound);
        for (auto pt: other.points) {
            add(pt.mean, pt.weight);
        }
    }

    void write(WriteBuffer & buf) const
    {
        buf.write(reinterpret_cast<const char *>(&lower_bound), sizeof(lower_bound));
        buf.write(reinterpret_cast<const char *>(&upper_bound), sizeof(upper_bound));

        writeVarUInt(points.size(), buf);
        buf.write(reinterpret_cast<const char *>(&points[0]), points.size() * sizeof(points[0]));
    }

    void read(ReadBuffer & buf)
    {
        buf.read(reinterpret_cast<char *>(&lower_bound), sizeof(lower_bound));
        buf.read(reinterpret_cast<char *>(&upper_bound), sizeof(upper_bound));

        size_t size = 0;
        readVarUInt(size, buf);

        if (size > max_bins)
            throw Exception("Too many bins", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        points.resize(size);
        buf.read(reinterpret_cast<char *>(&points[0]), size * sizeof(points[0]));
    }

};

template <typename T>
class AggregateFunctionHistogram final: public IAggregateFunctionHelper<AggregateFunctionHistogram<T>>
{
private:
    using Data = AggregateFunctionHistogramData;

    UInt32 max_bins;

    Data& data(AggregateDataPtr place) const
    {
        return *reinterpret_cast<Data*>(place);
    }

    const Data& data(ConstAggregateDataPtr place) const
    {
        return *reinterpret_cast<const Data*>(place);
    }

public:
    AggregateFunctionHistogram(UInt32 max_bins)
        : max_bins(max_bins)
    {
    }

    void destroy(AggregateDataPtr place) const noexcept override
    {
        data(place).~Data();
    }

    bool hasTrivialDestructor() const override
    {
        return std::is_trivially_destructible_v<Data>;
    }

    size_t alignOfData() const override
    {
        return alignof(Data);
    }

    size_t sizeOfData() const override
    {
        return sizeof(Data);
    }

    void create(AggregateDataPtr place) const override
    {
        new (place) Data(max_bins);
    }

    DataTypePtr getReturnType() const override
    {
        DataTypes types;
        auto mean = std::make_shared<DataTypeNumber<Data::Mean>>();
        auto weight = std::make_shared<DataTypeNumber<Data::Weight>>();

        // lower bound
        types.emplace_back(mean);
        // upper bound
        types.emplace_back(mean);
        // weight
        types.emplace_back(weight);
        auto tuple = std::make_shared<DataTypeTuple>(types);

        return std::make_shared<DataTypeArray>(tuple);
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto val = static_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
        this->data(place).add(static_cast<Data::Mean>(val), 1);
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

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        auto& data = this->data(const_cast<AggregateDataPtr>(place));

        auto & to_array = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = to_array.getOffsets();
        auto & to_tuple = static_cast<ColumnTuple &>(to_array.getData());

        auto & to_lower = static_cast<ColumnVector<Data::Mean> &>(to_tuple.getColumn(0));
        auto & to_upper = static_cast<ColumnVector<Data::Mean> &>(to_tuple.getColumn(1));
        auto & to_weights = static_cast<ColumnVector<Data::Weight> &>(to_tuple.getColumn(2));
        data.insertResultInto(to_lower, to_upper, to_weights);

        offsets_to.push_back(to_tuple.size());
    }

    const char * getHeaderFilePath() const override { return __FILE__; }

    String getName() const override { return "histogram"; }
};

}
