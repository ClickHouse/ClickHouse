#pragma once

#include <Common/Arena.h>
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
    UInt32 size;
    Mean lower_bound;
    Mean upper_bound;

    static constexpr Float32 epsilon = 1e-8;

    // Weighted values representation of histogram.
    // We allow up to max_bins * 2 values stored in intermediate states
    WeightedValue* points;

private:
    void sort()
    {
        RadixSort<RadixSortTraits>::execute(points, size);
    }

    /**
     * Repeatedly fuse until max_bins bins left
     */
    void compress()
    {
        sort();
        while (size > max_bins)
        {
            size_t min_index = 0;
            auto quality = [&](size_t i)
            {
                return points[i + 1].mean - points[i].mean;
            };
            for (size_t i = 0; i + 1 != size; ++i)
            {
                if (quality(min_index) > quality(i))
                {
                    min_index = i;
                }
            }

            points[min_index] = points[min_index] + points[min_index + 1];
            for (size_t i = min_index + 1; i + 1 < size; ++i)
            {
                points[i] = points[i + 1];
            }
            size--;
        }
    }

    /***
     * Delete too close points from histogram.
     * Assume that points are sorted.
     */
    void unique()
    {
        size_t l = 0;
        for (auto r = l + 1; r != size; r++)
        {
            if (abs(points[l].mean - points[r].mean) < epsilon)
            {
                points[l] = points[l] + points[r];
            }
            else
            {
                l++;
                points[l] = points[r];
            }
        }
        size = l + 1;
    }

    void init(Arena* arena)
    {
        points = reinterpret_cast<WeightedValue*>(arena->alloc(max_bins * 2 * sizeof(WeightedValue)));
    }

public:
    AggregateFunctionHistogramData(UInt32 max_bins)
        : max_bins(max_bins)
        , size(0)
        , points(nullptr)
    {
    }

    void insertResultInto(ColumnVector<Mean>& to_lower, ColumnVector<Mean>& to_upper, ColumnVector<Weight>& to_weights) {
        compress();
        unique();

        for (size_t i = 0; i < size; i++)
        {
            to_lower.insert((i == 0) ? lower_bound : (points[i].mean + points[i - 1].mean) / 2);
            to_upper.insert((i + 1 == size) ? upper_bound : (points[i].mean + points[i + 1].mean) / 2);

            // linear density approximation
            Weight lower_weight = (i == 0) ? points[i].weight : ((points[i - 1].weight) + points[i].weight * 3) / 4;
            Weight upper_weight = (i + 1 == size) ? points[i].weight : (points[i + 1].weight + points[i].weight * 3) / 4;
            to_weights.insert((lower_weight + upper_weight) / 2);
        }
    }

    void add(Mean value, Weight weight, Arena* arena)
    {
        if (!points)
            init(arena);
        points[size++] = {value, weight};
        lower_bound = std::min(lower_bound, value);
        upper_bound = std::max(upper_bound, value);

        if (size > max_bins * 2)
        {
            compress();
        }
    }

    void merge(const AggregateFunctionHistogramData& other, Arena* arena)
    {
        lower_bound = std::min(lower_bound, other.lower_bound);
        upper_bound = std::max(lower_bound, other.upper_bound);
        for (size_t i = 0; i < other.size; i++)
        {
            add(other.points[i].mean, other.points[i].weight, arena);
        }
    }

    void write(WriteBuffer & buf) const
    {
        buf.write(reinterpret_cast<const char *>(&lower_bound), sizeof(lower_bound));
        buf.write(reinterpret_cast<const char *>(&upper_bound), sizeof(upper_bound));

        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const char *>(points), size * sizeof(WeightedValue));
    }

    void read(ReadBuffer & buf, Arena* arena)
    {
        buf.read(reinterpret_cast<char *>(&lower_bound), sizeof(lower_bound));
        buf.read(reinterpret_cast<char *>(&upper_bound), sizeof(upper_bound));

        readVarUInt(size, buf);

        if (size > max_bins)
            throw Exception("Too many bins", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        if (!points)
            init(arena);

        buf.read(reinterpret_cast<char *>(points), size * sizeof(points[0]));
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

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto val = static_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
        this->data(place).add(static_cast<Data::Mean>(val), 1, arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        this->data(place).read(buf, arena);
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
