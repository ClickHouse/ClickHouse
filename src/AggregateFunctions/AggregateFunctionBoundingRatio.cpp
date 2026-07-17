#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/assert_cast.h>
#include <Common/transformEndianness.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/** Tracks the leftmost and rightmost (x, y) data points.
  */
struct AggregateFunctionBoundingRatioData
{
    struct Point
    {
        Float64 x;
        Float64 y;
    };

    bool empty = true;
    Point left;
    Point right;

    void add(Float64 x, Float64 y)
    {
        Point point{x, y};

        if (empty)
        {
            left = point;
            right = point;
            empty = false;
        }
        else if (point.x < left.x)
        {
            left = point;
        }
        else if (point.x > right.x)
        {
            right = point;
        }
    }

    void merge(const AggregateFunctionBoundingRatioData & other)
    {
        if (empty)
        {
            *this = other;
        }
        else if (other.empty)
        {
            // if other.empty = true, other.x/other.y may be uninitialized values,
            // so don't use them to update this->state
        }
        else
        {
            if (other.left.x < left.x)
                left = other.left;
            if (other.right.x > right.x)
                right = other.right;
        }
    }

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
};

template <std::endian endian>
inline void transformEndianness(AggregateFunctionBoundingRatioData::Point & p)
{
    DB::transformEndianness<endian>(p.x);
    DB::transformEndianness<endian>(p.y);
}

void AggregateFunctionBoundingRatioData::serialize(WriteBuffer & buf) const
{
    writeBinaryLittleEndian(empty, buf);

    if (!empty)
    {
        writeBinaryLittleEndian(left, buf);
        writeBinaryLittleEndian(right, buf);
    }
}

void AggregateFunctionBoundingRatioData::deserialize(ReadBuffer & buf)
{
    readBinaryLittleEndian(empty, buf);

    if (!empty)
    {
        readBinaryLittleEndian(left, buf);
        readBinaryLittleEndian(right, buf);
    }
}

inline void writeBinary(const AggregateFunctionBoundingRatioData::Point & p, WriteBuffer & buf)
{
    writePODBinary(p, buf);
}

inline void readBinary(AggregateFunctionBoundingRatioData::Point & p, ReadBuffer & buf)
{
    readPODBinary(p, buf);
}


class AggregateFunctionBoundingRatio final : public IAggregateFunctionDataHelper<AggregateFunctionBoundingRatioData, AggregateFunctionBoundingRatio>
{
private:
    /** Calculates the slope of a line between leftmost and rightmost data points.
      * (y2 - y1) / (x2 - x1)
      */
    static Float64 NO_SANITIZE_UNDEFINED getBoundingRatio(const AggregateFunctionBoundingRatioData & data)
    {
        if (data.empty)
            return std::numeric_limits<Float64>::quiet_NaN();

        return (data.right.y - data.left.y) / (data.right.x - data.left.x);
    }

public:
    String getName() const override
    {
        return "boundingRatio";
    }

    explicit AggregateFunctionBoundingRatio(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<AggregateFunctionBoundingRatioData, AggregateFunctionBoundingRatio>(arguments, {}, std::make_shared<DataTypeFloat64>())
    {
        const auto * x_arg = arguments.at(0).get();
        const auto * y_arg = arguments.at(1).get();

        if (!x_arg->isValueRepresentedByNumber() || !y_arg->isValueRepresentedByNumber())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Illegal types of arguments of aggregate function {}, must have number representation.",
                            getName());
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        /// NOTE Slightly inefficient.
        const auto x = columns[0]->getFloat64(row_num);
        const auto y = columns[1]->getFloat64(row_num);
        data(place).add(x, y);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).merge(data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnFloat64 &>(to).getData().push_back(getBoundingRatio(data(place)));
    }
};


AggregateFunctionPtr createAggregateFunctionRate(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    if (argument_types.size() < 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Aggregate function {} requires at least two arguments",
                        name);

    return std::make_shared<AggregateFunctionBoundingRatio>(argument_types);
}

}

void registerAggregateFunctionRate(AggregateFunctionFactory & factory)
{
    factory.registerFunction("boundingRatio", createAggregateFunctionRate);
}

}
