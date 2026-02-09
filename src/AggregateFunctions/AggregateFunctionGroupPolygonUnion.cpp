#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Helpers.h>
#include <Functions/geometryConverters.h>
#include <boost/geometry.hpp>

namespace DB
{

template <typename T>
struct AggregateFunctionGroupPolygonUnionData
{
    // This is where you accumulate polygons during aggregation
    MultiPolygon<T> accumulated_union;
    bool is_first = true;  // Track if this is the first polygon
    
    // Optional: You might need an Arena allocator if storing large data
};

template <typename T>
class AggregateFunctionGroupPolygonUnion
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupPolygonUnionData<T>, AggregateFunctionGroupPolygonUnion<T>>
{

private:
    using State = AggregateFunctionGroupPolygonUnionData<T>;

public:
    AggregateFunctionGroupPolygonUnion(const DataTypePtr & argument_type, const Array & parameters_)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupPolygonUnionData<T>, 
        AggregateFunctionGroupPolygonUnion<T>>({argument_type}, parameters_, argument_type) {}
    
    AggregateFunctionGroupPolygonUnion(const DataTypePtr & argument_type, const Array & parameters_, const DataTypePtr & result_type_)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupPolygonUnionData<T>,
        AggregateFunctionGroupPolygonUnion<T>>({argument_type}, parameters_, result_type_) {}

    String getName() const override { return "groupPolygonUnion"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & state = this->data(place);
        const auto & column = assert_cast<const ColumnArray &>(*columns[0]);
        const auto & offsets = column.getOffsets();
        const auto & data_column = column.getData();

        // Get the current polygon from the column
        size_t offset = offsets[row_num - 1];
        size_t size = offsets[row_num] - offset;
        
        // Convert the column data to a MultiPolygon
        MultiPolygon<T> current_polygon = ColumnToMultiPolygonsConverter<T>::convert(column.getDataPtr(), offset, size);

        // Union the current polygon with the accumulated union
        if (state.is_first)
        {
            state.accumulated_union = current_polygon;  // Initialize with the first polygon
            state.is_first = false;
        }
        else
        {
            MultiPolygon<T> new_union;
            boost::geometry::union_(state.accumulated_union, current_polygon, new_union);
            state.accumulated_union = std::move(new_union);  // Update the accumulated union
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & state = this->data(place);
        const auto & rhs_state = this->data(rhs);

        if (rhs_state.is_first)
            return;  // Nothing to merge

        if (state.is_first)
        {
            state.accumulated_union = rhs_state.accumulated_union;  // Take the union from rhs
            state.is_first = false;
        }
        else
        {
            MultiPolygon<T> new_union;
            boost::geometry::union_(state.accumulated_union, rhs_state.accumulated_union, new_union);
            state.accumulated_union = std::move(new_union);  // Update the accumulated union
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        // Serialize the accumulated union (you'll need to implement this)
        // For example, you could write the number of polygons and then each polygon's data
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        // Deserialize the accumulated union (you'll need to implement this)
        // This should mirror the serialize method
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        // Insert the final accumulated union into the result column
        // You might need to convert the MultiPolygon back to a column format
    }
};


}
