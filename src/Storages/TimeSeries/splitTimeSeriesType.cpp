#include <Storages/TimeSeries/splitTimeSeriesType.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
}


std::pair<DataTypePtr, DataTypePtr> splitTimeSeriesType(const DataTypePtr & time_series_type)
{
    const auto * array_type = typeid_cast<const DataTypeArray *>(time_series_type.get());
    if (!array_type)
        throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD,
            "Expected `Array(Tuple(timestamp, value))` for the `time_series` column, got {}", time_series_type->getName());
    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get());
    if (!tuple_type || (tuple_type->getElements().size() != 2))
        throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD,
            "Expected `Tuple(timestamp, value)` as the element type of the `time_series` column, got {}",
            array_type->getNestedType()->getName());
    return {tuple_type->getElement(0), tuple_type->getElement(1)};
}

}
