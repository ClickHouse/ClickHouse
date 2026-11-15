#pragma once

#include <optional>
#include <utility>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <base/defines.h>


namespace DB
{

/// Writes the result column of a `timeSeries*ToGrid` aggregate function: one row per aggregation state, and each row is
/// an array with one element per grid point, which is the result for that grid point or NULL if there is no result.
template <typename ResultType>
class AggregateFunctionTimeSeriesResultWriter
{
public:
    /// Array(Nullable(ResultType)) for a numeric result.
    static DataTypePtr createResultType()
    {
        return createResultType(std::make_shared<DataTypeNumber<ResultType>>());
    }

    /// Array(Nullable(element_type)) for a result keeping the data type of an argument (e.g. DateTime64 with its scale).
    static DataTypePtr createResultType(const DataTypePtr & element_type)
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(element_type));
    }

    AggregateFunctionTimeSeriesResultWriter(IColumn & column, size_t grid_size_)
        : grid_size(grid_size_)
        , offsets(typeid_cast<ColumnArray &>(column).getOffsets())
        , data(typeid_cast<ColumnVectorOrDecimal<ResultType> &>(typeid_cast<ColumnNullable &>(typeid_cast<ColumnArray &>(column).getData()).getNestedColumn()).getData())
        , null_map(typeid_cast<ColumnNullable &>(typeid_cast<ColumnArray &>(column).getData()).getNullMapData())
    {
        chassert(data.size() == null_map.size(), "Sizes of nested column and null map of Nullable column are not equal");
    }

    /// Reserves space for `rows_count` more rows.
    void reserve(size_t rows_count)
    {
        offsets.reserve(offsets.size() + rows_count);
        data.reserve(data.size() + rows_count * grid_size);
        null_map.reserve(null_map.size() + rows_count * grid_size);
    }

    /// Adds a row of `grid_size` elements, which are then filled by `store`.
    void addRow()
    {
        offsets.push_back(offsets.empty() ? grid_size : offsets.back() + grid_size);
        row_begin = data.size();
        data.resize(row_begin + grid_size);
        null_map.resize(row_begin + grid_size);
    }

    /// Stores the result for the grid point `grid_index` of the current row.
    void store(size_t grid_index, const std::optional<ResultType> & result)
    {
        chassert(grid_index < grid_size);
        data[row_begin + grid_index] = result ? *result : ResultType{};
        null_map[row_begin + grid_index] = !result;
    }

private:
    const size_t grid_size;
    ColumnArray::Offsets & offsets;
    typename ColumnVectorOrDecimal<ResultType>::Container & data;
    NullMap & null_map;
    size_t row_begin = 0;
};


/// The results are pairs of numbers, stored as Array(Tuple(Nullable(T1), Nullable(T2))) with both elements NULL where
/// there is no result.
template <typename T1, typename T2>
class AggregateFunctionTimeSeriesResultWriter<std::pair<T1, T2>>
{
public:
    /// Array(Tuple(<name1> Nullable(T1), <name2> Nullable(T2)))
    static DataTypePtr createResultType(const Strings & tuple_element_names)
    {
        DataTypes element_types{
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNumber<T1>>()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNumber<T2>>())};
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(std::move(element_types), tuple_element_names));
    }

    AggregateFunctionTimeSeriesResultWriter(IColumn & column, size_t grid_size_)
        : grid_size(grid_size_)
        , offsets(typeid_cast<ColumnArray &>(column).getOffsets())
        , first_data(getData<T1>(column, 0))
        , first_null_map(getNullMap(column, 0))
        , second_data(getData<T2>(column, 1))
        , second_null_map(getNullMap(column, 1))
    {
        chassert(first_data.size() == first_null_map.size() && second_data.size() == second_null_map.size(),
                 "Sizes of nested column and null map of Nullable column are not equal");
    }

    void reserve(size_t rows_count)
    {
        offsets.reserve(offsets.size() + rows_count);
        first_data.reserve(first_data.size() + rows_count * grid_size);
        first_null_map.reserve(first_null_map.size() + rows_count * grid_size);
        second_data.reserve(second_data.size() + rows_count * grid_size);
        second_null_map.reserve(second_null_map.size() + rows_count * grid_size);
    }

    void addRow()
    {
        offsets.push_back(offsets.empty() ? grid_size : offsets.back() + grid_size);
        row_begin = first_data.size();
        first_data.resize(row_begin + grid_size);
        first_null_map.resize(row_begin + grid_size);
        second_data.resize(row_begin + grid_size);
        second_null_map.resize(row_begin + grid_size);
    }

    void store(size_t grid_index, const std::optional<std::pair<T1, T2>> & result)
    {
        chassert(grid_index < grid_size);
        first_data[row_begin + grid_index] = result ? result->first : T1{};
        second_data[row_begin + grid_index] = result ? result->second : T2{};
        first_null_map[row_begin + grid_index] = !result;
        second_null_map[row_begin + grid_index] = !result;
    }

private:
    /// The Nullable column of the tuple element `index`.
    static ColumnNullable & getElementColumn(IColumn & column, size_t index)
    {
        return typeid_cast<ColumnNullable &>(typeid_cast<ColumnTuple &>(typeid_cast<ColumnArray &>(column).getData()).getColumn(index));
    }

    template <typename T>
    static typename ColumnVector<T>::Container & getData(IColumn & column, size_t index)
    {
        return typeid_cast<ColumnVector<T> &>(getElementColumn(column, index).getNestedColumn()).getData();
    }

    static NullMap & getNullMap(IColumn & column, size_t index)
    {
        return getElementColumn(column, index).getNullMapData();
    }

    const size_t grid_size;
    ColumnArray::Offsets & offsets;
    typename ColumnVector<T1>::Container & first_data;
    NullMap & first_null_map;
    typename ColumnVector<T2>::Container & second_data;
    NullMap & second_null_map;
    size_t row_begin = 0;
};

}
