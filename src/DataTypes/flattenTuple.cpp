#include <DataTypes/flattenTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>

namespace DB
{

namespace
{

void flattenTupleTypeImpl(const DataTypePtr & type, const String & current_path, std::vector<String> & flattened_paths, DataTypes & flattened_types)
{
    if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(removeNullable(type).get()))
    {
        const auto & tuple_names = type_tuple->getElementNames();
        const auto & tuple_types = type_tuple->getElements();

        for (size_t i = 0; i < tuple_names.size(); ++i)
        {
            String path = current_path.empty() ? tuple_names[i] : current_path + "." + tuple_names[i];
            flattenTupleTypeImpl(tuple_types[i], path, flattened_paths, flattened_types);
        }
    }
    else if (const auto * type_array = typeid_cast<const DataTypeArray *>(type.get()))
    {
        std::vector<String> flattened_element_paths;
        DataTypes flattened_element_types;

        flattenTupleTypeImpl(type_array->getNestedType(), current_path, flattened_element_paths, flattened_element_types);
        chassert(flattened_element_paths.size() == flattened_element_types.size());

        for (size_t i = 0; i < flattened_element_paths.size(); ++i)
        {
            flattened_paths.push_back(flattened_element_paths[i]);
            flattened_types.push_back(std::make_shared<DataTypeArray>(flattened_element_types[i]));
        }
    }
    else
    {
        flattened_paths.push_back(current_path);
        flattened_types.push_back(type);
    }
}

void flattenTupleColumnImpl(const ColumnPtr & column, Columns & flattened_columns, Columns & offsets_columns)
{
    if (const auto * column_tuple = checkAndGetColumn<ColumnTuple>(removeNullable(column).get()))
    {
        const auto & subcolumns = column_tuple->getColumns();
        for (const auto & subcolumn : subcolumns)
            flattenTupleColumnImpl(subcolumn, flattened_columns, offsets_columns);
    }
    else if (const auto * column_array = checkAndGetColumn<ColumnArray>(column.get()))
    {
        offsets_columns.push_back(column_array->getOffsetsPtr());
        flattenTupleColumnImpl(column_array->getDataPtr(), flattened_columns, offsets_columns);
        offsets_columns.pop_back();
    }
    else
    {
        if (!offsets_columns.empty())
        {
            auto new_column = ColumnArray::create(column, offsets_columns.back());
            for (auto it = offsets_columns.rbegin() + 1; it != offsets_columns.rend(); ++it)
                new_column = ColumnArray::create(new_column, *it);

            flattened_columns.push_back(std::move(new_column));
        }
        else
        {
            flattened_columns.push_back(column);
        }
    }
}

}

DataTypePtr flattenTuple(const DataTypePtr & type)
{
    bool is_nullable_tuple = typeid_cast<const DataTypeNullable *>(type.get()) != nullptr;
    std::vector<String> flattened_paths;
    DataTypes flattened_types;
    flattenTupleTypeImpl(removeNullable(type), "", flattened_paths, flattened_types);

    DataTypePtr result_type = std::make_shared<DataTypeTuple>(flattened_types, flattened_paths);
    return is_nullable_tuple ? makeNullable(result_type) : result_type;
}

ColumnPtr flattenTuple(const ColumnPtr & column)
{
    bool is_nullable_tuple = typeid_cast<const ColumnNullable *>(column.get()) != nullptr;
    Columns flattened_columns;
    Columns offset_columns;
    flattenTupleColumnImpl(removeNullable(column), flattened_columns, offset_columns);
    ColumnPtr result_column = ColumnTuple::create(flattened_columns);
    return is_nullable_tuple ? makeNullable(result_column) : result_column;
}

}
