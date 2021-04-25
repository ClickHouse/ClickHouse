#include <DataTypes/ObjectUtils.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnTuple.h>
#include <Common/FieldVisitors.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int LOGICAL_ERROR;
}

static const IDataType * getTypeObject(const DataTypePtr & type)
{
    return typeid_cast<const DataTypeObject *>(type.get());
}

DataTypePtr getDataTypeByColumn(const IColumn & column)
{
    if (column.empty())
        return std::make_shared<DataTypeNothing>();

    return applyVisitor(FieldToDataType(true), column[0]);
}

void convertObjectsToTuples(NamesAndTypesList & columns_list, Block & block)
{
    for (auto & name_type : columns_list)
    {
        if (const auto * type_object = getTypeObject(name_type.type))
        {
            auto & column = block.getByName(name_type.name);

            if (!getTypeObject(column.type))
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "Type for column '{}' mismatch in columns list and in block. In list: {}, in block: {}",
                    name_type.name, name_type.type->getName(), column.type->getName());

            const auto & column_object = assert_cast<const ColumnObject &>(*column.column);
            const auto & subcolumns_map = column_object.getSubcolumns();

            Names tuple_names;
            DataTypes tuple_types;
            Columns tuple_columns;

            for (const auto & [key, subcolumn] : subcolumns_map)
            {
                tuple_names.push_back(key);
                tuple_types.push_back(getDataTypeByColumn(*subcolumn));
                tuple_columns.push_back(subcolumn);
            }

            auto type_tuple = std::make_shared<DataTypeTuple>(tuple_types, tuple_names);
            auto column_tuple = ColumnTuple::create(tuple_columns);

            name_type.type = type_tuple;
            column.type = type_tuple;
            column.column = column_tuple;
        }
    }
}

DataTypePtr getLeastCommonTypeForObject(const DataTypes & types)
{
    std::unordered_map<String, DataTypes> subcolumns_types;
    for (const auto & type : types)
    {
        const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get());
        if (!type_tuple)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Least common type for object can be deduced only from tuples, but {} given", type->getName());

        const auto & tuple_names = type_tuple->getElementNames();
        const auto & tuple_types = type_tuple->getElements();
        assert(tuple_names.size() == tuple_type.size());

        for (size_t i = 0; i < tuple_names.size(); ++i)
            subcolumns_types[tuple_names[i]].push_back(tuple_types[i]);
    }

    Names tuple_names;
    DataTypes tuple_types;

    for (const auto & [name, subtypes] : subcolumns_types)
    {
        tuple_names.push_back(name);
        tuple_types.push_back(getLeastSupertype(subtypes));
    }

    return std::make_shared<DataTypeTuple>(tuple_types, tuple_names);
}

}
