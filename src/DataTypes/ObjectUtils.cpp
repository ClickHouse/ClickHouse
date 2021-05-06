#include <DataTypes/ObjectUtils.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>
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

size_t getNumberOfDimensions(const IDataType & type)
{
    if (const auto * type_array = typeid_cast<const DataTypeArray *>(&type))
        return type_array->getNumberOfDimensions();
    return 0;
}

size_t getNumberOfDimensions(const IColumn & column)
{
    if (const auto * column_array = checkAndGetColumn<ColumnArray>(column))
        return column_array->getNumberOfDimensions();
    return 0;
}

DataTypePtr createArrayOfType(DataTypePtr type, size_t dimension)
{
    for (size_t i = 0; i < dimension; ++i)
        type = std::make_shared<DataTypeArray>(type);
    return type;
}

DataTypePtr getDataTypeByColumn(const IColumn & column)
{
    auto idx = column.getDataType();
    if (WhichDataType(column.getDataType()).isSimple())
        return DataTypeFactory::instance().get(getTypeName(idx));

    if (const auto * column_array = typeid_cast<const ColumnArray *>(&column))
        return std::make_shared<DataTypeArray>(getDataTypeByColumn(column_array->getData()));

    /// TODO: add more types.
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get data type of column {}", column.getFamilyName());
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
                tuple_types.push_back(getDataTypeByColumn(*subcolumn.data));
                tuple_columns.push_back(subcolumn.data);
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
        assert(!subtypes.empty());

        size_t first_dim = getNumberOfDimensions(*subtypes[0]);
        for (size_t i = 1; i < subtypes.size(); ++i)
            if (first_dim != getNumberOfDimensions(*subtypes[i]))
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "Uncompatible types of subcolumn '{}': {} and {}",
                    name, subtypes[0]->getName(), subtypes[i]->getName());

        tuple_names.push_back(name);
        tuple_types.push_back(getLeastSupertype(subtypes, /*allow_conversion_to_string=*/ true));
    }

    return std::make_shared<DataTypeTuple>(tuple_types, tuple_names);
}

void optimizeTypesOfObjectColumns(MutableColumns & columns)
{
    for (auto & column : columns)
        if (auto * column_object = typeid_cast<ColumnObject *>(column.get()))
            column_object->optimizeTypesOfSubcolumns();
}

}
