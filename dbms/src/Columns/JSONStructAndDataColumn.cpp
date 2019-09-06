#include <Columns/JSONStructAndDataColumn.h>
#include <Columns/ColumnJSONB.h>
#include "JSONStructAndDataColumn.h"


namespace DB
{

JSONStructAndDataColumn::JSONStructAndDataColumn(
    IColumn * column_, const String & name_, const std::vector<StringRef> & access_path_, JSONStructAndDataColumn * parent_)
    : column(column_), name(name_), access_path(access_path_), parent(parent_)
{
    if (!name.empty())
        access_path.push_back(StringRef(name));
}

IColumn * JSONStructAndDataColumn::getOrCreateMarkColumn()
{
    if (!mark_column)
        mark_column = static_cast<ColumnJSONB *>(column)->createMarkColumn();

    return mark_column;
}

IColumn * JSONStructAndDataColumn::getDataColumn(const DataTypePtr & type)
{
    for (size_t index = 0; index < data_columns.size(); ++index)
        if (type->getTypeId() == data_columns[index].first->getTypeId())
            return data_columns[index].second;

    throw Exception("LOGICAL_ERROR cannot find data column.", ErrorCodes::LOGICAL_ERROR);
}

IColumn * JSONStructAndDataColumn::getOrCreateDataColumn(const DataTypePtr & type)
{
    for (size_t index = 0; index < data_columns.size(); ++index)
        if (type->getTypeId() == data_columns[index].first->getTypeId())
            return data_columns[index].second;

    data_columns.push_back(std::make_pair(type, static_cast<ColumnJSONB *>(column)->createDataColumn(type)));
    return data_columns.back().second;
}

JSONStructAndDataColumn::JSONStructAndDataColumnPtr JSONStructAndDataColumn::getParent()
{
    return !parent ? this->shared_from_this() : parent->shared_from_this();
}

JSONStructAndDataColumn::JSONStructAndDataColumnPtr JSONStructAndDataColumn::getChildren(const StringRef & name)
{
    for (size_t index = 0; index < children.size(); ++index)
        if (children[index]->name == name)
            return children[index];

    return JSONStructAndDataColumnPtr{};
}

JSONStructAndDataColumn::JSONStructAndDataColumnPtr JSONStructAndDataColumn::getOrCreateChildren(const StringRef & name)
{
    if (JSONStructAndDataColumnPtr res = getChildren(name))
        return res;

    children.push_back(std::make_shared<JSONStructAndDataColumn>(column, name.toString(), access_path, this));
    return children.back();
}

JSONStructAndDataColumn::JSONStructAndDataColumnPtr JSONStructAndDataColumn::clone(IColumn * column, const DataCloneFunc & data_clone_function)
{
    JSONStructAndDataColumnPtr res = std::make_shared<JSONStructAndDataColumn>(column);
    return cloneChildren(res, data_clone_function);
}

JSONStructAndDataColumn::JSONStructAndDataColumnPtr JSONStructAndDataColumn::cloneChildren(
    JSONStructAndDataColumn::JSONStructAndDataColumnPtr & cloning_to, const JSONStructAndDataColumn::DataCloneFunc & data_clone_function)
{
    if (!data_columns.empty())
    {
        cloning_to->data_columns.reserve(data_columns.size());
        cloning_to->mark_column = data_clone_function(mark_column, true);
        for (size_t index = 0; index < data_columns.size(); ++index)
            cloning_to->data_columns.push_back(std::pair(data_columns[index].first, data_clone_function(data_columns[index].second, false)));
    }

    if (!children.empty())
    {
        for (size_t index = 0; index < children.size(); ++index)
        {
            JSONStructAndDataColumnPtr clone_children = cloning_to->getOrCreateChildren(StringRef(children[index]->name));
            children[index]->cloneChildren(clone_children, data_clone_function);
        }
    }

    return cloning_to;
}

void JSONStructAndDataColumn::serialize(WriteBuffer & ostr, IDataType::SerializeBinaryBulkSettings & settings,
    const std::function<void(const JSONStructAndDataColumn &, IDataType::SerializeBinaryBulkSettings &)> & data_serialize)
{
    writeBinary(name, ostr);
    writeVarUInt(children.size(), ostr);
    writeVarUInt(data_columns.size(), ostr);
    settings.path.back().tuple_element_name = name;
    settings.path.back().type = IDataType::Substream::JSONElements;

    if (!data_columns.empty())
    {
        data_serialize(*this, settings);
        settings.path.back().tuple_element_name = name;
        settings.path.back().type = IDataType::Substream::JSONElements;
    }

    if (children.size())
    {
        settings.path.push_back(IDataType::Substream::JSONElements);
        for (size_t index = 0; index < children.size(); ++index)
            children[index]->serialize(ostr, settings, data_serialize);
        settings.path.pop_back();
    }
}

void JSONStructAndDataColumn::deserialize(ReadBuffer & istr, IDataType::DeserializeBinaryBulkSettings & settings,
    const std::function<void(JSONStructAndDataColumn &, size_t, IDataType::DeserializeBinaryBulkSettings &)> & data_deserialize)
{
    size_t children_size, data_column_size;

    readBinary(name, istr);
    readVarUInt(children_size, istr);
    readVarUInt(data_column_size, istr);
    settings.path.back().tuple_element_name = name;
    settings.path.back().type = IDataType::Substream::JSONElements;

    if (!name.empty())
        access_path.push_back(StringRef(name));

    if (data_column_size)
    {
        data_deserialize(*this, data_column_size, settings);
        settings.path.back().tuple_element_name = name;
        settings.path.back().type = IDataType::Substream::JSONElements;
    }

    if (children_size)
    {
        children.reserve(children_size);
        settings.path.push_back(IDataType::Substream::JSONElements);
        for (size_t index = 0; index < children_size; ++index)
        {
            children.push_back(std::make_shared<JSONStructAndDataColumn>(column, String{}, access_path, this));
            children.back()->deserialize(istr, settings, data_deserialize);
        }

        settings.path.pop_back();
    }
}

}
