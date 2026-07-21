#include <DataTypes/Serializations/SerializationInfoTuple.h>

#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeTuple.h>

namespace DB
{

SerializationInfoTuple::SerializationInfoTuple(MutableSerializationInfos elems_, Names names_, const Settings & settings_)
    : SerializationInfoNamed(std::move(elems_), std::move(names_), settings_)
{
}

void SerializationInfoTuple::add(const IColumn & column)
{
    SerializationInfoNamed::add(column);

    const auto & column_tuple = assert_cast<const ColumnTuple &>(column);
    const auto & right_elems = column_tuple.getColumns();
    chassert(elems.size() == right_elems.size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->add(*right_elems[i]);
}

MutableSerializationInfoPtr SerializationInfoTuple::clone() const
{
    auto result = std::make_shared<SerializationInfoTuple>(cloneElements(), names, settings);
    result->data = data;
    return result;
}

MutableSerializationInfoPtr SerializationInfoTuple::createWithType(
    const IDataType & old_type,
    const IDataType & new_type,
    const Settings & new_settings) const
{
    const auto & old_tuple = assert_cast<const DataTypeTuple &>(old_type);
    const auto & new_tuple = assert_cast<const DataTypeTuple &>(new_type);

    const auto & old_elements = old_tuple.getElements();
    const auto & new_elements = new_tuple.getElements();
    chassert(elems.size() == old_elements.size());
    chassert(elems.size() == new_elements.size());

    MutableSerializationInfos infos;
    infos.reserve(elems.size());
    for (size_t i = 0; i < elems.size(); ++i)
        infos.push_back(elems[i]->createWithType(*old_elements[i], *new_elements[i], new_settings));

    return std::make_shared<SerializationInfoTuple>(std::move(infos), names, new_settings);
}

}
