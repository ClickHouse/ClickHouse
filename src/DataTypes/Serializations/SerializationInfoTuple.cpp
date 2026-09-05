#include <DataTypes/Serializations/SerializationInfoTuple.h>

#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Serializations/SerializationInfoNullable.h>

namespace DB
{

SerializationInfoTuple::SerializationInfoTuple(MutableSerializationInfos elems_, Names names_, const Settings & settings_)
    : SerializationInfoNamed(std::move(elems_), std::move(names_), settings_)
{
}

bool SerializationInfoTuple::structureEquals(const SerializationInfo & rhs) const
{
    const auto * rhs_tuple = typeid_cast<const SerializationInfoTuple *>(&rhs);
    if (!rhs_tuple || elems.size() != rhs_tuple->elems.size())
        return false;

    for (size_t i = 0; i < elems.size(); ++i)
        if (!elems[i]->structureEquals(*rhs_tuple->elems[i]))
            return false;

    return true;
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
    const auto & new_names = new_tuple.getElementNames();
    chassert(elems.size() == old_elements.size());

    MutableSerializationInfos infos;
    infos.reserve(new_elements.size());
    for (size_t i = 0; i < new_elements.size(); ++i)
    {
        auto elem_settings = new_settings;
        if (!new_settings.shouldCollectSerializationInfo(*new_elements[i]))
            elem_settings.version = MergeTreeSerializationInfoVersion::WITH_TYPES;
        auto info = new_elements[i]->createSerializationInfo(elem_settings);

        std::optional<size_t> old_position;
        if (old_tuple.hasExplicitNames() && new_tuple.hasExplicitNames())
            old_position = old_tuple.tryGetPositionByName(new_names[i]);
        else if (i < old_elements.size())
            old_position = i;

        if (old_position)
        {
            const auto & old_info = elems[*old_position];
            if (canReuseSerializationInfoForTypeChange(*old_info, *info))
                info = old_info->createWithType(*old_elements[*old_position], *new_elements[i], elem_settings);
            else if (auto reused = tryReuseSerializationInfoThroughNullable(
                         *old_info, *old_elements[*old_position], info, *new_elements[i], elem_settings))
                info = std::move(reused);
        }
        else if (!old_position)
            info->addDefaults(data.num_rows);

        infos.push_back(std::move(info));
    }

    return std::make_shared<SerializationInfoTuple>(std::move(infos), new_names, new_settings);
}

}
