#include <DataTypes/Serializations/SerializationInfoObject.h>

#include <Columns/ColumnObject.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeObject.h>

namespace DB
{

void SerializationInfoObject::add(const IColumn & column)
{
    SerializationInfoNamed::add(column);

    const auto & typed_paths = assert_cast<const ColumnObject &>(column).getTypedPaths();
    for (size_t i = 0; i < names.size(); ++i)
        elems[i]->add(*typed_paths.at(names[i]));
}

MutableSerializationInfoPtr SerializationInfoObject::clone() const
{
    auto result = std::make_shared<SerializationInfoObject>(cloneElements(), names, settings);
    result->data = data;
    return result;
}

MutableSerializationInfoPtr SerializationInfoObject::createWithType(
    const IDataType & old_type,
    const IDataType & new_type,
    const Settings & new_settings) const
{
    const auto & old_object = assert_cast<const DataTypeObject &>(old_type);
    const auto & new_object = assert_cast<const DataTypeObject &>(new_type);

    Names new_names;
    new_names.reserve(new_object.getTypedPaths().size());
    for (const auto & [path, _] : new_object.getTypedPaths())
        new_names.push_back(path);
    std::sort(new_names.begin(), new_names.end());

    MutableSerializationInfos new_infos;
    new_infos.reserve(new_names.size());
    for (const auto & path : new_names)
    {
        auto old_type_it = old_object.getTypedPaths().find(path);
        auto old_info_it = name_to_elem.find(path);
        if (old_type_it != old_object.getTypedPaths().end() && old_info_it != name_to_elem.end())
        {
            new_infos.push_back(old_info_it->second->createWithType(
                *old_type_it->second,
                *new_object.getTypedPaths().at(path),
                new_settings));
        }
        else
        {
            auto info = new_object.getTypedPaths().at(path)->createSerializationInfo(new_settings);
            info->addDefaults(data.num_rows);
            new_infos.push_back(std::move(info));
        }
    }

    return std::make_shared<SerializationInfoObject>(std::move(new_infos), std::move(new_names), new_settings);
}

}
