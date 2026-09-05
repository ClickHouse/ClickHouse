#include <DataTypes/Serializations/SerializationInfoObject.h>

#include <Columns/ColumnObject.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/Serializations/SerializationInfoNullable.h>
#include <DataTypes/Serializations/SerializationInfoTuple.h>

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
        const auto & new_path_type = new_object.getTypedPaths().at(path);
        auto path_settings = new_settings;
        if (!new_settings.shouldCollectSerializationInfo(*new_path_type))
            path_settings.version = MergeTreeSerializationInfoVersion::WITH_TYPES;
        auto new_info = new_path_type->createSerializationInfo(path_settings);
        auto old_type_it = old_object.getTypedPaths().find(path);
        auto old_info_it = name_to_elem.find(path);
        const auto * old_info = old_info_it != name_to_elem.end() ? old_info_it->second.get() : nullptr;
        const auto * new_info_ptr = new_info.get();
        if (old_type_it != old_object.getTypedPaths().end()
            && old_info
            && canReuseSerializationInfoForTypeChange(*old_info, *new_info_ptr))
        {
            new_info = old_info_it->second->createWithType(*old_type_it->second, *new_path_type, path_settings);
        }
        else if (old_type_it != old_object.getTypedPaths().end() && old_info)
        {
            if (auto reused = tryReuseSerializationInfoThroughNullable(
                    *old_info, *old_type_it->second, new_info, *new_path_type, path_settings))
                new_info = std::move(reused);
        }
        else if (old_type_it == old_object.getTypedPaths().end() || old_info_it == name_to_elem.end())
        {
            new_info->addDefaults(data.num_rows);
        }

        new_infos.push_back(std::move(new_info));
    }

    return std::make_shared<SerializationInfoObject>(std::move(new_infos), std::move(new_names), new_settings);
}

}
