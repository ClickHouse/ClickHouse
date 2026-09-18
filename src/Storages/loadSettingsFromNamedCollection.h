#pragma once

#include <Common/NamedCollections/NamedCollections.h>
#include <Storages/SettingsWithRecordedOrigin.h>

namespace DB
{

/// Assigns every setting of `impl` that `collection` holds, and records that the collection supplied it - except
/// a key the engine arguments overrode (`ENGINE = Kafka(collection, key = value)`), which holds their value, not
/// the collection's.
template <typename TTraits>
void loadSettingsFromNamedCollection(SettingsWithRecordedOrigin<TTraits> & impl, const NamedCollection & collection)
{
    for (const auto & setting : impl.all())
    {
        const auto & name = setting.getName();
        if (!collection.has(name))
            continue;

        const auto value = collection.get<String>(name);
        if (collection.isQueryOverridden(name))
            impl.set(name, value);
        else
            impl.template setWithOrigin<SettingOrigin::NamedCollection>(name, value);
    }
}

}
