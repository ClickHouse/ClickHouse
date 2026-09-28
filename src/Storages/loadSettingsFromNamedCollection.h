#pragma once

#include <Common/NamedCollections/NamedCollections.h>
#include <Core/BaseSettings.h>

namespace DB
{

/// Assigns every setting of `impl` that `collection` holds, and records that the collection supplied it - except
/// a key the engine arguments overrode (`ENGINE = Kafka(collection, key = value)`), which holds their value, not
/// the collection's, and is recorded as `overridden_origin`: `Definition` for a table, whose own `CREATE` states
/// it as surely as its `SETTINGS` clause; `Default`, meaning none, where the collection builds something else - a
/// database, whose tables state none of its settings.
template <typename TTraits>
void loadSettingsFromNamedCollection(
    BaseSettings<TTraits> & impl, const NamedCollection & collection, SettingOrigin overridden_origin)
    requires TTraits::record_origin
{
    impl.recordNamedCollection(collection.getName());

    for (const auto & setting : impl.all())
    {
        const auto & name = setting.getName();
        if (!collection.has(name))
            continue;

        impl.setWithOrigin(name, collection.get<String>(name),
            collection.isQueryOverridden(name) ? overridden_origin : SettingOrigin::NamedCollection);
    }
}

}
