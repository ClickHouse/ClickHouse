#pragma once

#include <Core/SettingsEnums.h>

namespace DB
{

class IDataType;

struct SerializationInfoSettings
{
    double ratio_of_defaults_for_sparse = 1.0;
    bool choose_kind = false;

    MergeTreeSerializationInfoVersion version = MergeTreeSerializationInfoVersion::BASIC;
    MergeTreeStringSerializationVersion string_serialization_version = MergeTreeStringSerializationVersion::SINGLE_STREAM;
    MergeTreeNullableSerializationVersion nullable_serialization_version = MergeTreeNullableSerializationVersion::BASIC;

    SerializationInfoSettings() = default;

    SerializationInfoSettings(
        double ratio_of_defaults_for_sparse_,
        bool choose_kind_,
        MergeTreeSerializationInfoVersion version_,
        MergeTreeStringSerializationVersion string_serialization_version_,
        MergeTreeNullableSerializationVersion nullable_serialization_version_);

    /// Downgrade `version` to BASIC when all type-level serialization versions are still at their defaults.
    void tryDowngradeToBasic();

    bool isAlwaysDefault() const { return ratio_of_defaults_for_sparse >= 1.0; }

    bool canUseSparseSerialization(const IDataType & type) const;

    /// Build a settings object that enables the broadest set of serialization capabilities. This is intended for
    /// readers that operate on in-memory state (e.g. NativeReader), which must handle all serialization variants.
    /// Additional serialization versions can be added here in the future.
    static SerializationInfoSettings enableAllSupportedSerializations();
};

}
