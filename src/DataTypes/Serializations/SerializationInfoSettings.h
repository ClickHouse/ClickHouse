#pragma once

#include <Core/SettingsEnums.h>

namespace DB
{

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
        MergeTreeNullableSerializationVersion nullable_serialization_version_)
        : ratio_of_defaults_for_sparse(ratio_of_defaults_for_sparse_)
        , choose_kind(choose_kind_)
        , version(version_)
        , string_serialization_version(string_serialization_version_)
        , nullable_serialization_version(nullable_serialization_version_)
    {
        /// New type specialized serialization version is valid only when using MergeTreeSerializationInfoVersion::WITH_TYPES.
        /// For older versions, it is automatically defaulted to preserve compatibility.
        if (version < MergeTreeSerializationInfoVersion::WITH_TYPES)
        {
            string_serialization_version = MergeTreeStringSerializationVersion::SINGLE_STREAM;
            nullable_serialization_version = MergeTreeNullableSerializationVersion::BASIC;
        }
    }

    bool isAlwaysDefault() const { return ratio_of_defaults_for_sparse >= 1.0; }
};

}
