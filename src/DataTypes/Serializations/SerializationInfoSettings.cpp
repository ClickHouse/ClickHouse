#include <DataTypes/Serializations/SerializationInfoSettings.h>

#include <DataTypes/IDataType.h>

namespace DB
{

SerializationInfoSettings::SerializationInfoSettings(
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

void SerializationInfoSettings::tryDowngradeToBasic()
{
    if (version == MergeTreeSerializationInfoVersion::BASIC)
        return;

    bool no_specialization = string_serialization_version == MergeTreeStringSerializationVersion::SINGLE_STREAM
        && nullable_serialization_version == MergeTreeNullableSerializationVersion::BASIC;

    if (no_specialization)
        version = MergeTreeSerializationInfoVersion::BASIC;
}

bool SerializationInfoSettings::canUseSparseSerialization(const IDataType & type) const
{
    if (type.isNullable())
    {
        if (nullable_serialization_version == MergeTreeNullableSerializationVersion::BASIC)
            return false;
    }

    return type.supportsSparseSerialization();
}

SerializationInfoSettings SerializationInfoSettings::enableAllSupportedSerializations()
{
    SerializationInfoSettings settings;
    settings.version = MergeTreeSerializationInfoVersion::WITH_TYPES;
    settings.nullable_serialization_version = MergeTreeNullableSerializationVersion::ALLOW_SPARSE;
    return settings;
}

}
