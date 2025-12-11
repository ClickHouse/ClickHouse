#include <DataTypes/Serializations/SerializationInfoSettings.h>

#include <DataTypes/IDataType.h>

namespace DB
{

bool SerializationInfoSettings::supportsSparseSerialization(const IDataType & type) const
{
    if (type.isNullable())
        return nullable_serialization_version == MergeTreeNullableSerializationVersion::ALLOW_SPARSE;

    return type.supportsSparseSerialization();
}

}
