#include <DataTypes/Serializations/SerializationInfoSettings.h>

#include <DataTypes/IDataType.h>

namespace DB
{

bool SerializationInfoSettings::canUseSparseSerialization(const IDataType & type) const
{
    if (type.isNullable())
    {
        if (nullable_serialization_version == MergeTreeNullableSerializationVersion::BASIC)
            return false;
    }

    return type.supportsSparseSerialization();
}

}
