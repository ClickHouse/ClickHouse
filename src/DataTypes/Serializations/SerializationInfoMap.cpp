#include <DataTypes/Serializations/SerializationInfoMap.h>
#include "DataTypes/Serializations/ISerialization.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

static constexpr auto KEY_NUM_SHARDS = "num_shards";

SerializationInfoMap::SerializationInfoMap(size_t num_shards_, ISerialization::Kind kind_, const SerializationInfoSettings & settings_)
    : SerializationInfo(kind_, settings_)
    , num_shards(num_shards_)
{
}

Poco::JSON::Object SerializationInfoMap::toJSON() const
{
    auto object = SerializationInfo::toJSON();
    object.set(KEY_NUM_SHARDS, num_shards);
    return object;
}

void SerializationInfoMap::fromJSON(const Poco::JSON::Object & object)
{
    SerializationInfo::fromJSON(object);

    if (!object.has(KEY_NUM_SHARDS))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Missed field 'subcolumns' in SerializationInfo of columns SerializationInfoTuple");

    num_shards = object.getValue<size_t>(KEY_NUM_SHARDS);
}

}
