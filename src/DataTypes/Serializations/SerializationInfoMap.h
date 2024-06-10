#pragma once
#include <DataTypes/Serializations/SerializationInfo.h>

namespace DB
{

class SerializationInfoMap : public SerializationInfo
{
public:
    SerializationInfoMap(size_t num_shards_, ISerialization::Kind kind_, const SerializationInfoSettings & settings_);

    Poco::JSON::Object toJSON() const override;
    void fromJSON(const Poco::JSON::Object & object) override;
    size_t getNumShards() const { return num_shards; }

private:
    size_t num_shards;
};

}
