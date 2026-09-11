#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>

#include <DataTypes/Serializations/SerializationInfo.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

namespace DB
{

ISerialization::StreamFileNameSettings IMergeTreeDataPartInfoForReader::getStreamFileNameSettings() const
{
    return ISerialization::StreamFileNameSettings(*getStorageSettings(), &getSerializationInfos().getSettings());
}

}
