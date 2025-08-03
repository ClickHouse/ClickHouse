#pragma once

#include <Core/Types_fwd.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <Storages/Statistics/Statistics.h>

namespace Poco::JSON
{
class Object;
}

namespace DB
{

class ReadBuffer;
class ReadBuffer;
class WriteBuffer;
class NamesAndTypesList;
class Block;

constexpr size_t SERIALIZATION_INFO_VERSION_WITH_STATS = 0;
constexpr size_t SERIALIZATION_INFO_VERSION_WITHOUT_STATS = 1;

/** Contains information about kind of serialization of column and its subcolumns.
 *  Also contains information about content of columns,
 *  that helps to choose kind of serialization of column.
 *
 *  Currently has only information about number of default rows,
 *  that helps to choose sparse serialization.
 *
 *  Should be extended, when new kinds of serialization will be implemented.
 */
class SerializationInfo
{
public:
    using Settings = SerializationInfoSettings;
    SerializationInfo(ISerialization::Kind kind_, const SerializationInfoSettings & settings_);

    virtual ~SerializationInfo() = default;

    virtual bool hasCustomSerialization() const { return kind != ISerialization::Kind::DEFAULT; }
    virtual bool structureEquals(const SerializationInfo & rhs) const { return typeid(SerializationInfo) == typeid(rhs); }
    virtual std::shared_ptr<SerializationInfo> clone() const;

    virtual std::shared_ptr<SerializationInfo> createWithType(
        const IDataType & old_type,
        const IDataType & new_type,
        const SerializationInfoSettings & new_settings) const;

    virtual void serialializeKindBinary(WriteBuffer & out) const;
    virtual void deserializeFromKindsBinary(ReadBuffer & in);

    virtual void toJSON(Poco::JSON::Object & object) const;
    virtual void toJSONWithStats(Poco::JSON::Object & object, const StatisticsInfo & stats) const;

    virtual void fromJSON(const Poco::JSON::Object & object);
    virtual void fromJSONWithStats(const Poco::JSON::Object & object, StatisticsInfo & stats);

    void setKind(ISerialization::Kind kind_) { kind = kind_; }
    const SerializationInfoSettings & getSettings() const { return settings; }
    ISerialization::Kind getKind() const { return kind; }

protected:
    const SerializationInfoSettings settings;
    ISerialization::Kind kind;
};

using SerializationInfoPtr = std::shared_ptr<const SerializationInfo>;
using MutableSerializationInfoPtr = std::shared_ptr<SerializationInfo>;

using SerializationInfos = std::vector<SerializationInfoPtr>;
using MutableSerializationInfos = std::vector<MutableSerializationInfoPtr>;

/// The order is important because info is serialized to part metadata.
class SerializationInfoByName : public std::map<String, MutableSerializationInfoPtr>
{
public:
    using Settings = SerializationInfoSettings;

    SerializationInfoByName() = default;
    SerializationInfoByName(const NamesAndTypesList & columns, const Settings & settings);

    SerializationInfoPtr tryGet(const String & name) const;
    MutableSerializationInfoPtr tryGet(const String & name);
    ISerialization::Kind getKind(const String & column_name) const;

    void writeJSON(WriteBuffer & out) const;
    void writeJSONWithStats(WriteBuffer & out, const StatisticsInfos & stats) const;

private:
    template <typename ElementWriter>
    void writeJSONImpl(size_t version, WriteBuffer & out, ElementWriter && write_element) const;
};

struct SerializationInfosLoadResult
{
    SerializationInfoByName infos;
    std::optional<StatisticsInfos> stats;
};

SerializationInfosLoadResult loadSerializationInfosFromBuffer(ReadBuffer & in, const SerializationInfoSettings & settings);
SerializationInfosLoadResult loadSerializationInfosFromString(const std::string & str, const SerializationInfoSettings & settings);
SerializationInfoByName loadSerializationInfosFromStatistics(const ColumnsStatistics & statistics, const SerializationInfoSettings & settings);

ColumnsStatistics getImplicitStatisticsForSparseSerialization(const Block & block, const SerializationInfoSettings & settings);

}
