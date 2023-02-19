#pragma once

#include <Core/Types.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Poco/JSON/Object.h>


namespace DB
{

class ReadBuffer;
class WriteBuffer;
class NamesAndTypesList;
class Block;

constexpr auto SERIALIZATION_INFO_VERSION = 0;

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
    struct Data
    {
        size_t num_rows = 0;
        size_t num_defaults = 0;

        void add(const IColumn & column);
        void add(const Data & other);
        void addDefaults(size_t length);
    };

    struct Settings
    {
        const double ratio_of_defaults_for_sparse = 1.0;
        const bool choose_kind = false;

        bool isAlwaysDefault() const { return ratio_of_defaults_for_sparse >= 1.0; }
    };

    SerializationInfo(ISerialization::Kind kind_, const Settings & settings_);
    SerializationInfo(ISerialization::Kind kind_, const Settings & settings_, const Data & data_);

    virtual ~SerializationInfo() = default;

    virtual bool hasCustomSerialization() const { return kind != ISerialization::Kind::DEFAULT; }

    virtual void add(const IColumn & column);
    virtual void add(const SerializationInfo & other);
    virtual void addDefaults(size_t length);
    virtual void replaceData(const SerializationInfo & other);

    virtual std::shared_ptr<SerializationInfo> clone() const;

    virtual void serialializeKindBinary(WriteBuffer & out) const;
    virtual void deserializeFromKindsBinary(ReadBuffer & in);

    virtual Poco::JSON::Object toJSON() const;
    virtual void fromJSON(const Poco::JSON::Object & object);

    void setKind(ISerialization::Kind kind_) { kind = kind_; }
    const Settings & getSettings() const { return settings; }
    const Data & getData() const { return data; }
    ISerialization::Kind getKind() const { return kind; }

    static ISerialization::Kind chooseKind(const Data & data, const Settings & settings);

protected:
    const Settings settings;

    ISerialization::Kind kind;
    Data data;
};

using SerializationInfoPtr = std::shared_ptr<const SerializationInfo>;
using MutableSerializationInfoPtr = std::shared_ptr<SerializationInfo>;

using SerializationInfos = std::vector<SerializationInfoPtr>;
using MutableSerializationInfos = std::vector<MutableSerializationInfoPtr>;

class SerializationInfoByName : public std::unordered_map<String, MutableSerializationInfoPtr>
{
public:
    SerializationInfoByName() = default;
    SerializationInfoByName(const NamesAndTypesList & columns, const SerializationInfo::Settings & settings);

    void add(const Block & block);
    void add(const SerializationInfoByName & other);

    /// Takes data from @other, but keeps current serialization kinds.
    /// If column exists in @other infos, but not in current infos,
    /// it's cloned to current infos.
    void replaceData(const SerializationInfoByName & other);

    void writeJSON(WriteBuffer & out) const;
    void readJSON(ReadBuffer & in);
};

}
