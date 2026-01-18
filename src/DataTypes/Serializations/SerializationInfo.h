#pragma once

#include <Core/Types_fwd.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>

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

    struct Data
    {
        size_t num_rows = 0;
        size_t num_defaults = 0;

        void add(const IColumn & column);
        void add(const Data & other);
        void remove(const Data & other);
        void addDefaults(size_t length);
    };

    SerializationInfo(ISerialization::KindStack kind_stack_, const SerializationInfoSettings & settings_);
    SerializationInfo(ISerialization::KindStack kind_stack_, const SerializationInfoSettings & settings_, const Data & data_);

    virtual ~SerializationInfo() = default;

    virtual bool hasCustomSerialization() const { return kind_stack.size() > 1; }
    virtual bool structureEquals(const SerializationInfo & rhs) const { return typeid(*this) == typeid(rhs); }

    virtual void add(const IColumn & column);
    virtual void add(const SerializationInfo & other);
    virtual void remove(const SerializationInfo & other);
    virtual void addDefaults(size_t length);
    virtual void replaceData(const SerializationInfo & other);

    virtual std::shared_ptr<SerializationInfo> clone() const;

    virtual std::shared_ptr<SerializationInfo> createWithType(
        const IDataType & old_type,
        const IDataType & new_type,
        const SerializationInfoSettings & new_settings) const;

    virtual void serialializeKindStackBinary(WriteBuffer & out) const;
    virtual void deserializeFromKindsBinary(ReadBuffer & in);

    virtual void toJSON(Poco::JSON::Object & object) const;
    virtual void fromJSON(const Poco::JSON::Object & object);

    void setKindStack(ISerialization::KindStack kind_stack_) { kind_stack = kind_stack_; }
    void appendToKindStack(ISerialization::Kind kind) { kind_stack.push_back(kind); }
    const SerializationInfoSettings & getSettings() const { return settings; }
    const Data & getData() const { return data; }
    ISerialization::KindStack getKindStack() const { return kind_stack; }

    static ISerialization::KindStack chooseKindStack(const Data & data, const SerializationInfoSettings & settings);

protected:
    const SerializationInfoSettings settings;

    ISerialization::KindStack kind_stack;
    Data data;
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

    explicit SerializationInfoByName(const Settings & settings_);
    SerializationInfoByName(const NamesAndTypesList & columns, const Settings & settings_);

    void add(const Block & block);
    void add(const SerializationInfoByName & other);
    void add(const String & name, const SerializationInfo & info);

    void remove(const SerializationInfoByName & other);
    void remove(const String & name, const SerializationInfo & info);

    SerializationInfoPtr tryGet(const String & name) const;
    MutableSerializationInfoPtr tryGet(const String & name);
    ISerialization::KindStack getKindStack(const String & column_name) const;

    /// Takes data from @other, but keeps current serialization kinds.
    /// If column exists in @other infos, but not in current infos,
    /// it's cloned to current infos.
    void replaceData(const SerializationInfoByName & other);

    void writeJSON(WriteBuffer & out) const;

    SerializationInfoByName clone() const;

    const Settings & getSettings() const { return settings; }

    MergeTreeSerializationInfoVersion getVersion() const;

    bool needsPersistence() const;

    static SerializationInfoByName readJSON(const NamesAndTypesList & columns, ReadBuffer & in);

    static SerializationInfoByName readJSONFromString(const NamesAndTypesList & columns, const std::string & str);

private:
    /// This field stores all configuration options that are not tied to a
    /// specific column entry in `SerializationInfoByName`. For example:
    /// - Per-type serialization versions (`types_serialization_versions`), e.g.,
    ///   specifying different versions for `String` or other types.
    ///
    /// Design notes:
    /// - We intentionally keep such options out of `SerializationInfo::Data`,
    ///   because the mere existence of a `SerializationInfo` entry triggers
    ///   sparse encoding logic. This would produce misleading content in
    ///   `serializations.json` for types that do not support sparse encoding.
    ///
    /// - By storing them centrally in `settings`, we avoid polluting
    ///   per-column entries and maintain a clear separation between
    ///   "global defaults" and "per-column overrides".
    ///
    /// - The default constructor was removed. Constructors now require
    ///   explicit `SerializationInfoSettings`, ensuring that in MergeTree
    ///   or other engines, the correct settings must always be provided for
    ///   consistent serialization behavior.
    Settings settings;
};

}
