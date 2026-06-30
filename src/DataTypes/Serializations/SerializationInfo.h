#pragma once

#include <Core/MergeTreeSerializationEnums.h>
#include <Core/Names.h>
#include <Core/Types_fwd.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <DataTypes/Serializations/SerializationStatistics.h>

#include <map>

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

    SerializationInfo(ISerialization::KindStack kind_stack_, const SerializationInfoSettings & settings_);
    SerializationInfo(ISerialization::KindStack kind_stack_, const SerializationInfoSettings & settings_, const SerializationStatistics & statistics_);

    virtual ~SerializationInfo() = default;

    virtual bool hasCustomSerialization() const { return kind_stack.size() > 1; }
    virtual bool structureEquals(const SerializationInfo & rhs) const { return typeid(*this) == typeid(rhs); }

    virtual std::shared_ptr<SerializationInfo> clone() const;

    virtual std::shared_ptr<SerializationInfo> createWithType(
        const IDataType & old_type,
        const IDataType & new_type,
        const SerializationInfoSettings & new_settings) const;

    virtual void serialializeKindStackBinary(WriteBuffer & out) const;
    virtual void deserializeFromKindsBinary(ReadBuffer & in);

    /// `has_internal_statistics` is only used under `WITH_EXTERNAL_STATISTICS`: when false, the
    /// per-column counts are omitted (they live in external statistics). It is ignored for older
    /// versions, which always store the counts inline.
    virtual void writeJSON(WriteBuffer & out, const String * name, bool has_internal_statistics = true) const;
    virtual void toJSON(Poco::JSON::Object & object) const;
    virtual void fromJSON(const Poco::JSON::Object & object);

    void setKindStack(ISerialization::KindStack kind_stack_) { kind_stack = kind_stack_; }
    void appendToKindStack(ISerialization::Kind kind) { kind_stack.push_back(kind); }
    const SerializationInfoSettings & getSettings() const { return settings; }
    const SerializationStatistics & getStatistics() const { return statistics; }
    ISerialization::KindStack getKindStack() const { return kind_stack; }

    /// True when the counts were not stored in `serialization.json` (read from external statistics).
    /// Set while reading a `WITH_EXTERNAL_STATISTICS` part; the caller must backfill the counts from
    /// the external statistics via `backfillStatistics` before the info is used for a kind decision.
    bool countsAreExternal() const { return counts_are_external; }
    void backfillStatistics(size_t num_rows, size_t num_defaults)
    {
        statistics.num_rows = num_rows;
        statistics.num_defaults = num_defaults;
        counts_are_external = false;
    }

    /// Set the statistics computed by `SerializationStatisticsBuilder`. The info itself no longer
    /// builds or updates statistics; it only carries them for persistence and for the next merge.
    void setStatistics(const SerializationStatistics & statistics_)
    {
        statistics = statistics_;
        counts_are_external = false;
    }

protected:
    virtual void writeJSONFields(WriteBuffer & out, const String * name, bool has_internal_statistics) const;

    const SerializationInfoSettings settings;

    ISerialization::KindStack kind_stack;
    SerializationStatistics statistics;

    /// Transient (not serialized): set while reading a `WITH_EXTERNAL_STATISTICS` part for a column
    /// whose counts were omitted from `serialization.json`. Cleared by `backfillStatistics`.
    bool counts_are_external = false;
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

    SerializationInfoPtr tryGet(const String & name) const;
    MutableSerializationInfoPtr tryGet(const String & name);
    ISerialization::KindStack getKindStack(const String & column_name) const;

    /// Under `WITH_EXTERNAL_STATISTICS`, columns listed in `columns_with_external_statistics` have
    /// their serialization-relevant counts persisted in the external statistics files, so their
    /// `has_internal_statistics` flag is written as false and the inline counts are omitted.
    void writeJSON(WriteBuffer & out, const NameSet & columns_with_external_statistics = {}) const;

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
    /// - We intentionally keep such options out of per-column `SerializationInfo` entries,
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
