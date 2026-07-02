#pragma once

#include <Core/MergeTreeSerializationEnums.h>
#include <Core/Names.h>
#include <Core/Types_fwd.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>

#include <map>
#include <unordered_map>

namespace Poco::JSON
{
class Object;
}

namespace DB
{

class ReadBuffer;
class WriteBuffer;
class NamesAndTypesList;
class Block;

/// Per-column estimates (row and default counts, and richer statistics) keyed by column/subcolumn path.
/// Defined in `Storages/Statistics/Estimate.h`; only referenced here by const reference, so a forward
/// declaration (the alias must match the one there; the compiler rejects a divergence) keeps this
/// lower-level header independent of the statistics subsystem.
struct Estimate;
using Estimates = std::unordered_map<String, Estimate>;

/** Contains information about kind of serialization of column and its subcolumns.
 *
 *  The counts that help choosing the serialization kind (currently the number of default rows, used to
 *  choose sparse serialization) are no longer stored here: they are carried by `Estimate` and are passed
 *  in explicitly when writing `serialization.json` and returned explicitly when reading it back
 *  (see `EstimatesBuilder`). This class only carries the chosen kind stack.
 *
 *  Should be extended, when new kinds of serialization will be implemented.
 */
class SerializationInfo
{
public:
    using Settings = SerializationInfoSettings;

    SerializationInfo(ISerialization::KindStack kind_stack_, const SerializationInfoSettings & settings_);

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

    /// Write the kind stack and the counts of this column (and its subcolumns), looked up in `estimates`
    /// by `key` (the column/subcolumn path). A (sub)column absent from `estimates` is written with zero
    /// counts.
    virtual void writeJSON(WriteBuffer & out, const String * name, const String & key, const Estimates & estimates) const;

    /// Read the kind stack, and put the counts of this column (and its subcolumns) into `estimates` keyed
    /// by `key` (the column/subcolumn path).
    virtual void fromJSON(const Poco::JSON::Object & object, const String & key, Estimates & estimates);

    void setKindStack(ISerialization::KindStack kind_stack_) { kind_stack = kind_stack_; }
    void appendToKindStack(ISerialization::Kind kind) { kind_stack.push_back(kind); }
    const SerializationInfoSettings & getSettings() const { return settings; }
    ISerialization::KindStack getKindStack() const { return kind_stack; }

protected:
    virtual void writeJSONFields(WriteBuffer & out, const String * name, const String & key, const Estimates & estimates) const;

    const SerializationInfoSettings settings;

    ISerialization::KindStack kind_stack;
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

    /// Write the infos together with the counts from `estimates` (keyed by column/subcolumn path). Columns
    /// and subcolumns absent from `estimates` are written with zero counts.
    void writeJSON(WriteBuffer & out, const Estimates & estimates) const;

    SerializationInfoByName clone() const;

    const Settings & getSettings() const { return settings; }

    MergeTreeSerializationInfoVersion getVersion() const;

    bool needsPersistence() const;

    /// Read the infos, returning the per-column/subcolumn counts in `estimates`.
    static SerializationInfoByName readJSON(const NamesAndTypesList & columns, ReadBuffer & in, Estimates & estimates);

    static SerializationInfoByName readJSONFromString(const NamesAndTypesList & columns, const std::string & str, Estimates & estimates);

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
