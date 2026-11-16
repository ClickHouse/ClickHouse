#pragma once
#include <Compression/ICompressionCodec.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ColumnSize.h>
#include <Core/NamesAndTypes.h>
#include <base/types.h>

namespace DB
{

class IDataPartStorage;
using DataPartStoragePtr = std::shared_ptr<const IDataPartStorage>;

class IMergeTreeDataPart;

struct MergeTreeSettings;
using MergeTreeSettingsPtr = std::shared_ptr<const MergeTreeSettings>;

class MergeTreeIndexGranularity;
struct MergeTreePartInfo;
struct MergeTreePartition;
struct MergeTreeDataPartChecksums;
struct MergeTreeIndexGranularityInfo;

class ISerialization;
using SerializationPtr = std::shared_ptr<const ISerialization>;
class SerializationInfoByName;

class AlterConversions;
using AlterConversionsPtr = std::shared_ptr<const AlterConversions>;

using Index = Columns;
using IndexPtr = std::shared_ptr<const Index>;

/**
 * A class which contains all information about a data part that is required
 * in order to use MergeTreeDataPartReader's.
 * It is a separate interface and not a simple struct because
 * otherwise it will need to copy all the information which might not
 * be even used (for example, an IndexGranularity class object is quite heavy).
 */
class IMergeTreeDataPartInfoForReader : public WithContext
{
public:
    explicit IMergeTreeDataPartInfoForReader(ContextPtr context_) : WithContext(context_) {}

    virtual ~IMergeTreeDataPartInfoForReader() = default;

    virtual bool isCompactPart() const = 0;

    virtual bool isWidePart() const = 0;

    virtual bool isProjectionPart() const = 0;

    virtual bool hasLightweightDelete() const = 0;

    virtual const String & getPartName() const = 0;

    virtual const MergeTreePartInfo & getPartInfo() const = 0;

    virtual const MergeTreePartition & getPartition() const = 0;

    virtual Int64 getMinDataVersion() const = 0;

    virtual Int64 getMaxDataVersion() const = 0;

    virtual IndexPtr getIndexPtr() const = 0;

    virtual DataPartStoragePtr getDataPartStorage() const = 0;

    virtual const NamesAndTypesList & getColumns() const = 0;

    virtual const ColumnsDescription & getColumnsDescription() const = 0;

    virtual const ColumnsDescription & getColumnsDescriptionWithCollectedNested() const = 0;

    virtual const ColumnsSubstreams & getColumnsSubstreams() const = 0;

    virtual std::optional<size_t> getColumnPosition(const String & column_name) const = 0;

    /// Look up a (sub)column present in the part, if any.
    virtual std::optional<NameAndTypePair> tryGetColumn(const String & column_name) const = 0;

    virtual bool isSystemColumnInvalidated(const String & column_name) const = 0;

    virtual String getColumnNameWithMinimumCompressedSize(const NamesAndTypesList & available_columns) const = 0;

    /// Name of the parent part when this is a projection part, empty otherwise.
    /// Used by the read pool/select processor to build a qualified part name.
    virtual String getParentPartName() const = 0;

    /// Per-column on-disk sizes, used for read-task sizing and the block size predictor.
    /// A borrowed part (stateless worker) has no size information: the scalar getters return zero
    /// (size predictor falls back to a default estimate) and `getColumnSizes` returns null. The map is
    /// returned by shared pointer, not by value, so hot callers (e.g. the per-block dataflow-statistics
    /// callback) reuse the part's cached map instead of copying it on every block.
    virtual ColumnSize getColumnSize(const String & column_name) const = 0;
    virtual std::shared_ptr<const std::unordered_map<String, ColumnSize>> getColumnSizes() const = 0;
    /// The codec the part was written with, before any per-column `CODEC` override.
    virtual CompressionCodecPtr getDefaultCompressionCodec() const = 0;
    virtual ColumnSize getSubcolumnSize(const String & subcolumn_name) const = 0;

    /// MergeTree settings governing how the part is read.
    virtual MergeTreeSettingsPtr getStorageSettings() const = 0;

    /// The underlying concrete data part, or nullptr for a borrowed part (stateless worker).
    /// Only for the few coordinator-only features (patches, projections, index-read-tasks)
    /// that are never exercised on the stateless-worker read path. Hot-path/both-paths code
    /// must use the abstraction accessors above, not this.
    virtual std::shared_ptr<const IMergeTreeDataPart> getDataPart() const = 0;

    virtual const MergeTreeDataPartChecksums & getChecksums() const = 0;

    virtual AlterConversionsPtr getAlterConversions() const = 0;

    virtual size_t getMarksCount() const = 0;

    virtual size_t getFileSizeOrZero(const std::string & file_name) const = 0;

    virtual const MergeTreeIndexGranularityInfo & getIndexGranularityInfo() const = 0;

    virtual const MergeTreeIndexGranularity & getIndexGranularity() const = 0;

    virtual SerializationPtr getSerialization(const NameAndTypePair & column) const = 0;

    virtual const SerializationInfoByName & getSerializationInfos() const = 0;

    virtual String getTableName() const = 0;

    virtual void reportBroken() = 0;

    virtual size_t getRowCount() const = 0;
};

using MergeTreeDataPartInfoForReaderPtr = std::shared_ptr<IMergeTreeDataPartInfoForReader>;

}
