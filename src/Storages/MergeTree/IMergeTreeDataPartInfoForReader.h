#pragma once
#include <Interpreters/Context.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/ColumnsDescription.h>
#include <Core/NamesAndTypes.h>

namespace DB
{

class IDataPartStorage;
using DataPartStoragePtr = std::shared_ptr<const IDataPartStorage>;

class MergeTreeIndexGranularity;
struct MergeTreeDataPartChecksums;
struct MergeTreeIndexGranularityInfo;
class ISerialization;
using SerializationPtr = std::shared_ptr<const ISerialization>;
class SerializationInfoByName;

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

    virtual DataPartStoragePtr getDataPartStorage() const = 0;

    virtual const NamesAndTypesList & getColumns() const = 0;

    virtual const ColumnsDescription & getColumnsDescription() const = 0;

    virtual const ColumnsDescription & getColumnsDescriptionWithCollectedNested() const = 0;

    virtual std::optional<size_t> getColumnPosition(const String & column_name) const = 0;

    virtual String getColumnNameWithMinimumCompressedSize(const NamesAndTypesList & available_columns) const = 0;

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
};

using MergeTreeDataPartInfoForReaderPtr = std::shared_ptr<IMergeTreeDataPartInfoForReader>;

}
