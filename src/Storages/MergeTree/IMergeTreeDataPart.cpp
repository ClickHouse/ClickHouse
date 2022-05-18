#include "IMergeTreeDataPart.h"

#include <optional>
#include <boost/algorithm/string/join.hpp>
#include <string_view>
#include <Core/Defines.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/HashingReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/localBackup.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/PartMetadataManagerOrdinary.h>
#include <Storages/MergeTree/PartMetadataManagerWithCache.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/CurrentMetrics.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <base/JSON.h>
#include <Common/logger_useful.h>
#include <Compression/getCompressionCodecForFile.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ExpressionElementParsers.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/TransactionLog.h>


namespace CurrentMetrics
{
    extern const Metric PartsTemporary;
    extern const Metric PartsPreCommitted;
    extern const Metric PartsCommitted;
    extern const Metric PartsPreActive;
    extern const Metric PartsActive;
    extern const Metric PartsOutdated;
    extern const Metric PartsDeleting;
    extern const Metric PartsDeleteOnDestroy;

    extern const Metric PartsWide;
    extern const Metric PartsCompact;
    extern const Metric PartsInMemory;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
    extern const int FILE_DOESNT_EXIST;
    extern const int NO_FILE_IN_DATA_PART;
    extern const int EXPECTED_END_OF_FILE;
    extern const int CORRUPTED_DATA;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
    extern const int BAD_TTL_FILE;
    extern const int NOT_IMPLEMENTED;
}

static std::unique_ptr<ReadBufferFromFileBase> openForReading(const DiskPtr & disk, const String & path)
{
    size_t file_size = disk->getFileSize(path);
    return disk->readFile(path, ReadSettings().adjustBufferSize(file_size), file_size);
}

void IMergeTreeDataPart::MinMaxIndex::load(const MergeTreeData & data, const PartMetadataManagerPtr & manager)
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    const auto & partition_key = metadata_snapshot->getPartitionKey();

    auto minmax_column_names = data.getMinMaxColumnsNames(partition_key);
    auto minmax_column_types = data.getMinMaxColumnsTypes(partition_key);
    size_t minmax_idx_size = minmax_column_types.size();

    hyperrectangle.reserve(minmax_idx_size);
    for (size_t i = 0; i < minmax_idx_size; ++i)
    {
        String file_name = "minmax_" + escapeForFileName(minmax_column_names[i]) + ".idx";
        auto file = manager->read(file_name);
        auto serialization = minmax_column_types[i]->getDefaultSerialization();

        Field min_val;
        serialization->deserializeBinary(min_val, *file);
        Field max_val;
        serialization->deserializeBinary(max_val, *file);

        // NULL_LAST
        if (min_val.isNull())
            min_val = POSITIVE_INFINITY;
        if (max_val.isNull())
            max_val = POSITIVE_INFINITY;

        hyperrectangle.emplace_back(min_val, true, max_val, true);
    }
    initialized = true;
}

IMergeTreeDataPart::MinMaxIndex::WrittenFiles IMergeTreeDataPart::MinMaxIndex::store(
    const MergeTreeData & data, const DiskPtr & disk_, const String & part_path, Checksums & out_checksums) const
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    const auto & partition_key = metadata_snapshot->getPartitionKey();

    auto minmax_column_names = data.getMinMaxColumnsNames(partition_key);
    auto minmax_column_types = data.getMinMaxColumnsTypes(partition_key);

    return store(minmax_column_names, minmax_column_types, disk_, part_path, out_checksums);
}

IMergeTreeDataPart::MinMaxIndex::WrittenFiles IMergeTreeDataPart::MinMaxIndex::store(
    const Names & column_names,
    const DataTypes & data_types,
    const DiskPtr & disk_,
    const String & part_path,
    Checksums & out_checksums) const
{
    if (!initialized)
        throw Exception("Attempt to store uninitialized MinMax index for part " + part_path + ". This is a bug.",
            ErrorCodes::LOGICAL_ERROR);

    WrittenFiles written_files;

    for (size_t i = 0; i < column_names.size(); ++i)
    {
        String file_name = "minmax_" + escapeForFileName(column_names[i]) + ".idx";
        auto serialization = data_types.at(i)->getDefaultSerialization();

        auto out = disk_->writeFile(fs::path(part_path) / file_name);
        HashingWriteBuffer out_hashing(*out);
        serialization->serializeBinary(hyperrectangle[i].left, out_hashing);
        serialization->serializeBinary(hyperrectangle[i].right, out_hashing);
        out_hashing.next();
        out_checksums.files[file_name].file_size = out_hashing.count();
        out_checksums.files[file_name].file_hash = out_hashing.getHash();
        out->preFinalize();
        written_files.emplace_back(std::move(out));
    }

    return written_files;
}

void IMergeTreeDataPart::MinMaxIndex::update(const Block & block, const Names & column_names)
{
    if (!initialized)
        hyperrectangle.reserve(column_names.size());

    for (size_t i = 0; i < column_names.size(); ++i)
    {
        FieldRef min_value;
        FieldRef max_value;
        const ColumnWithTypeAndName & column = block.getByName(column_names[i]);
        if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(column.column.get()))
            column_nullable->getExtremesNullLast(min_value, max_value);
        else
            column.column->getExtremes(min_value, max_value);

        if (!initialized)
            hyperrectangle.emplace_back(min_value, true, max_value, true);
        else
        {
            hyperrectangle[i].left
                = applyVisitor(FieldVisitorAccurateLess(), hyperrectangle[i].left, min_value) ? hyperrectangle[i].left : min_value;
            hyperrectangle[i].right
                = applyVisitor(FieldVisitorAccurateLess(), hyperrectangle[i].right, max_value) ? max_value : hyperrectangle[i].right;
        }
    }

    initialized = true;
}

void IMergeTreeDataPart::MinMaxIndex::merge(const MinMaxIndex & other)
{
    if (!other.initialized)
        return;

    if (!initialized)
    {
        hyperrectangle = other.hyperrectangle;
        initialized = true;
    }
    else
    {
        for (size_t i = 0; i < hyperrectangle.size(); ++i)
        {
            hyperrectangle[i].left = std::min(hyperrectangle[i].left, other.hyperrectangle[i].left);
            hyperrectangle[i].right = std::max(hyperrectangle[i].right, other.hyperrectangle[i].right);
        }
    }
}

void IMergeTreeDataPart::MinMaxIndex::appendFiles(const MergeTreeData & data, Strings & files)
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    const auto & partition_key = metadata_snapshot->getPartitionKey();
    auto minmax_column_names = data.getMinMaxColumnsNames(partition_key);
    size_t minmax_idx_size = minmax_column_names.size();
    for (size_t i = 0; i < minmax_idx_size; ++i)
    {
        String file_name = "minmax_" + escapeForFileName(minmax_column_names[i]) + ".idx";
        files.push_back(file_name);
    }
}


static void incrementStateMetric(IMergeTreeDataPart::State state)
{
    switch (state)
    {
        case IMergeTreeDataPart::State::Temporary:
            CurrentMetrics::add(CurrentMetrics::PartsTemporary);
            return;
        case IMergeTreeDataPart::State::PreActive:
            CurrentMetrics::add(CurrentMetrics::PartsPreActive);
            CurrentMetrics::add(CurrentMetrics::PartsPreCommitted);
            return;
        case IMergeTreeDataPart::State::Active:
            CurrentMetrics::add(CurrentMetrics::PartsActive);
            CurrentMetrics::add(CurrentMetrics::PartsCommitted);
            return;
        case IMergeTreeDataPart::State::Outdated:
            CurrentMetrics::add(CurrentMetrics::PartsOutdated);
            return;
        case IMergeTreeDataPart::State::Deleting:
            CurrentMetrics::add(CurrentMetrics::PartsDeleting);
            return;
        case IMergeTreeDataPart::State::DeleteOnDestroy:
            CurrentMetrics::add(CurrentMetrics::PartsDeleteOnDestroy);
            return;
    }
}

static void decrementStateMetric(IMergeTreeDataPart::State state)
{
    switch (state)
    {
        case IMergeTreeDataPart::State::Temporary:
            CurrentMetrics::sub(CurrentMetrics::PartsTemporary);
            return;
        case IMergeTreeDataPart::State::PreActive:
            CurrentMetrics::sub(CurrentMetrics::PartsPreActive);
            CurrentMetrics::sub(CurrentMetrics::PartsPreCommitted);
            return;
        case IMergeTreeDataPart::State::Active:
            CurrentMetrics::sub(CurrentMetrics::PartsActive);
            CurrentMetrics::sub(CurrentMetrics::PartsCommitted);
            return;
        case IMergeTreeDataPart::State::Outdated:
            CurrentMetrics::sub(CurrentMetrics::PartsOutdated);
            return;
        case IMergeTreeDataPart::State::Deleting:
            CurrentMetrics::sub(CurrentMetrics::PartsDeleting);
            return;
        case IMergeTreeDataPart::State::DeleteOnDestroy:
            CurrentMetrics::sub(CurrentMetrics::PartsDeleteOnDestroy);
            return;
    }
}

static void incrementTypeMetric(MergeTreeDataPartType type)
{
    switch (type.getValue())
    {
        case MergeTreeDataPartType::Wide:
            CurrentMetrics::add(CurrentMetrics::PartsWide);
            return;
        case MergeTreeDataPartType::Compact:
            CurrentMetrics::add(CurrentMetrics::PartsCompact);
            return;
        case MergeTreeDataPartType::InMemory:
            CurrentMetrics::add(CurrentMetrics::PartsInMemory);
            return;
        case MergeTreeDataPartType::Unknown:
            return;
    }
}

static void decrementTypeMetric(MergeTreeDataPartType type)
{
    switch (type.getValue())
    {
        case MergeTreeDataPartType::Wide:
            CurrentMetrics::sub(CurrentMetrics::PartsWide);
            return;
        case MergeTreeDataPartType::Compact:
            CurrentMetrics::sub(CurrentMetrics::PartsCompact);
            return;
        case MergeTreeDataPartType::InMemory:
            CurrentMetrics::sub(CurrentMetrics::PartsInMemory);
            return;
        case MergeTreeDataPartType::Unknown:
            return;
    }
}


IMergeTreeDataPart::IMergeTreeDataPart(
    const MergeTreeData & storage_,
    const String & name_,
    const VolumePtr & volume_,
    const std::optional<String> & relative_path_,
    Type part_type_,
    const IMergeTreeDataPart * parent_part_)
    : storage(storage_)
    , name(name_)
    , info(MergeTreePartInfo::fromPartName(name_, storage.format_version))
    , volume(parent_part_ ? parent_part_->volume : volume_)
    , relative_path(relative_path_.value_or(name_))
    , index_granularity_info(storage_, part_type_)
    , part_type(part_type_)
    , parent_part(parent_part_)
    , use_metadata_cache(storage.use_metadata_cache)
{
    if (parent_part)
        state = State::Active;
    incrementStateMetric(state);
    incrementTypeMetric(part_type);

    minmax_idx = std::make_shared<MinMaxIndex>();

    initializePartMetadataManager();
}

IMergeTreeDataPart::IMergeTreeDataPart(
    const MergeTreeData & storage_,
    const String & name_,
    const MergeTreePartInfo & info_,
    const VolumePtr & volume_,
    const std::optional<String> & relative_path_,
    Type part_type_,
    const IMergeTreeDataPart * parent_part_)
    : storage(storage_)
    , name(name_)
    , info(info_)
    , volume(parent_part_ ? parent_part_->volume : volume_)
    , relative_path(relative_path_.value_or(name_))
    , index_granularity_info(storage_, part_type_)
    , part_type(part_type_)
    , parent_part(parent_part_)
    , use_metadata_cache(storage.use_metadata_cache)
{
    if (parent_part)
        state = State::Active;
    incrementStateMetric(state);
    incrementTypeMetric(part_type);

    minmax_idx = std::make_shared<MinMaxIndex>();

    initializePartMetadataManager();
}

IMergeTreeDataPart::~IMergeTreeDataPart()
{
    decrementStateMetric(state);
    decrementTypeMetric(part_type);
}


String IMergeTreeDataPart::getNewName(const MergeTreePartInfo & new_part_info) const
{
    if (storage.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        /// NOTE: getting min and max dates from the part name (instead of part data) because we want
        /// the merged part name be determined only by source part names.
        /// It is simpler this way when the real min and max dates for the block range can change
        /// (e.g. after an ALTER DELETE command).
        DayNum min_date;
        DayNum max_date;
        MergeTreePartInfo::parseMinMaxDatesFromPartName(name, min_date, max_date);
        return new_part_info.getPartNameV0(min_date, max_date);
    }
    else
        return new_part_info.getPartName();
}

std::optional<size_t> IMergeTreeDataPart::getColumnPosition(const String & column_name) const
{
    auto it = column_name_to_position.find(column_name);
    if (it == column_name_to_position.end())
        return {};
    return it->second;
}


void IMergeTreeDataPart::setState(IMergeTreeDataPart::State new_state) const
{
    decrementStateMetric(state);
    state = new_state;
    incrementStateMetric(state);
}

IMergeTreeDataPart::State IMergeTreeDataPart::getState() const
{
    return state;
}


std::pair<DayNum, DayNum> IMergeTreeDataPart::getMinMaxDate() const
{
    if (storage.minmax_idx_date_column_pos != -1 && minmax_idx->initialized)
    {
        const auto & hyperrectangle = minmax_idx->hyperrectangle[storage.minmax_idx_date_column_pos];
        return {DayNum(hyperrectangle.left.get<UInt64>()), DayNum(hyperrectangle.right.get<UInt64>())};
    }
    else
        return {};
}

std::pair<time_t, time_t> IMergeTreeDataPart::getMinMaxTime() const
{
    if (storage.minmax_idx_time_column_pos != -1 && minmax_idx->initialized)
    {
        const auto & hyperrectangle = minmax_idx->hyperrectangle[storage.minmax_idx_time_column_pos];

        /// The case of DateTime
        if (hyperrectangle.left.getType() == Field::Types::UInt64)
        {
            assert(hyperrectangle.right.getType() == Field::Types::UInt64);
            return {hyperrectangle.left.get<UInt64>(), hyperrectangle.right.get<UInt64>()};
        }
        /// The case of DateTime64
        else if (hyperrectangle.left.getType() == Field::Types::Decimal64)
        {
            assert(hyperrectangle.right.getType() == Field::Types::Decimal64);

            auto left = hyperrectangle.left.get<DecimalField<Decimal64>>();
            auto right = hyperrectangle.right.get<DecimalField<Decimal64>>();

            assert(left.getScale() == right.getScale());

            return { left.getValue() / left.getScaleMultiplier(), right.getValue() / right.getScaleMultiplier() };
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part minmax index by time is neither DateTime or DateTime64");
    }
    else
        return {};
}


void IMergeTreeDataPart::setColumns(const NamesAndTypesList & new_columns)
{
    columns = new_columns;

    column_name_to_position.clear();
    column_name_to_position.reserve(new_columns.size());
    size_t pos = 0;

    for (const auto & column : columns)
        column_name_to_position.emplace(column.name, pos++);
}

void IMergeTreeDataPart::setSerializationInfos(const SerializationInfoByName & new_infos)
{
    serialization_infos = new_infos;
}

SerializationPtr IMergeTreeDataPart::getSerialization(const NameAndTypePair & column) const
{
    auto it = serialization_infos.find(column.getNameInStorage());
    return it == serialization_infos.end()
        ? IDataType::getSerialization(column)
        : IDataType::getSerialization(column, *it->second);
}

void IMergeTreeDataPart::removeIfNeeded()
{
    assert(assertHasValidVersionMetadata());
    if (!is_temp && state != State::DeleteOnDestroy)
        return;

    try
    {
        auto path = getFullRelativePath();

        if (!volume->getDisk()->exists(path))
            return;

        if (is_temp)
        {
            String file_name = fileName(relative_path);

            if (file_name.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "relative_path {} of part {} is invalid or not set", relative_path, name);

            if (!startsWith(file_name, "tmp") && !endsWith(file_name, ".tmp_proj"))
            {
                LOG_ERROR(
                    storage.log,
                    "~DataPart() should remove part {} but its name doesn't start with \"tmp\" or end with \".tmp_proj\". Too "
                    "suspicious, keeping the part.",
                    path);
                return;
            }
        }

        if (parent_part)
        {
            auto [can_remove, _] = canRemovePart();
            projectionRemove(parent_part->getFullRelativePath(), !can_remove);
        }
        else
            remove();

        if (state == State::DeleteOnDestroy)
        {
            LOG_TRACE(storage.log, "Removed part from old location {}", path);
        }
    }
    catch (...)
    {
        /// FIXME If part it temporary, then directory will not be removed for 1 day (temporary_directories_lifetime).
        /// If it's tmp_merge_<part_name> or tmp_fetch_<part_name>,
        /// then all future attempts to execute part producing operation will fail with "directory already exists".
        /// Seems like it's especially important for remote disks, because removal may fail due to network issues.
        tryLogCurrentException(__PRETTY_FUNCTION__);
        assert(!is_temp);
        assert(state != State::DeleteOnDestroy);
        assert(state != State::Temporary);
    }
}


UInt64 IMergeTreeDataPart::getIndexSizeInBytes() const
{
    UInt64 res = 0;
    for (const ColumnPtr & column : index)
        res += column->byteSize();
    return res;
}

UInt64 IMergeTreeDataPart::getIndexSizeInAllocatedBytes() const
{
    UInt64 res = 0;
    for (const ColumnPtr & column : index)
        res += column->allocatedBytes();
    return res;
}

void IMergeTreeDataPart::assertState(const std::initializer_list<IMergeTreeDataPart::State> & affordable_states) const
{
    if (!checkState(affordable_states))
    {
        String states_str;
        for (auto affordable_state : affordable_states)
        {
            states_str += stateString(affordable_state);
            states_str += ' ';
        }

        throw Exception("Unexpected state of part " + getNameWithState() + ". Expected: " + states_str, ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART);
    }
}

void IMergeTreeDataPart::assertOnDisk() const
{
    if (!isStoredOnDisk())
        throw Exception("Data part '" + name + "' with type '"
            + getType().toString() + "' is not stored on disk", ErrorCodes::LOGICAL_ERROR);
}


UInt64 IMergeTreeDataPart::getMarksCount() const
{
    return index_granularity.getMarksCount();
}

size_t IMergeTreeDataPart::getFileSizeOrZero(const String & file_name) const
{
    auto checksum = checksums.files.find(file_name);
    if (checksum == checksums.files.end())
        return 0;
    return checksum->second.file_size;
}

String IMergeTreeDataPart::getColumnNameWithMinimumCompressedSize(
    const StorageSnapshotPtr & storage_snapshot, bool with_subcolumns) const
{
    auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withExtendedObjects();
    if (with_subcolumns)
        options.withSubcolumns();

    auto storage_columns = storage_snapshot->getColumns(options);
    MergeTreeData::AlterConversions alter_conversions;
    if (!parent_part)
        alter_conversions = storage.getAlterConversionsForPart(shared_from_this());

    std::optional<std::string> minimum_size_column;
    UInt64 minimum_size = std::numeric_limits<UInt64>::max();

    for (const auto & column : storage_columns)
    {
        auto column_name = column.name;
        auto column_type = column.type;
        if (alter_conversions.isColumnRenamed(column.name))
            column_name = alter_conversions.getColumnOldName(column.name);

        if (!hasColumnFiles(column))
            continue;

        const auto size = getColumnSize(column_name).data_compressed;
        if (size < minimum_size)
        {
            minimum_size = size;
            minimum_size_column = column_name;
        }
    }

    if (!minimum_size_column)
        throw Exception("Could not find a column of minimum size in MergeTree, part " + getFullPath(), ErrorCodes::LOGICAL_ERROR);

    return *minimum_size_column;
}

String IMergeTreeDataPart::getFullPath() const
{
    if (relative_path.empty())
        throw Exception("Part relative_path cannot be empty. It's bug.", ErrorCodes::LOGICAL_ERROR);

    return fs::path(storage.getFullPathOnDisk(volume->getDisk())) / (parent_part ? parent_part->relative_path : "") / relative_path / "";
}

String IMergeTreeDataPart::getFullRelativePath() const
{
    if (relative_path.empty())
        throw Exception("Part relative_path cannot be empty. It's bug.", ErrorCodes::LOGICAL_ERROR);

    return fs::path(storage.relative_data_path) / (parent_part ? parent_part->relative_path : "") / relative_path / "";
}

void IMergeTreeDataPart::loadColumnsChecksumsIndexes(bool require_columns_checksums, bool check_consistency)
{
    assertOnDisk();

    /// Memory should not be limited during ATTACH TABLE query.
    /// This is already true at the server startup but must be also ensured for manual table ATTACH.
    /// Motivation: memory for index is shared between queries - not belong to the query itself.
    MemoryTrackerBlockerInThread temporarily_disable_memory_tracker(VariableContext::Global);

    try
    {
        loadUUID();
        loadColumns(require_columns_checksums);
        loadChecksums(require_columns_checksums);
        loadIndexGranularity();
        calculateColumnsAndSecondaryIndicesSizesOnDisk();
        loadIndex(); /// Must be called after loadIndexGranularity as it uses the value of `index_granularity`
        loadRowsCount(); /// Must be called after loadIndexGranularity() as it uses the value of `index_granularity`.
        loadPartitionAndMinMaxIndex();
        if (!parent_part)
        {
            loadTTLInfos();
            loadProjections(require_columns_checksums, check_consistency);
        }

        if (check_consistency)
            checkConsistency(require_columns_checksums);

        loadDefaultCompressionCodec();
    }
    catch (...)
    {
        // There could be conditions that data part to be loaded is broken, but some of meta infos are already written
        // into meta data before exception, need to clean them all.
        metadata_manager->deleteAll(/*include_projection*/ true);
        metadata_manager->assertAllDeleted(/*include_projection*/ true);
        throw;
    }
}

void IMergeTreeDataPart::appendFilesOfColumnsChecksumsIndexes(Strings & files, bool include_projection) const
{
    if (isStoredOnDisk())
    {
        appendFilesOfUUID(files);
        appendFilesOfColumns(files);
        appendFilesOfChecksums(files);
        appendFilesOfIndexGranularity(files);
        appendFilesOfIndex(files);
        appendFilesOfRowsCount(files);
        appendFilesOfPartitionAndMinMaxIndex(files);
        appendFilesOfTTLInfos(files);
        appendFilesOfDefaultCompressionCodec(files);
    }

    if (!parent_part && include_projection)
    {
        for (const auto & [projection_name, projection_part] : projection_parts)
        {
            Strings projection_files;
            projection_part->appendFilesOfColumnsChecksumsIndexes(projection_files, true);
            for (const auto & projection_file : projection_files)
                files.push_back(fs::path(projection_part->relative_path) / projection_file);
        }
    }
}

void IMergeTreeDataPart::loadProjections(bool require_columns_checksums, bool check_consistency)
{
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    for (const auto & projection : metadata_snapshot->projections)
    {
        String path = getFullRelativePath() + projection.name + ".proj";
        if (volume->getDisk()->exists(path))
        {
            auto part = storage.createPart(projection.name, {"all", 0, 0, 0}, volume, projection.name + ".proj", this);
            part->loadColumnsChecksumsIndexes(require_columns_checksums, check_consistency);
            projection_parts.emplace(projection.name, std::move(part));
        }
    }
}

void IMergeTreeDataPart::loadIndexGranularity()
{
    throw Exception("Method 'loadIndexGranularity' is not implemented for part with type " + getType().toString(), ErrorCodes::NOT_IMPLEMENTED);
}

/// Currently we don't cache mark files of part, because cache other meta files is enough to speed up loading.
void IMergeTreeDataPart::appendFilesOfIndexGranularity(Strings & /* files */) const
{
}

void IMergeTreeDataPart::loadIndex()
{
    /// It can be empty in case of mutations
    if (!index_granularity.isInitialized())
        throw Exception("Index granularity is not loaded before index loading", ErrorCodes::LOGICAL_ERROR);

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    if (parent_part)
        metadata_snapshot = metadata_snapshot->projections.get(name).metadata;
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    size_t key_size = primary_key.column_names.size();

    if (key_size)
    {
        MutableColumns loaded_index;
        loaded_index.resize(key_size);

        for (size_t i = 0; i < key_size; ++i)
        {
            loaded_index[i] = primary_key.data_types[i]->createColumn();
            loaded_index[i]->reserve(index_granularity.getMarksCount());
        }

        String index_name = "primary.idx";
        String index_path = fs::path(getFullRelativePath()) / index_name;
        auto index_file = metadata_manager->read(index_name);
        size_t marks_count = index_granularity.getMarksCount();

        Serializations key_serializations(key_size);
        for (size_t j = 0; j < key_size; ++j)
            key_serializations[j] = primary_key.data_types[j]->getDefaultSerialization();

        for (size_t i = 0; i < marks_count; ++i) //-V756
            for (size_t j = 0; j < key_size; ++j)
                key_serializations[j]->deserializeBinary(*loaded_index[j], *index_file);

        for (size_t i = 0; i < key_size; ++i)
        {
            loaded_index[i]->protect();
            if (loaded_index[i]->size() != marks_count)
                throw Exception("Cannot read all data from index file " + index_path
                    + "(expected size: " + toString(marks_count) + ", read: " + toString(loaded_index[i]->size()) + ")",
                    ErrorCodes::CANNOT_READ_ALL_DATA);
        }

        if (!index_file->eof())
            throw Exception("Index file " + fullPath(volume->getDisk(), index_path) + " is unexpectedly long", ErrorCodes::EXPECTED_END_OF_FILE);

        index.assign(std::make_move_iterator(loaded_index.begin()), std::make_move_iterator(loaded_index.end()));
    }
}

void IMergeTreeDataPart::appendFilesOfIndex(Strings & files) const
{
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    if (parent_part)
        metadata_snapshot = metadata_snapshot->projections.has(name) ? metadata_snapshot->projections.get(name).metadata : nullptr;

    if (!metadata_snapshot)
        return;

    if (metadata_snapshot->hasPrimaryKey())
        files.push_back("primary.idx");
}

NameSet IMergeTreeDataPart::getFileNamesWithoutChecksums() const
{
    if (!isStoredOnDisk())
        return {};

    NameSet result = {"checksums.txt", "columns.txt"};
    String default_codec_path = fs::path(getFullRelativePath()) / DEFAULT_COMPRESSION_CODEC_FILE_NAME;
    String txn_version_path = fs::path(getFullRelativePath()) / TXN_VERSION_METADATA_FILE_NAME;

    if (volume->getDisk()->exists(default_codec_path))
        result.emplace(DEFAULT_COMPRESSION_CODEC_FILE_NAME);

    if (volume->getDisk()->exists(txn_version_path))
        result.emplace(TXN_VERSION_METADATA_FILE_NAME);

    return result;
}

void IMergeTreeDataPart::loadDefaultCompressionCodec()
{
    /// In memory parts doesn't have any compression
    if (!isStoredOnDisk())
    {
        default_codec = CompressionCodecFactory::instance().get("NONE", {});
        return;
    }

    String path = fs::path(getFullRelativePath()) / DEFAULT_COMPRESSION_CODEC_FILE_NAME;
    bool exists = metadata_manager->exists(DEFAULT_COMPRESSION_CODEC_FILE_NAME);
    if (!exists)
    {
        default_codec = detectDefaultCompressionCodec();
    }
    else
    {
        auto file_buf = metadata_manager->read(DEFAULT_COMPRESSION_CODEC_FILE_NAME);
        String codec_line;
        readEscapedStringUntilEOL(codec_line, *file_buf);

        ReadBufferFromString buf(codec_line);

        if (!checkString("CODEC", buf))
        {
            LOG_WARNING(
                storage.log,
                "Cannot parse default codec for part {} from file {}, content '{}'. Default compression codec will be deduced "
                "automatically, from data on disk",
                name,
                path,
                codec_line);
            default_codec = detectDefaultCompressionCodec();
        }

        try
        {
            ParserCodec codec_parser;
            auto codec_ast = parseQuery(codec_parser, codec_line.data() + buf.getPosition(), codec_line.data() + codec_line.length(), "codec parser", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
            default_codec = CompressionCodecFactory::instance().get(codec_ast, {});
        }
        catch (const DB::Exception & ex)
        {
            LOG_WARNING(storage.log, "Cannot parse default codec for part {} from file {}, content '{}', error '{}'. Default compression codec will be deduced automatically, from data on disk.", name, path, codec_line, ex.what());
            default_codec = detectDefaultCompressionCodec();
        }
    }
}

void IMergeTreeDataPart::appendFilesOfDefaultCompressionCodec(Strings & files)
{
    files.push_back(DEFAULT_COMPRESSION_CODEC_FILE_NAME);
}

CompressionCodecPtr IMergeTreeDataPart::detectDefaultCompressionCodec() const
{
    /// In memory parts doesn't have any compression
    if (!isStoredOnDisk())
        return CompressionCodecFactory::instance().get("NONE", {});

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();

    const auto & storage_columns = metadata_snapshot->getColumns();
    CompressionCodecPtr result = nullptr;
    for (const auto & part_column : columns)
    {
        /// It was compressed with default codec and it's not empty
        auto column_size = getColumnSize(part_column.name);
        if (column_size.data_compressed != 0 && !storage_columns.hasCompressionCodec(part_column.name))
        {
            String path_to_data_file;
            getSerialization(part_column)->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
            {
                if (path_to_data_file.empty())
                {
                    String candidate_path = fs::path(getFullRelativePath()) / (ISerialization::getFileNameForStream(part_column, substream_path) + ".bin");

                    /// We can have existing, but empty .bin files. Example: LowCardinality(Nullable(...)) columns and column_name.dict.null.bin file.
                    if (volume->getDisk()->exists(candidate_path) && volume->getDisk()->getFileSize(candidate_path) != 0)
                        path_to_data_file = candidate_path;
                }
            });

            if (path_to_data_file.empty())
            {
                LOG_WARNING(storage.log, "Part's {} column {} has non zero data compressed size, but all data files don't exist or empty", name, backQuoteIfNeed(part_column.name));
                continue;
            }

            result = getCompressionCodecForFile(volume->getDisk(), path_to_data_file);
            break;
        }
    }

    if (!result)
        result = CompressionCodecFactory::instance().getDefaultCodec();

    return result;
}

void IMergeTreeDataPart::loadPartitionAndMinMaxIndex()
{
    if (storage.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING && !parent_part)
    {
        DayNum min_date;
        DayNum max_date;
        MergeTreePartInfo::parseMinMaxDatesFromPartName(name, min_date, max_date);

        const auto & date_lut = DateLUT::instance();
        partition = MergeTreePartition(date_lut.toNumYYYYMM(min_date));
        minmax_idx = std::make_shared<MinMaxIndex>(min_date, max_date);
    }
    else
    {
        String path = getFullRelativePath();
        if (!parent_part)
            partition.load(storage, metadata_manager);

        if (!isEmpty())
        {
            if (parent_part)
                // projection parts don't have minmax_idx, and it's always initialized
                minmax_idx->initialized = true;
            else
                minmax_idx->load(storage, metadata_manager);
        }
        if (parent_part)
            return;
    }

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    String calculated_partition_id = partition.getID(metadata_snapshot->getPartitionKey().sample_block);
    if (calculated_partition_id != info.partition_id)
        throw Exception(
            "While loading part " + getFullPath() + ": calculated partition ID: " + calculated_partition_id
            + " differs from partition ID in part name: " + info.partition_id,
            ErrorCodes::CORRUPTED_DATA);
}

void IMergeTreeDataPart::appendFilesOfPartitionAndMinMaxIndex(Strings & files) const
{
    if (storage.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING && !parent_part)
        return;

    if (!parent_part)
        partition.appendFiles(storage, files);

    if (!parent_part)
        minmax_idx->appendFiles(storage, files);
}

void IMergeTreeDataPart::loadChecksums(bool require)
{
    const String path = fs::path(getFullRelativePath()) / "checksums.txt";
    bool exists = metadata_manager->exists("checksums.txt");
    if (exists)
    {
        auto buf = metadata_manager->read("checksums.txt");
        if (checksums.read(*buf))
        {
            assertEOF(*buf);
            bytes_on_disk = checksums.getTotalSizeOnDisk();
        }
        else
            bytes_on_disk = calculateTotalSizeOnDisk(volume->getDisk(), getFullRelativePath());
    }
    else
    {
        if (require)
            throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "No checksums.txt in part {}", name);

        /// If the checksums file is not present, calculate the checksums and write them to disk.
        /// Check the data while we are at it.
        LOG_WARNING(storage.log, "Checksums for part {} not found. Will calculate them from data on disk.", name);

        checksums = checkDataPart(shared_from_this(), false);

        {
            auto out = volume->getDisk()->writeFile(fs::path(getFullRelativePath()) / "checksums.txt.tmp", 4096);
            checksums.write(*out);
        }

        volume->getDisk()->moveFile(fs::path(getFullRelativePath()) / "checksums.txt.tmp", fs::path(getFullRelativePath()) / "checksums.txt");

        bytes_on_disk = checksums.getTotalSizeOnDisk();
    }
}

void IMergeTreeDataPart::appendFilesOfChecksums(Strings & files)
{
    files.push_back("checksums.txt");
}

void IMergeTreeDataPart::loadRowsCount()
{
    String path = fs::path(getFullRelativePath()) / "count.txt";

    auto read_rows_count = [&]()
    {
        auto buf = metadata_manager->read("count.txt");
        readIntText(rows_count, *buf);
        assertEOF(*buf);
    };

    if (index_granularity.empty())
    {
        rows_count = 0;
    }
    else if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING || part_type == Type::Compact || parent_part)
    {
        bool exists = metadata_manager->exists("count.txt");
        if (!exists)
            throw Exception("No count.txt in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

        read_rows_count();

#ifndef NDEBUG
        /// columns have to be loaded
        for (const auto & column : getColumns())
        {
            /// Most trivial types
            if (column.type->isValueRepresentedByNumber()
                && !column.type->haveSubtypes()
                && getSerialization(column)->getKind() == ISerialization::Kind::DEFAULT)
            {
                auto size = getColumnSize(column.name);

                if (size.data_uncompressed == 0)
                    continue;

                size_t rows_in_column = size.data_uncompressed / column.type->getSizeOfValueInMemory();
                if (rows_in_column != rows_count)
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Column {} has rows count {} according to size in memory "
                        "and size of single value, but data part {} has {} rows", backQuote(column.name), rows_in_column, name, rows_count);
                }

                size_t last_possibly_incomplete_mark_rows = index_granularity.getLastNonFinalMarkRows();
                /// All this rows have to be written in column
                size_t index_granularity_without_last_mark = index_granularity.getTotalRows() - last_possibly_incomplete_mark_rows;
                /// We have more rows in column than in index granularity without last possibly incomplete mark
                if (rows_in_column < index_granularity_without_last_mark)
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Column {} has rows count {} according to size in memory "
                        "and size of single value, but index granularity in part {} without last mark has {} rows, which is more than in column",
                        backQuote(column.name), rows_in_column, name, index_granularity.getTotalRows());
                }

                /// In last mark we actually written less or equal rows than stored in last mark of index granularity
                if (rows_in_column - index_granularity_without_last_mark > last_possibly_incomplete_mark_rows)
                {
                     throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Column {} has rows count {} in last mark according to size in memory "
                        "and size of single value, but index granularity in part {} in last mark has {} rows which is less than in column",
                        backQuote(column.name), rows_in_column - index_granularity_without_last_mark, name, last_possibly_incomplete_mark_rows);
                }
            }
        }
#endif
    }
    else
    {
        if (volume->getDisk()->exists(path))
        {
            read_rows_count();
            return;
        }

        for (const NameAndTypePair & column : columns)
        {
            ColumnPtr column_col = column.type->createColumn(*getSerialization(column));
            if (!column_col->isFixedAndContiguous() || column_col->lowCardinality())
                continue;

            size_t column_size = getColumnSize(column.name).data_uncompressed;
            if (!column_size)
                continue;

            size_t sizeof_field = column_col->sizeOfValueIfFixed();
            rows_count = column_size / sizeof_field;

            if (column_size % sizeof_field != 0)
            {
                throw Exception(
                    "Uncompressed size of column " + column.name + "(" + toString(column_size)
                    + ") is not divisible by the size of value (" + toString(sizeof_field) + ")",
                    ErrorCodes::LOGICAL_ERROR);
            }

            size_t last_mark_index_granularity = index_granularity.getLastNonFinalMarkRows();
            size_t rows_approx = index_granularity.getTotalRows();
            if (!(rows_count <= rows_approx && rows_approx < rows_count + last_mark_index_granularity))
                throw Exception(
                    "Unexpected size of column " + column.name + ": " + toString(rows_count) + " rows, expected "
                    + toString(rows_approx) + "+-" + toString(last_mark_index_granularity) + " rows according to the index",
                    ErrorCodes::LOGICAL_ERROR);

            return;
        }

        throw Exception("Data part doesn't contain fixed size column (even Date column)", ErrorCodes::LOGICAL_ERROR);
    }
}

void IMergeTreeDataPart::appendFilesOfRowsCount(Strings & files)
{
    files.push_back("count.txt");
}

void IMergeTreeDataPart::loadTTLInfos()
{
    bool exists = metadata_manager->exists("ttl.txt");
    if (exists)
    {
        auto in = metadata_manager->read("ttl.txt");
        assertString("ttl format version: ", *in);
        size_t format_version;
        readText(format_version, *in);
        assertChar('\n', *in);

        if (format_version == 1)
        {
            try
            {
                ttl_infos.read(*in);
            }
            catch (const JSONException &)
            {
                throw Exception("Error while parsing file ttl.txt in part: " + name, ErrorCodes::BAD_TTL_FILE);
            }
        }
        else
            throw Exception("Unknown ttl format version: " + toString(format_version), ErrorCodes::BAD_TTL_FILE);
    }
}


void IMergeTreeDataPart::appendFilesOfTTLInfos(Strings & files)
{
    files.push_back("ttl.txt");
}

void IMergeTreeDataPart::loadUUID()
{
    bool exists = metadata_manager->exists(UUID_FILE_NAME);
    if (exists)
    {
        auto in = metadata_manager->read(UUID_FILE_NAME);
        readText(uuid, *in);
        if (uuid == UUIDHelpers::Nil)
            throw Exception("Unexpected empty " + String(UUID_FILE_NAME) + " in part: " + name, ErrorCodes::LOGICAL_ERROR);
    }
}

void IMergeTreeDataPart::appendFilesOfUUID(Strings & files)
{
    files.push_back(UUID_FILE_NAME);
}

void IMergeTreeDataPart::loadColumns(bool require)
{
    String path = fs::path(getFullRelativePath()) / "columns.txt";
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    if (parent_part)
        metadata_snapshot = metadata_snapshot->projections.get(name).metadata;
    NamesAndTypesList loaded_columns;

    bool exists = metadata_manager->exists("columns.txt");
    if (!exists)
    {
        /// We can get list of columns only from columns.txt in compact parts.
        if (require || part_type == Type::Compact)
            throw Exception("No columns.txt in part " + name + ", expected path " + path + " on drive " + volume->getDisk()->getName(),
                ErrorCodes::NO_FILE_IN_DATA_PART);

        /// If there is no file with a list of columns, write it down.
        for (const NameAndTypePair & column : metadata_snapshot->getColumns().getAllPhysical())
            if (volume->getDisk()->exists(fs::path(getFullRelativePath()) / (getFileNameForColumn(column) + ".bin")))
                loaded_columns.push_back(column);

        if (columns.empty())
            throw Exception("No columns in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

        {
            auto buf = volume->getDisk()->writeFile(path + ".tmp", 4096);
            loaded_columns.writeText(*buf);
        }
        volume->getDisk()->moveFile(path + ".tmp", path);
    }
    else
    {
        auto in = metadata_manager->read("columns.txt");
        loaded_columns.readText(*in);

        for (const auto & column : loaded_columns)
        {
            const auto * aggregate_function_data_type = typeid_cast<const DataTypeAggregateFunction *>(column.type.get());
            if (aggregate_function_data_type && aggregate_function_data_type->isVersioned())
                aggregate_function_data_type->setVersion(0, /* if_empty */true);
        }
    }

    SerializationInfo::Settings settings =
    {
        .ratio_of_defaults_for_sparse = storage.getSettings()->ratio_of_defaults_for_sparse_serialization,
        .choose_kind = false,
    };

    SerializationInfoByName infos(loaded_columns, settings);
    exists =  metadata_manager->exists(SERIALIZATION_FILE_NAME);
    if (exists)
    {
        auto in = metadata_manager->read(SERIALIZATION_FILE_NAME);
        infos.readJSON(*in);
    }

    setColumns(loaded_columns);
    setSerializationInfos(infos);
}

void IMergeTreeDataPart::assertHasVersionMetadata(MergeTreeTransaction * txn) const
{
    TransactionID expected_tid = txn ? txn->tid : Tx::PrehistoricTID;
    if (version.creation_tid != expected_tid)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "CreationTID of part {} (table {}) is set to unexpected value {}, it's a bug. Current transaction: {}",
                        name, storage.getStorageID().getNameForLogs(), version.creation_tid, txn ? txn->dumpDescription() : "<none>");

    assert(!txn || storage.supportsTransactions());
    assert(!txn || volume->getDisk()->exists(fs::path(getFullRelativePath()) / TXN_VERSION_METADATA_FILE_NAME));
}

void IMergeTreeDataPart::storeVersionMetadata() const
{
    if (!wasInvolvedInTransaction())
        return;

    LOG_TEST(storage.log, "Writing version for {} (creation: {}, removal {})", name, version.creation_tid, version.removal_tid);
    assert(storage.supportsTransactions());

    if (!isStoredOnDisk())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Transactions are not supported for in-memory parts (table: {}, part: {})",
                        storage.getStorageID().getNameForLogs(), name);

    String version_file_name = fs::path(getFullRelativePath()) / TXN_VERSION_METADATA_FILE_NAME;
    String tmp_version_file_name = version_file_name + ".tmp";
    DiskPtr disk = volume->getDisk();
    {
        /// TODO IDisk interface does not allow to open file with O_EXCL flag (for DiskLocal),
        /// so we create empty file at first (expecting that createFile throws if file already exists)
        /// and then overwrite it.
        disk->createFile(tmp_version_file_name);
        auto out = disk->writeFile(tmp_version_file_name, 256, WriteMode::Rewrite);
        version.write(*out);
        out->finalize();
        out->sync();
    }

    SyncGuardPtr sync_guard;
    if (storage.getSettings()->fsync_part_directory)
        sync_guard = disk->getDirectorySyncGuard(getFullRelativePath());
    disk->replaceFile(tmp_version_file_name, version_file_name);
}

void IMergeTreeDataPart::appendCSNToVersionMetadata(VersionMetadata::WhichCSN which_csn) const
{
    assert(!version.creation_tid.isEmpty());
    assert(!(which_csn == VersionMetadata::WhichCSN::CREATION && version.creation_tid.isPrehistoric()));
    assert(!(which_csn == VersionMetadata::WhichCSN::CREATION && version.creation_csn == 0));
    assert(!(which_csn == VersionMetadata::WhichCSN::REMOVAL && (version.removal_tid.isPrehistoric() || version.removal_tid.isEmpty())));
    assert(!(which_csn == VersionMetadata::WhichCSN::REMOVAL && version.removal_csn == 0));
    assert(isStoredOnDisk());

    /// Small enough appends to file are usually atomic,
    /// so we append new metadata instead of rewriting file to reduce number of fsyncs.
    /// We don't need to do fsync when writing CSN, because in case of hard restart
    /// we will be able to restore CSN from transaction log in Keeper.

    String version_file_name = fs::path(getFullRelativePath()) / TXN_VERSION_METADATA_FILE_NAME;
    DiskPtr disk = volume->getDisk();
    auto out = disk->writeFile(version_file_name, 256, WriteMode::Append);
    version.writeCSN(*out, which_csn);
    out->finalize();
}

void IMergeTreeDataPart::appendRemovalTIDToVersionMetadata(bool clear) const
{
    assert(!version.creation_tid.isEmpty());
    assert(version.removal_csn == 0);
    assert(!version.removal_tid.isEmpty());
    assert(isStoredOnDisk());

    if (version.creation_tid.isPrehistoric() && !clear)
    {
        /// Metadata file probably does not exist, because it was not written on part creation, because it was created without a transaction.
        /// Let's create it (if needed). Concurrent writes are not possible, because creation_csn is prehistoric and we own removal_tid_lock.
        storeVersionMetadata();
        return;
    }

    if (clear)
        LOG_TEST(storage.log, "Clearing removal TID for {} (creation: {}, removal {})", name, version.creation_tid, version.removal_tid);
    else
        LOG_TEST(storage.log, "Appending removal TID for {} (creation: {}, removal {})", name, version.creation_tid, version.removal_tid);

    String version_file_name = fs::path(getFullRelativePath()) / TXN_VERSION_METADATA_FILE_NAME;
    DiskPtr disk = volume->getDisk();
    auto out = disk->writeFile(version_file_name, 256, WriteMode::Append);
    version.writeRemovalTID(*out, clear);
    out->finalize();

    /// fsync is not required when we clearing removal TID, because after hard restart we will fix metadata
    if (!clear)
        out->sync();
}

void IMergeTreeDataPart::loadVersionMetadata() const
try
{
    String version_file_name = fs::path(getFullRelativePath()) / TXN_VERSION_METADATA_FILE_NAME;
    String tmp_version_file_name = version_file_name + ".tmp";
    DiskPtr disk = volume->getDisk();

    auto remove_tmp_file = [&]()
    {
        auto last_modified = disk->getLastModified(tmp_version_file_name);
        auto buf = openForReading(disk, tmp_version_file_name);
        String content;
        readStringUntilEOF(content, *buf);
        LOG_WARNING(storage.log, "Found file {} that was last modified on {}, has size {} and the following content: {}",
                    tmp_version_file_name, last_modified.epochTime(), content.size(), content);
        disk->removeFile(tmp_version_file_name);
    };

    if (disk->exists(version_file_name))
    {
        auto buf = openForReading(disk, version_file_name);
        version.read(*buf);
        if (disk->exists(tmp_version_file_name))
            remove_tmp_file();
        return;
    }

    /// Four (?) cases are possible:
    /// 1. Part was created without transactions.
    /// 2. Version metadata file was not renamed from *.tmp on part creation.
    /// 3. Version metadata were written to *.tmp file, but hard restart happened before fsync.
    /// 4. Fsyncs in storeVersionMetadata() work incorrectly.

    if (!disk->exists(tmp_version_file_name))
    {
        /// Case 1.
        /// We do not have version metadata and transactions history for old parts,
        /// so let's consider that such parts were created by some ancient transaction
        /// and were committed with some prehistoric CSN.
        /// NOTE It might be Case 3, but version metadata file is written on part creation before other files,
        /// so it's not Case 3 if part is not broken.
        version.setCreationTID(Tx::PrehistoricTID, nullptr);
        version.creation_csn = Tx::PrehistoricCSN;
        return;
    }

    /// Case 2.
    /// Content of *.tmp file may be broken, just use fake TID.
    /// Transaction was not committed if *.tmp file was not renamed, so we should complete rollback by removing part.
    version.setCreationTID(Tx::DummyTID, nullptr);
    version.creation_csn = Tx::RolledBackCSN;
    remove_tmp_file();
}
catch (Exception & e)
{
    e.addMessage("While loading version metadata from table {} part {}", storage.getStorageID().getNameForLogs(), name);
    throw;
}

bool IMergeTreeDataPart::wasInvolvedInTransaction() const
{
    assert(!version.creation_tid.isEmpty() || (state == State::Temporary /* && std::uncaught_exceptions() */));
    bool created_by_transaction = !version.creation_tid.isPrehistoric();
    bool removed_by_transaction = version.isRemovalTIDLocked() && version.removal_tid_lock != Tx::PrehistoricTID.getHash();
    return created_by_transaction || removed_by_transaction;
}

bool IMergeTreeDataPart::assertHasValidVersionMetadata() const
{
    /// We don't have many tests with server restarts and it's really inconvenient to write such tests.
    /// So we use debug assertions to ensure that part version is written correctly.
    /// This method is not supposed to be called in release builds.

    if (isProjectionPart())
        return true;

    if (!wasInvolvedInTransaction())
        return true;

    if (!isStoredOnDisk())
        return false;

    if (part_is_probably_removed_from_disk)
        return true;

    if (state == State::Temporary)
        return true;

    DiskPtr disk = volume->getDisk();
    if (!disk->exists(getFullRelativePath()))
        return true;

    String content;
    String version_file_name = fs::path(getFullRelativePath()) / TXN_VERSION_METADATA_FILE_NAME;
    try
    {
        auto buf = openForReading(disk, version_file_name);
        readStringUntilEOF(content, *buf);
        ReadBufferFromString str_buf{content};
        VersionMetadata file;
        file.read(str_buf);
        bool valid_creation_tid = version.creation_tid == file.creation_tid;
        bool valid_removal_tid = version.removal_tid == file.removal_tid || version.removal_tid == Tx::PrehistoricTID;
        bool valid_creation_csn = version.creation_csn == file.creation_csn || version.creation_csn == Tx::RolledBackCSN;
        bool valid_removal_csn = version.removal_csn == file.removal_csn || version.removal_csn == Tx::PrehistoricCSN;
        if (!valid_creation_tid || !valid_removal_tid || !valid_creation_csn || !valid_removal_csn)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid version metadata file");
        return true;
    }
    catch (...)
    {
        WriteBufferFromOwnString expected;
        version.write(expected);
        tryLogCurrentException(storage.log, fmt::format("File {} contains:\n{}\nexpected:\n{}", version_file_name, content, expected.str()));
        return false;
    }
}


void IMergeTreeDataPart::appendFilesOfColumns(Strings & files)
{
    files.push_back("columns.txt");
    files.push_back(SERIALIZATION_FILE_NAME);
}

bool IMergeTreeDataPart::shallParticipateInMerges(const StoragePolicyPtr & storage_policy) const
{
    /// `IMergeTreeDataPart::volume` describes space where current part belongs, and holds
    /// `SingleDiskVolume` object which does not contain up-to-date settings of corresponding volume.
    /// Therefore we shall obtain volume from storage policy.
    auto volume_ptr = storage_policy->getVolume(storage_policy->getVolumeIndexByDisk(volume->getDisk()));

    return !volume_ptr->areMergesAvoided();
}

UInt64 IMergeTreeDataPart::calculateTotalSizeOnDisk(const DiskPtr & disk_, const String & from)
{
    if (disk_->isFile(from))
        return disk_->getFileSize(from);
    std::vector<std::string> files;
    disk_->listFiles(from, files);
    UInt64 res = 0;
    for (const auto & file : files)
        res += calculateTotalSizeOnDisk(disk_, fs::path(from) / file);
    return res;
}


void IMergeTreeDataPart::renameTo(const String & new_relative_path, bool remove_new_dir_if_exists) const
try
{
    assertOnDisk();

    String from = getFullRelativePath();
    String to = fs::path(storage.relative_data_path) / (parent_part ? parent_part->relative_path : "") / new_relative_path / "";

    if (!volume->getDisk()->exists(from))
        throw Exception("Part directory " + fullPath(volume->getDisk(), from) + " doesn't exist. Most likely it is a logical error.", ErrorCodes::FILE_DOESNT_EXIST);

    if (volume->getDisk()->exists(to))
    {
        if (remove_new_dir_if_exists)
        {
            Names files;
            volume->getDisk()->listFiles(to, files);

            LOG_WARNING(storage.log, "Part directory {} already exists and contains {} files. Removing it.", fullPath(volume->getDisk(), to), files.size());

            volume->getDisk()->removeRecursive(to);
        }
        else
        {
            throw Exception("Part directory " + fullPath(volume->getDisk(), to) + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);
        }
    }

    metadata_manager->deleteAll(true);
    metadata_manager->assertAllDeleted(true);
    volume->getDisk()->setLastModified(from, Poco::Timestamp::fromEpochTime(time(nullptr)));
    volume->getDisk()->moveDirectory(from, to);
    relative_path = new_relative_path;
    metadata_manager->updateAll(true);

    SyncGuardPtr sync_guard;
    if (storage.getSettings()->fsync_part_directory)
        sync_guard = volume->getDisk()->getDirectorySyncGuard(to);
}
catch (...)
{
    if (startsWith(new_relative_path, "detached/"))
    {
        // Don't throw when the destination is to the detached folder. It might be able to
        // recover in some cases, such as fetching parts into multi-disks while some of the
        // disks are broken.
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
    else
        throw;
}

std::pair<bool, NameSet> IMergeTreeDataPart::canRemovePart() const
{
    /// NOTE: It's needed for zero-copy replication
    if (force_keep_shared_data)
        return std::make_pair(false, NameSet{});

    return storage.unlockSharedData(*this);
}

void IMergeTreeDataPart::initializePartMetadataManager()
{
#if USE_ROCKSDB
    if (use_metadata_cache)
        metadata_manager = std::make_shared<PartMetadataManagerWithCache>(this, storage.getContext()->getMergeTreeMetadataCache());
    else
        metadata_manager = std::make_shared<PartMetadataManagerOrdinary>(this);
#else
        metadata_manager = std::make_shared<PartMetadataManagerOrdinary>(this);
#endif
}

void IMergeTreeDataPart::remove() const
{
    assert(assertHasValidVersionMetadata());
    part_is_probably_removed_from_disk = true;

    auto [can_remove, files_not_to_remove] = canRemovePart();

    if (!isStoredOnDisk())
        return;

    if (relative_path.empty())
        throw Exception("Part relative_path cannot be empty. This is bug.", ErrorCodes::LOGICAL_ERROR);

    if (isProjectionPart())
    {
        LOG_WARNING(storage.log, "Projection part {} should be removed by its parent {}.", name, parent_part->name);
        projectionRemove(parent_part->getFullRelativePath(), !can_remove);
        return;
    }

    metadata_manager->deleteAll(false);
    metadata_manager->assertAllDeleted(false);

    /** Atomic directory removal:
      * - rename directory to temporary name;
      * - remove it recursive.
      *
      * For temporary name we use "delete_tmp_" prefix.
      *
      * NOTE: We cannot use "tmp_delete_" prefix, because there is a second thread,
      *  that calls "clearOldTemporaryDirectories" and removes all directories, that begin with "tmp_" and are old enough.
      * But when we removing data part, it can be old enough. And rename doesn't change mtime.
      * And a race condition can happen that will lead to "File not found" error here.
      */

    /// NOTE We rename part to delete_tmp_<relative_path> instead of delete_tmp_<name> to avoid race condition
    /// when we try to remove two parts with the same name, but different relative paths,
    /// for example all_1_2_1 (in Deleting state) and tmp_merge_all_1_2_1 (in Temporary state).
    fs::path from = fs::path(storage.relative_data_path) / relative_path;
    fs::path to = fs::path(storage.relative_data_path) / ("delete_tmp_" + relative_path);
    // TODO directory delete_tmp_<name> is never removed if server crashes before returning from this function

    auto disk = volume->getDisk();
    if (disk->exists(to))
    {
        LOG_WARNING(storage.log, "Directory {} (to which part must be renamed before removing) already exists. Most likely this is due to unclean restart or race condition. Removing it.", fullPath(disk, to));
        try
        {
            disk->removeSharedRecursive(fs::path(to) / "", !can_remove, files_not_to_remove);
        }
        catch (...)
        {
            LOG_ERROR(storage.log, "Cannot recursively remove directory {}. Exception: {}", fullPath(disk, to), getCurrentExceptionMessage(false));
            throw;
        }
    }

    try
    {
        disk->moveDirectory(from, to);
    }
    catch (const fs::filesystem_error & e)
    {
        if (e.code() == std::errc::no_such_file_or_directory)
        {
            LOG_ERROR(storage.log, "Directory {} (part to remove) doesn't exist or one of nested files has gone. Most likely this is due to manual removing. This should be discouraged. Ignoring.", fullPath(disk, from));
            return;
        }
        throw;
    }

    // Record existing projection directories so we don't remove them twice
    std::unordered_set<String> projection_directories;
    for (const auto & [p_name, projection_part] : projection_parts)
    {
        /// NOTE: projections currently unsupported with zero copy replication.
        /// TODO: fix it.
        projection_part->projectionRemove(to, !can_remove);
        projection_directories.emplace(p_name + ".proj");
    }


    if (checksums.empty())
    {
        /// If the part is not completely written, we cannot use fast path by listing files.
        disk->removeSharedRecursive(fs::path(to) / "", !can_remove, files_not_to_remove);
    }
    else
    {
        try
        {
            /// Remove each expected file in directory, then remove directory itself.
            IDisk::RemoveBatchRequest request;

    #if !defined(__clang__)
    #    pragma GCC diagnostic push
    #    pragma GCC diagnostic ignored "-Wunused-variable"
    #endif
            for (const auto & [file, _] : checksums.files)
            {
                if (projection_directories.find(file) == projection_directories.end())
                    request.emplace_back(fs::path(to) / file);
            }
    #if !defined(__clang__)
    #    pragma GCC diagnostic pop
    #endif

            for (const auto & file : {"checksums.txt", "columns.txt"})
                request.emplace_back(fs::path(to) / file);

            request.emplace_back(fs::path(to) / DEFAULT_COMPRESSION_CODEC_FILE_NAME, true);
            request.emplace_back(fs::path(to) / DELETE_ON_DESTROY_MARKER_FILE_NAME, true);
            request.emplace_back(fs::path(to) / TXN_VERSION_METADATA_FILE_NAME, true);

            disk->removeSharedFiles(request, !can_remove, files_not_to_remove);
            disk->removeDirectory(to);
        }
        catch (...)
        {
            /// Recursive directory removal does many excessive "stat" syscalls under the hood.
            LOG_ERROR(storage.log, "Cannot quickly remove directory {} by removing files; fallback to recursive removal. Reason: {}", fullPath(disk, to), getCurrentExceptionMessage(false));

            disk->removeSharedRecursive(fs::path(to) / "", !can_remove, files_not_to_remove);
        }
    }
}


void IMergeTreeDataPart::projectionRemove(const String & parent_to, bool keep_shared_data) const
{
    metadata_manager->deleteAll(false);
    metadata_manager->assertAllDeleted(false);

    String to = fs::path(parent_to) / relative_path;
    auto disk = volume->getDisk();
    if (checksums.empty())
    {

        LOG_ERROR(
            storage.log,
            "Cannot quickly remove directory {} by removing files; fallback to recursive removal. Reason: checksums.txt is missing",
            fullPath(disk, to));
        /// If the part is not completely written, we cannot use fast path by listing files.
        disk->removeSharedRecursive(fs::path(to) / "", keep_shared_data, {});
    }
    else
    {
        try
        {
            /// Remove each expected file in directory, then remove directory itself.
            IDisk::RemoveBatchRequest request;

    #if !defined(__clang__)
    #    pragma GCC diagnostic push
    #    pragma GCC diagnostic ignored "-Wunused-variable"
    #endif
            for (const auto & [file, _] : checksums.files)
                request.emplace_back(fs::path(to) / file);
    #if !defined(__clang__)
    #    pragma GCC diagnostic pop
    #endif

            for (const auto & file : {"checksums.txt", "columns.txt"})
                request.emplace_back(fs::path(to) / file);
            request.emplace_back(fs::path(to) / DEFAULT_COMPRESSION_CODEC_FILE_NAME, true);
            request.emplace_back(fs::path(to) / DELETE_ON_DESTROY_MARKER_FILE_NAME, true);

            disk->removeSharedFiles(request, keep_shared_data, {});
            disk->removeSharedRecursive(to, keep_shared_data, {});
        }
        catch (...)
        {
            /// Recursive directory removal does many excessive "stat" syscalls under the hood.

            LOG_ERROR(storage.log, "Cannot quickly remove directory {} by removing files; fallback to recursive removal. Reason: {}", fullPath(disk, to), getCurrentExceptionMessage(false));

            disk->removeSharedRecursive(fs::path(to) / "", keep_shared_data, {});
         }
     }
 }

String IMergeTreeDataPart::getRelativePathForPrefix(const String & prefix, bool detached) const
{
    String res;

    /** If you need to detach a part, and directory into which we want to rename it already exists,
        *  we will rename to the directory with the name to which the suffix is added in the form of "_tryN".
        * This is done only in the case of `to_detached`, because it is assumed that in this case the exact name does not matter.
        * No more than 10 attempts are made so that there are not too many junk directories left.
        */

    auto full_relative_path = fs::path(storage.relative_data_path);
    if (detached)
        full_relative_path /= "detached";
    if (detached && parent_part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot detach projection");
    else if (parent_part)
        full_relative_path /= parent_part->relative_path;

    for (int try_no = 0; try_no < 10; ++try_no)
    {
        res = (prefix.empty() ? "" : prefix + "_") + name + (try_no ? "_try" + DB::toString(try_no) : "");

        if (!volume->getDisk()->exists(full_relative_path / res))
            return res;

        LOG_WARNING(storage.log, "Directory {} (to detach to) already exists. Will detach to directory with '_tryN' suffix.", res);
    }

    return res;
}

String IMergeTreeDataPart::getRelativePathForDetachedPart(const String & prefix) const
{
    /// Do not allow underscores in the prefix because they are used as separators.
    assert(prefix.find_first_of('_') == String::npos);
    assert(prefix.empty() || std::find(DetachedPartInfo::DETACH_REASONS.begin(),
                                       DetachedPartInfo::DETACH_REASONS.end(),
                                       prefix) != DetachedPartInfo::DETACH_REASONS.end());
    return "detached/" + getRelativePathForPrefix(prefix, /* detached */ true);
}

void IMergeTreeDataPart::renameToDetached(const String & prefix) const
{
    renameTo(getRelativePathForDetachedPart(prefix), true);
    part_is_probably_removed_from_disk = true;
}

void IMergeTreeDataPart::makeCloneInDetached(const String & prefix, const StorageMetadataPtr & /*metadata_snapshot*/) const
{
    String destination_path = fs::path(storage.relative_data_path) / getRelativePathForDetachedPart(prefix);
    localBackup(volume->getDisk(), getFullRelativePath(), destination_path);
    volume->getDisk()->removeFileIfExists(fs::path(destination_path) / DELETE_ON_DESTROY_MARKER_FILE_NAME);
}

void IMergeTreeDataPart::makeCloneOnDisk(const DiskPtr & disk, const String & directory_name) const
{
    assertOnDisk();

    if (disk->getName() == volume->getDisk()->getName())
        throw Exception("Can not clone data part " + name + " to same disk " + volume->getDisk()->getName(), ErrorCodes::LOGICAL_ERROR);
    if (directory_name.empty())
        throw Exception("Can not clone data part " + name + " to empty directory.", ErrorCodes::LOGICAL_ERROR);

    String path_to_clone = fs::path(storage.relative_data_path) / directory_name / "";

    if (disk->exists(fs::path(path_to_clone) / relative_path))
    {
        LOG_WARNING(storage.log, "Path {} already exists. Will remove it and clone again.", fullPath(disk, path_to_clone + relative_path));
        disk->removeRecursive(fs::path(path_to_clone) / relative_path / "");
    }
    disk->createDirectories(path_to_clone);
    volume->getDisk()->copy(getFullRelativePath(), disk, path_to_clone);
    volume->getDisk()->removeFileIfExists(fs::path(path_to_clone) / DELETE_ON_DESTROY_MARKER_FILE_NAME);
}

void IMergeTreeDataPart::checkConsistencyBase() const
{
    String path = getFullRelativePath();

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    if (parent_part)
        metadata_snapshot = metadata_snapshot->projections.get(name).metadata;
    else
    {
        // No need to check projections here because we already did consistent checking when loading projections if necessary.
    }

    const auto & pk = metadata_snapshot->getPrimaryKey();
    const auto & partition_key = metadata_snapshot->getPartitionKey();
    if (!checksums.empty())
    {
        if (!pk.column_names.empty() && !checksums.files.contains("primary.idx"))
            throw Exception("No checksum for primary.idx", ErrorCodes::NO_FILE_IN_DATA_PART);

        if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        {
            if (!checksums.files.contains("count.txt"))
                throw Exception("No checksum for count.txt", ErrorCodes::NO_FILE_IN_DATA_PART);

            if (metadata_snapshot->hasPartitionKey() && !checksums.files.contains("partition.dat"))
                throw Exception("No checksum for partition.dat", ErrorCodes::NO_FILE_IN_DATA_PART);

            if (!isEmpty() && !parent_part)
            {
                for (const String & col_name : storage.getMinMaxColumnsNames(partition_key))
                {
                    if (!checksums.files.contains("minmax_" + escapeForFileName(col_name) + ".idx"))
                        throw Exception("No minmax idx file checksum for column " + col_name, ErrorCodes::NO_FILE_IN_DATA_PART);
                }
            }
        }

        checksums.checkSizes(volume->getDisk(), path);
    }
    else
    {
        auto check_file_not_empty = [&path](const DiskPtr & disk_, const String & file_path)
        {
            UInt64 file_size;
            if (!disk_->exists(file_path) || (file_size = disk_->getFileSize(file_path)) == 0)
                throw Exception("Part " + fullPath(disk_, path) + " is broken: " + fullPath(disk_, file_path) + " is empty", ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
            return file_size;
        };

        /// Check that the primary key index is not empty.
        if (!pk.column_names.empty())
            check_file_not_empty(volume->getDisk(), fs::path(path) / "primary.idx");

        if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        {
            check_file_not_empty(volume->getDisk(), fs::path(path) / "count.txt");

            if (metadata_snapshot->hasPartitionKey())
                check_file_not_empty(volume->getDisk(), fs::path(path) / "partition.dat");

            if (!parent_part)
            {
                for (const String & col_name : storage.getMinMaxColumnsNames(partition_key))
                    check_file_not_empty(volume->getDisk(), fs::path(path) / ("minmax_" + escapeForFileName(col_name) + ".idx"));
            }
        }
    }
}

void IMergeTreeDataPart::checkConsistency(bool /* require_part_metadata */) const
{
    throw Exception("Method 'checkConsistency' is not implemented for part with type " + getType().toString(), ErrorCodes::NOT_IMPLEMENTED);
}

void IMergeTreeDataPart::calculateColumnsAndSecondaryIndicesSizesOnDisk()
{
    calculateColumnsSizesOnDisk();
    calculateSecondaryIndicesSizesOnDisk();
}

void IMergeTreeDataPart::calculateColumnsSizesOnDisk()
{
    if (getColumns().empty() || checksums.empty())
        throw Exception("Cannot calculate columns sizes when columns or checksums are not initialized", ErrorCodes::LOGICAL_ERROR);

    calculateEachColumnSizes(columns_sizes, total_columns_size);
}

void IMergeTreeDataPart::calculateSecondaryIndicesSizesOnDisk()
{
    if (checksums.empty())
        throw Exception("Cannot calculate secondary indexes sizes when columns or checksums are not initialized", ErrorCodes::LOGICAL_ERROR);

    auto secondary_indices_descriptions = storage.getInMemoryMetadataPtr()->secondary_indices;

    for (auto & index_description : secondary_indices_descriptions)
    {
        ColumnSize index_size;

        auto index_ptr = MergeTreeIndexFactory::instance().get(index_description);
        auto index_name = index_ptr->getFileName();
        auto index_name_escaped = escapeForFileName(index_name);

        auto index_file_name = index_name_escaped + index_ptr->getSerializedFileExtension();
        auto index_marks_file_name = index_name_escaped + index_granularity_info.marks_file_extension;

        /// If part does not contain index
        auto bin_checksum = checksums.files.find(index_file_name);
        if (bin_checksum != checksums.files.end())
        {
            index_size.data_compressed = bin_checksum->second.file_size;
            index_size.data_uncompressed = bin_checksum->second.uncompressed_size;
        }

        auto mrk_checksum = checksums.files.find(index_marks_file_name);
        if (mrk_checksum != checksums.files.end())
            index_size.marks = mrk_checksum->second.file_size;

        total_secondary_indices_size.add(index_size);
        secondary_index_sizes[index_description.name] = index_size;
    }
}

ColumnSize IMergeTreeDataPart::getColumnSize(const String & column_name) const
{
    /// For some types of parts columns_size maybe not calculated
    auto it = columns_sizes.find(column_name);
    if (it != columns_sizes.end())
        return it->second;

    return ColumnSize{};
}

IndexSize IMergeTreeDataPart::getSecondaryIndexSize(const String & secondary_index_name) const
{
    auto it = secondary_index_sizes.find(secondary_index_name);
    if (it != secondary_index_sizes.end())
        return it->second;

    return ColumnSize{};
}

void IMergeTreeDataPart::accumulateColumnSizes(ColumnToSize & column_to_size) const
{
    for (const auto & [column_name, size] : columns_sizes)
        column_to_size[column_name] = size.data_compressed;
}


bool IMergeTreeDataPart::checkAllTTLCalculated(const StorageMetadataPtr & metadata_snapshot) const
{
    if (!metadata_snapshot->hasAnyTTL())
        return false;

    if (metadata_snapshot->hasRowsTTL())
    {
        if (isEmpty()) /// All rows were finally deleted and we don't store TTL
            return true;
        else if (ttl_infos.table_ttl.min == 0)
            return false;
    }

    for (const auto & [column, desc] : metadata_snapshot->getColumnTTLs())
    {
        /// Part has this column, but we don't calculated TTL for it
        if (!ttl_infos.columns_ttl.contains(column) && getColumns().contains(column))
            return false;
    }

    for (const auto & move_desc : metadata_snapshot->getMoveTTLs())
    {
        /// Move TTL is not calculated
        if (!ttl_infos.moves_ttl.contains(move_desc.result_column))
            return false;
    }

    for (const auto & group_by_desc : metadata_snapshot->getGroupByTTLs())
    {
        if (!ttl_infos.group_by_ttl.contains(group_by_desc.result_column))
            return false;
    }

    for (const auto & rows_where_desc : metadata_snapshot->getRowsWhereTTLs())
    {
        if (!ttl_infos.rows_where_ttl.contains(rows_where_desc.result_column))
            return false;
    }

    return true;
}

String IMergeTreeDataPart::getUniqueId() const
{
    auto disk = volume->getDisk();
    if (!disk->supportZeroCopyReplication())
        throw Exception(fmt::format("Disk {} doesn't support zero-copy replication", disk->getName()), ErrorCodes::LOGICAL_ERROR);

    return disk->getUniqueId(fs::path(getFullRelativePath()) / FILE_FOR_REFERENCES_CHECK);
}

String IMergeTreeDataPart::getZeroLevelPartBlockID(std::string_view token) const
{
    if (info.level != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to get block id for non zero level part {}", name);

    SipHash hash;
    if (token.empty())
    {
        checksums.computeTotalChecksumDataOnly(hash);
    }
    else
    {
        hash.update(token.data(), token.size());
    }

    union
    {
        char bytes[16];
        UInt64 words[2];
    } hash_value;
    hash.get128(hash_value.bytes);

    return info.partition_id + "_" + toString(hash_value.words[0]) + "_" + toString(hash_value.words[1]);
}

IMergeTreeDataPart::uint128 IMergeTreeDataPart::getActualChecksumByFile(const String & file_path) const
{
    assert(use_metadata_cache);

    String file_name = std::filesystem::path(file_path).filename();
    const auto filenames_without_checksums = getFileNamesWithoutChecksums();
    auto it = checksums.files.find(file_name);
    if (!filenames_without_checksums.contains(file_name) && it != checksums.files.end())
    {
        return it->second.file_hash;
    }

    if (!volume->getDisk()->exists(file_path))
    {
        return {};
    }
    std::unique_ptr<ReadBufferFromFileBase> in_file = volume->getDisk()->readFile(file_path);
    HashingReadBuffer in_hash(*in_file);

    String value;
    readStringUntilEOF(value, in_hash);
    return in_hash.getHash();
}

std::unordered_map<String, IMergeTreeDataPart::uint128> IMergeTreeDataPart::checkMetadata() const
{
    return metadata_manager->check();
}

bool isCompactPart(const MergeTreeDataPartPtr & data_part)
{
    return (data_part && data_part->getType() == MergeTreeDataPartType::Compact);
}

bool isWidePart(const MergeTreeDataPartPtr & data_part)
{
    return (data_part && data_part->getType() == MergeTreeDataPartType::Wide);
}

bool isInMemoryPart(const MergeTreeDataPartPtr & data_part)
{
    return (data_part && data_part->getType() == MergeTreeDataPartType::InMemory);
}

}
