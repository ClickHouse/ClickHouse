#include "IMergeTreeDataPart.h"
#include <Storages/MergeTree/IDataPartStorage.h>
#include <base/types.h>

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
#include <Core/NamesAndTypes.h>
#include <Storages/ColumnsDescription.h>
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

#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>


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
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NO_FILE_IN_DATA_PART;
    extern const int EXPECTED_END_OF_FILE;
    extern const int CORRUPTED_DATA;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
    extern const int BAD_TTL_FILE;
    extern const int NOT_IMPLEMENTED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
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
        serialization->deserializeBinary(min_val, *file, {});
        Field max_val;
        serialization->deserializeBinary(max_val, *file, {});

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
    const MergeTreeData & data, IDataPartStorage & part_storage, Checksums & out_checksums) const
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    const auto & partition_key = metadata_snapshot->getPartitionKey();

    auto minmax_column_names = data.getMinMaxColumnsNames(partition_key);
    auto minmax_column_types = data.getMinMaxColumnsTypes(partition_key);

    return store(minmax_column_names, minmax_column_types, part_storage, out_checksums);
}

IMergeTreeDataPart::MinMaxIndex::WrittenFiles IMergeTreeDataPart::MinMaxIndex::store(
    const Names & column_names,
    const DataTypes & data_types,
    IDataPartStorage & part_storage,
    Checksums & out_checksums) const
{
    if (!initialized)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Attempt to store uninitialized MinMax index for part {}. This is a bug",
            part_storage.getFullPath());

    WrittenFiles written_files;

    for (size_t i = 0; i < column_names.size(); ++i)
    {
        String file_name = "minmax_" + escapeForFileName(column_names[i]) + ".idx";
        auto serialization = data_types.at(i)->getDefaultSerialization();

        auto out = part_storage.writeFile(file_name, DBMS_DEFAULT_BUFFER_SIZE, {});
        HashingWriteBuffer out_hashing(*out);
        serialization->serializeBinary(hyperrectangle[i].left, out_hashing, {});
        serialization->serializeBinary(hyperrectangle[i].right, out_hashing, {});
        out_hashing.finalize();
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


void IMergeTreeDataPart::incrementStateMetric(MergeTreeDataPartState state_) const
{
    switch (state_)
    {
        case MergeTreeDataPartState::Temporary:
            CurrentMetrics::add(CurrentMetrics::PartsTemporary);
            return;
        case MergeTreeDataPartState::PreActive:
            CurrentMetrics::add(CurrentMetrics::PartsPreActive);
            CurrentMetrics::add(CurrentMetrics::PartsPreCommitted);
            return;
        case MergeTreeDataPartState::Active:
            CurrentMetrics::add(CurrentMetrics::PartsActive);
            CurrentMetrics::add(CurrentMetrics::PartsCommitted);
            return;
        case MergeTreeDataPartState::Outdated:
            storage.total_outdated_parts_count.fetch_add(1, std::memory_order_relaxed);
            CurrentMetrics::add(CurrentMetrics::PartsOutdated);
            return;
        case MergeTreeDataPartState::Deleting:
            CurrentMetrics::add(CurrentMetrics::PartsDeleting);
            return;
        case MergeTreeDataPartState::DeleteOnDestroy:
            CurrentMetrics::add(CurrentMetrics::PartsDeleteOnDestroy);
            return;
    }
}

void IMergeTreeDataPart::decrementStateMetric(MergeTreeDataPartState state_) const
{
    switch (state_)
    {
        case MergeTreeDataPartState::Temporary:
            CurrentMetrics::sub(CurrentMetrics::PartsTemporary);
            return;
        case MergeTreeDataPartState::PreActive:
            CurrentMetrics::sub(CurrentMetrics::PartsPreActive);
            CurrentMetrics::sub(CurrentMetrics::PartsPreCommitted);
            return;
        case MergeTreeDataPartState::Active:
            CurrentMetrics::sub(CurrentMetrics::PartsActive);
            CurrentMetrics::sub(CurrentMetrics::PartsCommitted);
            return;
        case MergeTreeDataPartState::Outdated:
            storage.total_outdated_parts_count.fetch_sub(1, std::memory_order_relaxed);
            CurrentMetrics::sub(CurrentMetrics::PartsOutdated);
            return;
        case MergeTreeDataPartState::Deleting:
            CurrentMetrics::sub(CurrentMetrics::PartsDeleting);
            return;
        case MergeTreeDataPartState::DeleteOnDestroy:
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
    const MergeTreePartInfo & info_,
    const MutableDataPartStoragePtr & data_part_storage_,
    Type part_type_,
    const IMergeTreeDataPart * parent_part_)
    : DataPartStorageHolder(data_part_storage_)
    , storage(storage_)
    , mutable_name(name_)
    , name(mutable_name)
    , info(info_)
    , index_granularity_info(storage_, part_type_)
    , part_type(part_type_)
    , parent_part(parent_part_)
    , parent_part_name(parent_part ? parent_part->name : "")
    , use_metadata_cache(storage.use_metadata_cache)
{
    if (parent_part)
    {
        chassert(parent_part_name.starts_with(parent_part->info.partition_id));     /// Make sure there's no prefix
        state = MergeTreeDataPartState::Active;
    }

    incrementStateMetric(state);
    incrementTypeMetric(part_type);

    minmax_idx = std::make_shared<MinMaxIndex>();

    initializeIndexGranularityInfo();
    initializePartMetadataManager();
}

IMergeTreeDataPart::~IMergeTreeDataPart()
{
    decrementStateMetric(state);
    decrementTypeMetric(part_type);
}

void IMergeTreeDataPart::setName(const String & new_name)
{
    mutable_name = new_name;
    for (auto & proj_part : projection_parts)
        proj_part.second->parent_part_name = new_name;
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
        return new_part_info.getPartNameV1();
}

std::optional<size_t> IMergeTreeDataPart::getColumnPosition(const String & column_name) const
{
    auto it = column_name_to_position.find(column_name);
    if (it == column_name_to_position.end())
        return {};
    return it->second;
}


void IMergeTreeDataPart::setState(MergeTreeDataPartState new_state) const
{
    decrementStateMetric(state);
    state = new_state;
    incrementStateMetric(state);
}

MergeTreeDataPartState IMergeTreeDataPart::getState() const
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


void IMergeTreeDataPart::setColumns(const NamesAndTypesList & new_columns, const SerializationInfoByName & new_infos, int32_t metadata_version_)
{
    columns = new_columns;
    serialization_infos = new_infos;
    metadata_version = metadata_version_;

    column_name_to_position.clear();
    column_name_to_position.reserve(new_columns.size());
    size_t pos = 0;

    for (const auto & column : columns)
    {
        column_name_to_position.emplace(column.name, pos++);

        auto it = serialization_infos.find(column.name);
        auto serialization = it == serialization_infos.end()
            ? IDataType::getSerialization(column)
            : IDataType::getSerialization(column, *it->second);

        serializations.emplace(column.name, serialization);

        IDataType::forEachSubcolumn([&](const auto &, const auto & subname, const auto & subdata)
        {
            auto full_name = Nested::concatenateName(column.name, subname);
            serializations.emplace(full_name, subdata.serialization);
        }, ISerialization::SubstreamData(serialization));
    }

    columns_description = ColumnsDescription(columns);
    columns_description_with_collected_nested = ColumnsDescription(Nested::collect(columns));
}

NameAndTypePair IMergeTreeDataPart::getColumn(const String & column_name) const
{
    return columns_description.getColumnOrSubcolumn(GetColumnsOptions::AllPhysical, column_name);
}

std::optional<NameAndTypePair> IMergeTreeDataPart::tryGetColumn(const String & column_name) const
{
    return columns_description.tryGetColumnOrSubcolumn(GetColumnsOptions::AllPhysical, column_name);
}

SerializationPtr IMergeTreeDataPart::getSerialization(const String & column_name) const
{
    auto serialization = tryGetSerialization(column_name);
    if (!serialization)
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
            "There is no column or subcolumn {} in part {}", column_name, name);

    return serialization;
}

SerializationPtr IMergeTreeDataPart::tryGetSerialization(const String & column_name) const
{
    auto it = serializations.find(column_name);
    return it == serializations.end() ? nullptr : it->second;
}

void IMergeTreeDataPart::removeIfNeeded()
{
    assert(assertHasValidVersionMetadata());
    if (!is_temp && state != MergeTreeDataPartState::DeleteOnDestroy)
        return;

    std::string path;
    try
    {
        path = getDataPartStorage().getRelativePath();

        if (!getDataPartStorage().exists()) // path
            return;

        if (is_temp)
        {
            const auto & part_directory = getDataPartStorage().getPartDirectory();

            String file_name = fileName(part_directory);

            if (file_name.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "relative_path {} of part {} is invalid or not set",
                                getDataPartStorage().getPartDirectory(), name);

            fs::path part_directory_path = getDataPartStorage().getRelativePath();
            if (part_directory_path.filename().empty())
                part_directory_path = part_directory_path.parent_path();
            bool is_moving_part = part_directory_path.parent_path().filename() == "moving";
            if (!startsWith(file_name, "tmp") && !endsWith(file_name, ".tmp_proj") && !is_moving_part)
            {
                LOG_ERROR(
                    storage.log,
                    "~DataPart() should remove part {} but its name doesn't start with \"tmp\" or end with \".tmp_proj\". Too "
                    "suspicious, keeping the part.",
                    path);
                return;
            }

            if (is_moving_part)
            {
                LOG_TRACE(storage.log, "Removing unneeded moved part from {}", path);
            }
        }

        remove();

        if (state == MergeTreeDataPartState::DeleteOnDestroy)
        {
            LOG_TRACE(storage.log, "Removed part from old location {}", path);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, fmt::format("while removing part {} with path {}", name, path));

        /// FIXME If part it temporary, then directory will not be removed for 1 day (temporary_directories_lifetime).
        /// If it's tmp_merge_<part_name> or tmp_fetch_<part_name>,
        /// then all future attempts to execute part producing operation will fail with "directory already exists".
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

void IMergeTreeDataPart::assertState(const std::initializer_list<MergeTreeDataPartState> & affordable_states) const
{
    if (!checkState(affordable_states))
    {
        String states_str;
        for (auto affordable_state : affordable_states)
        {
            states_str += stateString(affordable_state);
            states_str += ' ';
        }

        if (!states_str.empty())
            states_str.pop_back();

        throw Exception(ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART, "Unexpected state of part {}. Expected: {}", getNameWithState(), states_str);
    }
}

void IMergeTreeDataPart::assertOnDisk() const
{
    if (!isStoredOnDisk())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Data part '{}' with type '{}' is not stored on disk",
            name, getType().toString());
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

String IMergeTreeDataPart::getColumnNameWithMinimumCompressedSize(bool with_subcolumns) const
{
    auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns(with_subcolumns);
    auto columns_list = columns_description.get(options);

    std::optional<std::string> minimum_size_column;
    UInt64 minimum_size = std::numeric_limits<UInt64>::max();

    for (const auto & column : columns_list)
    {
        if (!hasColumnFiles(column))
            continue;

        const auto size = getColumnSize(column.name).data_compressed;
        if (size < minimum_size)
        {
            minimum_size = size;
            minimum_size_column = column.name;
        }
    }

    if (!minimum_size_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not find a column of minimum size in MergeTree, part {}",
                        getDataPartStorage().getFullPath());

    return *minimum_size_column;
}

void IMergeTreeDataPart::loadColumnsChecksumsIndexes(bool require_columns_checksums, bool check_consistency)
{
    assertOnDisk();

    /// Memory should not be limited during ATTACH TABLE query.
    /// This is already true at the server startup but must be also ensured for manual table ATTACH.
    /// Motivation: memory for index is shared between queries - not belong to the query itself.
    MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;

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
            loadProjections(require_columns_checksums, check_consistency, false /* if_not_loaded */);
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
        appendFilesOfMetadataVersion(files);
    }

    if (!parent_part && include_projection)
    {
        for (const auto & [projection_name, projection_part] : projection_parts)
        {
            Strings projection_files;
            projection_part->appendFilesOfColumnsChecksumsIndexes(projection_files, true);
            for (const auto & projection_file : projection_files)
                files.push_back(fs::path(projection_part->name + ".proj") / projection_file);
        }
    }
}

MergeTreeDataPartBuilder IMergeTreeDataPart::getProjectionPartBuilder(const String & projection_name, bool is_temp_projection)
{
    const char * projection_extension = is_temp_projection ? ".tmp_proj" : ".proj";
    auto projection_storage = getDataPartStorage().getProjection(projection_name + projection_extension, !is_temp_projection);
    MergeTreeDataPartBuilder builder(storage, projection_name, projection_storage);
    return builder.withPartInfo({"all", 0, 0, 0}).withParentPart(this);
}

void IMergeTreeDataPart::addProjectionPart(
    const String & projection_name,
    std::shared_ptr<IMergeTreeDataPart> && projection_part)
{
    if (hasProjection(projection_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Projection part {} in part {} is already loaded. This is a bug", projection_name, name);

    projection_parts[projection_name] = std::move(projection_part);
}

void IMergeTreeDataPart::loadProjections(bool require_columns_checksums, bool check_consistency, bool if_not_loaded)
{
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    for (const auto & projection : metadata_snapshot->projections)
    {
        auto path = projection.name + ".proj";
        if (getDataPartStorage().exists(path))
        {
            if (hasProjection(projection.name))
            {
                if (!if_not_loaded)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR, "Projection part {} in part {} is already loaded. This is a bug", projection.name, name);
            }
            else
            {
                auto part = getProjectionPartBuilder(projection.name).withPartFormatFromDisk().build();
                part->loadColumnsChecksumsIndexes(require_columns_checksums, check_consistency);
                addProjectionPart(projection.name, std::move(part));
            }
        }
    }
}

void IMergeTreeDataPart::loadIndexGranularity()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Method 'loadIndexGranularity' is not implemented for part with type {}", getType().toString());
}

/// Currently we don't cache mark files of part, because cache other meta files is enough to speed up loading.
void IMergeTreeDataPart::appendFilesOfIndexGranularity(Strings & /* files */) const
{
}

void IMergeTreeDataPart::loadIndex()
{
    /// It can be empty in case of mutations
    if (!index_granularity.isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index granularity is not loaded before index loading");

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

        String index_name = "primary" + getIndexExtensionFromFilesystem(getDataPartStorage()).value();
        String index_path = fs::path(getDataPartStorage().getRelativePath()) / index_name;
        auto index_file = metadata_manager->read(index_name);
        size_t marks_count = index_granularity.getMarksCount();

        Serializations key_serializations(key_size);
        for (size_t j = 0; j < key_size; ++j)
            key_serializations[j] = primary_key.data_types[j]->getDefaultSerialization();

        for (size_t i = 0; i < marks_count; ++i)
            for (size_t j = 0; j < key_size; ++j)
                key_serializations[j]->deserializeBinary(*loaded_index[j], *index_file, {});

        for (size_t i = 0; i < key_size; ++i)
        {
            loaded_index[i]->protect();
            if (loaded_index[i]->size() != marks_count)
                throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read all data from index file {}(expected size: "
                    "{}, read: {})", index_path, marks_count, loaded_index[i]->size());
        }

        if (!index_file->eof())
            throw Exception(ErrorCodes::EXPECTED_END_OF_FILE, "Index file {} is unexpectedly long", index_path);

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
    {
        String index_name = "primary" + getIndexExtensionFromFilesystem(getDataPartStorage()).value();
        files.push_back(index_name);
    }
}

NameSet IMergeTreeDataPart::getFileNamesWithoutChecksums() const
{
    if (!isStoredOnDisk())
        return {};

    NameSet result = {"checksums.txt", "columns.txt"};

    if (getDataPartStorage().exists(DEFAULT_COMPRESSION_CODEC_FILE_NAME))
        result.emplace(DEFAULT_COMPRESSION_CODEC_FILE_NAME);

    if (getDataPartStorage().exists(TXN_VERSION_METADATA_FILE_NAME))
        result.emplace(TXN_VERSION_METADATA_FILE_NAME);

    if (getDataPartStorage().exists(METADATA_VERSION_FILE_NAME))
        result.emplace(METADATA_VERSION_FILE_NAME);

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

    String path = fs::path(getDataPartStorage().getRelativePath()) / DEFAULT_COMPRESSION_CODEC_FILE_NAME;
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

template <typename Writer>
void IMergeTreeDataPart::writeMetadata(const String & filename, const WriteSettings & settings, Writer && writer)
{
    auto & data_part_storage = getDataPartStorage();
    auto tmp_filename = filename + ".tmp";

    data_part_storage.beginTransaction();

    try
    {
        {
            auto out = data_part_storage.writeFile(tmp_filename, 4096, settings);
            writer(*out);
            out->finalize();
        }

        data_part_storage.moveFile(tmp_filename, filename);
    }
    catch (...)
    {
        try
        {
            if (data_part_storage.exists(tmp_filename))
            {
                data_part_storage.removeFile(tmp_filename);
                data_part_storage.commitTransaction();
            }
        }
        catch (...)
        {
            tryLogCurrentException("IMergeTreeDataPart");
        }

        throw;
    }

    data_part_storage.commitTransaction();
}

void IMergeTreeDataPart::writeChecksums(const MergeTreeDataPartChecksums & checksums_, const WriteSettings & settings)
{
    writeMetadata("checksums.txt", settings, [&checksums_](auto & buffer)
    {
        checksums_.write(buffer);
    });
}

void IMergeTreeDataPart::writeColumns(const NamesAndTypesList & columns_, const WriteSettings & settings)
{
    writeMetadata("columns.txt", settings, [&columns_](auto & buffer)
    {
        columns_.writeText(buffer);
    });
}

void IMergeTreeDataPart::writeVersionMetadata(const VersionMetadata & version_, bool fsync_part_dir) const
{
    static constexpr auto filename = "txn_version.txt";
    static constexpr auto tmp_filename = "txn_version.txt.tmp";
    auto & data_part_storage = const_cast<IDataPartStorage &>(getDataPartStorage());

    try
    {
        {
            /// TODO IDisk interface does not allow to open file with O_EXCL flag (for DiskLocal),
            /// so we create empty file at first (expecting that createFile throws if file already exists)
            /// and then overwrite it.
            data_part_storage.createFile(tmp_filename);
            auto write_settings = storage.getContext()->getWriteSettings();
            auto buf = data_part_storage.writeFile(tmp_filename, 256, write_settings);
            version_.write(*buf);
            buf->finalize();
            buf->sync();
        }

        SyncGuardPtr sync_guard;
        if (fsync_part_dir)
            sync_guard = data_part_storage.getDirectorySyncGuard();
        data_part_storage.replaceFile(tmp_filename, filename);
    }
    catch (...)
    {
        try
        {
            if (data_part_storage.exists(tmp_filename))
                data_part_storage.removeFile(tmp_filename);
        }
        catch (...)
        {
            tryLogCurrentException("DataPartStorageOnDiskFull");
        }

        throw;
    }
}

void IMergeTreeDataPart::removeDeleteOnDestroyMarker()
{
    getDataPartStorage().removeFileIfExists(DELETE_ON_DESTROY_MARKER_FILE_NAME_DEPRECATED);
}

void IMergeTreeDataPart::removeVersionMetadata()
{
    getDataPartStorage().removeFileIfExists("txn_version.txt");
}


void IMergeTreeDataPart::removeMetadataVersion()
{
    getDataPartStorage().removeFileIfExists(METADATA_VERSION_FILE_NAME);
}

void IMergeTreeDataPart::appendFilesOfDefaultCompressionCodec(Strings & files)
{
    files.push_back(DEFAULT_COMPRESSION_CODEC_FILE_NAME);
}

void IMergeTreeDataPart::appendFilesOfMetadataVersion(Strings & files)
{
    files.push_back(METADATA_VERSION_FILE_NAME);
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
            getSerialization(part_column.name)->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
            {
                if (path_to_data_file.empty())
                {
                    String candidate_path = /*fs::path(getRelativePath()) */ (ISerialization::getFileNameForStream(part_column, substream_path) + ".bin");

                    /// We can have existing, but empty .bin files. Example: LowCardinality(Nullable(...)) columns and column_name.dict.null.bin file.
                    if (getDataPartStorage().exists(candidate_path) && getDataPartStorage().getFileSize(candidate_path) != 0)
                        path_to_data_file = candidate_path;
                }
            });

            if (path_to_data_file.empty())
            {
                LOG_WARNING(storage.log, "Part's {} column {} has non zero data compressed size, but all data files don't exist or empty", name, backQuoteIfNeed(part_column.name));
                continue;
            }

            result = getCompressionCodecForFile(getDataPartStorage(), path_to_data_file);
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

        const auto & date_lut = DateLUT::serverTimezoneInstance();
        partition = MergeTreePartition(date_lut.toNumYYYYMM(min_date));
        minmax_idx = std::make_shared<MinMaxIndex>(min_date, max_date);
    }
    else
    {
        //String path = getRelativePath();
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
        throw Exception(ErrorCodes::CORRUPTED_DATA, "While loading part {}: "
            "calculated partition ID: {} differs from partition ID in part name: {}",
            getDataPartStorage().getFullPath(), calculated_partition_id, info.partition_id);
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
            bytes_on_disk = getDataPartStorage().calculateTotalSizeOnDisk();
    }
    else
    {
        if (require)
            throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "No checksums.txt in part {}", name);

        /// If the checksums file is not present, calculate the checksums and write them to disk.
        /// Check the data while we are at it.
        LOG_WARNING(storage.log, "Checksums for part {} not found. Will calculate them from data on disk.", name);

        checksums = checkDataPart(shared_from_this(), false);
        writeChecksums(checksums, {});

        bytes_on_disk = checksums.getTotalSizeOnDisk();
    }
}

void IMergeTreeDataPart::appendFilesOfChecksums(Strings & files)
{
    files.push_back("checksums.txt");
}

void IMergeTreeDataPart::loadRowsCount()
{
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
            throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "No count.txt in part {}", name);

        read_rows_count();

#ifndef NDEBUG
        /// columns have to be loaded
        for (const auto & column : getColumns())
        {
            /// Most trivial types
            if (column.type->isValueRepresentedByNumber()
                && !column.type->haveSubtypes()
                && getSerialization(column.name)->getKind() == ISerialization::Kind::DEFAULT)
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
                                    "and size of single value, but data part {} has {} rows",
                                    backQuote(column.name), rows_in_column, name, rows_count);
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
                                    "and size of single value, "
                                    "but index granularity in part {} without last mark has {} rows, which "
                                    "is more than in column",
                                    backQuote(column.name), rows_in_column, name, index_granularity.getTotalRows());
                }

                /// In last mark we actually written less or equal rows than stored in last mark of index granularity
                if (rows_in_column - index_granularity_without_last_mark > last_possibly_incomplete_mark_rows)
                {
                     throw Exception(
                                     ErrorCodes::LOGICAL_ERROR,
                                     "Column {} has rows count {} in last mark according to size in memory "
                                     "and size of single value, "
                                     "but index granularity in part {} "
                                     "in last mark has {} rows which is less than in column",
                                     backQuote(column.name), rows_in_column - index_granularity_without_last_mark,
                                     name, last_possibly_incomplete_mark_rows);
                }
            }
        }
#endif
    }
    else
    {
        if (getDataPartStorage().exists("count.txt"))
        {
            read_rows_count();
            return;
        }

        for (const NameAndTypePair & column : columns)
        {
            ColumnPtr column_col = column.type->createColumn(*getSerialization(column.name));
            if (!column_col->isFixedAndContiguous() || column_col->lowCardinality())
                continue;

            size_t column_size = getColumnSize(column.name).data_uncompressed;
            if (!column_size)
                continue;

            size_t sizeof_field = column_col->sizeOfValueIfFixed();
            rows_count = column_size / sizeof_field;

            if (column_size % sizeof_field != 0)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Uncompressed size of column {}({}) is not divisible by the size of value ({})",
                                column.name, column_size, sizeof_field);
            }

            size_t last_mark_index_granularity = index_granularity.getLastNonFinalMarkRows();
            size_t rows_approx = index_granularity.getTotalRows();
            if (!(rows_count <= rows_approx && rows_approx < rows_count + last_mark_index_granularity))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of column {}: "
                    "{} rows, expected {}+-{} rows according to the index",
                    column.name, rows_count, rows_approx, toString(last_mark_index_granularity));

            return;
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Data part doesn't contain fixed size column (even Date column)");
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
                throw Exception(ErrorCodes::BAD_TTL_FILE, "Error while parsing file ttl.txt in part: {}", name);
            }
        }
        else
            throw Exception(ErrorCodes::BAD_TTL_FILE, "Unknown ttl format version: {}", toString(format_version));
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected empty {} in part: {}", String(UUID_FILE_NAME), name);
    }
}

void IMergeTreeDataPart::appendFilesOfUUID(Strings & files)
{
    files.push_back(UUID_FILE_NAME);
}

void IMergeTreeDataPart::loadColumns(bool require)
{
    String path = fs::path(getDataPartStorage().getRelativePath()) / "columns.txt";
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    if (parent_part)
        metadata_snapshot = metadata_snapshot->projections.get(name).metadata;
    NamesAndTypesList loaded_columns;

    bool is_readonly_storage = getDataPartStorage().isReadonly();

    if (!metadata_manager->exists("columns.txt"))
    {
        /// We can get list of columns only from columns.txt in compact parts.
        if (require || part_type == Type::Compact)
            throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "No columns.txt in part {}, expected path {} on drive {}",
                name, path, getDataPartStorage().getDiskName());

        /// If there is no file with a list of columns, write it down.
        for (const NameAndTypePair & column : metadata_snapshot->getColumns().getAllPhysical())
            if (getDataPartStorage().exists(getFileNameForColumn(column) + ".bin"))
                loaded_columns.push_back(column);

        if (columns.empty())
            throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "No columns in part {}", name);

        if (!is_readonly_storage)
            writeColumns(loaded_columns, {});
    }
    else
    {
        auto in = metadata_manager->read("columns.txt");
        loaded_columns.readText(*in);

        for (auto & column : loaded_columns)
            setVersionToAggregateFunctions(column.type, true);
    }

    SerializationInfo::Settings settings =
    {
        .ratio_of_defaults_for_sparse = storage.getSettings()->ratio_of_defaults_for_sparse_serialization,
        .choose_kind = false,
    };

    SerializationInfoByName infos;
    if (metadata_manager->exists(SERIALIZATION_FILE_NAME))
    {
        auto in = metadata_manager->read(SERIALIZATION_FILE_NAME);
        infos = SerializationInfoByName::readJSON(loaded_columns, settings, *in);
    }

    int32_t loaded_metadata_version;
    if (metadata_manager->exists(METADATA_VERSION_FILE_NAME))
    {
        auto in = metadata_manager->read(METADATA_VERSION_FILE_NAME);
        readIntText(loaded_metadata_version, *in);
    }
    else
    {
        loaded_metadata_version = metadata_snapshot->getMetadataVersion();
        old_part_with_no_metadata_version_on_disk = true;
        if (storage.supportsReplication())
            LOG_WARNING(storage.log, "Part {} doesn't have metadata version on disk, setting it to {}. "
                    "It's okay if the part was created by an old version of ClickHouse", name, loaded_metadata_version);
    }

    setColumns(loaded_columns, infos, loaded_metadata_version);
}


/// Project part / part with project parts / compact part doesn't support LWD.
bool IMergeTreeDataPart::supportLightweightDeleteMutate() const
{
    return (part_type == MergeTreeDataPartType::Wide || part_type == MergeTreeDataPartType::Compact) &&
        parent_part == nullptr && projection_parts.empty();
}

void IMergeTreeDataPart::assertHasVersionMetadata(MergeTreeTransaction * txn) const
{
    TransactionID expected_tid = txn ? txn->tid : Tx::PrehistoricTID;
    if (version.creation_tid != expected_tid)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "CreationTID of part {} (table {}) is set to unexpected value {}, it's a bug. Current transaction: {}",
                        name, storage.getStorageID().getNameForLogs(), version.creation_tid, txn ? txn->dumpDescription() : "<none>");

    assert(!txn || storage.supportsTransactions());
    assert(!txn || getDataPartStorage().exists(TXN_VERSION_METADATA_FILE_NAME));
}

void IMergeTreeDataPart::storeVersionMetadata(bool force) const
{
    if (!wasInvolvedInTransaction() && !force)
        return;

    LOG_TEST(storage.log, "Writing version for {} (creation: {}, removal {}, creation csn {})", name, version.creation_tid, version.removal_tid, version.creation_csn);
    assert(storage.supportsTransactions());

    if (!isStoredOnDisk())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Transactions are not supported for in-memory parts (table: {}, part: {})",
                        storage.getStorageID().getNameForLogs(), name);

    writeVersionMetadata(version, storage.getSettings()->fsync_part_directory);
}

void IMergeTreeDataPart::appendCSNToVersionMetadata(VersionMetadata::WhichCSN which_csn) const
{
    chassert(!version.creation_tid.isEmpty());
    chassert(!(which_csn == VersionMetadata::WhichCSN::CREATION && version.creation_tid.isPrehistoric()));
    chassert(!(which_csn == VersionMetadata::WhichCSN::CREATION && version.creation_csn == 0));
    chassert(!(which_csn == VersionMetadata::WhichCSN::REMOVAL && (version.removal_tid.isPrehistoric() || version.removal_tid.isEmpty())));
    chassert(!(which_csn == VersionMetadata::WhichCSN::REMOVAL && version.removal_csn == 0));
    chassert(isStoredOnDisk());

    /// Small enough appends to file are usually atomic,
    /// so we append new metadata instead of rewriting file to reduce number of fsyncs.
    /// We don't need to do fsync when writing CSN, because in case of hard restart
    /// we will be able to restore CSN from transaction log in Keeper.

    auto out = getDataPartStorage().writeTransactionFile(WriteMode::Append);
    version.writeCSN(*out, which_csn);
    out->finalize();
}

void IMergeTreeDataPart::appendRemovalTIDToVersionMetadata(bool clear) const
{
    chassert(!version.creation_tid.isEmpty());
    chassert(version.removal_csn == 0 || (version.removal_csn == Tx::PrehistoricCSN && version.removal_tid.isPrehistoric()));
    chassert(!version.removal_tid.isEmpty());
    chassert(isStoredOnDisk());

    if (version.creation_tid.isPrehistoric() && !clear)
    {
        /// Metadata file probably does not exist, because it was not written on part creation, because it was created without a transaction.
        /// Let's create it (if needed). Concurrent writes are not possible, because creation_csn is prehistoric and we own removal_tid_lock.

        /// It can happen that VersionMetadata::isVisible sets creation_csn to PrehistoricCSN when creation_tid is Prehistoric
        /// In order to avoid a race always write creation_csn as PrehistoricCSN for Prehistoric creation_tid
        assert(version.creation_csn == Tx::UnknownCSN || version.creation_csn == Tx::PrehistoricCSN);
        version.creation_csn.store(Tx::PrehistoricCSN);

        storeVersionMetadata();
        return;
    }

    if (clear)
        LOG_TEST(storage.log, "Clearing removal TID for {} (creation: {}, removal {})", name, version.creation_tid, version.removal_tid);
    else
        LOG_TEST(storage.log, "Appending removal TID for {} (creation: {}, removal {})", name, version.creation_tid, version.removal_tid);

    auto out = getDataPartStorage().writeTransactionFile(WriteMode::Append);
    version.writeRemovalTID(*out, clear);
    out->finalize();

    /// fsync is not required when we clearing removal TID, because after hard restart we will fix metadata
    if (!clear)
        out->sync();
}

static std::unique_ptr<ReadBufferFromFileBase> openForReading(const IDataPartStorage & part_storage, const String & filename)
{
    size_t file_size = part_storage.getFileSize(filename);
    return part_storage.readFile(filename, ReadSettings().adjustBufferSize(file_size), file_size, file_size);
}

void IMergeTreeDataPart::loadVersionMetadata() const
try
{
    static constexpr auto version_file_name = "txn_version.txt";
    static constexpr auto tmp_version_file_name = "txn_version.txt.tmp";
    auto & data_part_storage = const_cast<IDataPartStorage &>(getDataPartStorage());

    auto remove_tmp_file = [&]()
    {
        auto last_modified = data_part_storage.getLastModified();
        auto buf = openForReading(data_part_storage, tmp_version_file_name);

        String content;
        readStringUntilEOF(content, *buf);
        LOG_WARNING(storage.log, "Found file {} that was last modified on {}, has size {} and the following content: {}",
                    tmp_version_file_name, last_modified.epochTime(), content.size(), content);
        data_part_storage.removeFile(tmp_version_file_name);
    };

    if (data_part_storage.exists(version_file_name))
    {
        auto buf = openForReading(data_part_storage, version_file_name);
        version.read(*buf);
        if (data_part_storage.exists(tmp_version_file_name))
            remove_tmp_file();
        return;
    }

    /// Four (?) cases are possible:
    /// 1. Part was created without transactions.
    /// 2. Version metadata file was not renamed from *.tmp on part creation.
    /// 3. Version metadata were written to *.tmp file, but hard restart happened before fsync.
    /// 4. Fsyncs in storeVersionMetadata() work incorrectly.

    if (!data_part_storage.exists(tmp_version_file_name))
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
    assert(!storage.data_parts_loading_finished || !version.creation_tid.isEmpty() || (state == MergeTreeDataPartState::Temporary /* && std::uncaught_exceptions() */));
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

    if (state == MergeTreeDataPartState::Temporary)
        return true;

    if (!getDataPartStorage().exists())
        return true;

    String content;
    String version_file_name = TXN_VERSION_METADATA_FILE_NAME;
    try
    {
        size_t file_size = getDataPartStorage().getFileSize(TXN_VERSION_METADATA_FILE_NAME);
        auto buf = getDataPartStorage().readFile(TXN_VERSION_METADATA_FILE_NAME, ReadSettings().adjustBufferSize(file_size), file_size, std::nullopt);

        /// FIXME https://github.com/ClickHouse/ClickHouse/issues/48465
        if (dynamic_cast<CachedOnDiskReadBufferFromFile *>(buf.get()))
            return true;

        readStringUntilEOF(content, *buf);
        ReadBufferFromString str_buf{content};
        VersionMetadata file;
        file.read(str_buf);
        bool valid_creation_tid = version.creation_tid == file.creation_tid;
        bool valid_removal_tid = version.removal_tid == file.removal_tid || version.removal_tid == Tx::PrehistoricTID;
        bool valid_creation_csn = version.creation_csn == file.creation_csn || version.creation_csn == Tx::RolledBackCSN;
        bool valid_removal_csn = version.removal_csn == file.removal_csn || version.removal_csn == Tx::PrehistoricCSN;
        bool valid_removal_tid_lock = (version.removal_tid.isEmpty() && version.removal_tid_lock == 0)
            || (version.removal_tid_lock == version.removal_tid.getHash());
        if (!valid_creation_tid || !valid_removal_tid || !valid_creation_csn || !valid_removal_csn || !valid_removal_tid_lock)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid version metadata file");
        return true;
    }
    catch (...)
    {
        WriteBufferFromOwnString expected;
        version.write(expected);
        tryLogCurrentException(storage.log, fmt::format("File {} contains:\n{}\nexpected:\n{}\nlock: {}\nname: {}",
                                                        version_file_name, content, expected.str(), version.removal_tid_lock, name));
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
    auto disk_name = getDataPartStorage().getDiskName();
    return !storage_policy->getVolumeByDiskName(disk_name)->areMergesAvoided();
}

void IMergeTreeDataPart::renameTo(const String & new_relative_path, bool remove_new_dir_if_exists)
try
{
    assertOnDisk();

    std::string relative_path = storage.relative_data_path;
    bool fsync_dir = storage.getSettings()->fsync_part_directory;

    if (parent_part)
    {
        /// For projections, move is only possible inside parent part dir.
        relative_path = parent_part->getDataPartStorage().getRelativePath();
    }

    auto old_projection_root_path = getDataPartStorage().getRelativePath();
    auto to = fs::path(relative_path) / new_relative_path;

    metadata_manager->deleteAll(true);
    metadata_manager->assertAllDeleted(true);
    getDataPartStorage().rename(to.parent_path(), to.filename(), storage.log, remove_new_dir_if_exists, fsync_dir);
    metadata_manager->updateAll(true);

    auto new_projection_root_path = to.string();

    for (const auto & [_, part] : projection_parts)
        part->getDataPartStorage().changeRootPath(old_projection_root_path, new_projection_root_path);
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
    {
        LOG_DEBUG(storage.log, "Blobs for part {} cannot be removed because it's forced to be keeped", name);
        return std::make_pair(false, NameSet{});
    }

    return storage.unlockSharedData(*this);
}

void IMergeTreeDataPart::initializePartMetadataManager()
{
#if USE_ROCKSDB
    if (auto metadata_cache = storage.getContext()->tryGetMergeTreeMetadataCache(); metadata_cache && use_metadata_cache)
        metadata_manager = std::make_shared<PartMetadataManagerWithCache>(this, metadata_cache);
    else
        metadata_manager = std::make_shared<PartMetadataManagerOrdinary>(this);
#else
        metadata_manager = std::make_shared<PartMetadataManagerOrdinary>(this);
#endif
}

void IMergeTreeDataPart::initializeIndexGranularityInfo()
{
    auto mrk_type = MergeTreeIndexGranularityInfo::getMarksTypeFromFilesystem(getDataPartStorage());
    if (mrk_type)
        index_granularity_info = MergeTreeIndexGranularityInfo(storage, *mrk_type);
    else
        index_granularity_info = MergeTreeIndexGranularityInfo(storage, part_type);
}

void IMergeTreeDataPart::remove()
{
    assert(assertHasValidVersionMetadata());
    part_is_probably_removed_from_disk = true;

    auto can_remove_callback = [this] ()
    {
        /// Temporary projections are "subparts" which are generated during projections materialization
        /// We can always remove them without any additional checks.
        if (isProjectionPart() && is_temp)
        {
            LOG_TRACE(storage.log, "Temporary projection part {} can be removed", name);
            return CanRemoveDescription{.can_remove_anything = true, .files_not_to_remove = {} };
        }

        auto [can_remove, files_not_to_remove] = canRemovePart();
        if (!can_remove)
            LOG_TRACE(storage.log, "Blobs of part {} cannot be removed", name);

        if (!files_not_to_remove.empty())
            LOG_TRACE(storage.log, "Some blobs ({}) of part {} cannot be removed", fmt::join(files_not_to_remove, ", "), name);

        return CanRemoveDescription{.can_remove_anything = can_remove, .files_not_to_remove = files_not_to_remove };
    };

    if (!isStoredOnDisk())
        return;

    /// Projections should be never removed by themselves, they will be removed
    /// with by parent part.
    if (isProjectionPart() && !is_temp)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Projection part {} should be removed by its parent {}.", name, parent_part->name);

    metadata_manager->deleteAll(false);
    metadata_manager->assertAllDeleted(false);

    GinIndexStoreFactory::instance().remove(getDataPartStoragePtr()->getRelativePath());

    std::list<IDataPartStorage::ProjectionChecksums> projection_checksums;

    for (const auto & [p_name, projection_part] : projection_parts)
    {
        projection_part->metadata_manager->deleteAll(false);
        projection_part->metadata_manager->assertAllDeleted(false);
        projection_checksums.emplace_back(IDataPartStorage::ProjectionChecksums{.name = p_name, .checksums = projection_part->checksums});
    }

    bool is_temporary_part = is_temp || state == MergeTreeDataPartState::Temporary;
    getDataPartStorage().remove(std::move(can_remove_callback), checksums, projection_checksums, is_temporary_part, storage.log);
}

std::optional<String> IMergeTreeDataPart::getRelativePathForPrefix(const String & prefix, bool detached, bool broken) const
{
    assert(!broken || detached);
    String res;

    /** If you need to detach a part, and directory into which we want to rename it already exists,
        *  we will rename to the directory with the name to which the suffix is added in the form of "_tryN".
        * This is done only in the case of `to_detached`, because it is assumed that in this case the exact name does not matter.
        * No more than 10 attempts are made so that there are not too many junk directories left.
        */

    if (detached && parent_part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot detach projection");

    return getDataPartStorage().getRelativePathForPrefix(storage.log, prefix, detached, broken);
}

std::optional<String> IMergeTreeDataPart::getRelativePathForDetachedPart(const String & prefix, bool broken) const
{
    /// Do not allow underscores in the prefix because they are used as separators.
    assert(prefix.find_first_of('_') == String::npos);
    assert(prefix.empty() || std::find(DetachedPartInfo::DETACH_REASONS.begin(),
                                       DetachedPartInfo::DETACH_REASONS.end(),
                                       prefix) != DetachedPartInfo::DETACH_REASONS.end());
    if (auto path = getRelativePathForPrefix(prefix, /* detached */ true, broken))
        return "detached/" + *path;
    return {};
}

void IMergeTreeDataPart::renameToDetached(const String & prefix)
{
    auto path_to_detach = getRelativePathForDetachedPart(prefix, /* broken */ false);
    assert(path_to_detach);
    renameTo(path_to_detach.value(), true);
    part_is_probably_removed_from_disk = true;
}

DataPartStoragePtr IMergeTreeDataPart::makeCloneInDetached(const String & prefix, const StorageMetadataPtr & /*metadata_snapshot*/,
                                                           const DiskTransactionPtr & disk_transaction) const
{
    /// Avoid unneeded duplicates of broken parts if we try to detach the same broken part multiple times.
    /// Otherwise it may pollute detached/ with dirs with _tryN suffix and we will fail to remove broken part after 10 attempts.
    bool broken = !prefix.empty();
    auto maybe_path_in_detached = getRelativePathForDetachedPart(prefix, broken);
    if (!maybe_path_in_detached)
        return nullptr;

    /// In case of zero-copy replication we copy directory instead of hardlinks
    /// because hardlinks tracking doesn't work for detached parts.
    auto storage_settings = storage.getSettings();
    IDataPartStorage::ClonePartParams params
    {
        .copy_instead_of_hardlink = isStoredOnRemoteDiskWithZeroCopySupport() && storage.supportsReplication() && storage_settings->allow_remote_fs_zero_copy_replication,
        .make_source_readonly = true,
        .external_transaction = disk_transaction
    };
    return getDataPartStorage().freeze(
        storage.relative_data_path,
        *maybe_path_in_detached,
        /*save_metadata_callback=*/ {},
        params);
}

MutableDataPartStoragePtr IMergeTreeDataPart::makeCloneOnDisk(const DiskPtr & disk, const String & directory_name) const
{
    assertOnDisk();

    if (disk->getName() == getDataPartStorage().getDiskName())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not clone data part {} to same disk {}", name, getDataPartStorage().getDiskName());
    if (directory_name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not clone data part {} to empty directory.", name);

    String path_to_clone = fs::path(storage.relative_data_path) / directory_name / "";
    return getDataPartStorage().clonePart(path_to_clone, getDataPartStorage().getPartDirectory(), disk, storage.log);
}

UInt64 IMergeTreeDataPart::getIndexSizeFromFile() const
{
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    if (parent_part)
        metadata_snapshot = metadata_snapshot->projections.get(name).metadata;
    const auto & pk = metadata_snapshot->getPrimaryKey();
    if (!pk.column_names.empty())
    {
        String file = "primary" + getIndexExtension(false);
        if (checksums.files.contains("primary" + getIndexExtension(true)))
            file = "primary" + getIndexExtension(true);
        return getFileSizeOrZero(file);
    }
    return 0;
}

void IMergeTreeDataPart::checkConsistencyBase() const
{
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
        if (!pk.column_names.empty()
            && (!checksums.files.contains("primary" + getIndexExtension(false))
                && !checksums.files.contains("primary" + getIndexExtension(true))))
            throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "No checksum for {} or {}",
                            toString("primary" + getIndexExtension(false)), toString("primary" + getIndexExtension(true)));

        if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        {
            if (!checksums.files.contains("count.txt"))
                throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "No checksum for count.txt");

            if (metadata_snapshot->hasPartitionKey() && !checksums.files.contains("partition.dat"))
                throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "No checksum for partition.dat");

            if (!isEmpty() && !parent_part)
            {
                for (const String & col_name : storage.getMinMaxColumnsNames(partition_key))
                {
                    if (!checksums.files.contains("minmax_" + escapeForFileName(col_name) + ".idx"))
                        throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "No minmax idx file checksum for column {}", col_name);
                }
            }
        }

        checksums.checkSizes(getDataPartStorage());
    }
    else
    {
        auto check_file_not_empty = [this](const String & file_path)
        {
            UInt64 file_size;
            if (!getDataPartStorage().exists(file_path) || (file_size = getDataPartStorage().getFileSize(file_path)) == 0)
                throw Exception(
                    ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART,
                    "Part {} is broken: {} is empty",
                    getDataPartStorage().getFullPath(),
                    std::string(fs::path(getDataPartStorage().getFullPath()) / file_path));
            return file_size;
        };

        /// Check that the primary key index is not empty.
        if (!pk.column_names.empty())
        {
            String index_name = "primary" + getIndexExtensionFromFilesystem(getDataPartStorage()).value();
            check_file_not_empty(index_name);
        }

        if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        {
            check_file_not_empty("count.txt");

            if (metadata_snapshot->hasPartitionKey())
                check_file_not_empty("partition.dat");

            if (!parent_part)
            {
                for (const String & col_name : storage.getMinMaxColumnsNames(partition_key))
                    check_file_not_empty("minmax_" + escapeForFileName(col_name) + ".idx");
            }
        }
    }
}

void IMergeTreeDataPart::checkConsistency(bool /* require_part_metadata */) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'checkConsistency' is not implemented for part with type {}", getType().toString());
}

void IMergeTreeDataPart::calculateColumnsAndSecondaryIndicesSizesOnDisk()
{
    calculateColumnsSizesOnDisk();
    calculateSecondaryIndicesSizesOnDisk();
}

void IMergeTreeDataPart::calculateColumnsSizesOnDisk()
{
    if (getColumns().empty() || checksums.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot calculate columns sizes when columns or checksums are not initialized");

    calculateEachColumnSizes(columns_sizes, total_columns_size);
}

void IMergeTreeDataPart::calculateSecondaryIndicesSizesOnDisk()
{
    if (checksums.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot calculate secondary indexes sizes when columns or checksums are not initialized");

    auto secondary_indices_descriptions = storage.getInMemoryMetadataPtr()->secondary_indices;

    for (auto & index_description : secondary_indices_descriptions)
    {
        ColumnSize index_size;

        auto index_ptr = MergeTreeIndexFactory::instance().get(index_description);
        auto index_name = index_ptr->getFileName();
        auto index_name_escaped = escapeForFileName(index_name);

        auto index_file_name = index_name_escaped + index_ptr->getSerializedFileExtension();
        auto index_marks_file_name = index_name_escaped + getMarksFileExtension();

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

bool IMergeTreeDataPart::hasSecondaryIndex(const String & index_name) const
{
    auto file_name = INDEX_FILE_PREFIX + index_name;
    return checksums.has(file_name + ".idx") || checksums.has(file_name + ".idx2");
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
    return getDataPartStorage().getUniqueId();
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

    const auto hash_value = hash.get128();
    return info.partition_id + "_" + toString(hash_value.items[0]) + "_" + toString(hash_value.items[1]);
}

IMergeTreeDataPart::uint128 IMergeTreeDataPart::getActualChecksumByFile(const String & file_name) const
{
    assert(use_metadata_cache);

    const auto filenames_without_checksums = getFileNamesWithoutChecksums();
    auto it = checksums.files.find(file_name);
    if (!filenames_without_checksums.contains(file_name) && it != checksums.files.end())
    {
        return it->second.file_hash;
    }

    if (!getDataPartStorage().exists(file_name))
    {
        return {};
    }
    std::unique_ptr<ReadBufferFromFileBase> in_file = getDataPartStorage().readFile(file_name, {}, std::nullopt, std::nullopt);
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

std::optional<std::string> getIndexExtensionFromFilesystem(const IDataPartStorage & data_part_storage)
{
    if (data_part_storage.exists())
    {
        for (auto it = data_part_storage.iterate(); it->isValid(); it->next())
        {
            const auto & extension = fs::path(it->name()).extension();
            if (extension == getIndexExtension(true))
                return extension;
        }
    }
    return {".idx"};
}

bool isCompressedFromIndexExtension(const String & index_extension)
{
    return index_extension == getIndexExtension(true);
}

Strings getPartsNames(const MergeTreeDataPartsVector & parts)
{
    Strings part_names;
    for (const auto & p : parts)
        part_names.push_back(p->name);
    return part_names;
}

}
