#include <optional>
#include <Access/ContextAccess.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/UDT/BoundObjectTypeReferences.h>
#include <DataTypes/UDT/CanonicalTypeArguments.h>
#include <Databases/IDatabase.h>
#include <Databases/UDT/ILifecycleAdapter.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/UDTTableIntrospection.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageAlias.h>
#include <Storages/System/StorageSystemColumns.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <map>
#include <memory>
#include <span>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

namespace DB
{
namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool show_data_lake_catalogs_in_system_tables;
    extern const SettingsBool show_remote_databases_in_system_tables;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

StorageSystemColumns::StorageSystemColumns(const StorageID & table_id_)
    : StorageWithCommonVirtualColumns(table_id_)
{
    StorageInMemoryMetadata storage_metadata;

    auto string = std::make_shared<DataTypeString>();
    auto type_arguments = std::make_shared<DataTypeArray>(string);
    auto logical_reference = std::make_shared<DataTypeTuple>(
        DataTypes{
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()),
            std::make_shared<DataTypeUInt64>(),
            string,
            std::make_shared<DataTypeUUID>(),
            std::make_shared<DataTypeUInt64>(),
            string,
            type_arguments,
            string,
            string,
            string,
        },
        Names{
            "path",
            "occurrence_ordinal",
            "declared_type",
            "type_uuid",
            "type_revision",
            "type_definition_hash",
            "type_arguments",
            "type_instantiation_hash",
            "physical_type",
            "storage_fingerprint",
        });

    /// NOTE: when changing the list of columns, take care of the ColumnsSource::generate method,
    /// when they are referenced by their numeric positions.
    auto description = ColumnsDescription({
        { "database",           std::make_shared<DataTypeString>(), "Database name."},
        { "table",              std::make_shared<DataTypeString>(), "Table name."},
        { "name",               std::make_shared<DataTypeString>(), "Column name."},
        { "type",               std::make_shared<DataTypeString>(), "Column type."},
        { "position",           std::make_shared<DataTypeUInt64>(), "Ordinal position of a column in a table starting with 1."},
        { "default_kind",       std::make_shared<DataTypeString>(), "Expression type (DEFAULT, MATERIALIZED, ALIAS) for the default value, or an empty string if it is not defined."},
        { "default_expression", std::make_shared<DataTypeString>(), "Expression for the default value, or an empty string if it is not defined."},
        { "data_compressed_bytes",      std::make_shared<DataTypeUInt64>(), "The size of compressed data, in bytes."},
        { "data_uncompressed_bytes",    std::make_shared<DataTypeUInt64>(), "The size of decompressed data, in bytes."},
        { "marks_bytes",                std::make_shared<DataTypeUInt64>(), "The size of marks, in bytes."},
        { "comment",                    std::make_shared<DataTypeString>(), "Comment on the column, or an empty string if it is not defined."},
        { "is_in_partition_key", std::make_shared<DataTypeUInt8>(), "Flag that indicates whether the column is in the partition expression."},
        { "is_in_sorting_key",   std::make_shared<DataTypeUInt8>(), "Flag that indicates whether the column is in the sorting key expression."},
        { "is_in_primary_key",   std::make_shared<DataTypeUInt8>(), "Flag that indicates whether the column is in the primary key expression."},
        { "is_in_sampling_key",  std::make_shared<DataTypeUInt8>(), "Flag that indicates whether the column is in the sampling key expression."},
        { "compression_codec",   std::make_shared<DataTypeString>(), "Compression codec name."},
        { "character_octet_length",     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "Maximum length in bytes for binary data, character data, or text data and images. In ClickHouse makes sense only for FixedString data type. Otherwise, the NULL value is returned."},
        { "numeric_precision",          std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "Accuracy of approximate numeric data, exact numeric data, integer data, or monetary data. In ClickHouse it is bit width for integer types and decimal precision for Decimal types. Otherwise, the NULL value is returned."},
        { "numeric_precision_radix",    std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "The base of the number system is the accuracy of approximate numeric data, exact numeric data, integer data or monetary data. In ClickHouse it's 2 for integer types and 10 for Decimal types. Otherwise, the NULL value is returned."},
        { "numeric_scale",              std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "The scale of approximate numeric data, exact numeric data, integer data, or monetary data. In ClickHouse makes sense only for Decimal types. Otherwise, the NULL value is returned."},
        { "datetime_precision",         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "Decimal precision of DateTime64 data type. For other data types, the NULL value is returned."},
        { "serialization_hint",         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "A hint for column to choose serialization on inserts according to statistics."},
        { "statistics",                 std::make_shared<DataTypeString>(), "The types of statistics created in this columns."},
        { "udt_declared_type",          string, "Current qualified declared UDT at the column root, or an empty string."},
        { "udt_uuid",                   std::make_shared<DataTypeUUID>(), "Stable identity of the declared UDT at the column root, or the nil UUID."},
        { "udt_revision",               std::make_shared<DataTypeUInt64>(), "Immutable revision of the declared UDT at the column root, or zero."},
        { "udt_definition_hash",        string, "Format-tagged definition hash of the declared UDT at the column root, or an empty string."},
        { "udt_arguments",              type_arguments, "Canonical physical representations of the declared UDT arguments at the column root."},
        { "udt_instantiation_hash",     string, "Format-tagged semantic instantiation hash of the declared UDT at the column root, or an empty string."},
        { "udt_references",             std::make_shared<DataTypeArray>(logical_reference), "All logical UDT occurrences in this physical column, in canonical path order."}
    });

    description.setAliases({
        {"column", std::make_shared<DataTypeString>(), "name"}
    });

    storage_metadata.setColumns(description);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageSystemColumns::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}


namespace
{
    using Storages = std::map<std::pair<std::string, std::string>, StoragePtr>;

    String lowerHexDigest(const UDT::Digest & digest)
    {
        static constexpr std::array<char, 16> digits{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
        String result(digest.size() * 2, '\0');
        for (size_t index = 0; index < digest.size(); ++index)
        {
            result[2 * index] = digits[digest[index] >> 4];
            result[2 * index + 1] = digits[digest[index] & 0x0f];
        }
        return result;
    }

    String formatTaggedDigest(const UDT::Digest & digest)
    {
        return "v2:" + lowerHexDigest(digest);
    }

    std::vector<String> formatCanonicalTypeArguments(const UDT::CanonicalTypeArguments & arguments)
    {
        std::vector<String> result;
        result.reserve(arguments.values().size());
        for (const auto & argument : arguments.values())
        {
            result.push_back(std::visit(
                [](const auto & value) -> String
                {
                    using Value = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<Value, UDT::CanonicalTypeArgument>)
                        return value.getCanonicalName();
                    else if constexpr (std::is_same_v<Value, bool>)
                        return value ? "true" : "false";
                    else if constexpr (std::is_same_v<Value, String>)
                        return quoteString(value);
                    else
                        return std::to_string(value);
                },
                argument.value));
        }
        return result;
    }

    Array makeArgumentsArray(const std::vector<String> & arguments)
    {
        Array result;
        result.reserve(arguments.size());
        for (const auto & argument : arguments)
            result.emplace_back(argument);
        return result;
    }

    struct LogicalTypeReferenceProjection
    {
        UDT::PersistedTypeOccurrencePath path;
        String declared_type;
        UUID type_uuid = UUIDHelpers::Nil;
        UInt64 type_revision = 0;
        String type_definition_hash;
        std::vector<String> type_arguments;
        String type_instantiation_hash;
        String physical_type;
        String storage_fingerprint;
    };

    struct ColumnLogicalTypeProjection
    {
        std::optional<LogicalTypeReferenceProjection> root;
        Array references;
    };

    using LogicalTypeOccurrenceKey = std::pair<std::vector<UInt64>, UInt64>;

    struct CurrentLogicalTypeOccurrenceProjection
    {
        UInt32 descriptor_index = 0;
        String declared_type;
    };
}


class ColumnsSource final : public ISource
{
public:
    ColumnsSource(
        std::vector<UInt8> columns_mask_,
        SharedHeader header_,
        UInt64 max_block_size_,
        ColumnPtr databases_,
        ColumnPtr tables_,
        Storages storages_,
        Databases catalog_databases_,
        ContextPtr context_)
        : ISource(header_)
        , columns_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
        , databases(std::move(databases_))
        , tables(std::move(tables_))
        , storages(std::move(storages_))
        , catalog_databases(std::move(catalog_databases_))
        , context(std::move(context_))
        , client_info_interface(context->getClientInfo().interface)
        , total_tables(tables->size())
        , access(context->getAccess())
        , query_id(context->getCurrentQueryId())
        , lock_acquire_timeout(std::chrono::milliseconds(context->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds()))
    {
        need_to_check_access_for_tables = !access->isGranted(AccessType::SHOW_COLUMNS);
    }

    String getName() const override { return "Columns"; }

protected:
    Chunk generate() override
    {
        if (db_table_num >= total_tables)
            return {};

        MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();
        size_t rows_count = 0;

        while (rows_count < max_block_size && db_table_num < total_tables)
        {
            const std::string database_name = (*databases)[db_table_num].safeGet<std::string>();
            const std::string table_name = (*tables)[db_table_num].safeGet<std::string>();
            ++db_table_num;

            /// A shortcut: if we don't allow to list this table in SHOW TABLES, also exclude it from system.columns.
            /// This check must precede all UDT-specific work so hidden tables cannot be used as a schema-lock timing oracle.
            if (need_to_check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
                continue;

            StoragePtr storage = storages.at(std::make_pair(database_name, table_name));
            const auto * alias = storage->as<StorageAlias>();
            const bool need_to_check_access_for_columns
                = need_to_check_access_for_tables && !access->isGranted(AccessType::SHOW_COLUMNS, database_name, table_name);
            const bool need_udt_columns
                = std::any_of(columns_mask.begin() + 23, columns_mask.end(), [](UInt8 selected) { return selected != 0; });
            /// Alias metadata belongs to its target table. Its UDT sidecar cannot be validated against
            /// the alias storage/database identity, and resolving the target here would precede the
            /// target-column access checks below. Keep alias UDT projection fail-closed.
            const bool may_read_udt_sidecar
                = need_udt_columns && !need_to_check_access_for_columns && !alias;

            ColumnsDescription columns;
            Names cols_required_for_partition_key;
            Names cols_required_for_sorting_key;
            Names cols_required_for_primary_key;
            Names cols_required_for_sampling;
            IStorage::ColumnSizeByName column_sizes;
            SerializationInfoByName serialization_hints{{}};
            std::shared_ptr<const UDT::BoundObjectTypeReferences> bound_type_references;
            StorageMetadataHandle metadata_snapshot;
            StorageID source_table_id = StorageID::createEmpty();
            DatabasePtr introspection_database;
            std::shared_ptr<void> udt_introspection_lease;

            {
                TableLockHolder table_lock = storage->tryLockForShare(query_id, Poco::Timespan(lock_acquire_timeout.count() * 1000));

                if (table_lock == nullptr)
                {
                    // Table was dropped while acquiring the lock, skipping table
                    continue;
                }

                source_table_id = storage->getStorageID();
                metadata_snapshot = storage->getInMemoryMetadataPtr(context, false);

                if (may_read_udt_sidecar && metadata_snapshot->getBoundUDTReferences())
                {
                    /// This first read is only a cheap physical-table fast path.
                    /// A mapped table is reread under share -> ALTER -> schema:
                    /// ALTER spans the storage's durable-commit/live-publication
                    /// interval, while schema pins the matching authority root.
                    /// Retaining share preserves the cross-database RENAME order.
                    const auto database_it = catalog_databases.find(database_name);
                    if (database_it == catalog_databases.end() || !database_it->second)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Bound-table UDT authority database is absent from system.columns");
                    introspection_database = database_it->second;
                    udt_introspection_lease
                        = introspection_database->getUDTLifecycleAdapter().acquireTableIntrospectionLease(
                            storage,
                            lock_acquire_timeout,
                            [query_context = context]
                            {
                                if (const auto process_list_element = query_context->getProcessListElementSafe())
                                    static_cast<void>(process_list_element->checkTimeLimit());
                            });
                    if (introspection_database->tryGetTable(table_name, context) != storage)
                        continue;
                    source_table_id = storage->getStorageID();
                    metadata_snapshot = storage->getInMemoryMetadataPtr(context, false);
                }

                columns = metadata_snapshot->getColumns();

                const bool needs_column_metadata = columns_mask[7] || columns_mask[8] || columns_mask[9] || columns_mask[21];
                bool can_expose_any_column_metadata = !needs_column_metadata;
                if (needs_column_metadata)
                {
                    for (const auto & column : columns)
                    {
                        if (!alias || alias->isTargetTableGranted(context, AccessType::SHOW_COLUMNS, column.name))
                        {
                            can_expose_any_column_metadata = true;
                            break;
                        }
                    }
                }

                /// Certain information about a table - should be calculated only when the corresponding columns are queried.
                if (can_expose_any_column_metadata && (columns_mask[7] || columns_mask[8] || columns_mask[9]))
                {
                    if (auto sizes = storage->tryGetColumnSizes())
                        column_sizes = std::move(*sizes);
                }

                if (columns_mask[11])
                    cols_required_for_partition_key = metadata_snapshot->getColumnsRequiredForPartitionKey();
                if (columns_mask[12])
                    cols_required_for_sorting_key = metadata_snapshot->getColumnsRequiredForSortingKey();
                if (columns_mask[13])
                    cols_required_for_primary_key = metadata_snapshot->getColumnsRequiredForPrimaryKey();
                if (columns_mask[14])
                    cols_required_for_sampling = metadata_snapshot->getColumnsRequiredForSampling();

                if (can_expose_any_column_metadata && columns_mask[21])
                {
                    if (auto hints = storage->tryGetSerializationHints())
                        serialization_hints = std::move(*hints);
                }

                if (may_read_udt_sidecar)
                    bound_type_references = metadata_snapshot->getBoundUDTReferences();
            }

            std::vector<UInt8> visible_columns;
            std::vector<UInt8> visible_physical_columns;
            visible_columns.reserve(columns.size());
            for (const auto & column : columns)
            {
                const bool visible = !need_to_check_access_for_columns
                    || access->isGranted(AccessType::SHOW_COLUMNS, database_name, table_name, column.name);
                visible_columns.push_back(visible);
                if (column.default_desc.kind != ColumnDefaultKind::Alias
                    && column.default_desc.kind != ColumnDefaultKind::Ephemeral)
                {
                    visible_physical_columns.push_back(visible);
                }
            }

            /// A persisted UDT sidecar and its authority expectation are
            /// table-wide integrity objects. Column-scoped visibility cannot
            /// safely decode them without making hidden-column structure and
            /// corruption observable through work or diagnostics. Preserve
            /// ordinary row filtering, but expose UDT-specific fields only to
            /// callers with table-wide SHOW COLUMNS.
            std::vector<ColumnLogicalTypeProjection> logical_type_projections;
            if (bound_type_references)
            {
                const size_t physical_column_count = visible_physical_columns.size();
                logical_type_projections.resize(physical_column_count);
                const bool need_root_projection = std::any_of(
                    columns_mask.begin() + 23, columns_mask.begin() + 29, [](UInt8 selected) { return selected != 0; });
                const bool need_reference_array = columns_mask[29];
                const auto descriptors = bound_type_references->getDescriptors();

                if (!introspection_database)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Bound-table UDT authority database is absent from system.columns");
                auto current_declared_columns = UDT::projectCurrentDeclaredTableColumnTypes(
                    source_table_id,
                    *metadata_snapshot,
                    *introspection_database,
                    std::span<const UInt8>(visible_physical_columns),
                    need_reference_array);
                if (current_declared_columns.size() != physical_column_count)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Bound-table UDT projection has a different physical-column count");

                std::vector<std::map<LogicalTypeOccurrenceKey, CurrentLogicalTypeOccurrenceProjection>> current_occurrences;
                if (need_reference_array)
                {
                    current_occurrences.resize(physical_column_count);
                    for (size_t physical_ordinal = 0; physical_ordinal < current_declared_columns.size(); ++physical_ordinal)
                    {
                        const auto & column_projection = current_declared_columns[physical_ordinal];
                        if (!visible_physical_columns[physical_ordinal])
                        {
                            if (!column_projection.logical_occurrences.empty())
                                throw Exception(ErrorCodes::LOGICAL_ERROR, "A hidden system.columns column received a UDT presentation");
                            continue;
                        }
                        auto & occurrence_map = current_occurrences[physical_ordinal];
                        for (const auto & occurrence : column_projection.logical_occurrences)
                        {
                            if (!occurrence.declared_type)
                                throw Exception(ErrorCodes::LOGICAL_ERROR, "A system.columns UDT occurrence has no declared type");
                            const bool inserted = occurrence_map.emplace(
                                LogicalTypeOccurrenceKey{occurrence.type_child_ordinals, occurrence.occurrence_ordinal},
                                CurrentLogicalTypeOccurrenceProjection{
                                    .descriptor_index = occurrence.descriptor_index,
                                    .declared_type = occurrence.declared_type->formatWithSecretsOneLine(),
                                }).second;
                            if (!inserted)
                                throw Exception(ErrorCodes::LOGICAL_ERROR, "A system.columns UDT occurrence presentation is duplicated");
                        }
                    }
                }

                for (const auto & use : bound_type_references->getUses())
                {
                    const auto & path = use.getPath();
                    if (path.section != UDT::PersistedTypePathSection::ColumnType
                        || path.site != UDT::PersistedTypeOccurrenceSite::Declaration
                        || path.object_ordinal >= logical_type_projections.size())
                    {
                        if (need_to_check_access_for_columns)
                            continue;
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Bound-table UDT reference has an invalid system.columns path");
                    }

                    const size_t physical_ordinal = static_cast<size_t>(path.object_ordinal);
                    if (!visible_physical_columns[physical_ordinal])
                        continue;

                    const bool is_root = path.type_child_ordinals.empty() && path.occurrence_ordinal == 0;
                    if ((!need_root_projection || !is_root) && !need_reference_array)
                        continue;

                    if (use.getDescriptorIndex() >= descriptors.size() || !descriptors[use.getDescriptorIndex()])
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Bound-table UDT reference has an invalid descriptor index");
                    String current_declared_type;
                    if (need_reference_array)
                    {
                        const auto presentation_it = current_occurrences[physical_ordinal].find(
                            LogicalTypeOccurrenceKey{path.type_child_ordinals, path.occurrence_ordinal});
                        if (presentation_it == current_occurrences[physical_ordinal].end()
                            || presentation_it->second.descriptor_index != use.getDescriptorIndex())
                        {
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Bound-table UDT reference lacks its checked system.columns presentation");
                        }
                        current_declared_type = presentation_it->second.declared_type;
                    }
                    else
                    {
                        const auto & column_projection = current_declared_columns[physical_ordinal];
                        if (!is_root || !column_projection.has_logical_references || !column_projection.declared_type)
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Bound-table UDT root lacks its checked system.columns presentation");
                        current_declared_type = column_projection.declared_type->formatWithSecretsOneLine();
                    }
                    const auto & descriptor = *descriptors[use.getDescriptorIndex()];
                    const auto & persisted = descriptor.getPersistedDescriptor();
                    const auto & identity = persisted.getDefinitionIdentity();

                    LogicalTypeReferenceProjection projection{
                        .path = path,
                        .declared_type = std::move(current_declared_type),
                        .type_uuid = identity.type_uuid,
                        .type_revision = identity.revision,
                        .type_definition_hash = formatTaggedDigest(persisted.getDefinitionHash()),
                        .type_arguments = formatCanonicalTypeArguments(descriptor.getCanonicalArguments()),
                        .type_instantiation_hash = formatTaggedDigest(persisted.getInstantiationSemanticHash()),
                        .physical_type = use.getPhysicalType()->getName(),
                        .storage_fingerprint = formatTaggedDigest(persisted.getStorageFingerprint()),
                    };

                    auto & column_projection = logical_type_projections[physical_ordinal];
                    if (need_root_projection && is_root)
                    {
                        if (column_projection.root)
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Bound-table UDT references contain duplicate root occurrences");
                        column_projection.root = projection;
                    }
                    if (need_reference_array)
                    {
                        Array type_path;
                        type_path.reserve(path.type_child_ordinals.size());
                        for (const UInt64 ordinal : path.type_child_ordinals)
                            type_path.emplace_back(ordinal);
                        column_projection.references.emplace_back(Tuple{
                            std::move(type_path),
                            path.occurrence_ordinal,
                            projection.declared_type,
                            projection.type_uuid,
                            projection.type_revision,
                            projection.type_definition_hash,
                            makeArgumentsArray(projection.type_arguments),
                            projection.type_instantiation_hash,
                            projection.physical_type,
                            projection.storage_fingerprint,
                        });
                    }
                }
            }

            size_t position = 0;
            size_t column_ordinal = 0;
            size_t physical_ordinal = 0;
            for (const auto & column : columns)
            {
                ++position;
                if (column_ordinal >= visible_columns.size())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "system.columns visibility mask is out of range");
                const bool visible = visible_columns[column_ordinal++];
                const bool is_physical
                    = column.default_desc.kind != ColumnDefaultKind::Alias && column.default_desc.kind != ColumnDefaultKind::Ephemeral;
                const ColumnLogicalTypeProjection * logical_projection = nullptr;
                if (is_physical && !logical_type_projections.empty())
                {
                    if (physical_ordinal >= logical_type_projections.size())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Bound-table UDT physical-column ordinal is out of range");
                    logical_projection = std::addressof(logical_type_projections[physical_ordinal]);
                }
                if (is_physical)
                    ++physical_ordinal;

                if (!visible)
                    continue;

                if (alias && !alias->isTargetTableGranted(context, AccessType::SHOW_COLUMNS, column.name))
                    continue;

                size_t src_index = 0;
                size_t res_index = 0;

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(database_name);
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(table_name);
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(column.name);
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(column.type->getName());
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(position);

                if (column.default_desc.expression)
                {
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(toString(column.default_desc.kind));
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(column.default_desc.expression->formatForLogging());
                }
                else
                {
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insertDefault();
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insertDefault();
                }

                {
                    const auto it = column_sizes.find(column.name);
                    if (it == std::end(column_sizes))
                    {
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();
                    }
                    else
                    {
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(it->second.data_compressed);
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(it->second.data_uncompressed);
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(it->second.marks);
                    }
                }

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(column.comment);

                {
                    auto find_in_vector = [&key = column.name](const Names& names)
                    {
                        return std::find(names.cbegin(), names.cend(), key) != names.end();
                    };

                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(find_in_vector(cols_required_for_partition_key));
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(find_in_vector(cols_required_for_sorting_key));
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(find_in_vector(cols_required_for_primary_key));
                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(find_in_vector(cols_required_for_sampling));
                }

                if (columns_mask[src_index++])
                {
                    if (column.codec)
                        res_columns[res_index++]->insert(column.codec->formatForLogging());
                    else
                        res_columns[res_index++]->insertDefault();
                }

                /// character_octet_length makes sense for FixedString only
                DataTypePtr not_nullable_type = removeNullable(column.type);
                if (columns_mask[src_index++])
                {
                    if (isFixedString(not_nullable_type))
                        res_columns[res_index++]->insert(not_nullable_type->getSizeOfValueInMemory());
                    else
                        res_columns[res_index++]->insertDefault();
                }

                /// numeric_precision
                if (columns_mask[src_index++])
                {
                    if (isInteger(not_nullable_type))
                        res_columns[res_index++]->insert(not_nullable_type->getSizeOfValueInMemory() * 8);  /// radix is 2
                    else if (isDecimal(not_nullable_type))
                        res_columns[res_index++]->insert(getDecimalPrecision(*not_nullable_type));  /// radix is 10
                    else
                        res_columns[res_index++]->insertDefault();
                }

                /// numeric_precision_radix
                if (columns_mask[src_index++])
                {
                    if (isInteger(not_nullable_type))
                        res_columns[res_index++]->insert(2);
                    else if (isDecimal(not_nullable_type))
                        res_columns[res_index++]->insert(10);
                    else
                        res_columns[res_index++]->insertDefault();
                }

                /// numeric_scale
                if (columns_mask[src_index++])
                {
                    if (isInteger(not_nullable_type))
                        res_columns[res_index++]->insert(0);
                    else if (isDecimal(not_nullable_type))
                        res_columns[res_index++]->insert(getDecimalScale(*not_nullable_type));
                    else
                        res_columns[res_index++]->insertDefault();
                }

                /// datetime_precision
                if (columns_mask[src_index++])
                {
                    if (isDateTime64(not_nullable_type))
                        res_columns[res_index++]->insert(assert_cast<const DataTypeDateTime64 &>(*not_nullable_type).getScale());
                    else if (isDateOrDate32(not_nullable_type) || isDateTime(not_nullable_type) || isDateTime64(not_nullable_type))
                        res_columns[res_index++]->insert(0);
                    else
                        res_columns[res_index++]->insertDefault();
                }

                /// serialization_hint
                if (columns_mask[src_index++])
                {
                    if (auto it = serialization_hints.find(column.name); it != serialization_hints.end())
                        res_columns[res_index++]->insert(ISerialization::kindStackToString(it->second->getKindStack()));
                    else
                        res_columns[res_index++]->insertDefault();
                }

                /// statistics
                if (columns_mask[src_index++])
                {
                    const ColumnStatisticsDescription & stats = column.statistics;
                    res_columns[res_index++]->insert(stats.getNameForLogs());
                }

                const LogicalTypeReferenceProjection * root_projection
                    = logical_projection && logical_projection->root ? std::addressof(*logical_projection->root) : nullptr;
                if (columns_mask[src_index++])
                    root_projection ? res_columns[res_index++]->insert(root_projection->declared_type) : res_columns[res_index++]->insertDefault();
                if (columns_mask[src_index++])
                    root_projection ? res_columns[res_index++]->insert(root_projection->type_uuid) : res_columns[res_index++]->insertDefault();
                if (columns_mask[src_index++])
                    root_projection ? res_columns[res_index++]->insert(root_projection->type_revision) : res_columns[res_index++]->insertDefault();
                if (columns_mask[src_index++])
                    root_projection ? res_columns[res_index++]->insert(root_projection->type_definition_hash) : res_columns[res_index++]->insertDefault();
                if (columns_mask[src_index++])
                    root_projection ? res_columns[res_index++]->insert(makeArgumentsArray(root_projection->type_arguments))
                                    : res_columns[res_index++]->insertDefault();
                if (columns_mask[src_index++])
                    root_projection ? res_columns[res_index++]->insert(root_projection->type_instantiation_hash)
                                    : res_columns[res_index++]->insertDefault();
                if (columns_mask[src_index++])
                    logical_projection ? res_columns[res_index++]->insert(logical_projection->references)
                                       : res_columns[res_index++]->insertDefault();

                ++rows_count;
            }

            if (bound_type_references && physical_ordinal != logical_type_projections.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Bound-table UDT physical-column count disagrees with system.columns metadata");
            if (column_ordinal != visible_columns.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "system.columns visibility mask has a different column count");
        }

        return Chunk(std::move(res_columns), rows_count);
    }

private:
    std::vector<UInt8> columns_mask;
    UInt64 max_block_size;
    ColumnPtr databases;
    ColumnPtr tables;
    Storages storages;
    Databases catalog_databases;
    ContextPtr context;
    ClientInfo::Interface client_info_interface;
    size_t db_table_num = 0;
    size_t total_tables;
    std::shared_ptr<const ContextAccessWrapper> access;
    bool need_to_check_access_for_tables;
    String query_id;
    std::chrono::milliseconds lock_acquire_timeout;
};

class ReadFromSystemColumns : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromSystemColumns"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromSystemColumns(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        std::shared_ptr<StorageSystemColumns> storage_,
        std::vector<UInt8> columns_mask_,
        size_t max_block_size_)
        : SourceStepWithFilter(
            std::make_shared<const Block>(std::move(sample_block)),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , storage(std::move(storage_))
        , columns_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
    {
    }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    std::shared_ptr<StorageSystemColumns> storage;
    std::vector<UInt8> columns_mask;
    const size_t max_block_size;
    std::optional<ActionsDAG> virtual_columns_filter;
};

void ReadFromSystemColumns::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    if (filter_actions_dag)
    {
        Block block_to_filter;
        block_to_filter.insert(ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "database"));
        block_to_filter.insert(ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "table"));

        virtual_columns_filter = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &block_to_filter, context);

        /// Must prepare sets here, initializePipeline() would be too late, see comment on FutureSetFromSubquery.
        if (virtual_columns_filter)
            VirtualColumnUtils::buildSetsForDAG(*virtual_columns_filter, context);
    }
}

void StorageSystemColumns::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);
    Block sample_block = storage_snapshot->metadata->getSampleBlock();

    auto [columns_mask, header] = getQueriedColumnsMaskAndHeader(sample_block, column_names);

    auto this_ptr = std::static_pointer_cast<StorageSystemColumns>(shared_from_this());

    auto reading = std::make_unique<ReadFromSystemColumns>(
        column_names, query_info, storage_snapshot,
        std::move(context), std::move(header), std::move(this_ptr), std::move(columns_mask), max_block_size);

    query_plan.addStep(std::move(reading));
}

void ReadFromSystemColumns::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    Block block_to_filter;
    Storages storages;
    Databases catalog_databases;
    Pipes pipes;
    auto header = getOutputHeader();

    {
        /// Add `database` column.
        MutableColumnPtr database_column_mut = ColumnString::create();

        const auto & context = getContext();
        const auto & settings = context->getSettingsRef();
        catalog_databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{
            .with_datalake_catalogs = settings[Setting::show_data_lake_catalogs_in_system_tables],
            .with_remote_databases = settings[Setting::show_remote_databases_in_system_tables]});
        for (const auto & [database_name, database] : catalog_databases)
        {
            if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
                continue; /// We don't want to show the internal database for temporary tables in system.columns
            database_column_mut->insert(database_name);
        }

        Tables external_tables;
        if (context->hasSessionContext())
        {
            external_tables = context->getSessionContext()->getExternalTables();
            if (!external_tables.empty())
                database_column_mut->insertDefault(); /// Empty database for external tables.
        }

        block_to_filter.insert(ColumnWithTypeAndName(std::move(database_column_mut), std::make_shared<DataTypeString>(), "database"));

        /// Filter block with `database` column.
        if (virtual_columns_filter)
            VirtualColumnUtils::filterBlockWithPredicate(virtual_columns_filter->getOutputs().at(0), block_to_filter, context);

        if (!block_to_filter.rows())
        {
            pipes.emplace_back(std::make_shared<NullSource>(std::move(header)));
            pipeline.init(Pipe::unitePipes(std::move(pipes)));
            return;
        }

        ColumnPtr & database_column = block_to_filter.getByName("database").column;

        /// Add `table` column.
        MutableColumnPtr table_column_mut = ColumnString::create();
        const auto num_databases = database_column->size();
        IColumn::Offsets offsets(num_databases);

        for (size_t i = 0; i < num_databases; ++i)
        {
            const std::string database_name = (*database_column)[i].safeGet<std::string>();
            if (database_name.empty())
            {
                for (auto & [table_name, table] : external_tables)
                {
                    storages[{"", table_name}] = table;
                    table_column_mut->insert(table_name);
                }
            }
            else
            {
                const DatabasePtr & database = catalog_databases.at(database_name);
                for (auto iterator = database->getTablesIterator(context); iterator->isValid(); iterator->next())
                {
                    if (const auto & table = iterator->table())
                    {
                        const String & table_name = iterator->name();
                        storages[{database_name, table_name}] = table;
                        table_column_mut->insert(table_name);
                    }
                }
            }
            offsets[i] = table_column_mut->size();
        }

        database_column = database_column->replicate(offsets);
        block_to_filter.insert(ColumnWithTypeAndName(std::move(table_column_mut), std::make_shared<DataTypeString>(), "table"));
    }

    /// Filter block with `database` and `table` columns.
    if (virtual_columns_filter)
        VirtualColumnUtils::filterBlockWithPredicate(virtual_columns_filter->getOutputs().at(0), block_to_filter, context);

    if (!block_to_filter.rows())
    {
        pipes.emplace_back(std::make_shared<NullSource>(std::move(header)));
        pipeline.init(Pipe::unitePipes(std::move(pipes)));
        return;
    }

    ColumnPtr filtered_database_column = block_to_filter.getByName("database").column;
    ColumnPtr filtered_table_column = block_to_filter.getByName("table").column;

    pipes.emplace_back(std::make_shared<ColumnsSource>(
            std::move(columns_mask), std::move(header), max_block_size,
            std::move(filtered_database_column), std::move(filtered_table_column),
            std::move(storages), std::move(catalog_databases), context));

    pipeline.init(Pipe::unitePipes(std::move(pipes)));
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemColumns) }
