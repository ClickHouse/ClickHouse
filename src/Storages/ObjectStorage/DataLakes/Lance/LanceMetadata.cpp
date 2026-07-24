#include "config.h"

#if USE_LANCE

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Common/FieldVisitorToString.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/PreparedSets.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceReadSource.h>
#include <Storages/StorageSnapshot.h>
#if USE_AWS_S3
#include <Storages/ObjectStorage/S3/Configuration.h>
#endif
#include <Storages/StorageInMemoryMetadata.h>

#include <fmt/ranges.h>

#include <unordered_map>

namespace DB
{

#if USE_AWS_S3
namespace S3AuthSetting
{
extern const S3AuthSettingsString access_key_id;
extern const S3AuthSettingsString secret_access_key;
extern const S3AuthSettingsString session_token;
extern const S3AuthSettingsString region;
extern const S3AuthSettingsString role_arn;
extern const S3AuthSettingsString role_session_name;
extern const S3AuthSettingsBool use_environment_credentials;
extern const S3AuthSettingsBool no_sign_request;
}
#endif

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}
}

namespace DB
{

namespace
{

class LanceDatasetObjectInfo final : public ObjectInfo
{
public:
    LanceDatasetObjectInfo(String dataset_path_, Lance::TableStateSnapshot snapshot_)
        : ObjectInfo(RelativePathWithMetadata(std::move(dataset_path_), createDatasetObjectMetadata()))
        , snapshot(std::move(snapshot_))
    {
    }

    const Lance::TableStateSnapshot snapshot;

private:
    static ObjectMetadata createDatasetObjectMetadata()
    {
        ObjectMetadata metadata;
        metadata.is_size_known = false;
        return metadata;
    }
};

class LanceDatasetIterator final : public IObjectIterator
{
public:
    LanceDatasetIterator(String dataset_path_, Lance::TableStateSnapshot snapshot_, IDataLakeMetadata::FileProgressCallback callback_)
        : dataset_path(std::move(dataset_path_))
        , snapshot(std::move(snapshot_))
        , callback(std::move(callback_))
    {
    }

    ObjectInfoPtr next(size_t) override
    {
        if (is_finished)
            return nullptr;

        is_finished = true;
        auto object_info = std::make_shared<LanceDatasetObjectInfo>(dataset_path, snapshot);
        if (callback)
            callback(FileProgress{0});
        return object_info;
    }

    size_t estimatedKeysCount() override { return is_finished ? 0 : 1; }

    std::optional<UInt64> getSnapshotVersion() const override { return snapshot.snapshot_id; }

private:
    String dataset_path;
    Lance::TableStateSnapshot snapshot;
    IDataLakeMetadata::FileProgressCallback callback;
    bool is_finished = false;
};

const Lance::TableStateSnapshot & extractSnapshot(const ObjectInfoPtr & object_info)
{
    const auto * lance_object_info = typeid_cast<const LanceDatasetObjectInfo *>(object_info.get());
    if (!lance_object_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Lance reader received an object without a Lance table state snapshot");
    return lance_object_info->snapshot;
}

String quoteLanceIdentifier(const String & name)
{
    String result;
    result.reserve(name.size() + 2);
    result.push_back('`');
    for (char c : name)
    {
        if (c == '`')
            result += "``";
        else
            result.push_back(c);
    }
    result.push_back('`');
    return result;
}

String quoteLanceStringLiteral(const String & value)
{
    String result;
    result.reserve(value.size() + 2);
    result.push_back('\'');
    for (char c : value)
    {
        if (c == '\'')
            result += "''";
        else
            result.push_back(c);
    }
    result.push_back('\'');
    return result;
}

std::optional<String> fieldToLanceLiteral(const Field & field, const DataTypePtr & data_type)
{
    if (field.isNull())
        return std::nullopt;

    const auto type = removeNullable(data_type);
    const WhichDataType which(type);

    if (which.isStringOrFixedString())
        return quoteLanceStringLiteral(field.safeGet<String>());

    if (which.isDate())
    {
        WriteBufferFromOwnString out;
        writeDateText(DayNum(static_cast<UInt16>(field.safeGet<UInt64>())), out);
        return fmt::format("DATE '{}'", out.str());
    }

    if (which.isDate32())
    {
        WriteBufferFromOwnString out;
        writeDateText(ExtendedDayNum(static_cast<Int32>(field.safeGet<Int64>())), out);
        return fmt::format("DATE '{}'", out.str());
    }

    if (which.isDateTime())
    {
        WriteBufferFromOwnString out;
        const auto & time_zone = assert_cast<const DataTypeDateTime &>(*type).getTimeZone();
        writeDateTimeText(static_cast<time_t>(field.safeGet<UInt64>()), out, time_zone);
        return fmt::format("TIMESTAMP '{}'", out.str());
    }

    if (which.isDateTime64())
    {
        WriteBufferFromOwnString out;
        const auto & date_time_type = assert_cast<const DataTypeDateTime64 &>(*type);
        writeDateTimeText(field.safeGet<DateTime64>(), date_time_type.getScale(), out, date_time_type.getTimeZone());
        return fmt::format("TIMESTAMP '{}'", out.str());
    }

    if (which.isNativeNumber())
        return applyVisitor(FieldVisitorToString(), field);

    return std::nullopt;
}

std::optional<String> constantNodeToLanceLiteral(const ActionsDAG::Node & node)
{
    if (node.type != ActionsDAG::ActionType::COLUMN || !node.column || node.column->empty())
        return std::nullopt;

    if (node.column->isNullAt(0))
        return std::nullopt;

    return fieldToLanceLiteral((*node.column)[0], node.result_type);
}

std::optional<String> inputNodeToLanceIdentifier(const ActionsDAG::Node & node)
{
    if (node.type != ActionsDAG::ActionType::INPUT)
        return std::nullopt;
    return quoteLanceIdentifier(node.result_name);
}

std::optional<String> reverseComparison(const String & function_name)
{
    if (function_name == "less")
        return ">";
    if (function_name == "lessOrEquals")
        return ">=";
    if (function_name == "greater")
        return "<";
    if (function_name == "greaterOrEquals")
        return "<=";
    if (function_name == "equals")
        return "=";
    if (function_name == "notEquals")
        return "!=";
    return std::nullopt;
}

std::optional<String> comparisonFunctionToLanceOperator(const String & function_name)
{
    if (function_name == "less")
        return "<";
    if (function_name == "lessOrEquals")
        return "<=";
    if (function_name == "greater")
        return ">";
    if (function_name == "greaterOrEquals")
        return ">=";
    if (function_name == "equals")
        return "=";
    if (function_name == "notEquals")
        return "!=";
    return std::nullopt;
}

std::optional<String> extractLancePredicate(const ActionsDAG::Node & node, const ContextPtr & context);

std::optional<String> tryBuildBooleanPredicate(const ActionsDAG::Node & node, const String & joiner, const ContextPtr & context)
{
    if (node.children.empty())
        return std::nullopt;

    std::vector<String> predicates;
    predicates.reserve(node.children.size());
    for (const auto * child : node.children)
    {
        if (auto predicate = extractLancePredicate(*child, context))
            predicates.push_back(fmt::format("({})", *predicate));
        else
            return std::nullopt;
    }
    return fmt::format("{}", fmt::join(predicates, joiner));
}

std::optional<String> tryBuildNullCheckPredicate(const ActionsDAG::Node & node, const String & function_name)
{
    if (node.children.size() != 1)
        return std::nullopt;

    auto identifier = inputNodeToLanceIdentifier(*node.children[0]);
    if (!identifier)
        return std::nullopt;

    if (function_name == "isNull")
        return fmt::format("{} IS NULL", *identifier);
    if (function_name == "isNotNull")
        return fmt::format("{} IS NOT NULL", *identifier);

    return std::nullopt;
}

std::optional<String> tryBuildComparisonPredicate(const ActionsDAG::Node & node, const String & function_name)
{
    if (node.children.size() != 2)
        return std::nullopt;

    const auto * lhs = node.children[0];
    const auto * rhs = node.children[1];
    if (auto identifier = inputNodeToLanceIdentifier(*lhs))
    {
        if (auto literal = constantNodeToLanceLiteral(*rhs))
        {
            if (auto op = comparisonFunctionToLanceOperator(function_name))
                return fmt::format("{} {} {}", *identifier, *op, *literal);
        }
    }

    if (auto identifier = inputNodeToLanceIdentifier(*rhs))
    {
        if (auto literal = constantNodeToLanceLiteral(*lhs))
        {
            if (auto op = reverseComparison(function_name))
                return fmt::format("{} {} {}", *identifier, *op, *literal);
        }
    }

    return std::nullopt;
}

std::optional<String> tryBuildInPredicate(const ActionsDAG::Node & node, const ContextPtr & context)
{
    if (!context || node.children.size() != 2)
        return std::nullopt;

    auto identifier = inputNodeToLanceIdentifier(*node.children[0]);
    if (!identifier || !node.children[1]->column)
        return std::nullopt;

    const IColumn * column = node.children[1]->column.get();
    if (const auto * column_const = typeid_cast<const ColumnConst *>(column))
        column = &column_const->getDataColumn();

    const auto * column_set = typeid_cast<const ColumnSet *>(column);
    if (!column_set)
        return std::nullopt;

    auto future_set = column_set->getData();
    if (!future_set)
        return std::nullopt;

    auto set = future_set->buildOrderedSetInplace(context);
    if (!set || !set->hasExplicitSetElements())
        return std::nullopt;

    set->checkColumnsNumber(1);
    const auto type = set->getElementsTypes()[0];
    const auto elements = set->getSetElements()[0];

    std::vector<String> literals;
    literals.reserve(elements->size());
    for (size_t i = 0; i < elements->size(); ++i)
    {
        if (elements->isNullAt(i))
            continue;

        if (auto literal = fieldToLanceLiteral((*elements)[i], type))
            literals.push_back(*literal);
        else
            return std::nullopt;
    }

    if (literals.empty())
        return std::nullopt;

    return fmt::format("{} IN ({})", *identifier, fmt::join(literals, ", "));
}

std::optional<String> extractLancePredicate(const ActionsDAG::Node & node, const ContextPtr & context)
{
    if (node.type == ActionsDAG::ActionType::ALIAS && node.children.size() == 1)
        return extractLancePredicate(*node.children.front(), context);

    if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base)
        return std::nullopt;

    const auto function_name = node.function_base->getName();
    if (function_name == "and")
        return tryBuildBooleanPredicate(node, " AND ", context);

    if (function_name == "or")
        return tryBuildBooleanPredicate(node, " OR ", context);

    if (function_name == "isNull" || function_name == "isNotNull")
        return tryBuildNullCheckPredicate(node, function_name);

    if (function_name == "in")
        return tryBuildInPredicate(node, context);

    return tryBuildComparisonPredicate(node, function_name);
}

std::optional<String> extractLancePredicate(const FormatFilterInfoPtr & format_filter_info)
{
    if (!format_filter_info || !format_filter_info->filter_actions_dag)
        return std::nullopt;

    const auto & outputs = format_filter_info->filter_actions_dag->getOutputs();
    if (outputs.size() != 1)
        return std::nullopt;

    return extractLancePredicate(*outputs.front(), format_filter_info->context.lock());
}

std::pair<Names, NamesAndTypesList> splitVirtualColumns(
    const Names & columns,
    const VirtualColumnsDescription & virtual_columns_description)
{
    Names physical_columns;
    NamesAndTypesList virtual_columns;

    for (const auto & column_name : columns)
    {
        if (auto virtual_column = virtual_columns_description.tryGet(column_name, VirtualsKind::All, VirtualsMaterializationPlace::Reader))
            virtual_columns.emplace_back(std::move(*virtual_column));
        else
            physical_columns.push_back(column_name);
    }

    return {physical_columns, virtual_columns};
}

void validateExplicitLanceSchema(const NamesAndTypesList & explicit_schema, const NamesAndTypesList & inferred_schema)
{
    std::unordered_map<String, DataTypePtr> inferred_columns;
    inferred_columns.reserve(inferred_schema.size());
    for (const auto & column : inferred_schema)
        inferred_columns.emplace(column.name, column.type);

    for (const auto & column : explicit_schema)
    {
        const auto it = inferred_columns.find(column.name);
        if (it == inferred_columns.end())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Explicit schema for Lance dataset contains column `{}` which does not exist in the dataset",
                column.name);
        }

        if (!column.type->equals(*it->second))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Explicit schema for Lance dataset has incompatible type for column `{}`: expected `{}`, got `{}`",
                column.name,
                it->second->getName(),
                column.type->getName());
        }
    }
}

}

LanceMetadata::LanceMetadata(StorageObjectStorageConfigurationWeakPtr configuration_)
    : configuration(std::move(configuration_))
{
}

DataLakeMetadataPtr LanceMetadata::create(
    const ObjectStoragePtr &,
    const StorageObjectStorageConfigurationWeakPtr & configuration,
    const ContextPtr &)
{
    return std::make_unique<LanceMetadata>(configuration);
}

void LanceMetadata::createInitial(
    const ObjectStoragePtr &,
    const StorageObjectStorageConfigurationWeakPtr & configuration,
    const ContextPtr & local_context,
    const std::optional<ColumnsDescription> & columns,
    ASTPtr,
    ASTPtr,
    bool,
    std::shared_ptr<DataLake::ICatalog>,
    const StorageID &)
{
    if (!columns.has_value())
        return;

    LanceMetadata metadata(configuration);
    validateExplicitLanceSchema(columns->getAllPhysical(), metadata.getTableSchema(local_context));
}

bool LanceMetadata::operator==(const IDataLakeMetadata & other) const
{
    return typeid(other) == typeid(LanceMetadata);
}

NamesAndTypesList LanceMetadata::getTableSchema(ContextPtr local_context) const
{
    auto dataset = Lance::Dataset::open(getDatasetOptions());
    const auto snapshot = dataset.currentSnapshot();
    return dataset.tableSchema(Lance::TableStateSnapshot{snapshot.snapshot_id, snapshot.schema_id}, local_context);
}

std::optional<DataLakeTableStateSnapshot> LanceMetadata::getTableStateSnapshot(ContextPtr) const
{
    const auto snapshot = Lance::Dataset::open(getDatasetOptions()).currentSnapshot();
    return DataLakeTableStateSnapshot{Lance::TableStateSnapshot{snapshot.snapshot_id, snapshot.schema_id}};
}

std::unique_ptr<StorageInMemoryMetadata> LanceMetadata::buildStorageMetadataFromState(
    const DataLakeTableStateSnapshot & state, ContextPtr local_context) const
{
    chassert(std::holds_alternative<Lance::TableStateSnapshot>(state));
    auto result = std::make_unique<StorageInMemoryMetadata>();
    const auto & lance_state = std::get<Lance::TableStateSnapshot>(state);
    result->setColumns(ColumnsDescription{Lance::Dataset::open(getDatasetOptions()).tableSchema(lance_state, local_context)});
    result->setDataLakeTableState(state);
    return result;
}

ReadFromFormatInfo LanceMetadata::prepareReadingFromFormat(
    const Strings & requested_columns,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr &,
    bool,
    bool)
{
    ReadFromFormatInfo info;
    Names physical_columns;
    std::tie(physical_columns, info.requested_virtual_columns)
        = splitVirtualColumns(requested_columns, storage_snapshot->metadata->virtuals);

    /// `Lance::ReadSource` reads the dataset itself and emits only physical columns.
    /// `StorageObjectStorageSource` appends requested file-like virtual columns after
    /// the custom source has produced a chunk, so the source header must describe the
    /// final physical-plus-virtual chunk while `requested_columns` stays physical-only.
    info.source_header = storage_snapshot->getSampleBlockForColumns(physical_columns);
    info.format_header = info.source_header;
    for (const auto & column : info.requested_virtual_columns)
        info.source_header.insert({column.type->createColumn(), column.type, column.name});

    info.requested_columns = storage_snapshot->getColumnsByNames(
        GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(),
        physical_columns);
    info.columns_description = ColumnsDescription{info.requested_columns};

    return info;
}

ObjectIterator LanceMetadata::iterate(
    const ActionsDAG *,
    FileProgressCallback callback,
    size_t,
    StorageMetadataPtr storage_metadata,
    ContextPtr) const
{
    if (!storage_metadata || !storage_metadata->datalake_table_state.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No table state snapshot found while iterating Lance dataset");

    const auto & state = storage_metadata->datalake_table_state.value();
    if (!std::holds_alternative<Lance::TableStateSnapshot>(state))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected table state snapshot type while iterating Lance dataset");

    const auto snapshot = std::get<Lance::TableStateSnapshot>(state);
    return std::make_shared<LanceDatasetIterator>(getDatasetOptions().uri, snapshot, std::move(callback));
}

std::optional<Pipe> LanceMetadata::read(
    ObjectInfoPtr object_info,
    const ReadFromFormatInfo & read_from_format_info,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr local_context,
    size_t max_block_size,
    FormatParserSharedResourcesPtr,
    FormatFilterInfoPtr format_filter_info,
    bool need_only_count) const
{
    const auto snapshot = extractSnapshot(object_info);
    Lance::ScanDescription scan
    {
        .snapshot = snapshot,
        .projection = read_from_format_info.requested_columns.getNames(),
        .predicate = extractLancePredicate(format_filter_info),
        .max_block_size = max_block_size,
        .need_only_count = need_only_count,
    };

    ColumnsWithTypeAndName physical_columns;
    physical_columns.reserve(read_from_format_info.requested_columns.size());
    for (const auto & column : read_from_format_info.requested_columns)
        physical_columns.emplace_back(column.type->createColumn(), column.type, column.name);

    /// `read_from_format_info.source_header` is the final header expected from
    /// `StorageObjectStorageSource` after virtual columns are appended. `Lance::ReadSource`
    /// itself must emit only physical Lance columns; otherwise virtual-only reads such as
    /// `_data_lake_snapshot_version` make the custom source produce an empty chunk for a
    /// non-empty header.
    auto source = std::make_shared<Lance::ReadSource>(
        Block(std::move(physical_columns)),
        std::move(object_info),
        getDatasetOptions(),
        std::move(scan),
        format_settings ? *format_settings : getFormatSettings(local_context));
    return Pipe(source);
}

std::optional<size_t> LanceMetadata::totalRows(ContextPtr) const
{
    auto dataset = Lance::Dataset::open(getDatasetOptions());
    const auto snapshot = dataset.currentSnapshot();
    return dataset.totalRows(Lance::TableStateSnapshot{snapshot.snapshot_id, snapshot.schema_id});
}

std::optional<size_t> LanceMetadata::totalBytes(ContextPtr) const
{
    return Lance::Dataset::open(getDatasetOptions()).totalBytes();
}

Lance::DatasetOptions LanceMetadata::getDatasetOptions() const
{
    const auto configuration_ptr = configuration.lock();
    if (!configuration_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Storage configuration for Lance metadata is expired");

    Lance::DatasetOptions options;

#if USE_AWS_S3
    if (const auto * s3_configuration = dynamic_cast<const StorageS3Configuration *>(configuration_ptr.get()))
    {
        const auto & auth_settings = s3_configuration->getAuthSettings();
        options.uri = fmt::format("s3://{}/{}", s3_configuration->url.bucket, s3_configuration->url.key);
        options.use_s3 = true;
        options.s3_endpoint = s3_configuration->url.endpoint;
        options.s3_region = auth_settings[S3AuthSetting::region].value;
        options.s3_access_key_id = auth_settings[S3AuthSetting::access_key_id].value;
        options.s3_secret_access_key = auth_settings[S3AuthSetting::secret_access_key].value;
        options.s3_session_token = auth_settings[S3AuthSetting::session_token].value;
        options.s3_role_arn = auth_settings[S3AuthSetting::role_arn].value;
        options.s3_role_session_name = auth_settings[S3AuthSetting::role_session_name].value;
        options.s3_use_environment_credentials = auth_settings[S3AuthSetting::use_environment_credentials].value;
        options.s3_no_sign_request = auth_settings[S3AuthSetting::no_sign_request].value;
        options.s3_allow_http = s3_configuration->url.uri.getScheme() == "http";
        options.s3_virtual_hosted_style_request = s3_configuration->url.is_virtual_hosted_style;
        return options;
    }
#endif

    options.uri = configuration_ptr->getRawPath().path;
    return options;
}

}

#endif
