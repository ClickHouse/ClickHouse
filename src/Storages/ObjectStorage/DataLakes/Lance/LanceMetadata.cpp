#include "config.h"

#if USE_LANCE

#include <Columns/ColumnConst.h>
#include <Common/FieldVisitorToString.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceReadSource.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <fmt/ranges.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
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
        : ObjectInfo(RelativePathWithMetadata(std::move(dataset_path_), ObjectMetadata{}))
        , snapshot(std::move(snapshot_))
    {
    }

    const Lance::TableStateSnapshot snapshot;
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

std::optional<String> constantNodeToLanceLiteral(const ActionsDAG::Node & node)
{
    if (node.type != ActionsDAG::ActionType::COLUMN || !node.column || node.column->empty())
        return std::nullopt;

    if (node.column->isNullAt(0))
        return std::nullopt;

    const auto type = removeNullable(node.result_type);
    const WhichDataType which(type);
    const auto field = (*node.column)[0];

    if (which.isStringOrFixedString())
    {
        WriteBufferFromOwnString out;
        writeQuotedString(field.safeGet<String>(), out);
        return out.str();
    }

    if (which.isNativeNumber() || which.isDateOrDate32OrDateTimeOrDateTime64())
        return applyVisitor(FieldVisitorToString(), field);

    return std::nullopt;
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

std::optional<String> extractLancePredicate(const ActionsDAG::Node & node)
{
    if (node.type == ActionsDAG::ActionType::ALIAS && node.children.size() == 1)
        return extractLancePredicate(*node.children.front());

    if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base)
        return std::nullopt;

    const auto function_name = node.function_base->getName();
    if (function_name == "and")
    {
        std::vector<String> predicates;
        predicates.reserve(node.children.size());
        for (const auto * child : node.children)
        {
            if (auto predicate = extractLancePredicate(*child))
                predicates.push_back(fmt::format("({})", *predicate));
            else
                return std::nullopt;
        }
        return fmt::format("{}", fmt::join(predicates, " AND "));
    }

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

std::optional<String> extractLancePredicate(const FormatFilterInfoPtr & format_filter_info)
{
    if (!format_filter_info || !format_filter_info->filter_actions_dag)
        return std::nullopt;

    const auto & outputs = format_filter_info->filter_actions_dag->getOutputs();
    if (outputs.size() != 1)
        return std::nullopt;

    return extractLancePredicate(*outputs.front());
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
    const StorageObjectStorageConfigurationWeakPtr &,
    const ContextPtr &,
    const std::optional<ColumnsDescription> &,
    ASTPtr,
    ASTPtr,
    bool,
    std::shared_ptr<DataLake::ICatalog>,
    const StorageID &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Creating Lance datasets from ClickHouse is not implemented yet");
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
    const std::optional<FormatSettings> &,
    ContextPtr,
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
    auto source = std::make_shared<Lance::ReadSource>(
        read_from_format_info.source_header,
        std::move(object_info),
        getDatasetOptions(),
        std::move(scan));
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
    options.uri = configuration_ptr->getRawPath().path;
    return options;
}

}

#endif
