#include "config.h"

#if USE_LANCE

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/PreparedSets.h>
#include <Storages/ColumnsDescription.h>
#include <algorithm>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceDataObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceQuerySession.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceReadSource.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/StorageSnapshot.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/FieldVisitorToString.h>
#include <Common/ProfileEvents.h>
#if USE_AWS_S3
#include <Storages/ObjectStorage/S3/Configuration.h>
#endif
#include <Storages/StorageInMemoryMetadata.h>

#include <fmt/ranges.h>

#include <limits>
#include <mutex>
#include <numeric>
#include <unordered_map>
#include <unordered_set>

namespace ProfileEvents
{
extern const Event LancePredicatePushdownComplete;
extern const Event LancePredicatePushdownPartial;
extern const Event LancePredicatePushdownDisabled;
extern const Event LanceLimitPushdown;
extern const Event LanceProjectedColumns;
extern const Event LanceScanUnordered;
extern const Event LanceFragmentsListed;
extern const Event LanceFragmentPacks;
extern const Event LanceFragmentParallelismDisabled;
}

namespace DB
{
namespace Setting
{
extern const SettingsSeconds http_connection_timeout;
extern const SettingsSeconds http_send_timeout;
extern const SettingsSeconds http_receive_timeout;
extern const SettingsBool lance_scan_in_order;
extern const SettingsUInt64 lance_fragment_readahead;
extern const SettingsUInt64 lance_batch_readahead;
extern const SettingsUInt64 lance_io_buffer_size;
extern const SettingsBool lance_enable_fragment_parallelism;
extern const SettingsBool lance_enable_predicate_pushdown;
extern const SettingsString lance_fragment_pack_mode;
extern const SettingsUInt64 lance_max_fragment_packs;
extern const SettingsUInt64 lance_min_rows_per_pack;
extern const SettingsUInt64 lance_min_bytes_per_pack;
extern const SettingsMaxThreads max_threads;
}

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

namespace FailPoints
{
extern const char lance_metadata_iterate_pause[];
}
}

namespace DB
{

namespace
{

struct FragmentPack
{
    std::vector<UInt64> fragment_ids;
    UInt64 weight_rows = 0;
    UInt64 weight_bytes = 0;
};

UInt64 fragmentPackWeight(const Lance::FragmentInfo & fragment)
{
    if (fragment.num_rows.has_value())
        return *fragment.num_rows;
    if (fragment.size_bytes > 0)
        return fragment.size_bytes;
    return 1;
}

enum class FragmentPackMode
{
    One,
    Pack,
    Auto,
};

FragmentPackMode parseFragmentPackMode(const String & mode)
{
    if (mode == "one")
        return FragmentPackMode::One;
    if (mode == "pack")
        return FragmentPackMode::Pack;
    if (mode == "auto")
        return FragmentPackMode::Auto;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid lance_fragment_pack_mode '{}'; expected one, pack, or auto", mode);
}

/// LPT (longest-processing-time): sort by weight descending, place each fragment into the lightest pack.
std::vector<FragmentPack> partitionFragmentsIntoPacks(
    const std::vector<Lance::FragmentInfo> & fragments,
    size_t target_packs,
    FragmentPackMode mode,
    UInt64 min_rows_per_pack,
    UInt64 min_bytes_per_pack)
{
    if (fragments.empty())
        return {};

    target_packs = std::max<size_t>(1, target_packs);

    FragmentPackMode effective_mode = mode;
    if (mode == FragmentPackMode::Auto)
        effective_mode = fragments.size() <= target_packs ? FragmentPackMode::One : FragmentPackMode::Pack;

    if (effective_mode == FragmentPackMode::One)
        target_packs = std::min(target_packs, fragments.size());
    else
        target_packs = std::min(target_packs, fragments.size());

    std::vector<size_t> order(fragments.size());
    std::iota(order.begin(), order.end(), 0);
    std::stable_sort(order.begin(), order.end(), [&](size_t lhs, size_t rhs)
    {
        return fragmentPackWeight(fragments[lhs]) > fragmentPackWeight(fragments[rhs]);
    });

    std::vector<FragmentPack> packs(target_packs);
    std::vector<UInt64> pack_weights(target_packs, 0);

    for (size_t index : order)
    {
        const auto & fragment = fragments[index];
        const size_t pack_index = static_cast<size_t>(
            std::min_element(pack_weights.begin(), pack_weights.end()) - pack_weights.begin());
        packs[pack_index].fragment_ids.push_back(fragment.id);
        if (fragment.num_rows)
            packs[pack_index].weight_rows += *fragment.num_rows;
        packs[pack_index].weight_bytes += fragment.size_bytes;
        pack_weights[pack_index] += fragmentPackWeight(fragment);
    }

    /// Soft min-size merge: repeatedly merge the lightest non-empty pack into the next lightest
    /// while under thresholds. Keeps at least one pack.
    auto pack_too_small = [&](const FragmentPack & pack)
    {
        if (pack.fragment_ids.empty())
            return true;
        const bool rows_ok = min_rows_per_pack == 0 || pack.weight_rows >= min_rows_per_pack;
        const bool bytes_ok = min_bytes_per_pack == 0 || pack.weight_bytes >= min_bytes_per_pack;
        /// Only apply thresholds when the corresponding weight is known/positive for the pack.
        if (min_rows_per_pack > 0 && pack.weight_rows > 0 && !rows_ok)
            return true;
        if (min_bytes_per_pack > 0 && pack.weight_bytes > 0 && !bytes_ok)
            return true;
        return false;
    };

    if (min_rows_per_pack > 0 || min_bytes_per_pack > 0)
    {
        while (packs.size() > 1)
        {
            size_t small_index = packs.size();
            for (size_t i = 0; i < packs.size(); ++i)
            {
                if (pack_too_small(packs[i]))
                {
                    small_index = i;
                    break;
                }
            }
            if (small_index == packs.size())
                break;

            size_t merge_into = small_index == 0 ? 1 : small_index - 1;
            for (size_t i = 0; i < packs.size(); ++i)
            {
                if (i == small_index || packs[i].fragment_ids.empty())
                    continue;
                if (pack_weights[i] < pack_weights[merge_into] || merge_into == small_index)
                    merge_into = i;
            }

            packs[merge_into].fragment_ids.insert(
                packs[merge_into].fragment_ids.end(),
                packs[small_index].fragment_ids.begin(),
                packs[small_index].fragment_ids.end());
            packs[merge_into].weight_rows += packs[small_index].weight_rows;
            packs[merge_into].weight_bytes += packs[small_index].weight_bytes;
            pack_weights[merge_into] += pack_weights[small_index];
            packs.erase(packs.begin() + static_cast<std::ptrdiff_t>(small_index));
            pack_weights.erase(pack_weights.begin() + static_cast<std::ptrdiff_t>(small_index));
        }
    }

    std::vector<FragmentPack> non_empty;
    non_empty.reserve(packs.size());
    for (auto & pack : packs)
    {
        if (!pack.fragment_ids.empty())
            non_empty.push_back(std::move(pack));
    }
    return non_empty;
}

String makeLancePackSyntheticPath(
    const String & dataset_path, UInt64 version, size_t pack_index, const std::vector<UInt64> & fragment_ids)
{
    const UInt64 first_id = fragment_ids.empty() ? 0 : fragment_ids.front();
    return fmt::format("{}#v{}/pack{}_f{}", dataset_path, version, pack_index, first_id);
}

class LanceDatasetIterator final : public IObjectIterator
{
public:
    LanceDatasetIterator(
        String dataset_path_,
        Lance::TableStateSnapshot snapshot_,
        Lance::DatasetHandle dataset_,
        std::vector<FragmentPack> packs_,
        IDataLakeMetadata::FileProgressCallback callback_)
        : dataset_path(std::move(dataset_path_))
        , snapshot(std::move(snapshot_))
        , dataset(std::move(dataset_))
        , packs(std::move(packs_))
        , total_packs(packs.size())
        , callback(std::move(callback_))
    {
    }

    ObjectInfoPtr next(size_t) override
    {
        std::lock_guard lock(mutex);
        if (next_index >= packs.size())
            return nullptr;

        const size_t pack_index = next_index++;
        auto & pack = packs[pack_index];
        auto object_info = std::make_shared<LanceDatasetObjectInfo>(
            makeLancePackSyntheticPath(dataset_path, snapshot.version, pack_index, pack.fragment_ids),
            snapshot,
            dataset,
            std::move(pack.fragment_ids),
            pack_index,
            total_packs);
        /// Dataset-level byte totals are often unavailable; per-batch progress is reported from
        /// `Lance::ReadSource` via `ISource::progress` (auto_progress).
        if (callback)
            callback(FileProgress{/*read_bytes=*/0, /*total_bytes_to_read=*/0});
        return object_info;
    }

    /// Fixed for the iterator lifetime so initializePipeline can open multi-stream correctly.
    size_t estimatedKeysCount() override { return total_packs; }

    std::optional<UInt64> getSnapshotVersion() const override { return snapshot.version; }

private:
    String dataset_path;
    Lance::TableStateSnapshot snapshot;
    Lance::DatasetHandle dataset;
    std::vector<FragmentPack> packs;
    const size_t total_packs;
    IDataLakeMetadata::FileProgressCallback callback;
    std::mutex mutex;
    size_t next_index = 0;
};

const LanceDatasetObjectInfo & extractLanceObjectInfo(const ObjectInfoPtr & object_info)
{
    const auto * lance_object_info = typeid_cast<const LanceDatasetObjectInfo *>(object_info.get());
    if (!lance_object_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Lance reader received an object without a Lance table state snapshot");
    return *lance_object_info;
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

struct LancePhysicalColumn
{
    DataTypePtr type;
    bool is_arrow_utf8 = false;
};

using LancePhysicalSchema = std::unordered_map<String, LancePhysicalColumn>;

LancePhysicalSchema makeLancePhysicalSchema(
    const NamesAndTypesList & schema,
    const std::unordered_set<String> & utf8_columns)
{
    LancePhysicalSchema result;
    result.reserve(schema.size());
    for (const auto & column : schema)
        result.emplace(column.name, LancePhysicalColumn{column.type, utf8_columns.contains(column.name)});
    return result;
}

const ActionsDAG::Node * unwrapTransparentAlias(const ActionsDAG::Node & node)
{
    const ActionsDAG::Node * current = &node;
    while (current->type == ActionsDAG::ActionType::ALIAS && current->children.size() == 1)
        current = current->children.front();
    return current;
}

struct LanceIdentifier
{
    String sql;
    const LancePhysicalColumn * column = nullptr;
};

std::optional<LanceIdentifier> inputNodeToLanceIdentifier(
    const ActionsDAG::Node & node,
    const LancePhysicalSchema & physical_schema)
{
    const auto * input = unwrapTransparentAlias(node);
    if (input->type != ActionsDAG::ActionType::INPUT)
        return std::nullopt;

    const auto it = physical_schema.find(input->result_name);
    if (it == physical_schema.end() || !input->result_type->equals(*it->second.type))
        return std::nullopt;

    return LanceIdentifier{
        .sql = quoteLanceIdentifier(input->result_name),
        .column = &it->second,
    };
}

bool isSupportedComparisonType(const LancePhysicalColumn & column, const String & function_name)
{
    if (column.type->isNullable())
        return false;

    const WhichDataType which(column.type);
    if (which.isNativeInt())
        return true;
    if (which.isString() && column.is_arrow_utf8)
        return function_name == "equals" || function_name == "notEquals";
    return false;
}

std::optional<String> fieldToLanceLiteral(
    const Field & field,
    const DataTypePtr & constant_type,
    const LancePhysicalColumn & column)
{
    if (field.isNull() || !constant_type->equals(*column.type))
        return std::nullopt;

    const WhichDataType which(column.type);
    if (which.isString() && column.is_arrow_utf8)
        return quoteLanceStringLiteral(field.safeGet<String>());
    if (which.isNativeInt())
        return applyVisitor(FieldVisitorToString(), field);
    return std::nullopt;
}

std::optional<String> constantNodeToLanceLiteral(
    const ActionsDAG::Node & node,
    const LancePhysicalColumn & column)
{
    if (node.type != ActionsDAG::ActionType::COLUMN || !node.column || node.column->empty())
        return std::nullopt;

    if (node.column->isNullAt(0))
        return std::nullopt;

    return fieldToLanceLiteral((*node.column)[0], node.result_type, column);
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

/// Result of translating a ClickHouse filter DAG into a Lance SQL-like predicate.
/// Partial AND pushdown may set `predicate` while `is_complete` is false; residual FilterStep
/// always re-evaluates the full filter. LIMIT and countRows require `is_complete`.
struct LancePredicatePushdown
{
    std::optional<String> predicate;
    bool is_complete = true;
};

std::optional<String> translateLancePredicateStrict(
    const ActionsDAG::Node & node,
    const ContextPtr & context,
    const LancePhysicalSchema & physical_schema);

/// All-or-nothing boolean tree (used for OR and for strict translation of nested AND).
std::optional<String> tryBuildBooleanPredicateStrict(
    const ActionsDAG::Node & node,
    const String & joiner,
    const ContextPtr & context,
    const LancePhysicalSchema & physical_schema)
{
    if (node.children.empty())
        return std::nullopt;

    std::vector<String> predicates;
    predicates.reserve(node.children.size());
    for (const auto * child : node.children)
    {
        if (auto predicate = translateLancePredicateStrict(*child, context, physical_schema))
            predicates.push_back(fmt::format("({})", *predicate));
        else
            return std::nullopt;
    }
    return fmt::format("{}", fmt::join(predicates, joiner));
}

std::optional<String> tryBuildNullCheckPredicate(
    const ActionsDAG::Node & node,
    const String & function_name,
    const LancePhysicalSchema & physical_schema)
{
    if (node.children.size() != 1)
        return std::nullopt;

    auto identifier = inputNodeToLanceIdentifier(*node.children[0], physical_schema);
    if (!identifier || !identifier->column->type->isNullable())
        return std::nullopt;

    if (function_name == "isNull")
        return fmt::format("{} IS NULL", identifier->sql);
    if (function_name == "isNotNull")
        return fmt::format("{} IS NOT NULL", identifier->sql);

    return std::nullopt;
}

std::optional<String> tryBuildComparisonPredicate(
    const ActionsDAG::Node & node,
    const String & function_name,
    const LancePhysicalSchema & physical_schema)
{
    if (node.children.size() != 2)
        return std::nullopt;

    const auto op = comparisonFunctionToLanceOperator(function_name);
    if (!op)
        return std::nullopt;

    const auto * lhs = node.children[0];
    const auto * rhs = node.children[1];
    if (auto identifier = inputNodeToLanceIdentifier(*lhs, physical_schema))
    {
        if (isSupportedComparisonType(*identifier->column, function_name))
        {
            if (auto literal = constantNodeToLanceLiteral(*rhs, *identifier->column))
                return fmt::format("{} {} {}", identifier->sql, *op, *literal);
        }
    }

    if (auto identifier = inputNodeToLanceIdentifier(*rhs, physical_schema))
    {
        if (isSupportedComparisonType(*identifier->column, function_name))
        {
            if (auto literal = constantNodeToLanceLiteral(*lhs, *identifier->column))
            {
                if (auto reverse_op = reverseComparison(function_name))
                    return fmt::format("{} {} {}", identifier->sql, *reverse_op, *literal);
            }
        }
    }

    return std::nullopt;
}

std::optional<String> tryBuildInPredicate(
    const ActionsDAG::Node & node,
    const ContextPtr & context,
    const LancePhysicalSchema & physical_schema)
{
    if (!context || node.children.size() != 2)
        return std::nullopt;

    auto identifier = inputNodeToLanceIdentifier(*node.children[0], physical_schema);
    if (!identifier || !node.children[1]->column)
        return std::nullopt;
    if (!isSupportedComparisonType(*identifier->column, "equals"))
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
    if (!type->equals(*identifier->column->type) || elements->empty())
        return std::nullopt;

    std::vector<String> literals;
    literals.reserve(elements->size());
    for (size_t i = 0; i < elements->size(); ++i)
    {
        if (elements->isNullAt(i))
            return std::nullopt;

        if (auto literal = fieldToLanceLiteral((*elements)[i], type, *identifier->column))
            literals.push_back(*literal);
        else
            return std::nullopt;
    }

    if (literals.empty())
        return std::nullopt;

    return fmt::format("{} IN ({})", identifier->sql, fmt::join(literals, ", "));
}

/// Strict (all-or-nothing) translation of a filter subtree, including nested AND/OR.
std::optional<String> translateLancePredicateStrict(
    const ActionsDAG::Node & node,
    const ContextPtr & context,
    const LancePhysicalSchema & physical_schema)
{
    if (node.type == ActionsDAG::ActionType::ALIAS && node.children.size() == 1)
        return translateLancePredicateStrict(*node.children.front(), context, physical_schema);

    if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base)
        return std::nullopt;

    const auto function_name = node.function_base->getName();
    if (function_name == "and")
        return tryBuildBooleanPredicateStrict(node, " AND ", context, physical_schema);

    if (function_name == "or")
        return tryBuildBooleanPredicateStrict(node, " OR ", context, physical_schema);

    if (function_name == "isNull" || function_name == "isNotNull")
        return tryBuildNullCheckPredicate(node, function_name, physical_schema);

    if (function_name == "in")
        return tryBuildInPredicate(node, context, physical_schema);

    return tryBuildComparisonPredicate(node, function_name, physical_schema);
}

/// Partial AND: flatten conjuncts with extractConjunctionAtoms, push every atom that
/// translates strictly (OR atoms remain all-or-nothing). Residual FilterStep stays in plan.
LancePredicatePushdown extractLancePredicatePushdown(
    const ActionsDAG::Node & node,
    const ContextPtr & context,
    const LancePhysicalSchema & physical_schema)
{
    const ActionsDAG::Node * root = &node;
    while (root->type == ActionsDAG::ActionType::ALIAS && root->children.size() == 1)
        root = root->children.front();

    const auto atoms = ActionsDAG::extractConjunctionAtoms(root);
    if (atoms.empty())
        return {.predicate = std::nullopt, .is_complete = false};

    std::vector<String> pushed;
    pushed.reserve(atoms.size());
    size_t failed = 0;
    for (const auto * atom : atoms)
    {
        if (auto predicate = translateLancePredicateStrict(*atom, context, physical_schema))
            pushed.push_back(fmt::format("({})", *predicate));
        else
            ++failed;
    }

    if (pushed.empty())
        return {.predicate = std::nullopt, .is_complete = false};

    return {
        .predicate = fmt::format("{}", fmt::join(pushed, " AND ")),
        .is_complete = failed == 0,
    };
}

LancePredicatePushdown extractLancePredicatePushdown(
    const FormatFilterInfoPtr & format_filter_info,
    const LancePhysicalSchema & physical_schema)
{
    if (!format_filter_info)
        return {.predicate = std::nullopt, .is_complete = true};

    if (format_filter_info->prewhere_info || format_filter_info->row_level_filter)
        return {.predicate = std::nullopt, .is_complete = false};

    if (!format_filter_info->filter_actions_dag)
        return {.predicate = std::nullopt, .is_complete = true};

    const auto & outputs = format_filter_info->filter_actions_dag->getOutputs();
    if (outputs.size() != 1)
        return {.predicate = std::nullopt, .is_complete = false};

    return extractLancePredicatePushdown(*outputs.front(), format_filter_info->context.lock(), physical_schema);
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
    const auto options = getDatasetOptions(local_context);
    if (local_context && local_context->hasQueryContext())
    {
        auto session = Lance::QuerySession::get(local_context);
        auto dataset = session->getOrOpen(options);
        const auto snapshot = dataset.currentSnapshot();
        session->pinSnapshot(dataset.identityKey(), snapshot);
        return dataset.tableSchema(snapshot, local_context, session->getCancelHandle());
    }

    auto dataset = Lance::DatasetHandle::openEphemeral(options);
    const auto snapshot = dataset.currentSnapshot();
    return dataset.tableSchema(snapshot, local_context);
}

std::optional<DataLakeTableStateSnapshot> LanceMetadata::getTableStateSnapshot(ContextPtr local_context) const
{
    const auto options = getDatasetOptions(local_context);
    Lance::DatasetHandle dataset;
    if (local_context && local_context->hasQueryContext())
    {
        auto session = Lance::QuerySession::get(local_context);
        dataset = session->getOrOpen(options);
        const auto snapshot = dataset.currentSnapshot();
        if (snapshot.version == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "`Lance` returned zero as the current dataset version");
        session->pinSnapshot(dataset.identityKey(), snapshot);
        return DataLakeTableStateSnapshot{snapshot};
    }

    dataset = Lance::DatasetHandle::openEphemeral(options);
    const auto snapshot = dataset.currentSnapshot();
    if (snapshot.version == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`Lance` returned zero as the current dataset version");
    return DataLakeTableStateSnapshot{snapshot};
}

std::unique_ptr<StorageInMemoryMetadata> LanceMetadata::buildStorageMetadataFromState(
    const DataLakeTableStateSnapshot & state, ContextPtr local_context) const
{
    const auto * lance_state = std::get_if<Lance::TableStateSnapshot>(&state);
    if (!lance_state)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected table state snapshot type while building `Lance` metadata");
    if (lance_state->version == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot build `Lance` metadata from zero dataset version");

    const auto options = getDatasetOptions(local_context);
    Lance::DatasetHandle dataset;
    if (local_context && local_context->hasQueryContext())
    {
        auto session = Lance::QuerySession::get(local_context);
        dataset = session->getPinned(options, *lance_state);
        auto result = std::make_unique<StorageInMemoryMetadata>();
        result->setColumns(ColumnsDescription{
            dataset.tableSchema(*lance_state, local_context, session->getCancelHandle())});
        result->setDataLakeTableState(state);
        return result;
    }
    else
    {
        dataset = Lance::DatasetHandle::openEphemeral(options);
    }

    auto result = std::make_unique<StorageInMemoryMetadata>();
    result->setColumns(ColumnsDescription{dataset.tableSchema(*lance_state, local_context)});
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
    ContextPtr local_context) const
{
    FailPointInjection::pauseFailPoint(FailPoints::lance_metadata_iterate_pause);

    if (!storage_metadata || !storage_metadata->datalake_table_state.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No table state snapshot found while iterating Lance dataset");

    const auto & state = storage_metadata->datalake_table_state.value();
    if (!std::holds_alternative<Lance::TableStateSnapshot>(state))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected table state snapshot type while iterating Lance dataset");

    const auto snapshot = std::get<Lance::TableStateSnapshot>(state);
    if (snapshot.version == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot iterate `Lance` dataset at zero dataset version");

    const auto options = getDatasetOptions(local_context);
    Lance::DatasetHandle dataset;
    std::shared_ptr<Lance::QuerySession> session;
    if (local_context && local_context->hasQueryContext())
    {
        session = Lance::QuerySession::get(local_context);
        dataset = session->getPinned(options, snapshot);
    }
    else
        dataset = Lance::DatasetHandle::openEphemeral(options);

    const auto fragments = dataset.listFragments(
        snapshot, session ? session->getCancelHandle() : Lance::CancelHandlePtr{});
    ProfileEvents::increment(ProfileEvents::LanceFragmentsListed, fragments.size());

    const auto & settings = local_context->getSettingsRef();
    const bool enable_parallelism = settings[Setting::lance_enable_fragment_parallelism];
    const bool session_force_single = session && session->getForceSingleFragmentPack();
    bool force_single_pack = !enable_parallelism || session_force_single || fragments.size() <= 1;

    std::vector<FragmentPack> packs;
    if (fragments.empty())
    {
        packs = {};
    }
    else if (force_single_pack)
    {
        /// Empty fragment_ids means full-table scan (T2 semantics). Prefer that over listing all ids.
        packs.push_back(FragmentPack{});
        ProfileEvents::increment(ProfileEvents::LanceFragmentParallelismDisabled);
    }
    else
    {
        size_t target_packs = settings[Setting::lance_max_fragment_packs] == 0
            ? static_cast<size_t>(std::max<UInt64>(1, static_cast<UInt64>(settings[Setting::max_threads])))
            : static_cast<size_t>(std::max<UInt64>(1, settings[Setting::lance_max_fragment_packs]));
        const auto mode = parseFragmentPackMode(settings[Setting::lance_fragment_pack_mode]);
        packs = partitionFragmentsIntoPacks(
            fragments,
            target_packs,
            mode,
            settings[Setting::lance_min_rows_per_pack],
            settings[Setting::lance_min_bytes_per_pack]);
        if (packs.size() <= 1)
            ProfileEvents::increment(ProfileEvents::LanceFragmentParallelismDisabled);
    }

    ProfileEvents::increment(ProfileEvents::LanceFragmentPacks, packs.size());

    return std::make_shared<LanceDatasetIterator>(
        options.uri, snapshot, std::move(dataset), std::move(packs), std::move(callback));
}

std::optional<Pipe> LanceMetadata::read(
    ObjectInfoPtr object_info,
    const ReadFromFormatInfo & read_from_format_info,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr local_context,
    size_t max_block_size,
    FormatParserSharedResourcesPtr,
    FormatFilterInfoPtr format_filter_info,
    bool need_only_count,
    std::optional<size_t> limit) const
{
    const auto & lance_object = extractLanceObjectInfo(object_info);
    const auto & snapshot = lance_object.snapshot;

    Lance::CancelHandlePtr cancel_handle = std::make_shared<Lance::CancelHandle>();
    Lance::DatasetHandle dataset = lance_object.dataset;
    if (!dataset)
    {
        const auto options = getDatasetOptions(local_context);
        if (local_context && local_context->hasQueryContext())
        {
            auto session = Lance::QuerySession::get(local_context);
            dataset = session->getPinned(options, snapshot);
            cancel_handle = session->getCancelHandle();
        }
        else
            dataset = Lance::DatasetHandle::openEphemeral(options);
    }
    else if (local_context && local_context->hasQueryContext())
    {
        /// Source of truth is the query session; ObjectInfo handle is a fast path that must match.
        auto session = Lance::QuerySession::get(local_context);
        auto session_handle = session->getPinned(getDatasetOptions(local_context), snapshot);
        if (session_handle.identityKey() != dataset.identityKey())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Lance ObjectInfo dataset identity does not match the query session handle");
        }
        dataset = std::move(session_handle);
        cancel_handle = session->getCancelHandle();
    }

    const auto & settings = local_context->getSettingsRef();
    const bool has_filter = format_filter_info
        && (format_filter_info->filter_actions_dag
            || format_filter_info->prewhere_info
            || format_filter_info->row_level_filter);

    LancePredicatePushdown predicate_pushdown;
    std::optional<NamesAndTypesList> pinned_physical_columns;
    if (!settings[Setting::lance_enable_predicate_pushdown])
    {
        predicate_pushdown = {
            .predicate = std::nullopt,
            .is_complete = !has_filter,
        };
        if (has_filter)
            ProfileEvents::increment(ProfileEvents::LancePredicatePushdownDisabled);
    }
    else if (has_filter)
    {
        std::unordered_set<String> utf8_columns;
        pinned_physical_columns = dataset.tableSchema(snapshot, local_context, cancel_handle, &utf8_columns);
        const auto physical_schema = makeLancePhysicalSchema(*pinned_physical_columns, utf8_columns);
        predicate_pushdown = extractLancePredicatePushdown(format_filter_info, physical_schema);
    }
    /// Partial predicates must not feed countRows (would over-count). Fall back to scan + residual.
    const bool effective_need_only_count = need_only_count && predicate_pushdown.is_complete;

    Names scan_projection = read_from_format_info.requested_columns.getNames();
    bool discard_output_columns = false;
    if (!effective_need_only_count && scan_projection.empty())
    {
        if (!pinned_physical_columns)
            pinned_physical_columns = dataset.tableSchema(snapshot, local_context, cancel_handle);
        const auto & schema = *pinned_physical_columns;
        if (schema.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot scan a `Lance` dataset without physical columns");

        scan_projection.push_back(schema.front().name);
        discard_output_columns = true;
    }

    /// LIMIT is safe only when the residual filter cannot drop rows after the source
    /// (no filter, or the full filter was translated into Lance).
    std::optional<UInt64> scan_limit;
    if (limit && *limit > 0 && !effective_need_only_count && predicate_pushdown.is_complete)
        scan_limit = static_cast<UInt64>(*limit);

    if (predicate_pushdown.is_complete)
        ProfileEvents::increment(ProfileEvents::LancePredicatePushdownComplete);
    else
        ProfileEvents::increment(ProfileEvents::LancePredicatePushdownPartial);
    if (scan_limit)
        ProfileEvents::increment(ProfileEvents::LanceLimitPushdown);
    ProfileEvents::increment(ProfileEvents::LanceProjectedColumns, scan_projection.size());

    const bool scan_in_order = settings[Setting::lance_scan_in_order];
    const auto to_u32_setting = [](UInt64 value, const char * setting_name) -> UInt32
    {
        if (value > std::numeric_limits<UInt32>::max())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting {} is too large: {}", setting_name, value);
        return static_cast<UInt32>(value);
    };
    const UInt32 fragment_readahead = to_u32_setting(settings[Setting::lance_fragment_readahead], "lance_fragment_readahead");
    const UInt32 batch_readahead = to_u32_setting(settings[Setting::lance_batch_readahead], "lance_batch_readahead");
    const UInt64 io_buffer_size = settings[Setting::lance_io_buffer_size];

    if (!scan_in_order)
        ProfileEvents::increment(ProfileEvents::LanceScanUnordered);

    Lance::ScanDescription scan
    {
        .snapshot = snapshot,
        .projection = std::move(scan_projection),
        .predicate = predicate_pushdown.predicate,
        .predicate_is_complete = predicate_pushdown.is_complete,
        .max_block_size = max_block_size,
        .limit = scan_limit,
        .need_only_count = effective_need_only_count,
        .discard_output_columns = discard_output_columns,
        .scan_in_order = scan_in_order,
        .fragment_readahead = fragment_readahead,
        .batch_readahead = batch_readahead,
        .io_buffer_size = io_buffer_size,
        .fragment_ids = lance_object.fragment_ids,
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
        std::move(dataset),
        std::move(scan),
        std::move(cancel_handle),
        format_settings ? *format_settings : getFormatSettings(local_context));
    return Pipe(source);
}

Lance::DatasetOptions LanceMetadata::getDatasetOptions(const ContextPtr & local_context) const
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

        /// Align Lance object_store deadlines with ClickHouse HTTP settings (Phase C safety net).
        /// Defaults match Core/Defines.h when context is unavailable.
        options.s3_connect_timeout_ms = DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT * 1000;
        options.s3_request_timeout_ms = DEFAULT_HTTP_READ_BUFFER_TIMEOUT * 1000;
        if (local_context)
        {
            const auto & settings = local_context->getSettingsRef();
            options.s3_connect_timeout_ms = static_cast<UInt64>(
                settings[Setting::http_connection_timeout].totalMilliseconds());
            const auto send_ms = settings[Setting::http_send_timeout].totalMilliseconds();
            const auto receive_ms = settings[Setting::http_receive_timeout].totalMilliseconds();
            options.s3_request_timeout_ms = static_cast<UInt64>(std::max(send_ms, receive_ms));
        }
        return options;
    }
#endif

    options.uri = configuration_ptr->getRawPath().path;
    return options;
}

}

#endif
