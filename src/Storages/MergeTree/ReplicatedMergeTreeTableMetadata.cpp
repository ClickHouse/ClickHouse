#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/IndicesDescription.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ExpressionListParsers.h>
#include <IO/Operators.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Common/SipHash.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <fmt/ranges.h>


namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool escape_index_filenames;
    extern const MergeTreeSettingsUInt64 index_granularity;
    extern const MergeTreeSettingsUInt64 index_granularity_bytes;
}

namespace ErrorCodes
{
    extern const int METADATA_MISMATCH;
}

/// User-written parentheses around individual key elements (e.g. `PRIMARY KEY (col)`) are
/// syntactically meaningless in stored metadata. Strip them so the canonical form matches
/// what `KeyDescription::parse` produces when reading metadata back from ZooKeeper.
static void stripArtificialParens(IAST & ast)
{
    ast.setParenthesized(false);
    if (auto * list = ast.as<ASTExpressionList>())
        for (auto & child : list->children)
            if (child)
                child->setParenthesized(false);
}

static String formattedAST(const ASTPtr & ast)
{
    if (!ast)
        return "";
    auto cloned = ast->clone();
    stripArtificialParens(*cloned);
    return cloned->formatWithSecretsOneLine();
}

static String formattedASTNormalized(const ASTPtr & ast)
{
    if (!ast)
        return "";
    auto ast_normalized = ast->clone();
    FunctionNameNormalizer::visit(ast_normalized.get());
    stripArtificialParens(*ast_normalized);
    return ast_normalized->formatWithSecretsOneLine();
}

ReplicatedMergeTreeTableMetadata::ReplicatedMergeTreeTableMetadata(const MergeTreeData & data, const StorageMetadataPtr & metadata_snapshot)
{
    if (data.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        auto minmax_idx_column_names = MergeTreeData::getMinMaxColumnsNames(metadata_snapshot->getPartitionKey());
        date_column = minmax_idx_column_names[data.minmax_idx_date_column_pos];
    }

    const auto data_settings = data.getSettings();
    sampling_expression = formattedASTNormalized(metadata_snapshot->getSamplingKeyAST());
    index_granularity = (*data_settings)[MergeTreeSetting::index_granularity];
    merging_params_mode = static_cast<int>(data.merging_params.mode);
    sign_column = data.merging_params.sign_column;
    is_deleted_column = data.merging_params.is_deleted_column;
    columns_to_sum = fmt::format("{}", fmt::join(data.merging_params.columns_to_sum.begin(), data.merging_params.columns_to_sum.end(), ","));
    version_column = data.merging_params.version_column;
    if (data.merging_params.mode == MergeTreeData::MergingParams::Graphite)
    {
        SipHash graphite_hash;
        data.merging_params.graphite_params.updateHash(graphite_hash);
        WriteBufferFromOwnString wb;
        writeText(graphite_hash.get128(), wb);
        graphite_params_hash = std::move(wb.str());
    }

    /// This code may looks strange, but previously we had only one entity: PRIMARY KEY (or ORDER BY, it doesn't matter)
    /// Now we have two different entities ORDER BY and it's optional prefix -- PRIMARY KEY.
    /// In most cases user doesn't specify PRIMARY KEY and semantically it's equal to ORDER BY.
    /// So rules in zookeeper metadata is following:
    /// - When we have only ORDER BY, than store it in "primary key:" row of /metadata
    /// - When we have both, than store PRIMARY KEY in "primary key:" row and ORDER BY in "sorting key:" row of /metadata
    if (metadata_snapshot->isPrimaryKeyDefined())
    {
        primary_key = formattedASTNormalized(metadata_snapshot->getPrimaryKey().expression_list_ast);
        /// We don't use preparsed AST `sorting_key.expression_list_ast` because
        /// it contain version column for VersionedCollapsingMergeTree, which
        /// is not stored in ZooKeeper for compatibility reasons. So the best
        /// compatible way is just to convert definition_ast to list and
        /// serialize it. In all other places key.expression_list_ast should be
        /// used.
        sorting_key = formattedASTNormalized(extractKeyExpressionList(metadata_snapshot->getSortingKey().definition_ast));
    }
    else
    {
        primary_key = formattedASTNormalized(metadata_snapshot->getPrimaryKey().getOriginalExpressionList());
    }

    data_format_version = data.format_version;

    if (data.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        partition_key = formattedASTNormalized(metadata_snapshot->getPartitionKey().expression_list_ast);

    ttl_table = formattedASTNormalized(metadata_snapshot->getTableTTLs().definition_ast);

    /// We only store skip indices that are explicitly defined by user
    skip_indices = metadata_snapshot->getSecondaryIndices().explicitToString();

    projections = metadata_snapshot->getProjections().toString();

    if (data.canUseAdaptiveGranularity())
        index_granularity_bytes = (*data_settings)[MergeTreeSetting::index_granularity_bytes];
    else
        index_granularity_bytes = 0;

    constraints = metadata_snapshot->getConstraints().toString();
}

void ReplicatedMergeTreeTableMetadata::write(WriteBuffer & out) const
{
    /// Important notes: new added field must always be append to the end of serialized metadata
    /// for backward compatible.

    /// In addition, two consecutive fields should not share any prefix, otherwise deserialize may fails.
    /// For example, if you have two field `v1` and `v2` serialized as:
    ///     if (!v1.empty()) out << "v1: " << v1 << "\n";
    ///     if (!v2.empty()) out << "v2: " << v2 << "\n";
    /// Let say if `v1` is empty and v2 is non-empty, then `v1` is not in serialized metadata.
    /// Later, to deserialize the metadata, `read` will sequentially check if each field with `checkString`.
    /// When it begin to check for `v1` and `v2`, the metadata buffer look like this:
    ///     v2: <v2 value>
    ///     ^
    ///   cursor
    /// `checkString("v1: ", in)` will be called first and it moves the cursor to `2` instead of `v`, so the
    /// subsequent call `checkString("v2: ", in)` will also fails.

    out << "metadata format version: 1\n"
        << "date column: " << date_column << "\n"
        << "sampling expression: " << sampling_expression << "\n"
        << "index granularity: " << index_granularity << "\n"
        << "mode: " << merging_params_mode << "\n"
        << "sign column: " << sign_column << "\n"
        << "primary key: " << primary_key << "\n";

    if (data_format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        out << "data format version: " << data_format_version.toUnderType() << "\n"
            << "partition key: " << partition_key << "\n";
    }

    if (!sorting_key.empty())
        out << "sorting key: " << sorting_key << "\n";

    if (!ttl_table.empty())
        out << "ttl: " << ttl_table << "\n";

    if (!skip_indices.empty())
        out << "indices: " << skip_indices << "\n";

    if (!projections.empty())
        out << "projections: " << projections << "\n";

    if (index_granularity_bytes != 0)
        out << "granularity bytes: " << index_granularity_bytes << "\n";

    if (!constraints.empty())
        out << "constraints: " << constraints << "\n";

    if (merge_params_version >= REPLICATED_MERGE_TREE_METADATA_WITH_ALL_MERGE_PARAMETERS)
    {
        out << "merge parameters format version: " << merge_params_version << "\n";
        if (!version_column.empty())
            out << "version column: " << version_column << "\n";
        if (!is_deleted_column.empty())
            out << "is_deleted column: " << is_deleted_column << "\n";
        if (!columns_to_sum.empty())
            out << "columns to sum: " << columns_to_sum << "\n";
        if (!graphite_params_hash.empty())
            out << "graphite hash: " << graphite_params_hash << "\n";
    }
}

String ReplicatedMergeTreeTableMetadata::toString() const
{
    WriteBufferFromOwnString out;
    write(out);
    return out.str();
}

void ReplicatedMergeTreeTableMetadata::read(ReadBuffer & in)
{
    in >> "metadata format version: 1\n";
    in >> "date column: " >> date_column >> "\n";
    in >> "sampling expression: " >> sampling_expression >> "\n";
    in >> "index granularity: " >> index_granularity >> "\n";
    in >> "mode: " >> merging_params_mode >> "\n";
    in >> "sign column: " >> sign_column >> "\n";
    in >> "primary key: " >> primary_key >> "\n";

    if (in.eof())
        data_format_version = 0;
    else if (checkString("data format version: ", in))
        in >> data_format_version.toUnderType() >> "\n";

    if (data_format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        in >> "partition key: " >> partition_key >> "\n";

    if (checkString("sorting key: ", in))
        in >> sorting_key >> "\n";

    if (checkString("ttl: ", in))
        in >> ttl_table >> "\n";

    if (checkString("indices: ", in))
        in >> skip_indices >> "\n";

    if (checkString("projections: ", in))
        in >> projections >> "\n";

    if (checkString("granularity bytes: ", in))
    {
        in >> index_granularity_bytes >> "\n";
        index_granularity_bytes_found_in_zk = true;
    }
    else
        index_granularity_bytes = 0;

    if (checkString("constraints: ", in))
        in >> constraints >> "\n";

    if (checkString("merge parameters format version: ", in))
        in >> merge_params_version >> "\n";
    else
        merge_params_version = REPLICATED_MERGE_TREE_METADATA_LEGACY_VERSION;

    if (merge_params_version >= REPLICATED_MERGE_TREE_METADATA_WITH_ALL_MERGE_PARAMETERS)
    {
        if (checkString("version column: ", in))
            in >> version_column >> "\n";

        if (checkString("is_deleted column: ", in))
            in >> is_deleted_column >> "\n";

        if (checkString("columns to sum: ", in))
            in >> columns_to_sum >> "\n";

        if (checkString("graphite hash: ", in))
            in >> graphite_params_hash >> "\n";
    }
}

ReplicatedMergeTreeTableMetadata ReplicatedMergeTreeTableMetadata::parseRaw(const String & s)
{
    ReplicatedMergeTreeTableMetadata metadata;
    ReadBufferFromString buf(s);
    metadata.read(buf);
    return metadata;
}

ReplicatedMergeTreeTableMetadata ReplicatedMergeTreeTableMetadata::parseAndNormalize(
    const String & s,
    const ColumnsDescription & columns,
    bool add_minmax_index_for_numeric_columns,
    bool add_minmax_index_for_string_columns,
    ContextPtr context)
{
    auto result = parseRaw(s);

    /// Backward compatibility: older replicas (before 25.12) stored implicit indices in Keeper
    /// metadata. Newer replicas only store explicit indices. Strip implicit indices from the
    /// parsed metadata so that all downstream comparisons work against the new format.
    if (result.skip_indices.empty()
        || (!add_minmax_index_for_numeric_columns && !add_minmax_index_for_string_columns))
        return result;

    constexpr bool escape_index_filenames = true; /// Does not matter here, we re-serialize the parsed result
    auto parsed = IndicesDescription::parse(result.skip_indices, columns, escape_index_filenames, context);

    bool has_implicit = false;
    for (auto & index : parsed)
    {
        if (!index.name.starts_with(IMPLICITLY_ADDED_MINMAX_INDEX_PREFIX))
            continue;

        String column_name = index.name.substr(strlen(IMPLICITLY_ADDED_MINMAX_INDEX_PREFIX));
        if (!columns.has(column_name))
            continue;

        const auto & col_type = columns.get(column_name).type;

        /// Only `add_minmax_index_for_numeric_columns` and `add_minmax_index_for_string_columns`
        /// need to be checked here. The temporal setting (`add_minmax_index_for_temporal_columns`)
        /// was introduced in 26.2 and never stored implicit indices in Keeper metadata.
        if ((add_minmax_index_for_numeric_columns && isNumber(col_type))
            || (add_minmax_index_for_string_columns && isString(col_type)))
        {
            index.is_implicitly_created = true;
            has_implicit = true;
        }
    }

    if (has_implicit)
        result.skip_indices = parsed.explicitToString();

    return result;
}

static void handleTableMetadataMismatch(
    const std::string & table_name_for_error_message,
    std::string_view differs_in,
    const auto & stored_in_zk,
    const std::string & parsed_from_zk,
    const auto & local,
    bool strict_check = true,
    LoggerPtr logger = nullptr)
{
    String metadata_string;
    if (!parsed_from_zk.empty())
        metadata_string = fmt::format("Stored in ZooKeeper: {}, parsed from ZooKeeper: {}, local: {}", stored_in_zk, parsed_from_zk, local);
    else
        metadata_string = fmt::format("Stored in ZooKeeper: {}, local: {}", stored_in_zk, local);

    if (strict_check)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH,
            "Metadata of table {} in ZooKeeper differs in {}. {}",
            table_name_for_error_message, differs_in, metadata_string);

    if (logger)
        LOG_WARNING(logger,
            "Metadata of table {} in ZooKeeper differs in {}. {}",
            table_name_for_error_message, differs_in, metadata_string);
};

void ReplicatedMergeTreeTableMetadata::checkImmutableFieldsEquals(
    const ReplicatedMergeTreeTableMetadata & from_zk,
    const ColumnsDescription & columns,
    const VirtualColumnsDescription & virtuals,
    const std::string & table_name_for_error_message,
    ContextPtr context,
    bool check_index_granularity) const
{

    if (data_format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        if (date_column != from_zk.date_column)
            handleTableMetadataMismatch(table_name_for_error_message, "date index column", from_zk.date_column, "", date_column);
    }
    else if (!from_zk.date_column.empty())
    {
        handleTableMetadataMismatch(table_name_for_error_message, "date index column", from_zk.date_column, "", "custom-partitioned");
    }

    if (check_index_granularity && index_granularity != from_zk.index_granularity)
        handleTableMetadataMismatch(table_name_for_error_message, "index granularity", DB::toString(from_zk.index_granularity), "", DB::toString(index_granularity));

    if (merging_params_mode != from_zk.merging_params_mode)
        handleTableMetadataMismatch(table_name_for_error_message, "mode of merge operation", DB::toString(from_zk.merging_params_mode), "", DB::toString(merging_params_mode));

    if (sign_column != from_zk.sign_column)
        handleTableMetadataMismatch(table_name_for_error_message, "sign column", from_zk.sign_column, "", sign_column);

    if (merge_params_version >= REPLICATED_MERGE_TREE_METADATA_WITH_ALL_MERGE_PARAMETERS && from_zk.merge_params_version >= REPLICATED_MERGE_TREE_METADATA_WITH_ALL_MERGE_PARAMETERS)
    {
        if (version_column != from_zk.version_column)
            handleTableMetadataMismatch(table_name_for_error_message, "version column", from_zk.version_column, "", version_column);

        if (is_deleted_column != from_zk.is_deleted_column)
            handleTableMetadataMismatch(table_name_for_error_message, "is_deleted column", from_zk.is_deleted_column, "", is_deleted_column);

        if (columns_to_sum != from_zk.columns_to_sum)
            handleTableMetadataMismatch(table_name_for_error_message, "sum columns", from_zk.columns_to_sum, "", columns_to_sum);

        if (graphite_params_hash != from_zk.graphite_params_hash)
            handleTableMetadataMismatch(table_name_for_error_message, "graphite params", from_zk.graphite_params_hash, "", graphite_params_hash);
    }

    String parsed_zk_primary_key = formattedAST(KeyDescription::parse(from_zk.primary_key, columns, virtuals, context, true).getOriginalExpressionList());
    String parsed_local_primary_key = formattedAST(KeyDescription::parse(primary_key, columns, virtuals, context, true).getOriginalExpressionList());
    if (parsed_local_primary_key != parsed_zk_primary_key)
        handleTableMetadataMismatch(table_name_for_error_message, "primary key", from_zk.primary_key, parsed_zk_primary_key, primary_key);

    if (data_format_version != from_zk.data_format_version)
        handleTableMetadataMismatch(table_name_for_error_message, "data format version", DB::toString(from_zk.data_format_version.toUnderType()), "", DB::toString(data_format_version.toUnderType()));

    String parsed_zk_partition_key = formattedAST(KeyDescription::parse(from_zk.partition_key, columns, virtuals, context, false).expression_list_ast);
    String parsed_local_partition_key = formattedAST(KeyDescription::parse(partition_key, columns, virtuals, context, false).expression_list_ast);
    if (parsed_local_partition_key != parsed_zk_partition_key)
        handleTableMetadataMismatch(table_name_for_error_message, "partition key expression", from_zk.partition_key, parsed_zk_partition_key, partition_key);
}

bool ReplicatedMergeTreeTableMetadata::checkEquals(
    const ReplicatedMergeTreeTableMetadata & from_zk,
    const ColumnsDescription & columns,
    const VirtualColumnsDescription & virtuals,
    const std::string & table_name_for_error_message,
    ContextPtr context,
    bool check_index_granularity,
    bool strict_check,
    LoggerPtr logger) const
{
    bool is_equal = true;
    checkImmutableFieldsEquals(from_zk, columns, virtuals, table_name_for_error_message, context, check_index_granularity);

    String parsed_zk_sampling_expression = formattedAST(KeyDescription::parse(from_zk.sampling_expression, columns, virtuals, context, false).definition_ast);
    if (sampling_expression != parsed_zk_sampling_expression)
    {
        handleTableMetadataMismatch(table_name_for_error_message, "sampling expression", from_zk.sampling_expression, parsed_zk_sampling_expression, sampling_expression, strict_check, logger);
        is_equal = false;
    }

    String parsed_zk_sorting_key = formattedAST(extractKeyExpressionList(KeyDescription::parse(from_zk.sorting_key, columns, virtuals, context, true).definition_ast));
    if (sorting_key != parsed_zk_sorting_key)
    {
        handleTableMetadataMismatch(table_name_for_error_message, "sorting key expression", from_zk.sorting_key, parsed_zk_sorting_key, sorting_key, strict_check, logger);
        is_equal = false;
    }

    auto parsed_primary_key = KeyDescription::parse(primary_key, columns, virtuals, context, true);
    // Strict checking of suspicious TTL is not needed here
    String parsed_zk_ttl_table = formattedAST(
        TTLTableDescription::parse(from_zk.ttl_table, columns, context, parsed_primary_key, /* is_attach = */ true).definition_ast);
    if (ttl_table != parsed_zk_ttl_table)
    {
        handleTableMetadataMismatch(table_name_for_error_message, "TTL", from_zk.ttl_table, parsed_zk_ttl_table, ttl_table, strict_check, logger);
        is_equal = false;
    }

    /// Implicit indices are stripped from Keeper metadata during `parseAndNormalize`,
    /// so at this point `from_zk.skip_indices` only contains explicit indices.
    constexpr bool escape_index_filenames = true; /// It doesn't matter here, as we compare parsed strings
    String parsed_zk_skip_indices = IndicesDescription::parse(from_zk.skip_indices, columns, escape_index_filenames, context).allToString();
    if (skip_indices != parsed_zk_skip_indices)
    {
        handleTableMetadataMismatch(table_name_for_error_message, "skip indexes", from_zk.skip_indices, parsed_zk_skip_indices, skip_indices, strict_check, logger);
        is_equal = false;
    }

    String parsed_zk_projections = ProjectionsDescription::parse(from_zk.projections, columns, nullptr, context).toString();
    if (projections != parsed_zk_projections)
    {
        handleTableMetadataMismatch(table_name_for_error_message, "projections", from_zk.projections, parsed_zk_projections, projections, strict_check, logger);
        is_equal = false;
    }

    String parsed_zk_constraints = ConstraintsDescription::parse(from_zk.constraints).toString();
    if (constraints != parsed_zk_constraints)
    {
        handleTableMetadataMismatch(table_name_for_error_message, "constraints", from_zk.constraints, parsed_zk_constraints, constraints, strict_check, logger);
        is_equal = false;
    }

    if (check_index_granularity && from_zk.index_granularity_bytes_found_in_zk && index_granularity_bytes != from_zk.index_granularity_bytes)
    {
        handleTableMetadataMismatch(table_name_for_error_message, "index granularity bytes", from_zk.index_granularity_bytes, "", index_granularity_bytes, strict_check, logger);
        is_equal = false;
    }

    return is_equal;
}

ReplicatedMergeTreeTableMetadata::Diff
ReplicatedMergeTreeTableMetadata::checkAndFindDiff(
    const ReplicatedMergeTreeTableMetadata & from_zk,
    const ColumnsDescription & columns,
    const VirtualColumnsDescription & virtuals,
    const std::string & table_name_for_error_message,
    ContextPtr context,
    bool check_index_granularity) const
{
    checkImmutableFieldsEquals(from_zk, columns, virtuals, table_name_for_error_message, context, check_index_granularity);

    Diff diff;

    if (sorting_key != from_zk.sorting_key)
    {
        diff.sorting_key_changed = true;
        diff.new_sorting_key = from_zk.sorting_key;
    }

    if (sampling_expression != from_zk.sampling_expression)
    {
        diff.sampling_expression_changed = true;
        diff.new_sampling_expression = from_zk.sampling_expression;
    }

    if (ttl_table != from_zk.ttl_table)
    {
        diff.ttl_table_changed = true;
        diff.new_ttl_table = from_zk.ttl_table;
    }

    if (skip_indices != from_zk.skip_indices)
    {
        diff.skip_indices_changed = true;
        diff.new_skip_indices = from_zk.skip_indices;
    }

    if (projections != from_zk.projections)
    {
        diff.projections_changed = true;
        diff.new_projections = from_zk.projections;
    }

    if (constraints != from_zk.constraints)
    {
        diff.constraints_changed = true;
        diff.new_constraints = from_zk.constraints;
    }

    return diff;
}

StorageInMemoryMetadata ReplicatedMergeTreeTableMetadata::Diff::getNewMetadata(const ColumnsDescription & new_columns, const VirtualColumnsDescription & virtuals, ContextPtr context, const StorageInMemoryMetadata & old_metadata) const
{
    StorageInMemoryMetadata new_metadata = old_metadata;
    new_metadata.columns = new_columns;

    if (!empty())
    {
        auto parse_key_expr = [] (const String & key_expr)
        {
            ParserNotEmptyExpressionList parser(false);
            auto new_sorting_key_expr_list = parseQuery(parser, key_expr, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

            ASTPtr order_by_ast;
            if (new_sorting_key_expr_list->children.size() == 1)
                order_by_ast = new_sorting_key_expr_list->children[0];
            else
            {
                auto tuple = makeASTOperator("tuple");
                tuple->arguments->children = new_sorting_key_expr_list->children;
                order_by_ast = tuple;
            }
            return order_by_ast;
        };

        if (sorting_key_changed)
        {
            auto order_by_ast = parse_key_expr(new_sorting_key);

            new_metadata.sorting_key.recalculateWithNewAST(order_by_ast, new_metadata.columns, virtuals, context);

            if (new_metadata.primary_key.definition_ast == nullptr)
            {
                /// Primary and sorting key become independent after this ALTER so we have to
                /// save the old ORDER BY expression as the new primary key.
                new_metadata.primary_key = KeyDescription::getKeyFromAST(old_metadata.sorting_key.definition_ast, new_metadata.columns, virtuals, context);
            }
        }

        if (sampling_expression_changed)
        {
            if (!new_sampling_expression.empty())
            {
                auto sample_by_ast = parse_key_expr(new_sampling_expression);
                new_metadata.sampling_key.recalculateWithNewAST(sample_by_ast, new_metadata.columns, virtuals, context);
            }
            else /// SAMPLE BY was removed
            {
                new_metadata.sampling_key = {};
            }
        }

        if (skip_indices_changed)
            new_metadata.secondary_indices = IndicesDescription::parse(new_skip_indices, new_columns, new_metadata.escape_index_filenames, context);

        if (constraints_changed)
            new_metadata.constraints = ConstraintsDescription::parse(new_constraints);

        if (projections_changed)
            new_metadata.projections = ProjectionsDescription::parse(new_projections, new_columns, &new_metadata.partition_key, context);

        if (ttl_table_changed)
        {
            if (!new_ttl_table.empty())
            {
                ParserTTLExpressionList parser;
                auto ttl_for_table_ast = parseQuery(parser, new_ttl_table, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
                new_metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(
                    ttl_for_table_ast, new_metadata.columns, context, new_metadata.primary_key, true /* allow_suspicious; because it is replication */);
            }
            else /// TTL was removed
            {
                new_metadata.table_ttl = TTLTableDescription{};
            }
        }
    }

    /// Changes in columns may affect following metadata fields
    new_metadata.column_ttls_by_name.clear();
    for (const auto & [name, ast] : new_metadata.columns.getColumnTTLs())
    {
        auto new_ttl_entry = TTLDescription::getTTLFromAST(ast, new_metadata.columns, context, new_metadata.primary_key, true /* allow_suspicious; because it is replication */);
        new_metadata.column_ttls_by_name[name] = new_ttl_entry;
    }

    if (new_metadata.partition_key.definition_ast != nullptr)
    {
        auto old_partition_key_sample_block = new_metadata.partition_key.sample_block;
        new_metadata.partition_key.recalculateWithNewColumns(new_metadata.columns, virtuals, context);

        /// If partition key expression structure changed we must rebuild minmax_count_projection,
        /// otherwise it retains stale column types (e.g. plain Int8 instead of LowCardinality(Int8))
        /// and the aggregation engine hits a type mismatch. See #100175.
        if (new_metadata.minmax_count_projection
            && !blocksHaveEqualStructure(new_metadata.partition_key.sample_block, old_partition_key_sample_block))
        {
            auto minmax_columns = new_metadata.getColumnsRequiredForPartitionKey();
            auto partition_key_ast = new_metadata.partition_key.expression_list_ast->clone();
            FunctionNameNormalizer::visit(partition_key_ast.get());
            new_metadata.minmax_count_projection.emplace(ProjectionDescription::getMinMaxCountProjection(
                new_metadata.columns, partition_key_ast, minmax_columns, new_metadata.primary_key, &new_metadata.partition_key, context));
        }
    }

    if (!sorting_key_changed) /// otherwise already updated
        new_metadata.sorting_key.recalculateWithNewColumns(new_metadata.columns, virtuals, context);

    /// Primary key is special, it exists even if not defined
    if (new_metadata.primary_key.definition_ast != nullptr)
    {
        new_metadata.primary_key.recalculateWithNewColumns(new_metadata.columns, virtuals, context);
    }
    else
    {
        new_metadata.primary_key = KeyDescription::getKeyFromAST(new_metadata.sorting_key.definition_ast, new_metadata.columns, virtuals, context);
        new_metadata.primary_key.definition_ast = nullptr;
    }

    if (!sampling_expression_changed && new_metadata.sampling_key.definition_ast != nullptr)
        new_metadata.sampling_key.recalculateWithNewColumns(new_metadata.columns, virtuals, context);

    if (!skip_indices_changed) /// otherwise already updated
    {
        /// Remove implicitly created indices and recalculate explicit ones
        IndicesDescription new_indices;
        for (auto & index : new_metadata.secondary_indices)
        {
            if (!index.isImplicitlyCreated())
            {
                index.recalculateWithNewColumns(new_metadata.columns, context);
                new_indices.push_back(index);
            }
        }
        new_metadata.secondary_indices = std::move(new_indices);
    }

    /// Regenerate implicit indices for the new columns regardless of whether indices were explicitly changed.
    /// Implicit indices are not stored in ZooKeeper, so they must be recreated locally based on the current
    /// columns and table settings.
    /// Note: addImplicitIndicesForColumn checks for existing minmax indices on each column and won't create
    /// duplicates if an explicit index already exists.
    for (const auto & column : new_metadata.columns)
        new_metadata.addImplicitIndicesForColumn(column, context);

    if (!ttl_table_changed && new_metadata.table_ttl.definition_ast != nullptr)
        new_metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(
            new_metadata.table_ttl.definition_ast, new_metadata.columns, context, new_metadata.primary_key, true /* allow_suspicious; because it is replication */);

    if (!projections_changed)
    {
        ProjectionsDescription recalculated_projections;
        for (const auto & projection : new_metadata.projections)
            recalculated_projections.add(ProjectionDescription::getProjectionFromAST(projection.definition_ast, new_metadata.columns, &new_metadata.partition_key, context));
        new_metadata.projections = std::move(recalculated_projections);
    }

    return new_metadata;
}

}
