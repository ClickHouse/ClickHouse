#include <optional>
#include <unordered_set>
#include "config.h"

#if USE_AVRO

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Common/DateLUTImpl.h>
#include <Common/GeoBbox.h>
#include <Common/WKB.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/logger_useful.h>
#include <Functions/IFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Common/quoteString.h>
#include <fmt/ranges.h>

#include <Interpreters/ExpressionActions.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

using namespace DB;

namespace
{

/// Extractor callback for the shared conjunctive collection template.
/// Returns an optional (column_name, bbox) pair when the node is a spatial
/// predicate with at least one constant geometry argument.
std::optional<std::pair<String, std::array<double, 4>>>
tryExtractSpatialPredicateFromNode(const ActionsDAG::Node & node)
{
    if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base)
        return std::nullopt;
    if (!node.function_base->isSpatialPredicate() || node.children.size() < 2)
        return std::nullopt;

    const ActionsDAG::Node * col_node = nullptr;
    for (const auto * child : node.children)
        if (child->type == ActionsDAG::ActionType::INPUT && !col_node)
            col_node = child;
    if (!col_node)
        return std::nullopt;

    double xmin = std::numeric_limits<double>::infinity();
    double ymin = std::numeric_limits<double>::infinity();
    double xmax = -std::numeric_limits<double>::infinity();
    double ymax = -std::numeric_limits<double>::infinity();
    bool found_const = false;
    bool any_extraction_failed = false;

    for (const auto * child : node.children)
    {
        if (child->type != ActionsDAG::ActionType::COLUMN || !child->column
            || !child->is_deterministic_constant)
            continue;
        double cxmin = 0;
        double cymin = 0;
        double cxmax = 0;
        double cymax = 0;
        if (!tryExtractBboxFromColumn(*child->column, cxmin, cymin, cxmax, cymax))
        {
            any_extraction_failed = true;
            continue;
        }
        xmin = std::min(xmin, cxmin);
        ymin = std::min(ymin, cymin);
        xmax = std::max(xmax, cxmax);
        ymax = std::max(ymax, cymax);
        found_const = true;
    }
    /// Fail-closed: if any constant arg could not be converted to a bbox, the union
    /// bbox is incomplete. Pruning on the partial bbox would incorrectly exclude files
    /// that match only the geometry that was skipped (unsafe for variadic predicates
    /// like `pointInPolygon` where polygon args are OR-ed).
    if (!found_const || any_extraction_failed)
        return std::nullopt;

    return std::make_pair(col_node->result_name, std::array<double, 4>{xmin, ymin, xmax, ymax});
}

} // anonymous namespace

namespace DB::Iceberg
{

DB::ASTPtr getASTFromTransform(const String & transform_name_src, const String & column_name)
{
    auto transform_and_argument = parseTransformAndArgument(transform_name_src);
    if (!transform_and_argument)
    {
        LOG_WARNING(&Poco::Logger::get("Iceberg Partition Pruning"), "Cannot parse iceberg transform name: {}.", transform_name_src);
        return nullptr;
    }

    std::string transform_name = Poco::toLower(transform_name_src);
    if (transform_name == "identity")
        return make_intrusive<ASTIdentifier>(column_name);

    if (transform_name == "void")
        return makeASTOperator("tuple");

    if (transform_and_argument->argument.has_value())
    {
        return makeASTFunction(
                transform_and_argument->transform_name, make_intrusive<ASTLiteral>(*transform_and_argument->argument), make_intrusive<ASTIdentifier>(column_name));
    }
    return makeASTFunction(transform_and_argument->transform_name, make_intrusive<ASTIdentifier>(column_name));
}

std::unique_ptr<DB::ActionsDAG> ManifestFilesPruner::transformFilterDagForManifest(const DB::ActionsDAG * source_dag, std::vector<Int32> & used_columns_in_filter) const
{
    const auto & inputs = source_dag->getInputs();

    for (const auto & input : inputs)
    {
        if (input->type == ActionsDAG::ActionType::INPUT)
        {
            std::string input_name = input->result_name;
            std::optional<Int32> input_id = schema_processor.tryGetColumnIDByName(current_schema_id, input_name);
            if (input_id)
                used_columns_in_filter.push_back(*input_id);
        }
    }

    ActionsDAG dag_with_renames;
    for (const auto column_id : used_columns_in_filter)
    {
        auto column = schema_processor.tryGetFieldCharacteristics(current_schema_id, column_id);

        /// Columns which we dropped and don't exist in current schema
        /// cannot be queried in WHERE expression.
        if (!column.has_value())
            continue;

        /// We take data type from manifest schema, not latest type
        auto column_from_manifest = schema_processor.tryGetFieldCharacteristics(initial_schema_id, column_id);
        if (!column_from_manifest.has_value())
            continue;

        auto numeric_column_name = DB::backQuote(DB::toString(column_id));
        const auto * node = &dag_with_renames.addInput(numeric_column_name, column_from_manifest->type);
        node = &dag_with_renames.addAlias(*node, column->name);
        dag_with_renames.getOutputs().push_back(node);
    }
    auto result = std::make_unique<DB::ActionsDAG>(DB::ActionsDAG::merge(std::move(dag_with_renames), source_dag->clone()));
    result->removeUnusedActions();
    return result;
}


ManifestFilesPruner::ManifestFilesPruner(
    const IcebergSchemaProcessor & schema_processor_,
    Int32 current_schema_id_,
    Int32 initial_schema_id_,
    const DB::ActionsDAG * filter_dag,
    const ManifestFileIterator & manifest_file,
    DB::ContextPtr context)
    : schema_processor(schema_processor_)
    , current_schema_id(current_schema_id_)
    , initial_schema_id(initial_schema_id_)
{
    if (filter_dag == nullptr)
    {
        return;
    }

    std::unique_ptr<ActionsDAG> transformed_dag;
    std::vector<Int32> used_columns_in_filter;
    transformed_dag = transformFilterDagForManifest(filter_dag, used_columns_in_filter);
    chassert(transformed_dag != nullptr);

    if (manifest_file.hasPartitionKey())
    {
        partition_key = &manifest_file.getPartitionKeyDescription();
        ActionsDAGWithInversionPushDown inverted_dag(transformed_dag->getOutputs().front(), context);
        partition_key_condition.emplace(
            inverted_dag, context, partition_key->column_names, partition_key->expression, true /* single_point */);
    }

    for (Int32 used_column_id : used_columns_in_filter)
    {
        auto name_and_type = schema_processor.tryGetFieldCharacteristics(initial_schema_id, used_column_id);
        if (!name_and_type.has_value())
            continue;

        name_and_type->name = DB::backQuote(DB::toString(used_column_id));

        ExpressionActionsPtr expression
            = std::make_shared<ExpressionActions>(ActionsDAG({name_and_type.value()}), ExpressionActionsSettings(context));

        ActionsDAGWithInversionPushDown inverted_dag(transformed_dag->getOutputs().front(), context);
        min_max_key_conditions.emplace(used_column_id, KeyCondition(inverted_dag, context, {name_and_type->name}, expression));
    }

    /// Spatial bbox pruning: for each spatial predicate in the filter DAG, try to find
    /// covering.bbox columns in the Iceberg schema using the naming convention
    /// {geo_col}_bbox.{xmin,ymin,xmax,ymax}. If found, register a SpatialBboxPruneInfo
    /// that can cheaply prune files whose bbox is disjoint from the query bbox.
    std::vector<std::pair<String, std::array<double, 4>>> spatial_predicates;
    std::unordered_set<const ActionsDAG::Node *> visited;
    for (const auto * output : filter_dag->getOutputs())
        collectSpatialFiltersConjunctive(
            *output, visited,
            [](const ActionsDAG::Node & n) { return tryExtractSpatialPredicateFromNode(n); },
            spatial_predicates);

    for (const auto & [geo_col_name, bbox] : spatial_predicates)
    {
        /// Try struct sub-field convention first: {geo_col}_bbox.{xmin,ymin,xmax,ymax}
        /// (used by GeoParquet writers that store bbox as a Struct column).
        auto xmin_id = schema_processor.tryGetColumnIDByName(current_schema_id, geo_col_name + "_bbox.xmin");
        auto ymin_id = schema_processor.tryGetColumnIDByName(current_schema_id, geo_col_name + "_bbox.ymin");
        auto xmax_id = schema_processor.tryGetColumnIDByName(current_schema_id, geo_col_name + "_bbox.xmax");
        auto ymax_id = schema_processor.tryGetColumnIDByName(current_schema_id, geo_col_name + "_bbox.ymax");

        /// Fall back to flat column convention: {geo_col}_bbox_{xmin,ymin,xmax,ymax}
        /// (used when bbox columns are written as separate top-level Float64 columns).
        if (!xmin_id)
            xmin_id = schema_processor.tryGetColumnIDByName(current_schema_id, geo_col_name + "_bbox_xmin");
        if (!ymin_id)
            ymin_id = schema_processor.tryGetColumnIDByName(current_schema_id, geo_col_name + "_bbox_ymin");
        if (!xmax_id)
            xmax_id = schema_processor.tryGetColumnIDByName(current_schema_id, geo_col_name + "_bbox_xmax");
        if (!ymax_id)
            ymax_id = schema_processor.tryGetColumnIDByName(current_schema_id, geo_col_name + "_bbox_ymax");
        if (!xmin_id || !ymin_id || !xmax_id || !ymax_id)
            continue;
        SpatialBboxPruneInfo pruner;
        pruner.xmin_col_id = *xmin_id;
        pruner.ymin_col_id = *ymin_id;
        pruner.xmax_col_id = *xmax_id;
        pruner.ymax_col_id = *ymax_id;
        pruner.query_xmin = bbox[0];
        pruner.query_ymin = bbox[1];
        pruner.query_xmax = bbox[2];
        pruner.query_ymax = bbox[3];
        spatial_bbox_pruners.push_back(std::move(pruner));
        LOG_DEBUG(
            getLogger("ManifestFilesPruner"),
            "Registered spatial bbox pruner for geometry column '{}': bbox=[{},{},{},{}], "
            "Iceberg column IDs xmin={} ymin={} xmax={} ymax={}",
            geo_col_name, bbox[0], bbox[1], bbox[2], bbox[3],
            *xmin_id, *ymin_id, *xmax_id, *ymax_id);
    }
}

PruningReturnStatus ManifestFilesPruner::canBePruned(
    const ProcessedManifestFileEntryPtr & entry, const std::unordered_map<Int32, DB::Range> & entry_hyperrectangles) const
{
    if (partition_key_condition.has_value())
    {
        const auto & partition_value = entry->parsed_entry->partition_key_value;
        std::vector<FieldRef> index_value(partition_value.begin(), partition_value.end());
        for (auto & field : index_value)
        {
            // NULL_LAST
            if (field.isNull())
                field = POSITIVE_INFINITY;
        }

        bool can_be_true = partition_key_condition->mayBeTrueInRange(
            partition_value.size(), index_value.data(), index_value.data(), partition_key->data_types);

        if (!can_be_true)
        {
            return PruningReturnStatus::PARTITION_PRUNED;
        }
    }

    for (const auto & [column_id, key_condition] : min_max_key_conditions)
    {
        std::optional<NameAndTypePair> name_and_type = schema_processor.tryGetFieldCharacteristics(initial_schema_id, column_id);

        /// There is no such column in this manifest file
        if (!name_and_type.has_value())
        {
            continue;
        }

        auto rect_it = entry_hyperrectangles.find(column_id);
        if (rect_it == entry_hyperrectangles.end())
            continue;

        auto info_it = entry->parsed_entry->columns_infos.find(column_id);
        bool has_no_nulls = info_it != entry->parsed_entry->columns_infos.end() && info_it->second.nulls_count.has_value()
            && *info_it->second.nulls_count == 0;

        if (has_no_nulls && !key_condition.mayBeTrueInRange(1, &rect_it->second.left, &rect_it->second.right, {name_and_type->type}))
        {
            return PruningReturnStatus::MIN_MAX_INDEX_PRUNED;
        }
    }

    /// Spatial bbox pruning via covering.bbox column bounds.
    /// All spatial bbox pruners here come from conjunctive-only extraction (AND branches only),
    /// so if ANY single pruner finds the file bbox disjoint from the query bbox, the full
    /// conjunction cannot be satisfied — the file can be safely pruned.
    for (const auto & sp : spatial_bbox_pruners)
    {
        auto xmin_it = entry_hyperrectangles.find(sp.xmin_col_id);
        auto ymin_it = entry_hyperrectangles.find(sp.ymin_col_id);
        auto xmax_it = entry_hyperrectangles.find(sp.xmax_col_id);
        auto ymax_it = entry_hyperrectangles.find(sp.ymax_col_id);
        if (xmin_it == entry_hyperrectangles.end() || ymin_it == entry_hyperrectangles.end()
            || xmax_it == entry_hyperrectangles.end() || ymax_it == entry_hyperrectangles.end())
            continue;

        /// Gate on zero nulls for all four bbox columns, matching the min/max path above.
        /// With nullable bbox columns, partial NULL stats can make bounds disjoint from
        /// the query while matching rows still exist.
        auto info_it = entry->parsed_entry->columns_infos.find(sp.xmin_col_id);
        bool xmin_ok = info_it != entry->parsed_entry->columns_infos.end()
            && info_it->second.nulls_count.has_value()
            && *info_it->second.nulls_count == 0;
        info_it = entry->parsed_entry->columns_infos.find(sp.ymin_col_id);
        bool ymin_ok = info_it != entry->parsed_entry->columns_infos.end()
            && info_it->second.nulls_count.has_value()
            && *info_it->second.nulls_count == 0;
        info_it = entry->parsed_entry->columns_infos.find(sp.xmax_col_id);
        bool xmax_ok = info_it != entry->parsed_entry->columns_infos.end()
            && info_it->second.nulls_count.has_value()
            && *info_it->second.nulls_count == 0;
        info_it = entry->parsed_entry->columns_infos.find(sp.ymax_col_id);
        bool ymax_ok = info_it != entry->parsed_entry->columns_infos.end()
            && info_it->second.nulls_count.has_value()
            && *info_it->second.nulls_count == 0;
        if (!xmin_ok || !ymin_ok || !xmax_ok || !ymax_ok)
            continue;

        const auto & xmin_range = xmin_it->second;
        const auto & xmax_range = xmax_it->second;
        const auto & ymin_range = ymin_it->second;
        const auto & ymax_range = ymax_it->second;

        if (xmin_range.left.getType() != Field::Types::Float64
            || xmax_range.right.getType() != Field::Types::Float64
            || ymin_range.left.getType() != Field::Types::Float64
            || ymax_range.right.getType() != Field::Types::Float64)
            continue;

        /// Global file bbox:
        ///   file_xmin = min(xmin_col) = xmin_range.left
        ///   file_xmax = max(xmax_col) = xmax_range.right
        ///   file_ymin = min(ymin_col) = ymin_range.left
        ///   file_ymax = max(ymax_col) = ymax_range.right
        double file_xmin = xmin_range.left.safeGet<double>();
        double file_xmax = xmax_range.right.safeGet<double>();
        double file_ymin = ymin_range.left.safeGet<double>();
        double file_ymax = ymax_range.right.safeGet<double>();

        bool disjoint = sp.query_xmax < file_xmin || sp.query_xmin > file_xmax
            || sp.query_ymax < file_ymin || sp.query_ymin > file_ymax;
        if (disjoint)
            return PruningReturnStatus::MIN_MAX_INDEX_PRUNED;
    }

    return PruningReturnStatus::NOT_PRUNED;
}
}

#endif
