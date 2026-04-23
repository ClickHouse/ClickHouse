#include <optional>
#include "config.h"

#if USE_AVRO

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsDateTime.h>
#include <Common/DateLUTImpl.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/logger_useful.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <IO/ReadHelpers.h>
#include <Common/quoteString.h>
#include <fmt/ranges.h>

#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

using namespace DB;

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

struct FunctionSubstitutionEntry
{
    String source_func;
    String target_func;
    std::function<bool(const DataTypePtr &)> accepts_const_type;
    std::function<const ActionsDAG::Node *(ActionsDAG &, const ActionsDAG::Node *, const ContextPtr &)> convert_const;
};

static std::vector<FunctionSubstitutionEntry> buildSubstitutionTable(const ContextPtr & context)
{
    std::vector<FunctionSubstitutionEntry> entries;

    auto apply_func_to_const = [&context](const String & func_name, ActionsDAG & dag, const ActionsDAG::Node * const_node) -> const ActionsDAG::Node *
    {
        auto builder = FunctionFactory::instance().tryGet(func_name, context);
        if (!builder)
            return nullptr;
        ColumnsWithTypeAndName arguments{{const_node->column, const_node->result_type, const_node->result_name}};
        auto func_base = builder->build(arguments);
        if (!func_base)
            return nullptr;
        return &dag.addFunction(func_base, {const_node}, const_node->result_name);
    };

    entries.push_back({"toDate", "toRelativeDayNum",
        [](const DataTypePtr & t) { return isDate(t); },
        [apply_func_to_const](ActionsDAG & dag, const ActionsDAG::Node * sibling, const ContextPtr &) -> const ActionsDAG::Node *
        {
            return apply_func_to_const("toRelativeDayNum", dag, sibling);
        }
    });

    entries.push_back({"toStartOfHour", "toRelativeHourNum",
        [](const DataTypePtr & t) { return isDateTime(t) || isDateTime64(t); },
        [apply_func_to_const](ActionsDAG & dag, const ActionsDAG::Node * sibling, const ContextPtr &) -> const ActionsDAG::Node *
        {
            return apply_func_to_const("toRelativeHourNum", dag, sibling);
        }
    });

    entries.push_back({"toYear", "toYearNumSinceEpoch",
        [](const DataTypePtr & t) { return isNumber(t); },
        [](ActionsDAG & dag, const ActionsDAG::Node * sibling, const ContextPtr &) -> const ActionsDAG::Node *
        {
            auto year_val = (*sibling->column)[0].safeGet<UInt64>();
            auto uint16_type = std::make_shared<DataTypeUInt16>();
            auto new_col = uint16_type->createColumnConst(1, static_cast<UInt64>(static_cast<UInt16>(year_val - 1970)));
            return &dag.addColumn({new_col, uint16_type, sibling->result_name});
        }
    });

    return entries;
}

static void substituteFilterFunctionsForPartitionKey(DB::ActionsDAG & dag, const DB::ContextPtr & context)
{
    auto substitutions = buildSubstitutionTable(context);

    std::unordered_map<String, size_t> source_to_idx;
    for (size_t i = 0; i < substitutions.size(); ++i)
        source_to_idx.emplace(substitutions[i].source_func, i);

    std::unordered_map<const ActionsDAG::Node *, size_t> substituted_nodes;

    for (auto & node : const_cast<ActionsDAG::Nodes &>(dag.getNodes()))
    {
        if (node.type != ActionsDAG::ActionType::FUNCTION
            || !node.function_base
            || node.children.size() != 1)
            continue;

        auto it = source_to_idx.find(node.function_base->getName());
        if (it == source_to_idx.end())
            continue;

        const auto * arg = node.children[0];
        if (arg->column && isColumnConst(*arg->column))
            continue;

        const auto & entry = substitutions[it->second];
        ColumnsWithTypeAndName arguments{{nullptr, arg->result_type, ""}};
        auto target_builder = FunctionFactory::instance().tryGet(entry.target_func, context);
        if (!target_builder)
            continue;
        auto new_function_base = target_builder->build(arguments);
        if (!new_function_base)
            continue;

        node.function_base = std::move(new_function_base);
        substituted_nodes.emplace(&node, it->second);
    }

    if (substituted_nodes.empty())
        return;

    for (auto & node : const_cast<ActionsDAG::Nodes &>(dag.getNodes()))
    {
        if (node.type != ActionsDAG::ActionType::FUNCTION || node.children.size() != 2)
            continue;

        for (size_t i = 0; i < 2; ++i)
        {
            auto sub_it = substituted_nodes.find(node.children[i]);
            if (sub_it == substituted_nodes.end())
                continue;

            size_t j = 1 - i;
            const auto * sibling = node.children[j];
            if (!sibling->column || !isColumnConst(*sibling->column))
                continue;

            const auto & entry = substitutions[sub_it->second];
            if (!entry.accepts_const_type(sibling->result_type))
                continue;

            const auto * new_node = entry.convert_const(dag, sibling, context);
            if (new_node)
                node.children[j] = new_node;
        }
    }
}

std::unique_ptr<DB::ActionsDAG> ManifestFilesPruner::transformFilterDagForManifest(
    const DB::ActionsDAG * source_dag, std::vector<Int32> & used_columns_in_filter, const DB::ContextPtr & context) const
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
    substituteFilterFunctionsForPartitionKey(*result, context);
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
    transformed_dag = transformFilterDagForManifest(filter_dag, used_columns_in_filter, context);
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

    return PruningReturnStatus::NOT_PRUNED;
}
}

#endif
