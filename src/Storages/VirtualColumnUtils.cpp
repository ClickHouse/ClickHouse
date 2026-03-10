#include <memory>
#include <stack>

#include <Storages/VirtualColumnUtils.h>

#include <Core/NamesAndTypes.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>


#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/FilterDescription.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeDateTime.h>

#include <Processors/Port.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <Columns/ColumnSet.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Formats/SchemaInferenceUtils.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/indexHint.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/HivePartitioningUtils.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/HashTable/HashSet.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool use_hive_partitioning;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace VirtualColumnUtils
{

void buildSetsForDagImpl(const ActionsDAG & dag, const ContextPtr & context, bool ordered)
{
    for (const auto & node : dag.getNodes())
    {
        if (node.type == ActionsDAG::ActionType::COLUMN)
        {
            const ColumnSet * column_set = checkAndGetColumnConstData<const ColumnSet>(node.column.get());
            if (!column_set)
                column_set = checkAndGetColumn<const ColumnSet>(node.column.get());

            if (column_set)
            {
                auto future_set = column_set->getData();
                if (!future_set->get())
                {
                    if (auto * set_from_subquery = typeid_cast<FutureSetFromSubquery *>(future_set.get()))
                    {
                        if (ordered)
                            set_from_subquery->buildOrderedSetInplace(context);
                        else
                            set_from_subquery->buildSetInplace(context);
                    }
                }
            }
        }
    }
}

void buildSetsForDAG(const ActionsDAG & dag, const ContextPtr & context)
{
    buildSetsForDagImpl(dag, context, /* ordered = */ false);
}

void buildOrderedSetsForDAG(const ActionsDAG & dag, const ContextPtr & context)
{
    buildSetsForDagImpl(dag, context, /* ordered = */ true);
}

ExpressionActionsPtr buildFilterExpression(ActionsDAG dag, ContextPtr context)
{
    buildSetsForDAG(dag, context);
    return std::make_shared<ExpressionActions>(std::move(dag));
}

void filterBlockWithExpression(const ExpressionActionsPtr & actions, Block & block)
{
    Block block_with_filter = block;
    actions->execute(block_with_filter, /*dry_run=*/ false, /*allow_duplicates_in_input=*/ true);

    /// Filter the block.
    String filter_column_name = actions->getActionsDAG().getOutputs().at(0)->result_name;
    ColumnPtr filter_column = block_with_filter.getByName(filter_column_name).column->convertToFullColumnIfConst();

    ConstantFilterDescription constant_filter(*filter_column);

    if (constant_filter.always_true)
    {
        return;
    }

    if (constant_filter.always_false)
    {
        block = block.cloneEmpty();
        return;
    }

    FilterDescription filter(*filter_column);

    for (size_t i = 0; i < block.columns(); ++i)
    {
        ColumnPtr & column = block.safeGetByPosition(i).column;
        column = column->filter(*filter.data, -1);
    }
}

static NamesAndTypesList getCommonVirtualsForFileLikeStorage()
{
    return {
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_size", makeNullable(std::make_shared<DataTypeUInt64>())},
        {"_time", makeNullable(std::make_shared<DataTypeDateTime>())},
        {"_etag", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_tags", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())},
        {"_data_lake_snapshot_version", makeNullable(std::make_shared<DataTypeUInt64>())},
        {"_row_number", makeNullable(std::make_shared<DataTypeInt64>())},
    };
}

NameSet getVirtualNamesForFileLikeStorage()
{
    return getCommonVirtualsForFileLikeStorage().getNameSet();
}

std::string_view findHivePartitioningInPath(const String & path)
{
    auto key_values = HivePartitioningUtils::parseHivePartitioningKeysAndValues(path);

    if (key_values.empty())
        return std::string_view();

    // All keys and values are string_view over 'path', so starts and ends must be inside 'path'
    auto kv = key_values.begin();
    const auto * start = kv->first.data();
    const auto * end = kv->second.data() + kv->second.size();
    ++kv;
    while (kv != key_values.end())
    {
        start = std::min(kv->first.data(), start);
        end = std::max(kv->second.data() + kv->second.size(), end);
        ++kv;
    }

    if (start < path.data() || start > path.data() + path.size()
            || end < path.data() || end > path.data() + path.size()
            || end < start)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "String views are not inside initial string");
    }

    return std::string_view(start, end - start);
}

VirtualColumnsDescription getVirtualsForFileLikeStorage(
    ColumnsDescription & storage_columns,
    ContextPtr context,
    const std::optional<FormatSettings> & format_settings,
    std::optional<PartitionStrategyFactory::StrategyType> partition_strategy,
    const std::string & path)
{
    VirtualColumnsDescription desc;

    auto add_virtual = [&](const NameAndTypePair & pair)
    {
        const auto & name = pair.getNameInStorage();
        if (storage_columns.has(name))
            return;

        const auto & type = pair.getTypeInStorage();
        desc.addEphemeral(name, type, "");
    };

    for (const auto & item : getCommonVirtualsForFileLikeStorage())
        add_virtual(item);

    if (!path.empty())
    {
        if (!partition_strategy.has_value())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected partition strategy to be specified");

        /// If partition_stategy == none, we add hive columns, if present, to virtual columns.
        if (context->getSettingsRef()[Setting::use_hive_partitioning]
            && partition_strategy == PartitionStrategyFactory::StrategyType::NONE)
        {
            auto hive_columns = HivePartitioningUtils::extractHivePartitionColumnsFromPath(storage_columns, path, format_settings, context);
            for (const auto & column : hive_columns)
                add_virtual(column);
        }
    }

    return desc;
}

static void addPathAndFileToVirtualColumns(
    Block & block,
    const String & path,
    size_t idx,
    const FormatSettings & format_settings,
    bool parse_hive_columns)
{
    if (block.has("_path"))
        block.getByName("_path").column->assumeMutableRef().insert(path);

    if (block.has("_file"))
    {
        auto pos = path.find_last_of('/');
        String file;
        if (pos != std::string::npos)
            file = path.substr(pos + 1);
        else
            file = path;

        block.getByName("_file").column->assumeMutableRef().insert(file);
    }

    if (parse_hive_columns)
    {
        const auto keys_and_values = HivePartitioningUtils::parseHivePartitioningKeysAndValues(path);
        for (const auto & [key, value] : keys_and_values)
        {
            if (const auto * column = block.findByName(key))
            {
                ReadBufferFromString buf(value);
                column->type->getDefaultSerialization()->deserializeWholeText(column->column->assumeMutableRef(), buf, format_settings);
            }
        }
    }

    block.getByName("_idx").column->assumeMutableRef().insert(idx);
}

std::optional<ActionsDAG> createPathAndFileFilterDAG(
    const ActionsDAG::Node * predicate,
    const NamesAndTypesList & virtual_columns,
    const ContextPtr & context,
    const NamesAndTypesList & hive_columns)
{
    if (!predicate || virtual_columns.empty())
        return {};

    Block block;
    NameSet common_virtuals = getVirtualNamesForFileLikeStorage();
    for (const auto & column : virtual_columns)
    {
        if (column.name == "_file" || column.name == "_path" || !common_virtuals.contains(column.name))
            block.insert({column.type->createColumn(), column.type, column.name});
    }

    for (const auto & column : hive_columns)
    {
        block.insert({column.type->createColumn(), column.type, column.name});
    }

    block.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "_idx"});
    return splitFilterDagForAllowedInputs(predicate, &block, context);
}

ColumnPtr getFilterByPathAndFileIndexes(
    const std::vector<String> & paths,
    const ExpressionActionsPtr & actions,
    const NamesAndTypesList & virtual_columns,
    const NamesAndTypesList & hive_columns,
    const ContextPtr & context)
{
    Block block;
    NameSet common_virtuals = getVirtualNamesForFileLikeStorage();
    for (const auto & column : virtual_columns)
    {
        if (column.name == "_file" || column.name == "_path" || !common_virtuals.contains(column.name))
            block.insert({column.type->createColumn(), column.type, column.name});
    }

    for (const auto & column : hive_columns)
    {
        block.insert({column.type->createColumn(), column.type, column.name});
    }

    block.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "_idx"});

    for (size_t i = 0; i != paths.size(); ++i)
    {
        addPathAndFileToVirtualColumns(
            block,
            paths[i],
            /* idx */i,
            getFormatSettings(context),
            /* parse_hive_columns */context->getSettingsRef()[Setting::use_hive_partitioning] || !hive_columns.empty());
    }

    filterBlockWithExpression(actions, block);

    return block.getByName("_idx").column;
}

void addRequestedFileLikeStorageVirtualsToChunk(
    Chunk & chunk,
    const NamesAndTypesList & requested_virtual_columns,
    VirtualsForFileLikeStorage virtual_values,
    ContextPtr context)
{
    HivePartitioningUtils::HivePartitioningKeysAndValues hive_map;
    if (context->getSettingsRef()[Setting::use_hive_partitioning])
        hive_map = HivePartitioningUtils::parseHivePartitioningKeysAndValues(virtual_values.path);

    for (const auto & virtual_column : requested_virtual_columns)
    {
        if (virtual_column.name == "_path")
        {
            chunk.addColumn(virtual_column.type->createColumnConst(chunk.getNumRows(), virtual_values.path)->convertToFullColumnIfConst());
        }
        else if (virtual_column.name == "_file")
        {
            if (virtual_values.filename)
            {
                chunk.addColumn(virtual_column.type->createColumnConst(chunk.getNumRows(), (*virtual_values.filename))->convertToFullColumnIfConst());
            }
            else
            {
                size_t last_slash_pos = virtual_values.path.find_last_of('/');
                auto filename_from_path = virtual_values.path.substr(last_slash_pos + 1);
                chunk.addColumn(virtual_column.type->createColumnConst(chunk.getNumRows(), filename_from_path)->convertToFullColumnIfConst());
            }
        }
        else if (virtual_column.name == "_size")
        {
            if (virtual_values.size)
                chunk.addColumn(virtual_column.type->createColumnConst(chunk.getNumRows(), *virtual_values.size)->convertToFullColumnIfConst());
            else
                chunk.addColumn(virtual_column.type->createColumnConstWithDefaultValue(chunk.getNumRows())->convertToFullColumnIfConst());
        }
        else if (virtual_column.name == "_time")
        {
            if (virtual_values.last_modified)
                chunk.addColumn(virtual_column.type->createColumnConst(chunk.getNumRows(), virtual_values.last_modified->epochTime())->convertToFullColumnIfConst());
            else
                chunk.addColumn(virtual_column.type->createColumnConstWithDefaultValue(chunk.getNumRows())->convertToFullColumnIfConst());
        }
        else if (virtual_column.name == "_etag")
        {
            if (virtual_values.etag)
                chunk.addColumn(virtual_column.type->createColumnConst(chunk.getNumRows(), (*virtual_values.etag))->convertToFullColumnIfConst());
            else
                chunk.addColumn(virtual_column.type->createColumnConstWithDefaultValue(chunk.getNumRows())->convertToFullColumnIfConst());
        }
        else if (virtual_column.name == "_tags")
        {
            if (virtual_values.tags)
            {
                Map map_value;
                for (const auto & [key, value] : *virtual_values.tags)
                    map_value.push_back(Field(Tuple{Field(key), Field(value)}));

                chunk.addColumn(virtual_column.type->createColumnConst(chunk.getNumRows(), map_value)->convertToFullColumnIfConst());
            }
            else
            {
                chunk.addColumn(virtual_column.type->createColumnConstWithDefaultValue(chunk.getNumRows())->convertToFullColumnIfConst());
            }
        }
        else if (virtual_column.name == "_data_lake_snapshot_version")
        {
            if (virtual_values.data_lake_snapshot_version)
                chunk.addColumn(virtual_column.type->createColumnConst(chunk.getNumRows(), *virtual_values.data_lake_snapshot_version)->convertToFullColumnIfConst());
            else
                chunk.addColumn(virtual_column.type->createColumnConstWithDefaultValue(chunk.getNumRows())->convertToFullColumnIfConst());
        }
        else if (virtual_column.name == "_row_number")
        {
#if USE_PARQUET
            auto chunk_info = chunk.getChunkInfos().get<ChunkInfoRowNumbers>();
            if (chunk_info)
            {
                size_t row_num_offset = chunk_info->row_num_offset;
                const auto & applied_filter = chunk_info->applied_filter;
                size_t num_indices = applied_filter.has_value() ? applied_filter->size() : chunk.getNumRows();
                auto column = ColumnInt64::create();
                for (size_t i = 0; i < num_indices; ++i)
                    if (!applied_filter.has_value() || applied_filter.value()[i])
                        column->insertValue(i + row_num_offset);
                auto null_map = ColumnUInt8::create(chunk.getNumRows(), static_cast<UInt8>(0));
                chunk.addColumn(ColumnNullable::create(std::move(column), std::move(null_map)));
                return;
            }
#endif
            /// Row numbers not known, _row_number = NULL.
            chunk.addColumn(virtual_column.type->createColumnConstWithDefaultValue(chunk.getNumRows())->convertToFullColumnIfConst());
        }
        else if (auto it = hive_map.find(virtual_column.getNameInStorage()); it != hive_map.end())
        {
            chunk.addColumn(
                virtual_column.type->createColumnConst(
                    chunk.getNumRows(),
                    convertFieldToType(Field(it->second), *virtual_column.type))->convertToFullColumnIfConst());
        }
    }
}

static bool canEvaluateSubtree(const ActionsDAG::Node * node, const Block * allowed_inputs)
{
    std::stack<const ActionsDAG::Node *> nodes;
    nodes.push(node);
    while (!nodes.empty())
    {
        const auto * cur = nodes.top();
        nodes.pop();

        if (cur->type == ActionsDAG::ActionType::ARRAY_JOIN)
            return false;

        if (cur->type == ActionsDAG::ActionType::INPUT && allowed_inputs && !allowed_inputs->has(cur->result_name))
            return false;

        for (const auto * child : cur->children)
            nodes.push(child);
    }

    return true;
}

bool isDeterministic(const ActionsDAG::Node * node)
{
    for (const auto * child : node->children)
    {
        if (!isDeterministic(child))
            return false;
    }

    if (node->type == ActionsDAG::ActionType::COLUMN)
        return node->isDeterministic();

    if (node->type != ActionsDAG::ActionType::FUNCTION)
        return true;

    if (!node->function_base->isDeterministic())
        return false;

    return true;
}

bool isDeterministicInScopeOfQuery(const ActionsDAG::Node * node)
{
    for (const auto * child : node->children)
    {
        if (!isDeterministicInScopeOfQuery(child))
            return false;
    }

    if (node->type != ActionsDAG::ActionType::FUNCTION)
        return true;

    if (!node->function_base->isDeterministicInScopeOfQuery())
        return false;

    return true;
}

static const ActionsDAG::Node * splitFilterNodeForAllowedInputs(
    const ActionsDAG::Node * node, const Block * allowed_inputs, ActionsDAG::Nodes & additional_nodes, const ContextPtr & context, bool allow_partial_result)
{
    if (node->type == ActionsDAG::ActionType::FUNCTION)
    {
        if (node->function_base->getName() == "and")
        {
            auto & node_copy = additional_nodes.emplace_back(*node);
            node_copy.children.clear();
            for (const auto * child : node->children)
                if (const auto * child_copy
                    = splitFilterNodeForAllowedInputs(child, allowed_inputs, additional_nodes, context, allow_partial_result))
                    node_copy.children.push_back(child_copy);
                /// Expression like (now_allowed AND allowed) is not allowed if allow_partial_result = true. This is important for
                /// trivial count optimization, otherwise we can get incorrect results. For example, if the query is
                /// SELECT count() FROM table WHERE _partition_id = '0' AND rowNumberInBlock() = 1, we cannot apply
                /// trivial count.
                else if (!allow_partial_result)
                    return nullptr;

            if (node_copy.children.empty())
                return nullptr;

            if (node_copy.children.size() == 1)
            {
                const ActionsDAG::Node * res = node_copy.children.front();
                /// Expression like (not_allowed AND 256) can't be reduced to (and(256)) because AND requires
                /// at least two arguments; also it can't be reduced to (256) because result type is different.
                if (!res->result_type->equals(*node->result_type))
                {
                    ActionsDAG tmp_dag;
                    res = &tmp_dag.addCast(*res, node->result_type, {}, context);
                    additional_nodes.splice(additional_nodes.end(), ActionsDAG::detachNodes(std::move(tmp_dag)));
                }

                return res;
            }

            return &node_copy;
        }
        if (node->function_base->getName() == "or")
        {
            auto & node_copy = additional_nodes.emplace_back(*node);
            for (auto & child : node_copy.children)
                if (child = splitFilterNodeForAllowedInputs(child, allowed_inputs, additional_nodes, context, allow_partial_result); !child)
                    return nullptr;

            return &node_copy;
        }
        if (node->function_base->getName() == "indexHint")
        {
            if (const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(node->function_base.get()))
            {
                if (const auto * index_hint = typeid_cast<const FunctionIndexHint *>(adaptor->getFunction().get()))
                {
                    auto index_hint_dag = index_hint->getActions().clone();
                    ActionsDAG::NodeRawConstPtrs atoms;
                    for (const auto & output : index_hint_dag.getOutputs())
                        if (const auto * child_copy
                            = splitFilterNodeForAllowedInputs(output, allowed_inputs, additional_nodes, context, allow_partial_result))
                            atoms.push_back(child_copy);

                    if (!atoms.empty())
                    {
                        const auto * res = atoms.at(0);

                        if (atoms.size() > 1)
                        {
                            FunctionOverloadResolverPtr func_builder_and
                                = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
                            res = &index_hint_dag.addFunction(func_builder_and, atoms, {});
                        }

                        if (!res->result_type->equals(*node->result_type))
                            res = &index_hint_dag.addCast(*res, node->result_type, {}, context);

                        additional_nodes.splice(additional_nodes.end(), ActionsDAG::detachNodes(std::move(index_hint_dag)));
                        return res;
                    }
                }
            }
        }
        else if (!isDeterministicInScopeOfQuery(node))
        {
            return nullptr;
        }
    }

    if (!canEvaluateSubtree(node, allowed_inputs))
        return nullptr;

    return node;
}

std::optional<ActionsDAG>
splitFilterDagForAllowedInputs(const ActionsDAG::Node * predicate, const Block * allowed_inputs, const ContextPtr & context, bool allow_partial_result)
{
    if (!predicate)
        return {};

    ActionsDAG::Nodes additional_nodes;
    const auto * res = splitFilterNodeForAllowedInputs(predicate, allowed_inputs, additional_nodes, context, allow_partial_result);
    if (!res)
        return {};

    return ActionsDAG::cloneSubDAG({res}, true);
}

void filterBlockWithPredicate(
    const ActionsDAG::Node * predicate, Block & block, ContextPtr context, bool allow_filtering_with_partial_predicate)
{
    auto dag = splitFilterDagForAllowedInputs(predicate, &block, context, /*allow_partial_result=*/allow_filtering_with_partial_predicate);
    if (dag)
        filterBlockWithExpression(buildFilterExpression(std::move(*dag), context), block);
}

std::optional<Strings> extractPathValuesFromFilter(const ActionsDAG * filter_dag, ContextPtr context, size_t limit)
{
    if (!filter_dag)
        return {};
    if (filter_dag->getOutputs().size() != 1)
        return {};

    const ActionsDAG::Node * path_node = nullptr;
    for (const auto * input : filter_dag->getInputs())
    {
        if (input->result_name == "_path")
        {
            path_node = input;
            break;
        }
    }
    if (!path_node)
        return {};

    auto variants = evaluateExpressionOverConstantCondition(filter_dag->getOutputs().at(0), {path_node}, context, limit);

    if (!variants)
        return {};

    Strings result;
    for (const auto & block : variants.value())
    {
        // Check for unexpected number of columns in block, or absent column
        if (block.size() != 1 || !block.at(0).column)
            return {};

        // Check for unexpected column data type
        if (!recursiveRemoveLowCardinality(block.at(0).type)->equals(DataTypeString()))
            return {};

        const auto & column = block.at(0).column;
        for (size_t i = 0; i < column->size(); ++i)
            result.push_back((*column)[i].safeGet<String>());
    }

    return result;
}

DataPartsVector filterDataPartsWithExpression(
    const DataPartsVector & data_parts,
    const std::shared_ptr<ExpressionActions> & virtual_columns_filter)
{
    if (!virtual_columns_filter)
        return data_parts;

    auto all_part_names = ColumnString::create();
    for (const auto & part : data_parts)
        all_part_names->insert(part->name);

    Block filtered_block{{std::move(all_part_names), std::make_shared<DataTypeString>(), "part_name"}};
    filterBlockWithExpression(virtual_columns_filter, filtered_block);

    if (!filtered_block.rows())
        return {};

    auto part_names = filtered_block.getByPosition(0).column;
    const auto & part_names_str = assert_cast<const ColumnString &>(*part_names);

    HashSet<std::string_view> part_names_set;
    for (size_t i = 0; i < part_names_str.size(); ++i)
        part_names_set.insert(part_names_str.getDataAt(i));

    DataPartsVector filtered_parts;
    for (const auto & part : data_parts)
        if (part_names_set.has(part->name))
            filtered_parts.push_back(part);

    return filtered_parts;
}

}

}
