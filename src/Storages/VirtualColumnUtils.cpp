#include <memory>
#include <stack>

#include <Storages/VirtualColumnUtils.h>

#include <Core/NamesAndTypes.h>
#include <Core/TypeId.h>

#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/misc.h>
#include <Interpreters/Set.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/FilterDescription.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeDateTime.h>

#include <Processors/Port.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>

#include <Columns/ColumnSet.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/FormatFactory.h>
#include <Formats/SchemaInferenceUtils.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/indexHint.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/HivePartitioningUtils.h>


namespace DB
{

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
    return {{"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
            {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
            {"_size", makeNullable(std::make_shared<DataTypeUInt64>())},
            {"_time", makeNullable(std::make_shared<DataTypeDateTime>())},
            {"_etag", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
            {"_data_lake_snapshot_version", makeNullable(std::make_shared<DataTypeUInt64>())},
            {"_row_number", makeNullable(std::make_shared<DataTypeInt64>())}
    };
}

NameSet getVirtualNamesForFileLikeStorage()
{
    return getCommonVirtualsForFileLikeStorage().getNameSet();
}

VirtualColumnsDescription getVirtualsForFileLikeStorage(ColumnsDescription & storage_columns)
{
    VirtualColumnsDescription desc;

    auto add_virtual = [&](const NameAndTypePair & pair)
    {
        const auto & name = pair.getNameInStorage();
        const auto & type = pair.getTypeInStorage();
        if (storage_columns.has(name))
        {
            return;
        }

        desc.addEphemeral(name, type, "");
    };

    for (const auto & item : getCommonVirtualsForFileLikeStorage())
        add_virtual(item);

    return desc;
}

static void addPathAndFileToVirtualColumns(Block & block, const String & path, size_t idx, const FormatSettings & format_settings, bool parse_hive_columns)
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

std::optional<ActionsDAG> createPathAndFileFilterDAG(const ActionsDAG::Node * predicate, const NamesAndTypesList & virtual_columns, const NamesAndTypesList & hive_columns)
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
    return splitFilterDagForAllowedInputs(predicate, &block);
}

/// Helper to check if a node is a call to splitByChar on _path
static const ActionsDAG::Node * findSplitByCharOnPath(const ActionsDAG::Node * node)
{
    if (!node || node->type != ActionsDAG::ActionType::FUNCTION)
        return nullptr;

    if (!node->function_base || node->function_base->getName() != "splitByChar")
        return nullptr;

    // splitByChar has 2 children: delimiter and string
    if (node->children.size() != 2)
        return nullptr;

    // Check if the second argument is _path
    const auto * path_arg = node->children[1];
    if (path_arg->type == ActionsDAG::ActionType::INPUT && path_arg->result_name == "_path")
        return node;

    return nullptr;
}

/// Helper to unwrap materialize() function calls
static const ActionsDAG::Node * unwrapMaterialize(const ActionsDAG::Node * node)
{
    if (!node || node->type != ActionsDAG::ActionType::FUNCTION)
        return node;
    if (node->function_base && node->function_base->getName() == "materialize" && node->children.size() == 1)
        return node->children[0];
    return node;
}

/// Helper to extract array element index from arrayElement function
static std::optional<size_t> extractArrayElementIndex(const ActionsDAG::Node * node)
{
    if (!node || node->type != ActionsDAG::ActionType::FUNCTION)
        return std::nullopt;

    if (!node->function_base || node->function_base->getName() != "arrayElement")
        return std::nullopt;

    // arrayElement has 2 children: array and index
    if (node->children.size() != 2)
        return std::nullopt;

    // Extract the index from the second child (should be a constant)
    const auto * index_node = node->children[1];
    if (index_node->type != ActionsDAG::ActionType::COLUMN || !index_node->column)
        return std::nullopt;

    // Get the index value (1-based in ClickHouse)
    if (index_node->column->size() != 1)
        return std::nullopt;

    Field index_field = (*index_node->column)[0];
    UInt64 index = index_field.safeGet<UInt64>();

    // Convert to 0-based index
    return index > 0 ? std::optional<size_t>(index - 1) : std::nullopt;
}

/// Helper to extract string values from equals/in conditions
static std::vector<String> extractFilterValues(const ActionsDAG::Node * node, const ActionsDAG::Node * target_node)
{
    std::vector<String> values;

    if (!node || node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base)
        return values;

    const auto & func_name = node->function_base->getName();

    // Handle equals: arrayElement(...) = 'value'
    if (func_name == "equals" && node->children.size() == 2)
    {
        const ActionsDAG::Node * value_node = nullptr;

        // Find which child is the target and which is the constant
        if (node->children[0] == target_node && node->children[1]->type == ActionsDAG::ActionType::COLUMN)
            value_node = node->children[1];
        else if (node->children[1] == target_node && node->children[0]->type == ActionsDAG::ActionType::COLUMN)
            value_node = node->children[0];

        if (value_node && value_node->column && value_node->column->size() == 1)
        {
            Field field = (*value_node->column)[0];
            if (field.getType() == Field::Types::String)
                values.push_back(field.safeGet<String>());
        }
    }
    // Handle IN: arrayElement(...) IN ('value1', 'value2', ...)
    else if (func_name == "in" && node->children.size() == 2)
    {
        if (node->children[0] != target_node)
            return values;

        const auto * set_node = node->children[1];
        if (set_node->type == ActionsDAG::ActionType::COLUMN && set_node->column)
        {
            const auto * column_set = checkAndGetColumn<const ColumnSet>(set_node->column.get());
            if (column_set)
            {
                auto future_set = column_set->getData();
                if (future_set)
                {
                    auto set_ptr = future_set->get();
                    if (set_ptr && set_ptr->hasSetElements())
                    {
                        // Extract values from the set
                        const auto set_elements = set_ptr->getSetElements();
                        if (!set_elements.empty())
                        {
                            const auto & column = set_elements[0];
                            for (size_t i = 0; i < column->size(); ++i)
                            {
                                Field field = (*column)[i];
                                if (field.getType() == Field::Types::String)
                                    values.push_back(field.safeGet<String>());
                            }
                        }
                    }
                }
            }
        }
    }

    return values;
}

std::vector<PathComponentFilter> extractPathComponentFilters(const ActionsDAG::Node * predicate)
{
    std::vector<PathComponentFilter> filters;

    if (!predicate)
        return filters;

    // Map to store filters by component index
    std::map<size_t, std::vector<String>> filters_map;

    // Traverse the DAG to find patterns like: arrayElement(splitByChar('/', _path), N) = 'value'
    std::function<void(const ActionsDAG::Node *)> traverse = [&](const ActionsDAG::Node * node)
    {
        if (!node)
            return;

        // Look for arrayElement calls
        if (node->type == ActionsDAG::ActionType::FUNCTION &&
            node->function_base &&
            node->function_base->getName() == "arrayElement" &&
            node->children.size() == 2)
        {
            // Check if first child is splitByChar on _path
            const auto * split_node = findSplitByCharOnPath(node->children[0]);
            if (split_node)
            {
                // Extract the array index
                auto index_opt = extractArrayElementIndex(node);
                if (index_opt.has_value())
                {
                    // This node represents: splitByChar('/', _path)[N]
                    // Now look for parent nodes that filter this value
                    // We need to search the entire DAG for equals/in conditions on this node
                    // For now, store the node and continue traversal
                }
            }
        }

        // Look for comparison functions that might filter arrayElement results
        if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base)
        {
            const auto & func_name = node->function_base->getName();

            if ((func_name == "equals" || func_name == "in") && node->children.size() >= 1)
            {
                // Check if any child is an arrayElement on splitByChar(_path)
                // Need to unwrap materialize() calls that ClickHouse adds
                for (const auto * child : node->children)
                {
                    // Unwrap materialize() if present
                    const auto * unwrapped_child = unwrapMaterialize(child);

                    if (unwrapped_child->type == ActionsDAG::ActionType::FUNCTION &&
                        unwrapped_child->function_base &&
                        unwrapped_child->function_base->getName() == "arrayElement" &&
                        unwrapped_child->children.size() == 2)
                    {
                        const auto * split_node = findSplitByCharOnPath(unwrapped_child->children[0]);
                        if (split_node)
                        {
                            auto index_opt = extractArrayElementIndex(unwrapped_child);
                            if (index_opt.has_value())
                            {
                                // Pass the original child (with materialize) to extractFilterValues
                                // since it needs to match against the node's children
                                auto values = extractFilterValues(node, child);
                                if (!values.empty())
                                {
                                    auto & filter_values = filters_map[index_opt.value()];
                                    filter_values.insert(filter_values.end(), values.begin(), values.end());
                                }
                            }
                        }
                    }
                }
            }
        }

        // Recursively traverse children
        for (const auto * child : node->children)
            traverse(child);
    };

    traverse(predicate);

    // Convert map to vector
    for (const auto & [index, values] : filters_map)
    {
        if (!values.empty())
        {
            PathComponentFilter filter;
            filter.component_index = index;
            filter.allowed_values = values;
            filters.push_back(filter);
        }
    }

    return filters;
}


std::vector<std::string> expandGlobWithPrefixFilters(
    const std::string & pattern,
    const std::vector<PathComponentFilter> & filters)
{
    if (filters.empty())
        return {pattern};

    // Create a map for quick lookup
    std::map<size_t, std::vector<String>> filters_map;
    for (const auto & filter : filters)
        filters_map[filter.component_index] = filter.allowed_values;

    // Split pattern by '/'
    std::vector<std::string> components;
    size_t start = 0;
    size_t end = pattern.find('/');

    while (end != std::string::npos)
    {
        if (end > start)
            components.push_back(pattern.substr(start, end - start));
        else if (end == start)
            components.push_back("");  // Leading slash case
        start = end + 1;
        end = pattern.find('/', start);
    }
    if (start < pattern.size())
        components.push_back(pattern.substr(start));

    // Debug: log the split components
    LOG_DEBUG(&Poco::Logger::get("VirtualColumnUtils"), "Pattern '{}' split into {} components:", pattern, components.size());
    for (size_t i = 0; i < components.size(); ++i)
    {
        LOG_DEBUG(&Poco::Logger::get("VirtualColumnUtils"), "  Component[{}]: '{}'", i, components[i]);
    }

    // Find which components to expand
    std::vector<std::vector<std::string>> expanded_components(components.size());
    for (size_t i = 0; i < components.size(); ++i)
    {
        const auto & component = components[i];

        // Check if this component is a wildcard and we have filters for it
        bool is_wildcard = (component == "*" || component == "**" ||
                           component.find('*') != std::string::npos ||
                           component.find('?') != std::string::npos ||
                           component.find('{') != std::string::npos);

        if (is_wildcard && filters_map.count(i))
        {
            // Replace with filter values
            expanded_components[i] = filters_map[i];
        }
        else
        {
            // Keep as is
            expanded_components[i] = {component};
        }
    }

    // Generate Cartesian product
    std::vector<std::string> result;
    std::function<void(size_t, std::string)> generate = [&](size_t index, std::string current)
    {
        if (index == expanded_components.size())
        {
            result.push_back(current);
            return;
        }

        for (const auto & value : expanded_components[index])
        {
            std::string next = current;
            if (!next.empty() && !next.ends_with('/'))
                next += '/';
            next += value;
            generate(index + 1, next);
        }
    };

    generate(0, "");

    return result.empty() ? std::vector<std::string>{pattern} : result;
}

ColumnPtr getFilterByPathAndFileIndexes(const std::vector<String> & paths, const ExpressionActionsPtr & actions, const NamesAndTypesList & virtual_columns, const NamesAndTypesList & hive_columns, const ContextPtr & context)
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
        addPathAndFileToVirtualColumns(block, paths[i], i, getFormatSettings(context), /* parse_hive_columns */ !hive_columns.empty());

    filterBlockWithExpression(actions, block);

    return block.getByName("_idx").column;
}

void addRequestedFileLikeStorageVirtualsToChunk(
    Chunk & chunk, const NamesAndTypesList & requested_virtual_columns,
    VirtualsForFileLikeStorage virtual_values, ContextPtr)
{
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
                auto null_map = ColumnUInt8::create(chunk.getNumRows(), 0);
                chunk.addColumn(ColumnNullable::create(std::move(column), std::move(null_map)));
                return;
            }
#endif
            /// Row numbers not known, _row_number = NULL.
            chunk.addColumn(virtual_column.type->createColumnConstWithDefaultValue(chunk.getNumRows())->convertToFullColumnIfConst());
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

    /// Special case: `in subquery or table` is non-deterministic
    if (node->type == ActionsDAG::ActionType::COLUMN)
    {
        if (const auto * column = typeid_cast<const ColumnSet *>(node->column.get()))
        {
            if (!column->getData()->isDeterministic())
            {
                return false;
            }
        }
    }

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
    const ActionsDAG::Node * node, const Block * allowed_inputs, ActionsDAG::Nodes & additional_nodes, bool allow_partial_result)
{
    if (node->type == ActionsDAG::ActionType::FUNCTION)
    {
        if (node->function_base->getName() == "and")
        {
            auto & node_copy = additional_nodes.emplace_back(*node);
            node_copy.children.clear();
            for (const auto * child : node->children)
                if (const auto * child_copy
                    = splitFilterNodeForAllowedInputs(child, allowed_inputs, additional_nodes, allow_partial_result))
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
                    res = &tmp_dag.addCast(*res, node->result_type, {});
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
                if (child = splitFilterNodeForAllowedInputs(child, allowed_inputs, additional_nodes, allow_partial_result); !child)
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
                            = splitFilterNodeForAllowedInputs(output, allowed_inputs, additional_nodes, allow_partial_result))
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
                            res = &index_hint_dag.addCast(*res, node->result_type, {});

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
splitFilterDagForAllowedInputs(const ActionsDAG::Node * predicate, const Block * allowed_inputs, bool allow_partial_result)
{
    if (!predicate)
        return {};

    ActionsDAG::Nodes additional_nodes;
    const auto * res = splitFilterNodeForAllowedInputs(predicate, allowed_inputs, additional_nodes, allow_partial_result);
    if (!res)
        return {};

    return ActionsDAG::cloneSubDAG({res}, true);
}

void filterBlockWithPredicate(
    const ActionsDAG::Node * predicate, Block & block, ContextPtr context, bool allow_filtering_with_partial_predicate)
{
    auto dag = splitFilterDagForAllowedInputs(predicate, &block, /*allow_partial_result=*/allow_filtering_with_partial_predicate);
    if (dag)
        filterBlockWithExpression(buildFilterExpression(std::move(*dag), context), block);
}

}

}
