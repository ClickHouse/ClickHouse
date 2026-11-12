#include <Storages/MergeTree/MergeTreeIndexAnalyzerNames.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/TableNode.h>
#include <Core/LogsLevel.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IndicesDescription.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeIndexMinMax.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/StorageDummy.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Columns/ColumnConst.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorHash.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>

#include <fmt/ranges.h>

#include <algorithm>
#include <unordered_map>

namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_functions_to_subcolumns;
}

namespace
{

/// The analyzer folds constant subexpressions with the settings of the query, which can give them a
/// different value than the stored key has (e.g. `least_greatest_legacy_null_behavior` changes the
/// value of `greatest(NULL, 1)`), and the rewrite passes then build on that value (e.g. an `if` with
/// a constant condition collapses to one of its branches). Then the rewritten expression is not
/// another name of the stored key, but a different expression. Check that every folded constant has
/// the same value as the subexpression of the same name in the stored key expression, which was folded
/// when the key was built. A constant that cannot be found there is not proven equal either, unless it
/// is a value-preserving conversion introduced by a rewrite pass, see `isEnumCastIntroducedByRewrite`.
bool foldedConstantMatchesKeyExpression(const ConstantNode & constant_node, const ActionsDAG & key_dag);

/// `IfTransformStringsToEnumPass` replaces the string constants of `if` and `transform` with folded
/// `_CAST(<string constant>, 'Enum...')`, where the enum is built from these very strings, and casts
/// the result back to `String`. Such a constant is not in the stored key, but the conversion of a
/// string to an enum does not depend on the settings, so it has a proven value if its argument has.
bool isEnumCastIntroducedByRewrite(const ConstantNode & constant_node, const ActionsDAG & key_dag)
{
    const auto * function = constant_node.getSourceExpression()->as<FunctionNode>();
    if (!function || function->getFunctionName() != "_CAST")
        return false;

    const auto & arguments = function->getArguments().getNodes();
    if (arguments.size() != 2)
        return false;

    const auto * argument = arguments[0]->as<ConstantNode>();
    const auto * type_name = arguments[1]->as<ConstantNode>();
    if (!argument || !type_name || type_name->hasSourceExpression())
        return false;

    auto unwrap_array = [](const DataTypePtr & type) -> DataTypePtr
    {
        if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
            return array_type->getNestedType();
        return nullptr;
    };

    DataTypePtr result_type = constant_node.getResultType();
    DataTypePtr argument_type = argument->getResultType();
    if (auto nested_result_type = unwrap_array(result_type))
    {
        result_type = nested_result_type;
        argument_type = unwrap_array(argument_type);
        if (!argument_type)
            return false;
    }

    if (!isEnum(result_type) || !isString(argument_type))
        return false;

    return !argument->hasSourceExpression() || foldedConstantMatchesKeyExpression(*argument, key_dag);
}

bool foldedConstantMatchesKeyExpression(const ConstantNode & constant_node, const ActionsDAG & key_dag)
{
    const String name = constant_node.getSourceExpression()->toAST()->getColumnName();
    const ActionsDAG::Node * key_node = nullptr;
    for (const auto & candidate : key_dag.getNodes())
    {
        if (candidate.result_name == name)
        {
            key_node = &candidate;
            break;
        }
    }

    if (!key_node)
        return isEnumCastIntroducedByRewrite(constant_node, key_dag);

    return key_node->column && isColumnConst(*key_node->column)
        && key_node->result_type->equals(*constant_node.getResultType())
        && (*key_node->column)[0] == constant_node.getValue();
}

bool foldedConstantsMatchKeyExpression(const QueryTreeNodePtr & node, const ActionsDAG & key_dag)
{
    if (const auto * constant_node = node->as<ConstantNode>(); constant_node && constant_node->hasSourceExpression()
        && !foldedConstantMatchesKeyExpression(*constant_node, key_dag))
        return false;

    for (const auto & child : node->getChildren())
        if (child && !foldedConstantsMatchKeyExpression(child, key_dag))
            return false;

    return true;
}

/// Reproduce the rewrites of the query analyzer (the query tree passes) on the key expressions.
/// Returns nullopt if the rewritten expressions are not proven to compute the stored key.
/// `columns_without_subcolumns` are the source columns whose functions the query does not rewrite to
/// subcolumn reads, see `getAlternativeExpression`.
/// If `in_filter`, the key expressions are placed in the `WHERE` clause of the dummy query rather than
/// in its projection: some rewrites are applied only in a filter (e.g. `FunctionToSubcolumnsPass`
/// rewrites `m['k']` to `m.key_k` in `WHERE` even if the whole `m` is used as well, as in
/// `m['k'] + length(toString(m))`).
std::optional<ActionsDAG> buildRewrittenExpressionWithAnalyzer(
    const ASTPtr & expression_list_ast,
    const ExpressionActionsPtr & expression,
    const Names & columns_without_subcolumns,
    bool in_filter,
    const std::function<void(StorageInMemoryMetadata &)> & register_key,
    const ContextMutablePtr & context)
{
    /// Resolve the key expressions against a dummy storage with the key's source columns,
    /// running the same query tree passes (with the same settings) the query itself goes through.
    auto source_columns = expression->getRequiredColumnsWithTypes();
    auto storage = std::make_shared<StorageDummy>(StorageID{"dummy", "dummy"}, ColumnsDescription(source_columns));
    /// `FunctionToSubcolumnsPass` rewrites `m['k']` to the subcolumn read `m.key_k` only for a storage
    /// with bucketed map serialization. Allow the rewrite here regardless of the real table: if the
    /// query did not apply it (the table has no bucketed maps, or the query uses `m` in another way),
    /// its filter does not read `m.key_k`, and `getAlternativeExpression` computes the alternative form
    /// again without the subcolumn rewrites of `m`. So the result does not depend on the table's
    /// serialization, and it is not a part of the cache identity.
    storage->setHasBucketedMapSerialization(true);

    /// The passes treat the columns of the table's keys and indexes specially (e.g.
    /// `FunctionToSubcolumnsPass` does not rewrite most functions of such columns), and every source
    /// column of the key is a column of this very key in the real table.
    auto dummy_metadata = storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ false);
    StorageInMemoryMetadata metadata = *dummy_metadata;
    register_key(metadata);
    /// In a filter, a plain use of a column elsewhere in the query does not stop the rewrites of
    /// `transformers_optimize_in_filter_with_full_column` (e.g. `m['k']` to `m.key_k`). But
    /// `FunctionToSubcolumnsPass` does not rewrite any function of a key column that has a use not
    /// safe with indexes. So make the columns that the query does not rewrite key columns (the
    /// partition key of the dummy table is not used otherwise), and add such a use of them below.
    if (in_filter && !columns_without_subcolumns.empty())
    {
        auto key_ast = makeASTFunction("tuple");
        for (const auto & column : columns_without_subcolumns)
            key_ast->arguments->children.push_back(make_intrusive<ASTIdentifier>(column));
        metadata.partition_key = KeyDescription::getKeyFromAST(key_ast, metadata.getColumns(), VirtualColumnsDescription{}, context);
    }
    storage->setInMemoryMetadata(metadata);

    auto table_expression = std::make_shared<TableNode>(storage, context);

    auto query_node = std::make_shared<QueryNode>(context);
    /// The description is shared through the metadata snapshot, so do not let the resolution
    /// touch its AST.
    /// `FunctionToSubcolumnsPass` does not rewrite the functions of a column to subcolumn reads when
    /// the query uses the whole column elsewhere. Add such a use of every column the query does not
    /// rewrite; these extra outputs are removed from the result.
    /// In a filter, the key expressions are the arguments of `ignore`, which is not constant folded
    /// and accepts any number of arguments of any types. The columns that the query does not rewrite
    /// are key columns there (see above), and their use in the projection is not safe with indexes:
    /// every function of a key column but `arrayElement` of a `Map` is not (see
    /// `transformers_safe_with_indexes`), so `mapKeys` is used for a `Map`, and the plain column for
    /// the other types, whose functions in the key are not safe with indexes by themselves.
    ASTPtr projection_ast;
    if (in_filter)
    {
        projection_ast = make_intrusive<ASTExpressionList>();
        projection_ast->children.push_back(make_intrusive<ASTLiteral>(UInt64(1)));
        for (const auto & column : columns_without_subcolumns)
        {
            auto source_column = source_columns.tryGetByName(column);
            if (source_column && isMap(source_column->type))
                projection_ast->children.push_back(makeASTFunction("mapKeys", make_intrusive<ASTIdentifier>(column)));
            else
                projection_ast->children.push_back(make_intrusive<ASTIdentifier>(column));
        }
        auto filter_ast = makeASTFunction("ignore");
        filter_ast->arguments->children = expression_list_ast->clone()->children;
        query_node->getWhere() = buildQueryTree(filter_ast, context);
    }
    else
    {
        projection_ast = expression_list_ast->clone();
        for (const auto & column : columns_without_subcolumns)
            projection_ast->children.push_back(make_intrusive<ASTIdentifier>(column));
    }
    query_node->getProjectionNode() = buildQueryTree(projection_ast, context);
    query_node->getJoinTreeNode() = table_expression;

    /// The list of the (rewritten) key expressions in the query tree.
    auto get_key_expressions_node = [&](const QueryTreeNodePtr & tree) -> QueryTreeNodePtr
    {
        auto & query = tree->as<QueryNode &>();
        if (!in_filter)
            return query.getProjectionNode();
        if (!query.hasWhere())
            return nullptr;
        auto * filter_function = query.getWhere()->as<FunctionNode>();
        if (!filter_function || filter_function->getFunctionName() != "ignore")
            return nullptr;
        return filter_function->getArgumentsNode();
    };

    QueryTreeNodePtr query_tree = query_node;
    QueryTreePassManager query_tree_pass_manager(context);
    addQueryTreePasses(query_tree_pass_manager);

    /// Check the folded constants after every pass: a pass can remove a folded constant
    /// (e.g. the condition of an `if`) after building on its value.
    const auto & key_dag = expression->getActionsDAG();
    for (const auto & pass : query_tree_pass_manager.getPasses())
    {
        pass->run(query_tree, context);
        auto key_expressions_node = get_key_expressions_node(query_tree);
        if (!key_expressions_node || !foldedConstantsMatchKeyExpression(key_expressions_node, key_dag))
            return std::nullopt;
    }

    auto projection_node = get_key_expressions_node(query_tree);

    GlobalPlannerContextPtr global_planner_context
        = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{});
    auto planner_context = std::make_shared<PlannerContext>(context, global_planner_context, SelectQueryOptions{});
    collectSetsAndSourceColumns(projection_node, planner_context, /*keep_alias_columns=*/ false);

    ColumnNodePtrWithHashSet empty_correlated_columns_set;
    auto [dag, correlated_subtrees] = buildActionsDAGFromExpressionNode(
        projection_node,
        /*input_columns=*/ {},
        planner_context,
        empty_correlated_columns_set,
        /*use_column_identifier_as_action_node_name=*/ false);
    correlated_subtrees.assertEmpty("in an index or key expression");

    auto & outputs = dag.getOutputs();
    /// In a filter, the extra uses are in the projection, not among the key expressions.
    const size_t extra_outputs = in_filter ? 0 : columns_without_subcolumns.size();
    if (outputs.size() != expression_list_ast->children.size() + extra_outputs)
        return std::nullopt;
    if (extra_outputs)
    {
        outputs.resize(expression_list_ast->children.size());
        dag.removeUnusedActions(/*allow_remove_inputs=*/ true, /*allow_constant_folding=*/ false);
    }

    return std::move(dag);
}

/// The storage columns an output of a key expression reads: a subcolumn read (e.g. `m.key_k` after
/// `FunctionToSubcolumnsPass`) reads its storage column.
NameSet getRequiredStorageColumns(const ActionsDAG::Node * output, const NameSet & storage_columns)
{
    NameSet result;
    std::unordered_set<const ActionsDAG::Node *> visited;
    std::vector<const ActionsDAG::Node *> stack{output};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();
        if (!visited.insert(node).second)
            continue;

        if (node->type == ActionsDAG::ActionType::INPUT)
        {
            String name = node->result_name;
            while (!storage_columns.contains(name))
            {
                auto pos = name.rfind('.');
                if (pos == String::npos)
                    break;
                name.resize(pos);
            }
            result.insert(storage_columns.contains(name) ? name : node->result_name);
        }

        for (const auto * child : node->children)
            stack.push_back(child);
    }
    return result;
}

/// The generic computation of the alternative (rewritten) form of a list of key expressions: it is
/// the same for a skip index, a primary key and a partition key. Throws if the key expression
/// cannot be analyzed.
AlternativeKeyExpressionPtr computeAlternativeExpression(
    const ASTPtr & expression_list_ast,
    const ExpressionActionsPtr & expression,
    const Names & column_names,
    const Names & columns_without_subcolumns,
    bool in_filter,
    const std::function<void(StorageInMemoryMetadata &)> & register_key,
    const ContextPtr & context)
{
    auto execution_context = Context::createCopy(context);

    auto dag = buildRewrittenExpressionWithAnalyzer(
        expression_list_ast, expression, columns_without_subcolumns, in_filter, register_key, execution_context);
    if (!dag)
        return nullptr;

    const auto & outputs = dag->getOutputs();
    if (outputs.size() != column_names.size())
        return nullptr;

    /// A rewrite renames an expression, it does not change what it computes: the rewritten form
    /// of every key column must have the same type and read the same storage columns. This
    /// rejects the forms that the checks of the folded constants cannot see.
    NameSet storage_columns;
    for (const auto & column : expression->getRequiredColumns())
        storage_columns.insert(column);

    for (size_t i = 0; i < outputs.size(); ++i)
    {
        /// The key expression can have more outputs than the key columns (e.g. the sorting key
        /// keeps its source columns), so look the key column up by name.
        const auto * original_output = expression->getActionsDAG().tryFindInOutputs(column_names[i]);
        if (!original_output
            || !outputs[i]->result_type->equals(*original_output->result_type)
            || getRequiredStorageColumns(outputs[i], storage_columns) != getRequiredStorageColumns(original_output, storage_columns))
            return nullptr;
    }

    /// Re-clone the rewritten DAG with the same canonicalization index analysis (RPNBuilder over
    /// `ActionsDAGWithInversionPushDown`) applies to the query filter expressions (e.g. of constant
    /// names), so that every (sub)expression node is named the way the matching filter node is.
    auto canonical_dag = cloneDAGForIndexAnalysisNames(outputs, context);
    const auto & canonical_outputs = canonical_dag.getOutputs();

    Names result(canonical_outputs.size());
    bool has_difference = false;
    for (size_t i = 0; i < canonical_outputs.size(); ++i)
    {
        result[i] = RPNBuilderTreeNode(canonical_outputs[i], context).getColumnName();
        has_difference |= (result[i] != column_names[i]);
    }

    if (!has_difference)
        return nullptr;

    auto alternative_key = std::make_shared<AlternativeKeyExpression>();
    for (const auto & node : canonical_dag.getNodes())
    {
        if (node.type == ActionsDAG::ActionType::INPUT)
            continue;
        alternative_key->computed_names.insert(node.result_name);
        alternative_key->computed_names.insert(RPNBuilderTreeNode(&node, context).getColumnName());
    }
    /// Only the computed outputs: a plain column output (e.g. `id` of a key `(multiIf(...), id)`) is
    /// a column of the table by definition, and its identity is checked as an input.
    for (size_t i = 0; i < canonical_outputs.size(); ++i)
        if (canonical_outputs[i]->type != ActionsDAG::ActionType::INPUT)
            alternative_key->computed_names.insert(result[i]);
    alternative_key->column_names = std::move(result);
    /// The expression carries the rewritten form of the key expressions; it is only searched
    /// by index analysis, never executed.
    alternative_key->expression = std::make_shared<ExpressionActions>(std::move(canonical_dag), ExpressionActionsSettings(execution_context));
    return alternative_key;
}

/// The alternative form of a key is determined by the key expression and by the query's settings and
/// current database, but computing it runs the query tree passes, which is noticeable for short
/// queries. Cache it by the content of the key: its AST, the stored key expression (its types and the
/// constants folded when the key was built) and the kind of the key, together with a hash of the
/// changed settings. The content, not the address of the expression object, is the identity, because
/// some callers get a fresh clone of the expression for every query (the partition key adjusted by
/// `MergeTreePartition::adjustPartitionKey`, the index description copied into every skip index
/// helper). An entry of a changed key is not reused: a change of the key or of the types of its
/// columns changes its content.
class AlternativeExpressionCache
{
public:
    static AlternativeExpressionCache & instance()
    {
        static AlternativeExpressionCache cache;
        return cache;
    }

    template <typename Compute>
    AlternativeKeyExpressionPtr getOrCompute(
        const ASTPtr & expression_list_ast,
        const ExpressionActionsPtr & expression,
        const Names & column_names,
        const String & key_kind,
        const ContextPtr & context,
        Compute && compute)
    {
        const UInt128 key = getKey(expression_list_ast, expression, column_names, key_kind, context);
        {
            std::lock_guard lock(mutex);
            if (auto it = entries.find(key); it != entries.end())
                return it->second;
        }

        auto result = compute();

        std::lock_guard lock(mutex);
        if (entries.size() >= max_entries)
            entries.clear();
        entries.insert_or_assign(key, result);
        return result;
    }

private:
    static UInt128 getKey(
        const ASTPtr & expression_list_ast,
        const ExpressionActionsPtr & expression,
        const Names & column_names,
        const String & key_kind,
        const ContextPtr & context)
    {
        SipHash hash;
        hash.update(key_kind);
        expression_list_ast->updateTreeHash(hash, /*ignore_aliases=*/ false);
        expression->getActionsDAG().updateHash(hash);
        for (const auto & column : expression->getRequiredColumnsWithTypes())
        {
            hash.update(column.name);
            hash.update(column.type->getName());
        }
        hash.update(column_names.size());
        for (const auto & name : column_names)
            hash.update(name);

        for (const auto & change : context->getSettingsRef().changes())
        {
            hash.update(change.name);
            applyVisitor(FieldVisitorHash(hash), change.value);
        }
        hash.update(context->getCurrentDatabase());
        return hash.get128();
    }

    static constexpr size_t max_entries = 10000;

    std::mutex mutex;
    std::unordered_map<UInt128, AlternativeKeyExpressionPtr> entries;
};

/// The storage column of the key a subcolumn input of the alternative form is read from: the longest
/// prefix that is a source column of the key, see `getRequiredStorageColumns`. Empty if there is none.
String getSourceColumnOfSubcolumn(const String & subcolumn_name, const NameSet & key_source_columns)
{
    for (size_t pos = subcolumn_name.rfind('.'); pos != String::npos && pos != 0; pos = subcolumn_name.rfind('.', pos - 1))
    {
        String prefix = subcolumn_name.substr(0, pos);
        if (key_source_columns.contains(prefix))
            return prefix;
    }
    return {};
}

/// Whether `prefix` of the name of a subcolumn input of the alternative form is a column of the table
/// that the name can resolve through, rather than through `source_column`, the key's source column the
/// subcolumn is read from (see `getSourceColumnOfSubcolumn`). The source column itself is not such a
/// column, and neither is the storage column that owns it when it is a subcolumn itself (e.g. the
/// column `c` of the key's input `c.m` for `c Tuple(m Map(String, Int32))`): the subcolumn `c.m.key_k`
/// is read through the same substream of `c` as `c.m`. A physical column named `c.m` is owned by
/// itself, so another column `c` of the table still counts.
bool isOtherTableColumnPrefix(const String & prefix, const String & source_column, const ColumnsDescription & table_columns)
{
    if (prefix == source_column || !table_columns.has(prefix))
        return false;

    if (source_column.starts_with(prefix + '.'))
    {
        auto source = table_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, source_column);
        if (source && source->isSubcolumn() && source->getNameInStorage() == prefix)
            return false;
    }

    return true;
}

bool isSameSubstream(const ISerialization::Substream & lhs, const ISerialization::Substream & rhs)
{
    return lhs.type == rhs.type && lhs.name_of_substream == rhs.name_of_substream
        && lhs.variant_element_name == rhs.variant_element_name
        && lhs.object_path_name == rhs.object_path_name && lhs.bucket == rhs.bucket;
}

/// The path of substreams of `column` from its storage column; empty for a storage column itself.
std::optional<ISerialization::SubstreamPath> getSubstreamPathInStorage(const NameAndTypePair & column)
{
    if (!column.isSubcolumn())
        return ISerialization::SubstreamPath{};
    auto info = column.getTypeInStorage()->tryGetSubcolumnInfo(column.getSubcolumnName());
    if (!info)
        return std::nullopt;
    return info->substreams_path;
}

/// Whether the subcolumn input `subcolumn_name` of the alternative form is, in the real table, the same
/// subcolumn the replay read it as: the subcolumn of the key's source column `source_column` (see
/// `getSourceColumnOfSubcolumn`) along the same substreams, with the same type. Subcolumn names are
/// flat, so another element of the same storage column can answer to the same name: for
/// ``c Tuple(`a.key_k` Int32, a Map(String, Int32))`` and the key's source column `c.a`, the replay
/// reads `c.a.key_k` as the key `k` of the `Map`, but in the table it is the `Tuple` element `a.key_k`.
/// `FunctionToSubcolumnsPass` does not rewrite to such a subcolumn in the query (it checks the
/// substreams the same way), so the name of the filter refers to the other subcolumn.
bool subcolumnResolvesAsInReplay(
    const String & subcolumn_name,
    const String & source_column,
    const ColumnsDescription & key_source_columns,
    const ColumnsDescription & table_columns)
{
    if (source_column.empty())
        return false;

    const auto options = GetColumnsOptions(GetColumnsOptions::All).withSubcolumns();

    /// How the replay against the dummy storage with the key's source columns resolves the name.
    auto replayed = key_source_columns.tryGetColumn(options, subcolumn_name);
    if (!replayed || !replayed->isSubcolumn() || replayed->getNameInStorage() != source_column)
        return false;
    auto replayed_path = getSubstreamPathInStorage(*replayed);

    auto source = table_columns.tryGetColumn(options, source_column);
    auto resolved = table_columns.tryGetColumn(options, subcolumn_name);
    if (!replayed_path || !source || !resolved || !resolved->isSubcolumn()
        || resolved->getNameInStorage() != source->getNameInStorage() || !resolved->type->equals(*replayed->type))
        return false;

    auto source_path = getSubstreamPathInStorage(*source);
    auto resolved_path = getSubstreamPathInStorage(*resolved);
    if (!source_path || !resolved_path || resolved_path->size() != source_path->size() + replayed_path->size())
        return false;

    return std::equal(source_path->begin(), source_path->end(), resolved_path->begin(), isSameSubstream)
        && std::equal(replayed_path->begin(), replayed_path->end(), resolved_path->begin() + source_path->size(), isSameSubstream);
}

/// The alternative form is computed against a storage with only the source columns of the key, so the
/// rewrite passes do not see the other columns of the table, and the names of the alternative form can
/// refer to other columns in the real table:
/// - A rewrite to a subcolumn read (e.g. of `m['k']` to `m.key_k` by `FunctionToSubcolumnsPass`) is not
///   applied in the query when the table has a column of the same name, and then that name refers to
///   the other column, not to the key. Likewise, the name can resolve as a subcolumn of another column
///   of the table (e.g. `j.m.key_k` as a subcolumn of a `JSON` column `j` rather than of the key's
///   column `j.m`). So the subcolumn read is rejected if its name, or any prefix of it other than the
///   key's source column and the storage column that owns it, is a column of the table, see
///   `isOtherTableColumnPrefix`. And the name must refer in the table to the same substream of the same
///   storage column, with the same type, as in the replay, see `subcolumnResolvesAsInReplay`.
/// - A rewritten (sub)expression can be named as another column of the table (e.g. a column named
///   `if(greater(v, 0), v, 0)` while the key is `multiIf(v > 0, v, 0)`), and the filter on that column
///   would be matched with the key by name. The name index analysis reconstructs for a node can differ
///   from the node's name (e.g. `arraySum(arrayMap(lambda(tuple(x), ...), a))` for a lambda), so all of
///   them are checked, see `AlternativeKeyExpression::computed_names`.
/// This does not depend on the settings, so it is checked for every table rather than cached.
bool collidesWithOtherTableColumn(
    const AlternativeKeyExpression & alternative_key,
    const ExpressionActionsPtr & expression,
    const ColumnsDescription & table_columns)
{
    NameSet key_source_columns;
    for (const auto & column : expression->getRequiredColumns())
        key_source_columns.insert(column);
    const ColumnsDescription key_source_columns_description(expression->getRequiredColumnsWithTypes());

    /// Check every name a filter expression can be matched by, not only the names of the nodes:
    /// the names index analysis reconstructs can differ from them (e.g. for lambdas).
    for (const auto & name : alternative_key.computed_names)
        if (table_columns.has(name))
            return true;

    for (const auto & node : alternative_key.expression->getActionsDAG().getNodes())
    {
        if (node.type != ActionsDAG::ActionType::INPUT)
            continue;

        if (key_source_columns.contains(node.result_name))
            continue;

        const String source_column = getSourceColumnOfSubcolumn(node.result_name, key_source_columns);

        if (table_columns.has(node.result_name))
            return true;
        if (!subcolumnResolvesAsInReplay(node.result_name, source_column, key_source_columns_description, table_columns))
            return true;
        for (size_t pos = node.result_name.find('.'); pos != String::npos; pos = node.result_name.find('.', pos + 1))
        {
            if (isOtherTableColumnPrefix(node.result_name.substr(0, pos), source_column, table_columns))
                return true;
        }
    }

    return false;
}

/// The names a filter is built from.
struct PredicateNames
{
    /// The storage columns the filter reads, including subcolumns (e.g. `m.key_k`).
    NameSet inputs;
    /// The names of all nodes of the filter.
    NameSet nodes;
};

PredicateNames getPredicateNames(const ActionsDAG::Node * predicate)
{
    PredicateNames result;
    std::unordered_set<const ActionsDAG::Node *> visited;
    std::vector<const ActionsDAG::Node *> stack{predicate};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();
        if (!node || !visited.insert(node).second)
            continue;

        result.nodes.insert(node->result_name);
        if (node->type == ActionsDAG::ActionType::INPUT)
            result.inputs.insert(node->result_name);

        for (const auto * child : node->children)
            stack.push_back(child);
    }
    return result;
}

/// The source columns of the key whose rewrites to subcolumn reads (e.g. `m.key_k` after
/// `FunctionToSubcolumnsPass`) the query did not apply, so the alternative form must be computed
/// without them:
/// - the subcolumn's name, or a prefix of it other than the source column and its owning column, is a
///   column of the table:
///   `FunctionToSubcolumnsPass` does not rewrite to it then, see `collidesWithOtherTableColumn`;
/// - the subcolumn's name refers to another subcolumn in the table than the one the replay read, see
///   `subcolumnResolvesAsInReplay`;
/// - the filter does not read the subcolumn (only checked if `predicate` is given);
/// - the filter still has a function of the source column from the key (e.g. `arrayElement(m, 'k')`)
///   that the replay rewrote to a subcolumn read. `FunctionToSubcolumnsPass` decides for every column
///   once, for all its uses in the filter, so the rewrite of this column was not applied even if the
///   filter reads the subcolumn elsewhere (e.g. `` `m.key_k` >= 0 `` written in the query).
///   The names of the key's functions are compared in the canonical form of index analysis, as the
///   filter's are, so that a folded constant argument (e.g. `concat('k', '')`) is named by its value.
/// Returns nullopt if such a subcolumn cannot be attributed to a source column of the key.
std::optional<NameSet> getSourceColumnsOfSubcolumnsNotInQuery(
    const AlternativeKeyExpression & alternative_key,
    const ExpressionActionsPtr & expression,
    const ColumnsDescription & table_columns,
    const PredicateNames * predicate,
    const ContextPtr & context)
{
    NameSet key_source_columns;
    for (const auto & column : expression->getRequiredColumns())
        key_source_columns.insert(column);
    const ColumnsDescription key_source_columns_description(expression->getRequiredColumnsWithTypes());

    const auto & alternative_dag = alternative_key.expression->getActionsDAG();
    NameSet alternative_node_names;
    for (const auto & node : alternative_dag.getNodes())
        alternative_node_names.insert(node.result_name);

    /// The source columns that have a function from the key, rewritten away by the replay, in the filter.
    NameSet source_columns_not_rewritten_in_filter;
    if (predicate)
    {
        const auto canonical_key_dag = cloneDAGForIndexAnalysisNames(expression->getActionsDAG().getOutputs(), context);
        for (const auto & node : canonical_key_dag.getNodes())
        {
            if (node.type != ActionsDAG::ActionType::FUNCTION || alternative_node_names.contains(node.result_name)
                || !predicate->nodes.contains(node.result_name))
                continue;
            for (const auto * child : node.children)
                if (child->type == ActionsDAG::ActionType::INPUT && key_source_columns.contains(child->result_name))
                    source_columns_not_rewritten_in_filter.insert(child->result_name);
        }
    }

    NameSet result;
    for (const auto & node : alternative_dag.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::INPUT || key_source_columns.contains(node.result_name))
            continue;

        String source_column = getSourceColumnOfSubcolumn(node.result_name, key_source_columns);

        bool shadowed = table_columns.has(node.result_name)
            || (!source_column.empty()
                && !subcolumnResolvesAsInReplay(node.result_name, source_column, key_source_columns_description, table_columns));
        for (size_t pos = node.result_name.find('.'); !shadowed && pos != String::npos; pos = node.result_name.find('.', pos + 1))
        {
            shadowed = isOtherTableColumnPrefix(node.result_name.substr(0, pos), source_column, table_columns);
        }

        bool rewritten_in_query = !predicate
            || (predicate->inputs.contains(node.result_name)
                && (source_column.empty() || !source_columns_not_rewritten_in_filter.contains(source_column)));
        if (!shadowed && rewritten_in_query)
            continue;

        if (source_column.empty())
            return std::nullopt;
        result.insert(std::move(source_column));
    }

    return result;
}

AlternativeKeyExpressionPtr getAlternativeExpressionImpl(
    const ASTPtr & expression_list_ast,
    const ExpressionActionsPtr & expression,
    const Names & column_names,
    const Names & columns_without_subcolumns,
    bool in_filter,
    const std::function<void(StorageInMemoryMetadata &)> & register_key,
    const String & key_kind,
    const String & description_for_log,
    const ContextPtr & context)
{
    if (!expression_list_ast || !expression)
        return nullptr;

    /// A key on plain columns cannot be named differently by the rewrites.
    bool all_columns_are_identifiers = true;
    for (const auto & child : expression_list_ast->children)
    {
        if (!child || !child->as<ASTIdentifier>())
        {
            all_columns_are_identifiers = false;
            break;
        }
    }
    if (all_columns_are_identifiers)
        return nullptr;

    /// The alternative names are a best-effort improvement of index analysis: on any failure
    /// fall back to matching by the original names only. A failure is not cached.
    try
    {
        auto alternative_key = AlternativeExpressionCache::instance().getOrCompute(
            expression_list_ast,
            expression,
            column_names,
            /// The columns that are not rewritten to subcolumn reads and the context of the key
            /// expressions in the dummy query are part of the identity of the result.
            fmt::format(
                "{}{}{}",
                key_kind,
                in_filter ? " in filter" : "",
                columns_without_subcolumns.empty() ? ""
                                                   : fmt::format(" without subcolumns of: {}", fmt::join(columns_without_subcolumns, ", "))),
            context,
            [&]
            {
                return computeAlternativeExpression(
                    expression_list_ast, expression, column_names, columns_without_subcolumns, in_filter, register_key, context);
            });
        return alternative_key;
    }
    catch (...)
    {
        tryLogCurrentException(
            getLogger("MergeTreeIndexAnalyzerNames"),
            fmt::format("Cannot compute the alternative form of {}, matching by the original names only", description_for_log),
            LogsLevel::debug);
        return nullptr;
    }
}

/// `FunctionToSubcolumnsPass` decides for every column of the query whether to rewrite its functions
/// to subcolumn reads: it does not when the query uses the column in another way (e.g. `mapKeys(m)` in
/// the projection), reads the table with `FINAL`, or when the table has a column with the name of the
/// subcolumn. The replay against the dummy table does not see that, so it can rewrite `m['k']` to
/// `m.key_k` while the query does not. If the alternative form reads subcolumns that are shadowed by
/// other columns of the table or that the filter does not read, compute it again without the rewrites
/// of their source columns, keeping the other ones (e.g. `multiIf` to `if`, or `n['k']` to `n.key_k`
/// when the filter reads `n.key_k`). The final candidate is still checked for collisions with the
/// other columns of the table.
AlternativeKeyExpressionPtr getAlternativeExpression(
    const ASTPtr & expression_list_ast,
    const ExpressionActionsPtr & expression,
    const Names & column_names,
    const ColumnsDescription & table_columns,
    const std::function<void(StorageInMemoryMetadata &)> & register_key,
    const String & key_kind,
    const String & description_for_log,
    const ActionsDAG::Node * predicate,
    const ContextPtr & context)
{
    std::optional<PredicateNames> predicate_names;
    if (predicate && context->getSettingsRef()[Setting::optimize_functions_to_subcolumns])
        predicate_names = getPredicateNames(predicate);

    /// Compute the alternative form in the given context of the key expressions in the dummy query,
    /// suppressing the subcolumn rewrites of the source columns that the query does not apply one by
    /// one. Returns nullptr if there is no such form.
    auto compute_in_context = [&](bool in_filter) -> AlternativeKeyExpressionPtr
    {
        NameSet columns_without_subcolumns;
        auto alternative_key = getAlternativeExpressionImpl(
            expression_list_ast, expression, column_names, {}, in_filter, register_key, key_kind, description_for_log, context);

        /// Every iteration adds at least one source column, so this terminates.
        while (alternative_key)
        {
            auto excluded = getSourceColumnsOfSubcolumnsNotInQuery(
                *alternative_key, expression, table_columns, predicate_names ? &*predicate_names : nullptr, context);
            if (!excluded)
                return nullptr;
            if (excluded->empty())
                break;

            size_t previous_size = columns_without_subcolumns.size();
            columns_without_subcolumns.insert(excluded->begin(), excluded->end());
            /// The rewrite of a column that is already excluded cannot be suppressed further.
            if (columns_without_subcolumns.size() == previous_size)
                return nullptr;

            Names sorted_columns(columns_without_subcolumns.begin(), columns_without_subcolumns.end());
            std::sort(sorted_columns.begin(), sorted_columns.end());
            alternative_key = getAlternativeExpressionImpl(
                expression_list_ast, expression, column_names, sorted_columns, in_filter, register_key, key_kind,
                description_for_log, context);
        }

        if (alternative_key && collidesWithOtherTableColumn(*alternative_key, expression, table_columns))
            return nullptr;
        return alternative_key;
    };

    /// First, the key expressions as a part of a filter, as the filter of the query is: this form has
    /// the rewrites that are applied only in a filter (e.g. `m['k']` to `m.key_k` while `toString(m)`
    /// keeps the whole column), together with the suppression of the rewrites of other columns
    /// (e.g. of `n` used by `mapKeys(n)` in the projection of the query).
    if (predicate_names)
    {
        if (auto filter_key = compute_in_context(/*in_filter=*/ true))
            return filter_key;
    }

    /// Otherwise, the key expressions as the projection of a query (e.g. the filter does not read the
    /// subcolumns because the query reads the table with `FINAL`, or there is no filter to compare with).
    return compute_in_context(/*in_filter=*/ false);
}

}

AlternativeKeyExpressionPtr getAlternativeIndexExpression(
    const IndexDescription & index,
    const StorageInMemoryMetadata & table_metadata,
    bool index_is_in_table,
    const ActionsDAG::Node * predicate,
    const ContextPtr & context)
{
    if (index_is_in_table)
        return getAlternativeExpression(
            index.expression_list_ast,
            index.expression,
            index.column_names,
            table_metadata.getColumns(),
            [&](StorageInMemoryMetadata & metadata)
            {
                IndicesDescription indices;
                indices.push_back(index);
                metadata.setSecondaryIndices(std::move(indices));
            },
            /// The passes see the key as an index of this type.
            fmt::format("index of type {}", index.type),
            fmt::format("the expression of index {}", index.name),
            predicate,
            context);

    /// The query was analyzed against the table without this index, so the passes treat specially
    /// only those source columns of the index that are columns of the keys and indexes of the table
    /// (e.g. `FunctionToSubcolumnsPass` rewrites `isNull(x)` to `x.null` in the query when `x` is
    /// not a key column of the table). Register exactly those columns, as a primary key on them.
    if (!index.expression)
        return nullptr;

    NameSet table_key_columns;
    for (const auto & column : table_metadata.getColumnsRequiredForPrimaryKey())
        table_key_columns.insert(column);
    for (const auto & column : table_metadata.getColumnsRequiredForPartitionKey())
        table_key_columns.insert(column);
    for (const auto & table_index : table_metadata.getSecondaryIndices())
        for (const auto & column : table_index.expression->getRequiredColumns())
            table_key_columns.insert(column);

    Names protected_columns;
    for (const auto & column : index.expression->getRequiredColumns())
        if (table_key_columns.contains(column))
            protected_columns.push_back(column);
    std::sort(protected_columns.begin(), protected_columns.end());

    return getAlternativeExpression(
        index.expression_list_ast,
        index.expression,
        index.column_names,
        table_metadata.getColumns(),
        [&](StorageInMemoryMetadata & metadata)
        {
            if (protected_columns.empty())
                return;
            auto key_ast = makeASTFunction("tuple");
            for (const auto & column : protected_columns)
                key_ast->arguments->children.push_back(make_intrusive<ASTIdentifier>(column));
            metadata.primary_key = KeyDescription::getKeyFromAST(key_ast, metadata.getColumns(), VirtualColumnsDescription{}, context);
        },
        /// The passes see the key columns of the table, which are part of the identity of the result.
        fmt::format("hypothetical index of type {} with key columns of the table: {}", index.type, fmt::join(protected_columns, ", ")),
        fmt::format("the expression of hypothetical index {}", index.name),
        predicate,
        context);
}

AlternativeKeyExpressionPtr getAlternativeKeyExpression(
    const KeyDescription & key, const ColumnsDescription & table_columns, const ActionsDAG::Node * predicate, const ContextPtr & context)
{
    return getAlternativeExpression(
        key.expression_list_ast,
        key.expression,
        key.column_names,
        table_columns,
        [&](StorageInMemoryMetadata & metadata) { metadata.primary_key = key; },
        "key",
        "the key expression",
        predicate,
        context);
}

AlternativeKeyExpressionPtr LazyAlternativeKeyExpression::get(
    const KeyDescription & key, const ColumnsDescription & table_columns, const ActionsDAG::Node * predicate, const ContextPtr & context) const
{
    std::call_once(initialized, [&] { alternative_key = getAlternativeKeyExpression(key, table_columns, predicate, context); });
    return alternative_key;
}

RewriteAwareIndexConditionFactory::RewriteAwareIndexConditionFactory(
    MergeTreeIndexPtr index_helper_, StorageMetadataPtr metadata_snapshot_, bool index_is_in_table_)
    : index_helper(std::move(index_helper_))
    , metadata_snapshot(std::move(metadata_snapshot_))
    , index_is_in_table(index_is_in_table_)
    , minmax_index(typeid_cast<const MergeTreeIndexMinMax *>(index_helper.get()))
{
}

MergeTreeIndexConditionPtr RewriteAwareIndexConditionFactory::create(const ActionsDAG::Node * predicate, const ContextPtr & context) const
{
    if (!predicate)
        return nullptr;

    if (minmax_index)
        std::call_once(alternative_key_initialized, [&] { alternative_key = getAlternativeIndexExpression(index_helper->index, *metadata_snapshot, index_is_in_table, predicate, context); });

    if (alternative_key)
        return minmax_index->createIndexConditionWithAlternativeKey(predicate, context, alternative_key);
    return index_helper->createIndexCondition(predicate, context);
}

}
