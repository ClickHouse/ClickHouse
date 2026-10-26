#include <Storages/MergeTree/MergeTreeIndexAnalyzerNames.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/TableNode.h>
#include <Core/LogsLevel.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ASTIdentifier.h>
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

#include <unordered_map>

namespace DB
{

namespace
{

/// The analyzer folds constant subexpressions with the settings of the query, which can give them a
/// different value than the stored key has (e.g. `least_greatest_legacy_null_behavior` changes the
/// value of `greatest(NULL, 1)`), and the rewrite passes then build on that value (e.g. an `if` with
/// a constant condition collapses to one of its branches). Then the rewritten expression is not
/// another name of the stored key, but a different expression. Check that every folded constant has
/// the same value as the subexpression of the same name in the stored key expression, which was folded
/// when the key was built. A constant that cannot be found there is not proven equal either.
bool foldedConstantsMatchKeyExpression(const QueryTreeNodePtr & node, const ActionsDAG & key_dag)
{
    if (const auto * constant_node = node->as<ConstantNode>(); constant_node && constant_node->hasSourceExpression())
    {
        const String name = constant_node->getSourceExpression()->toAST()->getColumnName();
        const ActionsDAG::Node * key_node = nullptr;
        for (const auto & candidate : key_dag.getNodes())
        {
            if (candidate.result_name == name)
            {
                key_node = &candidate;
                break;
            }
        }

        if (!key_node || !key_node->column || !isColumnConst(*key_node->column)
            || !key_node->result_type->equals(*constant_node->getResultType())
            || (*key_node->column)[0] != constant_node->getValue())
            return false;
    }

    for (const auto & child : node->getChildren())
        if (child && !foldedConstantsMatchKeyExpression(child, key_dag))
            return false;

    return true;
}

/// Reproduce the rewrites of the query analyzer (the query tree passes) on the key expressions.
/// Returns nullopt if the rewritten expressions are not proven to compute the stored key.
std::optional<ActionsDAG> buildRewrittenExpressionWithAnalyzer(
    const ASTPtr & expression_list_ast,
    const ExpressionActionsPtr & expression,
    const std::function<void(StorageInMemoryMetadata &)> & register_key,
    const ContextMutablePtr & context)
{
    /// Resolve the key expressions against a dummy storage with the key's source columns,
    /// running the same query tree passes (with the same settings) the query itself goes through.
    auto source_columns = expression->getRequiredColumnsWithTypes();
    auto storage = std::make_shared<StorageDummy>(StorageID{"dummy", "dummy"}, ColumnsDescription(source_columns));

    /// The passes treat the columns of the table's keys and indexes specially (e.g.
    /// `FunctionToSubcolumnsPass` does not rewrite most functions of such columns), and every source
    /// column of the key is a column of this very key in the real table.
    auto dummy_metadata = storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ false);
    StorageInMemoryMetadata metadata = *dummy_metadata;
    register_key(metadata);
    storage->setInMemoryMetadata(metadata);

    auto table_expression = std::make_shared<TableNode>(storage, context);

    auto query_node = std::make_shared<QueryNode>(context);
    /// The description is shared through the metadata snapshot, so do not let the resolution
    /// touch its AST.
    query_node->getProjectionNode() = buildQueryTree(expression_list_ast->clone(), context);
    query_node->getJoinTreeNode() = table_expression;

    QueryTreeNodePtr query_tree = query_node;
    QueryTreePassManager query_tree_pass_manager(context);
    addQueryTreePasses(query_tree_pass_manager);

    /// Check the folded constants after every pass: a pass can remove a folded constant
    /// (e.g. the condition of an `if`) after building on its value.
    const auto & key_dag = expression->getActionsDAG();
    for (const auto & pass : query_tree_pass_manager.getPasses())
    {
        pass->run(query_tree, context);
        if (!foldedConstantsMatchKeyExpression(query_tree->as<QueryNode &>().getProjectionNode(), key_dag))
            return std::nullopt;
    }

    auto projection_node = query_tree->as<QueryNode &>().getProjectionNode();

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
    const std::function<void(StorageInMemoryMetadata &)> & register_key,
    const ContextPtr & context)
{
    auto execution_context = Context::createCopy(context);

    auto dag = buildRewrittenExpressionWithAnalyzer(expression_list_ast, expression, register_key, execution_context);
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

/// The alternative form is computed against a storage with only the source columns of the key, so the
/// rewrite passes do not see the other columns of the table. In the query, a rewrite to a subcolumn
/// read (e.g. of `m['k']` to `m.key_k` by `FunctionToSubcolumnsPass`) is not applied when the table has
/// a column of the same name, and then that name refers to the other column, not to the key. This
/// does not depend on the settings, so it is checked for every table rather than cached.
bool readsOtherTableColumn(
    const AlternativeKeyExpression & alternative_key,
    const ExpressionActionsPtr & expression,
    const ColumnsDescription & table_columns)
{
    NameSet key_source_columns;
    for (const auto & column : expression->getRequiredColumns())
        key_source_columns.insert(column);

    for (const auto * input : alternative_key.expression->getActionsDAG().getInputs())
        if (!key_source_columns.contains(input->result_name) && table_columns.has(input->result_name))
            return true;

    return false;
}

AlternativeKeyExpressionPtr getAlternativeExpression(
    const ASTPtr & expression_list_ast,
    const ExpressionActionsPtr & expression,
    const Names & column_names,
    const ColumnsDescription & table_columns,
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
            key_kind,
            context,
            [&] { return computeAlternativeExpression(expression_list_ast, expression, column_names, register_key, context); });
        if (alternative_key && readsOtherTableColumn(*alternative_key, expression, table_columns))
            return nullptr;
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

}

AlternativeKeyExpressionPtr getAlternativeIndexExpression(
    const IndexDescription & index, const ColumnsDescription & table_columns, const ContextPtr & context)
{
    return getAlternativeExpression(
        index.expression_list_ast,
        index.expression,
        index.column_names,
        table_columns,
        [&](StorageInMemoryMetadata & metadata)
        {
            IndicesDescription indices;
            indices.push_back(index);
            metadata.setSecondaryIndices(std::move(indices));
        },
        /// The passes see the key as an index of this type.
        fmt::format("index of type {}", index.type),
        fmt::format("the expression of index {}", index.name),
        context);
}

AlternativeKeyExpressionPtr getAlternativeKeyExpression(
    const KeyDescription & key, const ColumnsDescription & table_columns, const ContextPtr & context)
{
    return getAlternativeExpression(
        key.expression_list_ast,
        key.expression,
        key.column_names,
        table_columns,
        [&](StorageInMemoryMetadata & metadata) { metadata.primary_key = key; },
        "key",
        "the key expression",
        context);
}

AlternativeKeyExpressionPtr LazyAlternativeKeyExpression::get(
    const KeyDescription & key, const ColumnsDescription & table_columns, const ContextPtr & context) const
{
    std::call_once(initialized, [&] { alternative_key = getAlternativeKeyExpression(key, table_columns, context); });
    return alternative_key;
}

RewriteAwareIndexConditionFactory::RewriteAwareIndexConditionFactory(MergeTreeIndexPtr index_helper_, StorageMetadataPtr metadata_snapshot_)
    : index_helper(std::move(index_helper_))
    , metadata_snapshot(std::move(metadata_snapshot_))
    , minmax_index(typeid_cast<const MergeTreeIndexMinMax *>(index_helper.get()))
{
}

MergeTreeIndexConditionPtr RewriteAwareIndexConditionFactory::create(const ActionsDAG::Node * predicate, const ContextPtr & context) const
{
    if (!predicate)
        return nullptr;

    if (minmax_index)
        std::call_once(alternative_key_initialized, [&] { alternative_key = getAlternativeIndexExpression(index_helper->index, metadata_snapshot->getColumns(), context); });

    if (alternative_key)
        return minmax_index->createIndexConditionWithAlternativeKey(predicate, context, alternative_key);
    return index_helper->createIndexCondition(predicate, context);
}

}
