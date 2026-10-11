#include <Analyzer/Passes/ConvertEmptyStringComparisonToFunctionPass.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageBuffer.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageProxy.h>
#include <Common/FieldVisitors.h>

#include <unordered_set>

namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_empty_string_comparisons;
}

namespace
{

/// The literal `''`.
bool isEmptyStringLiteral(const ASTPtr & ast)
{
    const auto * literal = ast->as<ASTLiteral>();
    return literal && literal->value.getType() == Field::Types::String && literal->value.safeGet<String>().empty();
}

/// The full name is kept, so `t.s` inside a tuple is looked up as the query names it and does not stand in for a plain `s`.
void collectIdentifiers(const ASTPtr & ast, NameSet & names)
{
    if (const auto * identifier = ast->as<ASTIdentifier>())
        names.insert(identifier->name());
    for (const auto & child : ast->children)
        collectIdentifiers(child, names);
}

/// What the keys, skip indexes and projections of a table compare with `''`: columns directly, and arrays whose
/// elements a lambda compares, as in `arrayMap(x -> if(x = '', 'e', x), arr)`.
struct DeclaredComparisons
{
    NameSet columns;
    NameSet arrays;
};

/// Collects into `out.columns` the names compared with `''` anywhere in `ast`, as `x = ''` or `x != ''` with the literal
/// on either side. For `if(s = '', 'abc', s)` that is `s`. A name that is a lambda parameter in scope is returned
/// instead, and the call that runs the lambda attributes it to the arrays the lambda runs over.
NameSet collectDeclaredComparisons(const ASTPtr & ast, const NameSet & params, DeclaredComparisons & out)
{
    NameSet compared_params;
    const auto * function = ast->as<ASTFunction>();

    if (function && (function->name == "equals" || function->name == "notEquals") && function->arguments
        && function->arguments->children.size() == 2)
    {
        const auto & args = function->arguments->children;
        NameSet names;
        if (isEmptyStringLiteral(args[0]))
            collectIdentifiers(args[1], names);
        else if (isEmptyStringLiteral(args[1]))
            collectIdentifiers(args[0], names);

        for (const auto & name : names)
            (params.contains(name) ? compared_params : out.columns).insert(name);
    }

    if (function && function->arguments)
    {
        for (const auto & argument : function->arguments->children)
        {
            const auto * lambda = argument->as<ASTFunction>();
            if (!lambda || lambda->name != "lambda" || !lambda->arguments || lambda->arguments->children.size() != 2)
            {
                compared_params.merge(collectDeclaredComparisons(argument, params, out));
                continue;
            }

            NameSet lambda_params;
            collectIdentifiers(lambda->arguments->children[0], lambda_params);
            NameSet in_scope = params;
            in_scope.insert(lambda_params.begin(), lambda_params.end());

            bool compares_own_param = false;
            for (const auto & name : collectDeclaredComparisons(lambda->arguments->children[1], in_scope, out))
            {
                if (lambda_params.contains(name))
                    compares_own_param = true;
                else
                    compared_params.insert(name);
            }

            /// An array that is itself a parameter of an enclosing lambda is reported upwards instead, so that
            /// `arrayCount(x -> arrayCount(y -> y = '', x) > 0, nested)` attributes the comparison to `nested`.
            if (compares_own_param)
            {
                NameSet arrays;
                for (const auto & other : function->arguments->children)
                    if (other != argument)
                        collectIdentifiers(other, arrays);
                for (const auto & name : arrays)
                    (params.contains(name) ? compared_params : out.arrays).insert(name);
            }
        }
        return compared_params;
    }

    for (const auto & child : ast->children)
        compared_params.merge(collectDeclaredComparisons(child, params, out));
    return compared_params;
}

/// A projection is checked as a whole, since its filter and its own skip indexes are matched by their written text
/// just like its keys. Skip index expressions are stored with ALIAS columns already expanded, so aliases need no
/// separate check.
DeclaredComparisons declaredComparisons(const StorageInMemoryMetadata & metadata)
{
    DeclaredComparisons out;
    for (const auto * key : {&metadata.getPartitionKey(), &metadata.getSortingKey(), &metadata.getPrimaryKey()})
        if (key->expression_list_ast)
            collectDeclaredComparisons(key->expression_list_ast, {}, out);

    for (const auto & index : metadata.getSecondaryIndices())
        if (index.expression_list_ast)
            collectDeclaredComparisons(index.expression_list_ast, {}, out);

    for (const auto & projection : metadata.getProjections())
        if (projection.definition_ast)
            collectDeclaredComparisons(projection.definition_ast, {}, out);

    return out;
}

/// The table a column is read from, through the wrappers that forward the read to another table: a lazily loaded
/// table, an Alias, a materialized view and a Buffer. The loop is bounded in case the wrappers form a cycle.
/// Distributed, Merge and View are not followed: they run the already rewritten query on their tables,
/// so an index behind them cannot be helped here.
StoragePtr getUnderlyingStorage(const QueryTreeNodePtr & source)
{
    StoragePtr storage;
    if (const auto * table_node = source->as<TableNode>())
        storage = table_node->getStorage();
    else if (const auto * table_function_node = source->as<TableFunctionNode>())
        storage = table_function_node->getStorage();

    for (size_t i = 0; storage && i < 16; ++i)
    {
        if (const auto * proxy = dynamic_cast<const StorageProxy *>(storage.get()))
            storage = proxy->getNested();
        else if (const auto * alias = storage->as<StorageAlias>())
            storage = alias->tryGetTargetTable();
        else if (const auto * materialized_view = storage->as<StorageMaterializedView>())
            storage = materialized_view->tryGetTargetTable();
        else if (const auto * buffer = storage->as<StorageBuffer>())
            storage = buffer->getDestinationTable();
        else
            break;
    }
    return storage;
}

/// The analyzer hands an alias out as one shared node, so the tree is a graph and every node is walked once.
void collectColumnNodes(const QueryTreeNodePtr & node, std::vector<const ColumnNode *> & columns, std::unordered_set<const IQueryTreeNode *> & visited)
{
    if (!visited.insert(node.get()).second)
        return;
    if (const auto * column_node = node->as<ColumnNode>())
        columns.push_back(column_node);
    for (const auto & child : node->getChildren())
        if (child)
            collectColumnNodes(child, columns, visited);
}

class ConvertEmptyStringComparisonToFunctionVisitor
    : public InDepthQueryTreeVisitorWithContext<ConvertEmptyStringComparisonToFunctionVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ConvertEmptyStringComparisonToFunctionVisitor>;

    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_empty_string_comparisons])
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        rememberLambdaArrays(*function_node);

        const String & func_name = function_node->getFunctionName();
        if (func_name != "equals" && func_name != "notEquals")
            return;

        const auto & args = function_node->getArguments().getNodes();
        if (args.size() != 2)
            return;

        // Identify which argument is the empty string literal
        int const_idx = -1;

        for (size_t i = 0; i < 2; ++i)
        {
            if (const auto * constant_node = args[i]->as<ConstantNode>())
            {
                if (isStringOrFixedString(constant_node->getResultType()))
                {
                    const Field & val = constant_node->getValue();
                    if (val.getType() == Field::Types::String && val.safeGet<String>().empty())
                    {
                        const_idx = static_cast<int>(i);
                        break;
                    }
                }
            }
        }

        if (const_idx == -1)
            return;

        size_t expr_idx = 1 - const_idx;
        const auto & expr_node = args[expr_idx];

        const auto expr_type = expr_node->getResultType();
        if (!expr_type || !isStringOrFixedString(expr_type))
            return;

        if (mustStayAsWritten(expr_node))
            return;

        const String replacement_func = (func_name == "equals") ? "empty" : "notEmpty";

        auto replacement_node = std::make_shared<FunctionNode>(replacement_func);
        replacement_node->getArguments().getNodes().push_back(expr_node);

        resolveOrdinaryFunctionNodeByName(*replacement_node, replacement_func, getContext());

        node = std::move(replacement_node);
    }

private:
    /// The declarations of the table a column is read from, or null when it is not read from a table.
    const DeclaredComparisons * declarationsFor(const ColumnNode & column_node)
    {
        auto source = column_node.getColumnSourceOrNull();
        if (!source)
            return nullptr;

        auto storage = getUnderlyingStorage(source);
        if (!storage)
            return nullptr;

        auto [it, inserted] = declarations_by_storage.try_emplace(storage.get());
        if (inserted)
        {
            auto metadata = storage->getInMemoryMetadataPtr(getContext(), false);
            it->second = declaredComparisons(*metadata);
        }
        return &it->second;
    }

    /// The array arguments of every call that runs a lambda, by the lambda's arguments node. Noted on the way down,
    /// since the call is entered before its lambda body, and looked at only when a parameter of that lambda is
    /// compared with `''`.
    void rememberLambdaArrays(const FunctionNode & function)
    {
        const auto & arguments = function.getArguments().getNodes();
        const LambdaNode * lambda = nullptr;
        for (const auto & argument : arguments)
            if (const auto * candidate = argument->as<LambdaNode>())
                lambda = candidate;
        if (!lambda)
            return;

        auto & arrays = arrays_by_lambda[&lambda->getArguments()];
        for (const auto & argument : arguments)
            if (!argument->as<LambdaNode>())
                arrays.push_back(argument);
    }

    /// Whether the lambda runs over an array whose elements a declaration compares with `''`, worked out once per
    /// lambda: the arrays may be large shared expressions and may be parameters of an enclosing lambda in turn.
    bool lambdaRunsOverDeclaredArray(const IQueryTreeNode * lambda_arguments)
    {
        if (auto it = lambda_over_declared_array.find(lambda_arguments); it != lambda_over_declared_array.end())
            return it->second;
        lambda_over_declared_array[lambda_arguments] = false;

        std::vector<const ColumnNode *> columns;
        std::unordered_set<const IQueryTreeNode *> visited;
        for (const auto & array : arrays_by_lambda[lambda_arguments])
            collectColumnNodes(array, columns, visited);

        bool declared = false;
        for (const auto * column_node : columns)
            if (isDeclaredArray(*column_node))
            {
                declared = true;
                break;
            }
        lambda_over_declared_array[lambda_arguments] = declared;
        return declared;
    }

    /// Whether the column is an array whose elements a declaration compares with `''`: directly, or through the
    /// arrays of its lambda when the column is itself a lambda parameter.
    bool isDeclaredArray(const ColumnNode & column_node)
    {
        auto source = column_node.getColumnSourceOrNull();
        if (!source)
            return false;

        if (arrays_by_lambda.contains(source.get()))
            return lambdaRunsOverDeclaredArray(source.get());

        const auto * declared = declarationsFor(column_node);
        return declared && declared->arrays.contains(column_node.getColumnName());
    }

    /// Whether a column of the expression belongs to a table whose key, skip index or projection compares that
    /// column with `''`, or is the parameter of a lambda that runs over such an array. Such a comparison must keep
    /// its written text, or the index no longer matches the query.
    bool mustStayAsWritten(const QueryTreeNodePtr & expr)
    {
        std::vector<const ColumnNode *> columns;
        std::unordered_set<const IQueryTreeNode *> visited;
        collectColumnNodes(expr, columns, visited);

        for (const auto * column_node : columns)
        {
            auto source = column_node->getColumnSourceOrNull();
            if (source && arrays_by_lambda.contains(source.get()))
            {
                if (lambdaRunsOverDeclaredArray(source.get()))
                    return true;
                continue;
            }

            const auto * declared = declarationsFor(*column_node);
            if (declared && declared->columns.contains(column_node->getColumnName()))
                return true;
        }
        return false;
    }

    /// Looked up once per table and query: most queries never reach this.
    std::unordered_map<const IStorage *, DeclaredComparisons> declarations_by_storage;
    std::unordered_map<const IQueryTreeNode *, QueryTreeNodes> arrays_by_lambda;
    std::unordered_map<const IQueryTreeNode *, bool> lambda_over_declared_array;
};

}

void ConvertEmptyStringComparisonToFunctionPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    ConvertEmptyStringComparisonToFunctionVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
