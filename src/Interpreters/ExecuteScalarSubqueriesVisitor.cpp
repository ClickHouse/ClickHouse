#include <Interpreters/ExecuteScalarSubqueriesVisitor.h>

#include <Analyzer/Utils.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Core/ProtocolDefines.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/stripQuerySettings.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Common/FieldVisitorToString.h>
#include <Common/ProfileEvents.h>
#include <Common/config_version.h>

#include <array>
#include <string_view>

namespace ProfileEvents
{
extern const Event ScalarSubqueriesGlobalCacheHit;
extern const Event ScalarSubqueriesLocalCacheHit;
extern const Event ScalarSubqueriesCacheMiss;
}

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool enable_scalar_subquery_optimization;
    extern const SettingsBool extremes;
    extern const SettingsUInt64 interactive_delay;
    extern const SettingsUInt64 max_result_rows;
    extern const SettingsBool use_concurrency_control;
    extern const SettingsUInt64 use_structure_from_insertion_table_in_table_functions;
    extern const SettingsString implicit_table_at_top_level;
}

namespace ErrorCodes
{
    extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
}


bool ExecuteScalarSubqueriesMatcher::needChildVisit(ASTPtr & node, const ASTPtr & child)
{
    /// Processed
    if (node->as<ASTSubquery>() || node->as<ASTFunction>())
        return false;

    /// Don't descend into subqueries in FROM section
    if (node->as<ASTTableExpression>())
        return false;

    /// Do not go to subqueries defined in with statement
    if (node->as<ASTWithElement>())
        return false;

    if (node->as<ASTSelectQuery>())
    {
        /// Do not go to FROM, JOIN, UNION.
        if (child->as<ASTTableExpression>() || child->as<ASTSelectQuery>())
            return false;
    }

    if (auto * tables = node->as<ASTTablesInSelectQueryElement>())
    {
        /// Contrary to what's said in the code block above, ARRAY JOIN needs to resolve the subquery if possible
        /// and assign an alias for 02367_optimize_trivial_count_with_array_join to pass. Otherwise it will fail in
        /// ArrayJoinedColumnsVisitor (`No alias for non-trivial value in ARRAY JOIN: _a`)
        /// This looks 100% as a incomplete code working on top of a bug, but this code has already been made obsolete
        /// by the analyzer, so it's an inconvenience we can live with until we deprecate it.
        if (child == tables->array_join)
            return true;
        return false;
    }

    return true;
}

void ExecuteScalarSubqueriesMatcher::visit(ASTPtr & ast, Data & data)
{
    if (const auto * t = ast->as<ASTSubquery>())
        visit(*t, ast, data);
    if (const auto * t = ast->as<ASTFunction>())
        visit(*t, ast, data);
}

static auto getQueryInterpreter(const ASTSubquery & subquery, ExecuteScalarSubqueriesMatcher::Data & data)
{
    auto subquery_context = Context::createCopy(data.getContext());
    Settings subquery_settings = data.getContext()->getSettingsCopy();
    subquery_settings[Setting::max_result_rows] = 1;
    subquery_settings[Setting::extremes] = false;
    subquery_settings[Setting::implicit_table_at_top_level] = "";
    /// `QueryAnalyzer` reads this one from the scope context, which the query context below does not reach.
    subquery_settings[Setting::use_structure_from_insertion_table_in_table_functions] = false;
    /// `Planner`'s constructor inspects the subquery tree for parallel replica candidates.
    subquery_settings[Setting::allow_experimental_parallel_reading_from_replicas] = 0;
    subquery_context->setSettings(subquery_settings);

    /// A standalone expression - a `CHECK` constraint, a `TTL` expression - is analysed with the global
    /// context: `StorageFactory` hands the storage `args.getContext()`, which never went through
    /// `makeQueryContext` and so carries a zero client version. This server is the real initiator of the
    /// subquery, so fill in its own version, exactly as `Context::makeQueryContext` does for the other
    /// server-initiated queries. Otherwise reading a `Distributed` table here sends a shard query that
    /// `RemoteQueryExecutor::sendQueryUnlocked` rejects. A context of a real query keeps its client info.
    const auto & client_info = subquery_context->getClientInfo();
    if (client_info.client_version_major == 0 && client_info.client_version_minor == 0 && client_info.client_version_patch == 0)
        subquery_context->setClientVersion(VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH, DBMS_TCP_PROTOCOL_VERSION);

    if (subquery_context->hasQueryContext())
    {
        /// When execute `INSERT INTO t WITH ... SELECT ...`, it may lead to `Unknown columns`
        /// exception with this settings enabled(https://github.com/ClickHouse/ClickHouse/issues/52494).
        subquery_context->getQueryContext()->setSetting("use_structure_from_insertion_table_in_table_functions", false);
        if (!data.only_analyze)
        {
            /// Save current cached scalars in the context before analyzing the query
            /// This is specially helpful when analyzing CTE scalars
            auto context = subquery_context->getQueryContext();
            for (const auto & it : data.scalars)
                context->addScalar(it.first, it.second);
        }
    }

    /// `QueryTreeBuilder` re-applies a SELECT's own `SETTINGS` clause over the node context `Planner` reads
    /// for that decision; the AST belongs to the analysed statement, whose text is persisted, so strip a clone.
    static constexpr std::array parallel_replica_settings{
        std::string_view{"allow_experimental_parallel_reading_from_replicas"},
        std::string_view{"enable_parallel_replicas"},
    };
    ASTPtr subquery_select = subquery.children.at(0)->clone();
    removeSettingsFromQuery(subquery_select, parallel_replica_settings);

    auto options = SelectQueryOptions(QueryProcessingStage::Complete, data.subquery_depth + 1, true);
    options.is_create_parameterized_view = data.is_create_parameterized_view;
    options.analyze(data.only_analyze);
    /// `collectMaterializedCTEs` returns nothing for subquery options unless materialization is forced.
    options.forceMaterializeCTE();

    return std::make_unique<InterpreterSelectQueryAnalyzer>(
        subquery_select, subquery_context, options, subquery_context->getViewSource());
}

static bool subqueryUsesViewSource(const InterpreterSelectQueryAnalyzer & interpreter, const ContextPtr & context)
{
    auto view_source = context->getViewSource();
    return view_source && isStorageUsedInTree(view_source, interpreter.getQueryTree().get());
}

void ExecuteScalarSubqueriesMatcher::visit(const ASTSubquery & subquery, ASTPtr & ast, Data & data)
{
    /// subquery and ast can be the same object and ast will be moved.
    /// Save these fields to avoid use after move.
    String subquery_alias = subquery.alias;
    bool prefer_alias_to_column_name = subquery.preferAliasToColumnName();

    auto hash = subquery.getTreeHash(/*ignore_aliases=*/ true);
    const auto scalar_query_hash_str = toString(hash);

    std::unique_ptr<InterpreterSelectQueryAnalyzer> interpreter;
    bool hit = false;
    bool is_local = false;

    Block scalar;
    if (data.only_analyze)
    {
        /// Don't use scalar cache during query analysis
    }
    else if (data.local_scalars.contains(scalar_query_hash_str))
    {
        hit = true;
        scalar = data.local_scalars[scalar_query_hash_str];
        is_local = true;
        ProfileEvents::increment(ProfileEvents::ScalarSubqueriesLocalCacheHit);
    }
    else if (data.scalars.contains(scalar_query_hash_str))
    {
        hit = true;
        scalar = data.scalars[scalar_query_hash_str];
        ProfileEvents::increment(ProfileEvents::ScalarSubqueriesGlobalCacheHit);
    }
    else
    {
        if (data.getContext()->hasQueryContext() && data.getContext()->getQueryContext()->hasScalar(scalar_query_hash_str))
        {
            if (!data.getContext()->getViewSource())
            {
                /// We aren't using storage views so we can safely use the context cache
                scalar = data.getContext()->getQueryContext()->getScalar(scalar_query_hash_str);
                ProfileEvents::increment(ProfileEvents::ScalarSubqueriesGlobalCacheHit);
                hit = true;
            }
            else
            {
                /// If we are under a context that uses views that means that the cache might contain values that reference
                /// the original table and not the view, so in order to be able to check the global cache we need to first
                /// make sure that the query doesn't use the view
                /// Note in any case the scalar will end up cached in *data* so this won't be repeated inside this context
                interpreter = getQueryInterpreter(subquery, data);
                if (!subqueryUsesViewSource(*interpreter, data.getContext()))
                {
                    scalar = data.getContext()->getQueryContext()->getScalar(scalar_query_hash_str);
                    ProfileEvents::increment(ProfileEvents::ScalarSubqueriesGlobalCacheHit);
                    hit = true;
                }
            }
        }
    }

    if (!hit)
    {
        if (!interpreter)
            interpreter = getQueryInterpreter(subquery, data);

        ProfileEvents::increment(ProfileEvents::ScalarSubqueriesCacheMiss);
        is_local = subqueryUsesViewSource(*interpreter, data.getContext());

        Block block;

        if (data.only_analyze)
        {
            /// If query is only analyzed, then constants are not correct.
            block = *interpreter->getSampleBlock();
            for (auto & column : block)
            {
                if (column.column->empty())
                {
                    auto mut_col = column.column->cloneEmpty();
                    /// The value is a placeholder, but it still takes part in the analysis of the enclosing
                    /// expression: a header is computed by executing the functions over it, so a `NULL`
                    /// placeholder makes a conversion to a non-Nullable type throw. A `Nullable` column
                    /// therefore gets the default of its nested type, not `NULL`; the only exception is
                    /// `Nothing`, which has no value other than `NULL`.
                    auto nested_type = removeNullable(removeLowCardinality(column.type));
                    if (isNothing(nested_type))
                        mut_col->insertDefault();
                    else
                        mut_col->insert(nested_type->getDefault());
                    column.column = std::move(mut_col);
                }
            }
        }
        else
        {
            auto io = interpreter->execute();

            PullingAsyncPipelineExecutor executor(io.pipeline);
            io.pipeline.setProgressCallback(data.getContext()->getProgressCallback());
            io.pipeline.setConcurrencyControl(data.getContext()->getSettingsRef()[Setting::use_concurrency_control]);
            if (auto cancel_cb = data.getContext()->hasQueryContext() ? data.getContext()->getQueryContext()->getInteractiveCancelCallback() : nullptr)
                executor.setCancelCallback(std::move(cancel_cb), std::max(UInt64(100), data.getContext()->getSettingsRef()[Setting::interactive_delay] / 1000));

            while (block.rows() == 0 && executor.pull(block))
            {
            }

            if (block.rows() == 0)
            {
                auto types = interpreter->getSampleBlock()->getDataTypes();
                if (types.size() != 1)
                    types = {std::make_shared<DataTypeTuple>(types)};

                auto & type = types[0];
                if (!type->isNullable())
                {
                    if (!type->canBeInsideNullable())
                        throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY,
                                        "Scalar subquery returned empty result of type {} which cannot be Nullable",
                                        type->getName());

                    type = makeNullable(type);
                }

                ASTPtr ast_new = make_intrusive<ASTLiteral>(Null());
                ast_new = addTypeConversionToAST(std::move(ast_new), type->getName());

                ast_new->setAlias(ast->tryGetAlias());
                ast = std::move(ast_new);

                /// Empty subquery result is equivalent to NULL
                block = interpreter->getSampleBlock()->cloneEmpty();
                String column_name = block.columns() > 0 ?  block.safeGetByPosition(0).name : "dummy";
                block = Block({
                    ColumnWithTypeAndName(type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst(), type, column_name)
                });
            }

            if (block.rows() != 1)
                throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar subquery returned more than one row");

            Block tmp_block;
            while (tmp_block.rows() == 0 && executor.pull(tmp_block))
            {
            }

            if (tmp_block.rows() != 0)
                throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar subquery returned more than one row");

            logProcessorProfile(data.getContext(), io.pipeline.getProcessors());

            /// Finalize write in query cache to save scalar subquery result (no-op if no cache writers exist in the pipeline)
            io.pipeline.finalizeWriteInQueryResultCache();
        }

        block = materializeBlock(block);
        size_t columns = block.columns();

        if (columns == 1)
        {
            auto & column = block.getByPosition(0);
            /// Here we wrap type to nullable if we can.
            /// It is needed cause if subquery return no rows, it's result will be Null.
            /// In case of many columns, do not check it cause tuple can't be nullable.
            if (!column.type->isNullable() && column.type->canBeInsideNullable())
            {
                column.type = makeNullable(column.type);
                column.column = makeNullable(column.column);
            }
            scalar = block;
        }
        else
        {
            scalar.insert({
                ColumnTuple::create(block.getColumns()),
                std::make_shared<DataTypeTuple>(block.getDataTypes()),
                "tuple"});
        }
    }

    const Settings & settings = data.getContext()->getSettingsRef();

    // Always convert to literals when there is no query context.
    if (data.only_analyze || !settings[Setting::enable_scalar_subquery_optimization] || worthConvertingScalarToLiteral(scalar, data.max_literal_size)
        || !data.getContext()->hasQueryContext())
    {
        auto lit = make_intrusive<ASTLiteral>((*scalar.safeGetByPosition(0).column)[0]);
        lit->alias = subquery_alias;
        lit->setPreferAliasToColumnName(prefer_alias_to_column_name);
        ast = addTypeConversionToAST(std::move(lit), scalar.safeGetByPosition(0).type->getName());

        /// If only analyze was requested the expression is not suitable for constant folding, disable it.
        if (data.only_analyze)
        {
            ast->as<ASTFunction>()->alias.clear();
            auto func = makeASTFunction("__scalarSubqueryResult", std::move(ast));
            func->alias = subquery_alias;
            func->setPreferAliasToColumnName(prefer_alias_to_column_name);
            ast = std::move(func);
        }
    }
    else if (!data.replace_only_to_literals)
    {
        auto func = makeASTFunction("__getScalar", make_intrusive<ASTLiteral>(scalar_query_hash_str));
        func->alias = subquery_alias;
        func->setPreferAliasToColumnName(prefer_alias_to_column_name);
        ast = std::move(func);
    }

    if (is_local)
        data.local_scalars[scalar_query_hash_str] = std::move(scalar);
    else
        data.scalars[scalar_query_hash_str] = std::move(scalar);
}

void ExecuteScalarSubqueriesMatcher::visit(const ASTFunction & func, ASTPtr & ast, Data & data)
{
    /// Don't descend into subqueries in arguments of IN operator.
    /// But if an argument is not subquery, then deeper may be scalar subqueries and we need to descend in them.

    std::vector<ASTPtr *> out;
    if (checkFunctionIsInOrGlobalInOperator(func))
    {
        for (auto & child : ast->children)
        {
            if (child != func.arguments)
                out.push_back(&child);
            else
                for (size_t i = 0, size = func.arguments->children.size(); i < size; ++i)
                    if (i != 1 || !func.arguments->children[i]->as<ASTSubquery>())
                        out.push_back(&func.arguments->children[i]);
        }
    }
    else if (func.name == "exists")
    {
        /// Since exists does not use parameters, and the only
        /// argument to exists function is a subquery, out
        /// should not have arguments. Thus, the following lines could
        /// probably be changed with just `return`. However, we follow
        /// the style that is provided in the first if.
        for (auto & child : ast->children)
        {
            if (child != func.arguments)
                out.push_back(&child);
            else
                for (size_t i = 0, size = func.arguments->children.size(); i < size; ++i)
                    if (i != 0 || !func.arguments->children[i]->as<ASTSubquery>())
                        out.push_back(&func.arguments->children[i]);
        }
    }
    else
        for (auto & child : ast->children)
            out.push_back(&child);

    for (ASTPtr * add_node : out)
        Visitor(data).visit(*add_node);
}

static size_t getSizeOfSerializedLiteral(const Field & field)
{
    auto field_str = applyVisitor(FieldVisitorToString(), field);
    return field_str.size();
}

bool worthConvertingScalarToLiteral(const Block & scalar, std::optional<size_t> max_literal_size)
{
    /// Converting to literal values might take a fair amount of overhead when the value is large, (e.g.
    /// Array, BitMap, etc.), This conversion is required for constant folding, index lookup, branch
    /// elimination. However, these optimizations should never be related to large values, thus we blacklist them here.
    const auto * scalar_type_name = scalar.safeGetByPosition(0).type->getFamilyName();
    static const std::set<std::string_view> maybe_large_literal_types = {"Array", "Tuple", "AggregateFunction", "Function", "Set", "LowCardinality"};

    if (!maybe_large_literal_types.contains(scalar_type_name))
        return true;

    if (!max_literal_size)
        return false;

    /// Size of serialized literal cannot be less than size in bytes.
    if (scalar.bytes() > *max_literal_size)
        return false;

    return getSizeOfSerializedLiteral((*scalar.safeGetByPosition(0).column)[0]) <= *max_literal_size;
}

}
