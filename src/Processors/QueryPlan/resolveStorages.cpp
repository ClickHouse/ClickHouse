#include <Processors/QueryPlan/resolveStorages.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Processors/QueryPlan/ReadFromTableFunctionStep.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/Sources/NullSource.h>

#include <Analyzer/Resolve/IdentifierResolver.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>

#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Interpreters/Context.h>

#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <Storages/IStorage.h>
#include <Storages/getEffectiveRowPolicyFilter.h>
#include <Planner/Utils.h>
#include <Core/Settings.h>

#include <stack>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsSetOperationMode except_default_mode;
    extern const SettingsSetOperationMode intersect_default_mode;
    extern const SettingsSetOperationMode union_default_mode;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUInt64 max_columns_to_read;
}

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int UNKNOWN_TABLE;
    extern const int CANNOT_PARSE_TEXT;
    extern const int TOO_MANY_COLUMNS;
}

Identifier parseTableIdentifier(const std::string & str, const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();

    Tokens tokens(str.data(), str.data() + str.size(), settings[Setting::max_query_size]);
    IParser::Pos pos(tokens, static_cast<unsigned>(settings[Setting::max_parser_depth]), static_cast<unsigned>(settings[Setting::max_parser_backtracks]));
    Expected expected;

    ParserCompoundIdentifier parser(false, false);
    ASTPtr res;
    if (!parser.parse(pos, res, expected))
        throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Cannot parse itable identifier ({})", str);

    return Identifier(std::move(res->as<ASTIdentifier>()->name_parts));
}

std::shared_ptr<TableNode> resolveTable(const Identifier & identifier, const ContextPtr & context)
{
    auto resolve_result = IdentifierResolver::tryResolveTableIdentifier(identifier, context);
    if (!resolve_result)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Unknown table {}", identifier.getFullName());

    return resolve_result;
}

static QueryTreeNodePtr resolveTableFunction(const ASTPtr & table_function, const ContextPtr & context)
{
    QueryTreeNodePtr query_tree_node = buildTableFunctionQueryTree(table_function, context);

    bool only_analyze = false;
    QueryAnalyzer analyzer(only_analyze);
    analyzer.resolve(query_tree_node, nullptr, context);

    return query_tree_node;
}

static ASTPtr makeASTForReadingColumns(const Names & names, ASTPtr table_expression)
{
    auto select = make_intrusive<ASTSelectQuery>();
    auto columns = make_intrusive<ASTExpressionList>();
    for (const auto & name : names)
        columns->children.push_back(make_intrusive<ASTIdentifier>(name));

    auto tables = make_intrusive<ASTTablesInSelectQuery>();
    auto table_element = make_intrusive<ASTTablesInSelectQueryElement>();
    table_element->children.push_back(table_expression);
    table_element->table_expression = std::move(table_expression);
    tables->children.push_back(std::move(table_element));

    select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(columns));
    select->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));

    return select;
}

static ASTPtr wrapWithUnion(ASTPtr select)
{
    auto select_with_union = make_intrusive<ASTSelectWithUnionQuery>();
    auto selects = make_intrusive<ASTExpressionList>();
    selects->children.push_back(select);
    select_with_union->list_of_selects = selects;
    select_with_union->children.push_back(select_with_union->list_of_selects);

    return select_with_union;
}

using ParentNodes = std::unordered_map<const QueryPlan::Node *, QueryPlan::Node *>;

/// The node `steps` steps above `node`, if it and the nodes between are expression or filter steps with one child each.
static QueryPlan::Node * findStepAbove(const QueryPlan::Node & node, UInt64 steps, const ParentNodes & parents)
{
    const QueryPlan::Node * current = &node;
    QueryPlan::Node * found = nullptr;
    for (UInt64 i = 0; i < steps; ++i)
    {
        auto it = parents.find(current);
        if (it == parents.end() || it->second->children.size() != 1
            || (!typeid_cast<const ExpressionStep *>(it->second->step.get()) && !typeid_cast<const FilterStep *>(it->second->step.get())))
            return nullptr;
        current = found = it->second;
    }
    return found;
}

/// A step that computes the columns of the filter step `filter` applies on the sender, but keeps every row, if that
/// step is where `filter` says.
static std::unique_ptr<ExpressionStep> makeStepWithoutAdditionalFilter(
    const QueryPlan::Node & node, const ReadFromTableStep::AdditionalFilter & filter, const ParentNodes & parents)
{
    const auto * filter_node = findStepAbove(node, filter.steps_above, parents);
    const auto * filter_step = filter_node ? typeid_cast<const FilterStep *>(filter_node->step.get()) : nullptr;
    if (!filter_step || !filter_step->removesFilterColumn() || filter_step->getFilterColumnName() != filter.filter_column)
        return {};

    auto actions = filter_step->getExpression().clone();
    auto & outputs = actions.getOutputs();
    auto filter_output = std::ranges::find_if(outputs, [&](const auto * output) { return output->result_name == filter.filter_column; });
    if (filter_output == outputs.end())
        return {};
    outputs.erase(filter_output);
    /// The read applies the filter with its `arrayJoin`, which `removeUnusedActions` would keep here as well.
    std::unordered_set<const ActionsDAG::Node *> array_joins;
    for (const auto & action : actions.getNodes())
        if (action.type == ActionsDAG::ActionType::ARRAY_JOIN)
            array_joins.insert(&action);
    actions.removeNodes(array_joins);
    actions.removeUnusedActions(/*allow_remove_inputs=*/ false);
    if (actions.hasArrayJoin())
        return {};

    auto step = std::make_unique<ExpressionStep>(filter_step->getInputHeaders().front(), std::move(actions));
    if (!blocksHaveEqualStructure(*step->getOutputHeader(), *filter_step->getOutputHeader()))
        return {};
    return step;
}

/// A step that applies `filter` in place of a filter step with these headers, if the columns `filter` reads are there,
/// with the same types.
static std::unique_ptr<FilterStep>
makeFilterStepInPlaceOf(const SharedHeader & input_header, const Block & output_header, const FilterDAGInfo & filter)
{
    auto actions = filter.actions.clone();
    std::unordered_map<std::string_view, const ActionsDAG::Node *> inputs;
    for (const auto * input : actions.getInputs())
    {
        const auto * column = input_header->findByName(input->result_name);
        if (!column || !column->type->equals(*input->result_type))
            return {};
        inputs.emplace(input->result_name, input);
    }

    ActionsDAG::NodeRawConstPtrs outputs{&actions.findInOutputs(filter.column_name)};
    for (const auto & column : *input_header)
        if (!inputs.contains(column.name))
            inputs.emplace(column.name, &actions.addInput(column.name, column.type));
    for (const auto & column : output_header)
    {
        auto it = inputs.find(column.name);
        if (it == inputs.end())
            return {};
        outputs.push_back(it->second);
    }
    actions.getOutputs() = std::move(outputs);

    auto step = std::make_unique<FilterStep>(input_header, std::move(actions), filter.column_name, filter.do_remove_column);
    if (!blocksHaveEqualStructure(*step->getOutputHeader(), output_header))
        return {};
    return step;
}

/// The expression or filter step `step` over `input_header`, passing on its columns `carried` as well.
static QueryPlanStepPtr makeStepCarryingColumns(const IQueryPlanStep & step, const SharedHeader & input_header, const NameSet & carried)
{
    auto carry = [&](ActionsDAG actions)
    {
        auto & outputs = actions.getOutputs();
        for (const auto * input : actions.getInputs())
            if (carried.contains(input->result_name)
                && std::ranges::none_of(outputs, [&](const auto * output) { return output->result_name == input->result_name; }))
                outputs.push_back(input);
        return actions;
    };

    QueryPlanStepPtr result;
    if (const auto * expression = typeid_cast<const ExpressionStep *>(&step))
        result = std::make_unique<ExpressionStep>(input_header, carry(expression->getExpression().clone()));
    else
    {
        const auto & filter = typeid_cast<const FilterStep &>(step);
        result = std::make_unique<FilterStep>(
            input_header, carry(filter.getExpression().clone()), filter.getFilterColumnName(), filter.removesFilterColumn());
    }
    result->setStepDescription(step);
    return result;
}

static QueryPlanResourceHolder replaceReadingFromTable(
    QueryPlan::Node & node,
    QueryPlan::Nodes & nodes,
    const ContextPtr & context,
    const ParentNodes & parents,
    std::unordered_set<const FutureSet *> & removed_filter_sets)
{
    const auto * reading_from_table = typeid_cast<const ReadFromTableStep *>(node.step.get());
    const auto * reading_from_table_function = typeid_cast<const ReadFromTableFunctionStep *>(node.step.get());
    if (!reading_from_table && !reading_from_table_function)
        return {};

    const auto & header = node.step->getOutputHeader();
    auto column_names = header->getNames();

    StoragePtr storage;
    StorageSnapshotPtr snapshot;
    SelectQueryInfo select_query_info;
    ASTPtr table_function_ast;
    std::shared_ptr<TableNode> read_table_node;

    if (reading_from_table)
    {
        Identifier identifier = parseTableIdentifier(reading_from_table->getTable(), context);
        read_table_node = resolveTable(identifier, context);

        storage = read_table_node->getStorage();
        snapshot = read_table_node->getStorageSnapshot();
        select_query_info.table_expression_modifiers = reading_from_table->getTableExpressionModifiers();
    }
    else
    {
        auto serialized_ast = reading_from_table_function->getSerializedAST();
        ParserFunction parser(false, true);
        const auto & settings = context->getSettingsRef();
        table_function_ast = parseQuery(
            parser,
            serialized_ast,
            settings[Setting::max_query_size],
            settings[Setting::max_parser_depth],
            DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

        {
            SelectIntersectExceptQueryVisitor::Data data{settings[Setting::intersect_default_mode], settings[Setting::except_default_mode]};
            SelectIntersectExceptQueryVisitor{data}.visit(table_function_ast);
        }

        {
            /// Normalize SelectWithUnionQuery
            NormalizeSelectWithUnionQueryVisitor::Data data{settings[Setting::union_default_mode]};
            NormalizeSelectWithUnionQueryVisitor{data}.visit(table_function_ast);
        }

        auto query_tree_node = resolveTableFunction(table_function_ast, context);
        if (auto * table_function_node = query_tree_node->as<TableFunctionNode>())
        {
            storage = table_function_node->getStorage();
            snapshot = table_function_node->getStorageSnapshot();
            /// Carry the resolved table-function subtree so that the aggregation hash-table-stats
            /// cache key (see calculateHashTableCacheKeys) can tell different arguments apart. A
            /// table-function storage has no UUID and its StorageID does not depend on the arguments
            /// (any numbers(N) reads from `_table_function.numbers`), so without this the key would
            /// hash only the storage name and e.g. numbers(1) and numbers(1e6) would collide after a
            /// serialized-plan round-trip (distributed / serialize_query_plan). This mirrors the
            /// analyzer read path, which sets table_expression in PlannerJoinTree.
            /// FINAL / SAMPLE are serialized out-of-band from the AST (see ReadFromTableFunctionStep::serialize),
            /// so the node parsed back from serialized_ast does not carry them; copy them onto the node so the
            /// key also tells modifier-bearing reads apart.
            if (auto modifiers = reading_from_table_function->getTableExpressionModifiers(); modifiers != TableExpressionModifiers{})
                table_function_node->setTableExpressionModifiers(std::move(modifiers));
            select_query_info.table_expression = static_pointer_cast<ITableExpressionNode>(query_tree_node);
        }
        else if (auto * table_node = query_tree_node->as<TableNode>())
        {
            storage = table_node->getStorage();
            snapshot = table_node->getStorageSnapshot();
        }
        else
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Unexpected query tree node type {}\n{}",
                query_tree_node->getNodeTypeName(),
                query_tree_node->dumpTree());

        select_query_info.table_expression_modifiers = reading_from_table_function->getTableExpressionModifiers();
    }

    if (select_query_info.table_expression_modifiers)
        snapshot = snapshot->clone(extendMetadataWithModifiers(snapshot->metadata, *select_query_info.table_expression_modifiers), snapshot->data);

    auto table_lock = storage->lockForShare(context->getInitialQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);

    /// The `SelectQueryInfo` built above has no query: the step carries only a table name and the
    /// table expression modifiers. Reads that need one get a `SELECT` over that table synthesized
    /// and re-analyzed below; the rest read the storage directly.
    const bool read_via_interpreter = storage->readRequiresAnalyzedQuery();

    ASTPtr query;
    /// A policy that no step of the shipped plan applies is applied by planning the read again here.
    bool needs_row_policy = false;
    /// A read that needs an analyzed query is planned again below with its own options, which apply the policy.
    if (reading_from_table && !read_via_interpreter && getEffectiveRowPolicyFilter(*storage, context))
    {
        switch (reading_from_table->getRowPolicyPlacement())
        {
            case ReadFromTableStep::RowPolicyPlacement::NotInPlan:
                needs_row_policy = true;
                break;
            case ReadFromTableStep::RowPolicyPlacement::FilterStep:
                break;
            /// A sender that does not record the placement pushes the policy for sure only under a nullopt contract.
            case ReadFromTableStep::RowPolicyPlacement::Unknown:
                needs_row_policy = storage->supportsPrewhere() && !storage->supportedPrewhereColumns().has_value();
                break;
        }
    }
    bool replan_through_interpreter = read_via_interpreter || needs_row_policy;
    if (replan_through_interpreter)
    {
        auto table_expression = make_intrusive<ASTTableExpression>();
        if (table_function_ast)
        {
            table_expression->children.push_back(table_function_ast);
            table_expression->table_function = std::move(table_function_ast);
        }
        else
        {
            const auto & table_id = storage->getStorageID();
            auto table_identifier = make_intrusive<ASTTableIdentifier>(table_id.database_name, table_id.table_name);
            table_expression->children.push_back(table_identifier);
            table_identifier->uuid = table_id.uuid;
            table_expression->database_and_table_name = std::move(table_identifier);
        }

        /// Restore FINAL / SAMPLE onto the reconstructed table expression: they are serialized separately
        /// from the table name / table function AST (see ReadFromTableStep::serialize and
        /// ReadFromTableFunctionStep::serialize), and this AST is all InterpreterSelectQueryAnalyzer sees,
        /// so without this the modifiers would be silently dropped from the read.
        if (select_query_info.table_expression_modifiers)
        {
            const auto & modifiers = *select_query_info.table_expression_modifiers;
            table_expression->final = modifiers.hasFinal();
            if (modifiers.hasSampleSizeRatio())
            {
                table_expression->sample_size = make_intrusive<ASTSampleRatio>(*modifiers.getSampleSizeRatio());
                table_expression->children.push_back(table_expression->sample_size);
            }
            if (modifiers.hasSampleOffsetRatio())
            {
                table_expression->sample_offset = make_intrusive<ASTSampleRatio>(*modifiers.getSampleOffsetRatio());
                table_expression->children.push_back(table_expression->sample_offset);
            }
        }

        query = makeASTForReadingColumns(column_names, std::move(table_expression));
        // std::cerr << query->dumpTree() << std::endl;
    }

    QueryPlan reading_plan;
    std::unique_ptr<ExpressionStep> step_without_additional_filter;
    std::shared_ptr<FilterDAGInfo> additional_filter_actions;
    /// The columns the read gives to the steps above it.
    SharedHeader read_header = header;
    if (replan_through_interpreter)
    {
        SelectQueryOptions options(QueryProcessingStage::FetchColumns);
        options.ignore_rename_columns = true;

        /// A sender that records the placement planned this read itself, as does one whose policy is replanned here.
        const auto placement = reading_from_table ? reading_from_table->getRowPolicyPlacement() : ReadFromTableStep::RowPolicyPlacement::Unknown;
        if (needs_row_policy || placement != ReadFromTableStep::RowPolicyPlacement::Unknown)
        {
            /// The planner builds the filter of the entry of this node, evaluated here, and of the entry of the sender, unless
            /// the two entries are the same, and it replaces the filter step that applied the entry of the sender there. If
            /// that step is not found, it stays and stands for an entry of this node that is the same. Without that step,
            /// the filter runs after the steps of the read.
            std::optional<String> sender_additional_filter;
            bool additional_filter_in_plan = false;
            if (const auto & additional_filter = reading_from_table->getAdditionalFilter())
            {
                sender_additional_filter = additional_filter->text;
                step_without_additional_filter = makeStepWithoutAdditionalFilter(node, *additional_filter, parents);
                additional_filter_in_plan = !step_without_additional_filter;
            }
            if (step_without_additional_filter || reading_from_table->getFilterSteps())
                additional_filter_actions = std::make_shared<FilterDAGInfo>();

            /// `column_names` also has the columns of the filters above the read, so the grants and the limit are checked
            /// on the columns the query selects. A plain table is not checked, as with `storage->read()` below.
            if (read_via_interpreter)
            {
                checkAccessRights(
                    storage, storage->getStorageID(), snapshot, reading_from_table->getSelectedColumns().value_or(column_names), context);

                const auto & settings = context->getSettingsRef();
                /// The filter step above the read uses the columns of this filter, so they need the grants here as well.
                /// The planner checks them for a filter it applies itself.
                if (const auto & additional_filter = reading_from_table->getAdditionalFilter(); additional_filter && additional_filter_in_plan)
                {
                    ParserExpression parser;
                    auto filter_ast = parseQuery(
                        parser,
                        additional_filter->text,
                        settings[Setting::max_query_size],
                        settings[Setting::max_parser_depth],
                        settings[Setting::max_parser_backtracks]);
                    buildFilterQueryTree(std::move(filter_ast), read_table_node, context, /*check_access_rights=*/ true);
                }

                const size_t columns_to_read = reading_from_table->getColumnsToRead().value_or(column_names.size());
                if (settings[Setting::max_columns_to_read] && columns_to_read > settings[Setting::max_columns_to_read])
                    throw Exception(
                        ErrorCodes::TOO_MANY_COLUMNS,
                        "Limit for number of columns to read exceeded. Requested: {}, maximum: {}",
                        columns_to_read,
                        settings[Setting::max_columns_to_read].value);
            }
            options.shipped_read = SelectQueryOptions::ShippedRead{
                .row_policy_in_plan = placement == ReadFromTableStep::RowPolicyPlacement::FilterStep,
                .additional_filter = std::move(sender_additional_filter),
                .additional_filter_in_plan = additional_filter_in_plan,
                .additional_filter_actions = additional_filter_actions};
        }

        auto interpreter_context = context;
        if (needs_row_policy)
        {
            auto row_policy_context = Context::createCopy(context);
            /// Parallelism of the read is decided by the step, not by this node's setting.
            row_policy_context->setSetting(
                "allow_experimental_parallel_reading_from_replicas",
                reading_from_table && reading_from_table->useParallelReplicas());
            interpreter_context = std::move(row_policy_context);
        }

        InterpreterSelectQueryAnalyzer interpreter(wrapWithUnion(std::move(query)), interpreter_context, options);
        reading_plan = std::move(interpreter).extractQueryPlan();
        reading_plan.addInterpreterContext(std::move(interpreter_context));

        /// The filter runs in place of the sender's step, or else after the last step of the read, and the columns it needs
        /// that do not get there are carried from the read. If that fails, it runs on the read, and the sender's step keeps
        /// every row, or stays as it is over the sender's policy step, which must run first.
        if (additional_filter_actions && !additional_filter_actions->column_name.empty())
        {
            auto & filter = *additional_filter_actions;
            const bool in_place = step_without_additional_filter != nullptr;
            QueryPlan::Node * filter_node = findStepAbove(
                node, in_place ? reading_from_table->getAdditionalFilter()->steps_above : reading_from_table->getFilterSteps(), parents);
            const auto parent = filter_node ? parents.find(filter_node) : parents.end();

            NameSet carried;
            Block carried_header = *header;
            std::vector<std::pair<QueryPlan::Node *, QueryPlanStepPtr>> carrying_steps;
            UsefulSets filter_sets;
            UsefulSets kept_sets;
            QueryPlanStepPtr replacement;
            if (filter_node && (in_place || parent != parents.end()))
            {
                SharedHeader filter_input_header
                    = in_place ? filter_node->step->getInputHeaders().front() : filter_node->step->getOutputHeader();
                for (const auto * input : filter.actions.getInputs())
                {
                    if (filter_input_header->has(input->result_name))
                        continue;
                    carried.insert(input->result_name);
                    const auto * column = reading_plan.getCurrentHeader()->findByName(input->result_name);
                    if (column && !carried_header.has(column->name))
                        carried_header.insert({column->type->createColumn(), column->type, column->name});
                }

                if (!carried.empty())
                {
                    auto converting = ActionsDAG::makeConvertingActions(
                        reading_plan.getCurrentHeader()->getColumnsWithTypeAndName(),
                        carried_header.getColumnsWithTypeAndName(),
                        ActionsDAG::MatchColumnsMode::Name,
                        context);
                    filter_input_header = std::make_shared<const Block>(converting.updateHeader(*reading_plan.getCurrentHeader()));
                    const QueryPlan::Node * end = in_place ? filter_node : parent->second;
                    for (auto * current = parents.at(&node); current != end; current = parents.at(current))
                    {
                        carrying_steps.emplace_back(current, makeStepCarryingColumns(*current->step, filter_input_header, carried));
                        filter_input_header = carrying_steps.back().second->getOutputHeader();
                    }
                }

                if (in_place)
                    appendSetsFromActionsDAG(typeid_cast<const FilterStep &>(*filter_node->step).getExpression(), filter_sets);
                replacement = makeFilterStepInPlaceOf(filter_input_header, *filter_node->step->getOutputHeader(), filter);
            }

            if (replacement)
            {
                for (auto & [current, step] : carrying_steps)
                    current->step = std::move(step);
                if (!carried.empty())
                    read_header = std::make_shared<const Block>(std::move(carried_header));
            }
            else
            {
                reading_plan.addStep(std::make_unique<FilterStep>(
                    reading_plan.getCurrentHeader(), std::move(filter.actions), filter.column_name, filter.do_remove_column));
                if (in_place && placement != ReadFromTableStep::RowPolicyPlacement::FilterStep)
                {
                    appendSetsFromActionsDAG(step_without_additional_filter->getExpression(), kept_sets);
                    replacement = std::move(step_without_additional_filter);
                }
            }

            if (replacement && in_place)
            {
                for (const auto & set : filter_sets)
                    if (!kept_sets.contains(set))
                        removed_filter_sets.insert(set.get());
                filter_node->step = std::move(replacement);
            }
            else if (replacement)
            {
                auto & inserted = nodes.emplace_back(QueryPlan::Node{.step = std::move(replacement), .children = {filter_node}});
                std::ranges::replace(parent->second->children, filter_node, &inserted);
            }
        }
    }
    else
    {
        SelectQueryOptions options(QueryProcessingStage::FetchColumns);
        auto storage_limits = std::make_shared<StorageLimitsList>();
        storage_limits->emplace_back(buildStorageLimits(*context, options));
        select_query_info.storage_limits = std::move(storage_limits);
        select_query_info.query = std::move(query);

        bool use_parallel_replicas = false;
        if (reading_from_table)
            use_parallel_replicas = reading_from_table->useParallelReplicas();

        auto mutable_context = Context::createCopy(context);
        mutable_context->setSetting("allow_experimental_parallel_reading_from_replicas", use_parallel_replicas);

        storage->read(
            reading_plan,
            column_names,
            snapshot,
            select_query_info,
            mutable_context,
            QueryProcessingStage::FetchColumns,
            context->getSettingsRef()[Setting::max_block_size],
            context->getSettingsRef()[Setting::max_threads]
        );

        /// Preserve the mutable_context for the lifetime of query execution
        /// because source processors (e.g., StorageKeeperMapSource) may hold weak_ptr to it
        reading_plan.addInterpreterContext(mutable_context);
        /// The reading step keeps this copy and consults `make_distributed_plan` in it when it builds its
        /// IN sets, so a fallback of the plan has to be able to turn the setting off here as well.
        reading_plan.addDistributedPlanDecisionContext(mutable_context);
    }

    if (!reading_plan.isInitialized())
    {
        /// Create step which reads from empty source if storage has no data.
        auto source_header = std::make_shared<const Block>(snapshot->getSampleBlockForColumns(column_names));
        Pipe pipe(std::make_shared<NullSource>(source_header));
        auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        read_from_pipe->setStepDescription("Read from NullSource");
        reading_plan.addStep(std::move(read_from_pipe));
    }

    auto converting_actions = ActionsDAG::makeConvertingActions(
        reading_plan.getCurrentHeader()->getColumnsWithTypeAndName(),
        read_header->getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        context);

    node.step = std::make_unique<ExpressionStep>(reading_plan.getCurrentHeader(), std::move(converting_actions));
    node.children = {reading_plan.getRootNode()};

    reading_plan.addStorageHolder(std::move(storage));
    reading_plan.addTableLock(std::move(table_lock));

    auto nodes_and_resource = QueryPlan::detachNodesAndResources(std::move(reading_plan));

    nodes.splice(nodes.end(), std::move(nodes_and_resource.first));
    return std::move(nodes_and_resource.second);
}

void QueryPlan::resolveStorages(const ContextPtr & context)
{
    ParentNodes parents;
    std::unordered_set<const FutureSet *> removed_filter_sets;
    std::vector<QueryPlan::Node *> delayed_creating_sets_nodes;
    std::stack<QueryPlan::Node *> stack;
    stack.push(getRootNode());
    while (!stack.empty())
    {
        auto * node = stack.top();
        stack.pop();

        if (typeid_cast<const DelayedCreatingSetsStep *>(node->step.get()))
            delayed_creating_sets_nodes.push_back(node);

        for (auto * child : node->children)
        {
            parents[child] = node;
            stack.push(child);
        }

        if (node->children.empty())
            addResources(replaceReadingFromTable(*node, nodes, context, parents, removed_filter_sets));
    }

    for (auto * node : delayed_creating_sets_nodes)
    {
        if (!removed_filter_sets.empty())
        {
            /// A set that no column of the plan refers to any more is held only by the step that builds it.
            auto & delayed_creating_sets = typeid_cast<DelayedCreatingSetsStep &>(*node->step);
            auto sets = delayed_creating_sets.detachSets();
            std::erase_if(sets, [&](const auto & set) { return removed_filter_sets.contains(set.get()) && set.use_count() == 1; });
            node->step = std::make_unique<DelayedCreatingSetsStep>(
                delayed_creating_sets.getInputHeaders().front(),
                std::move(sets),
                delayed_creating_sets.getNetworkTransferLimits(),
                delayed_creating_sets.getPreparedSetsCache());
        }

        for (const auto & set : typeid_cast<const DelayedCreatingSetsStep &>(*node->step).getSets())
        {
            auto * set_plan = set->getQueryPlan();
            set_plan->resolveStorages(context);
            /// The set-source plan is kept aside, so the contexts its resolved reads captured must be handed to
            /// this plan for a fallback to reach them, as the planner does for the set sources it builds.
            takeContextsFrom(*set_plan);
        }
    }
}

}
