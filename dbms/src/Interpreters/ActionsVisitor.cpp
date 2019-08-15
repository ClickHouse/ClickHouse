#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsMiscellaneous.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/FieldToDataType.h>

#include <DataStreams/LazyBlockInputStream.h>

#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>

#include <Storages/StorageSet.h>

#include <Parsers/ASTFunction.h>
#include <Common/typeid_cast.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/QueryNormalizer.h>
#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Set.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/interpretSubquery.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_IDENTIFIER;
    extern const int NOT_AN_AGGREGATE;
    extern const int UNEXPECTED_EXPRESSION;
    extern const int TYPE_MISMATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

NamesAndTypesList::iterator findColumn(const String & name, NamesAndTypesList & cols)
{
    return std::find_if(cols.begin(), cols.end(),
                        [&](const NamesAndTypesList::value_type & val) { return val.name == name; });
}

SetPtr makeExplicitSet(
    const ASTFunction * node, const Block & sample_block, bool create_ordered_set,
    const Context & context, const SizeLimits & size_limits, PreparedSets & prepared_sets)
{
    const IAST & args = *node->arguments;

    if (args.children.size() != 2)
        throw Exception("Wrong number of arguments passed to function in", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const ASTPtr & left_arg = args.children.at(0);
    const ASTPtr & right_arg = args.children.at(1);

    const DataTypePtr & left_arg_type = sample_block.getByName(left_arg->getColumnName()).type;

    DataTypes set_element_types = {left_arg_type};
    auto left_tuple_type = typeid_cast<const DataTypeTuple *>(left_arg_type.get());
    if (left_tuple_type && left_tuple_type->getElements().size() != 1)
        set_element_types = left_tuple_type->getElements();

    for (auto & element_type : set_element_types)
        if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(element_type.get()))
            element_type = low_cardinality_type->getDictionaryType();

    auto set_key = PreparedSetKey::forLiteral(*right_arg, set_element_types);
    if (prepared_sets.count(set_key))
        return prepared_sets.at(set_key); /// Already prepared.

    auto getTupleTypeFromAst = [&context](const ASTPtr & tuple_ast) -> DataTypePtr
    {
        const auto * func = tuple_ast->as<ASTFunction>();
        if (func && func->name == "tuple" && !func->arguments->children.empty())
        {
            /// Won't parse all values of outer tuple.
            auto element = func->arguments->children.at(0);
            std::pair<Field, DataTypePtr> value_raw = evaluateConstantExpression(element, context);
            return std::make_shared<DataTypeTuple>(DataTypes({value_raw.second}));
        }

        return evaluateConstantExpression(tuple_ast, context).second;
    };

    const DataTypePtr & right_arg_type = getTupleTypeFromAst(right_arg);

    std::function<size_t(const DataTypePtr &)> getTupleDepth;
    getTupleDepth = [&getTupleDepth](const DataTypePtr & type) -> size_t
    {
        if (auto tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
            return 1 + (tuple_type->getElements().empty() ? 0 : getTupleDepth(tuple_type->getElements().at(0)));

        return 0;
    };

    size_t left_tuple_depth = getTupleDepth(left_arg_type);
    size_t right_tuple_depth = getTupleDepth(right_arg_type);

    ASTPtr elements_ast = nullptr;

    /// 1 in 1; (1, 2) in (1, 2); identity(tuple(tuple(tuple(1)))) in tuple(tuple(tuple(1))); etc.
    if (left_tuple_depth == right_tuple_depth)
    {
        ASTPtr exp_list = std::make_shared<ASTExpressionList>();
        exp_list->children.push_back(right_arg);
        elements_ast = exp_list;
    }
    /// 1 in (1, 2); (1, 2) in ((1, 2), (3, 4)); etc.
    else if (left_tuple_depth + 1 == right_tuple_depth)
    {
        const auto * set_func = right_arg->as<ASTFunction>();

        if (!set_func || set_func->name != "tuple")
            throw Exception("Incorrect type of 2nd argument for function " + node->name
                            + ". Must be subquery or set of elements with type " + left_arg_type->getName() + ".",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        elements_ast = set_func->arguments;
    }
    else
        throw Exception("Invalid types for IN function: "
                        + left_arg_type->getName() + " and " + right_arg_type->getName() + ".",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    SetPtr set = std::make_shared<Set>(size_limits, create_ordered_set);
    set->createFromAST(set_element_types, elements_ast, context);
    prepared_sets[set_key] = set;
    return set;
}

static String getUniqueName(const Block & block, const String & prefix)
{
    int i = 1;
    while (block.has(prefix + toString(i)))
        ++i;
    return prefix + toString(i);
}

ScopeStack::ScopeStack(const ExpressionActionsPtr & actions, const Context & context_)
    : context(context_)
{
    stack.emplace_back();
    stack.back().actions = actions;

    const Block & sample_block = actions->getSampleBlock();
    for (size_t i = 0, size = sample_block.columns(); i < size; ++i)
        stack.back().new_columns.insert(sample_block.getByPosition(i).name);
}

void ScopeStack::pushLevel(const NamesAndTypesList & input_columns)
{
    stack.emplace_back();
    Level & prev = stack[stack.size() - 2];

    ColumnsWithTypeAndName all_columns;
    NameSet new_names;

    for (NamesAndTypesList::const_iterator it = input_columns.begin(); it != input_columns.end(); ++it)
    {
        all_columns.emplace_back(nullptr, it->type, it->name);
        new_names.insert(it->name);
        stack.back().new_columns.insert(it->name);
    }

    const Block & prev_sample_block = prev.actions->getSampleBlock();
    for (size_t i = 0, size = prev_sample_block.columns(); i < size; ++i)
    {
        const ColumnWithTypeAndName & col = prev_sample_block.getByPosition(i);
        if (!new_names.count(col.name))
            all_columns.push_back(col);
    }

    stack.back().actions = std::make_shared<ExpressionActions>(all_columns, context);
}

size_t ScopeStack::getColumnLevel(const std::string & name)
{
    for (int i = static_cast<int>(stack.size()) - 1; i >= 0; --i)
        if (stack[i].new_columns.count(name))
            return i;

    throw Exception("Unknown identifier: " + name, ErrorCodes::UNKNOWN_IDENTIFIER);
}

void ScopeStack::addAction(const ExpressionAction & action)
{
    size_t level = 0;
    Names required = action.getNeededColumns();
    for (size_t i = 0; i < required.size(); ++i)
        level = std::max(level, getColumnLevel(required[i]));

    Names added;
    stack[level].actions->add(action, added);

    stack[level].new_columns.insert(added.begin(), added.end());

    for (size_t i = 0; i < added.size(); ++i)
    {
        const ColumnWithTypeAndName & col = stack[level].actions->getSampleBlock().getByName(added[i]);
        for (size_t j = level + 1; j < stack.size(); ++j)
            stack[j].actions->addInput(col);
    }
}

ExpressionActionsPtr ScopeStack::popLevel()
{
    ExpressionActionsPtr res = stack.back().actions;
    stack.pop_back();
    return res;
}

const Block & ScopeStack::getSampleBlock() const
{
    return stack.back().actions->getSampleBlock();
}


ActionsVisitor::ActionsVisitor(
        const Context & context_, SizeLimits set_size_limit_, size_t subquery_depth_,
        const NamesAndTypesList & source_columns_, const ExpressionActionsPtr & actions,
        PreparedSets & prepared_sets_, SubqueriesForSets & subqueries_for_sets_,
        bool no_subqueries_, bool only_consts_, bool no_storage_or_local_, std::ostream * ostr_)
:   context(context_),
    set_size_limit(set_size_limit_),
    subquery_depth(subquery_depth_),
    source_columns(source_columns_),
    prepared_sets(prepared_sets_),
    subqueries_for_sets(subqueries_for_sets_),
    no_subqueries(no_subqueries_),
    only_consts(only_consts_),
    no_storage_or_local(no_storage_or_local_),
    visit_depth(0),
    ostr(ostr_),
    actions_stack(actions, context)
{
}

void ActionsVisitor::visit(const ASTPtr & ast)
{
    DumpASTNode dump(*ast, ostr, visit_depth, "getActions");

    String ast_column_name;
    auto getColumnName = [&ast, &ast_column_name]()
    {
        if (ast_column_name.empty())
            ast_column_name = ast->getColumnName();

        return ast_column_name;
    };

    /// If the result of the calculation already exists in the block.
    if ((ast->as<ASTFunction>() || ast->as<ASTLiteral>()) && actions_stack.getSampleBlock().has(getColumnName()))
        return;

    if (const auto * identifier = ast->as<ASTIdentifier>())
    {
        if (!only_consts && !actions_stack.getSampleBlock().has(getColumnName()))
        {
            /// The requested column is not in the block.
            /// If such a column exists in the table, then the user probably forgot to surround it with an aggregate function or add it to GROUP BY.

            bool found = false;
            for (const auto & column_name_type : source_columns)
                if (column_name_type.name == getColumnName())
                    found = true;

            if (found)
                throw Exception("Column " + getColumnName() + " is not under aggregate function and not in GROUP BY.",
                    ErrorCodes::NOT_AN_AGGREGATE);

            /// Special check for WITH statement alias. Add alias action to be able to use this alias.
            if (identifier->prefer_alias_to_column_name && !identifier->alias.empty())
                actions_stack.addAction(ExpressionAction::addAliases({{identifier->name, identifier->alias}}));
        }
    }
    else if (const auto * node = ast->as<ASTFunction>())
    {
        if (node->name == "lambda")
            throw Exception("Unexpected lambda expression", ErrorCodes::UNEXPECTED_EXPRESSION);

        /// Function arrayJoin.
        if (node->name == "arrayJoin")
        {
            if (node->arguments->children.size() != 1)
                throw Exception("arrayJoin requires exactly 1 argument", ErrorCodes::TYPE_MISMATCH);

            ASTPtr arg = node->arguments->children.at(0);
            visit(arg);
            if (!only_consts)
            {
                String result_name = getColumnName();
                actions_stack.addAction(ExpressionAction::copyColumn(arg->getColumnName(), result_name));
                NameSet joined_columns;
                joined_columns.insert(result_name);
                actions_stack.addAction(ExpressionAction::arrayJoin(joined_columns, false, context));
            }

            return;
        }

        SetPtr prepared_set;
        if (functionIsInOrGlobalInOperator(node->name))
        {
            /// Let's find the type of the first argument (then getActionsImpl will be called again and will not affect anything).
            visit(node->arguments->children.at(0));

            if (!no_subqueries)
            {
                /// Transform tuple or subquery into a set.
                prepared_set = makeSet(node, actions_stack.getSampleBlock());
            }
            else
            {
                if (!only_consts)
                {
                    /// We are in the part of the tree that we are not going to compute. You just need to define types.
                    /// Do not subquery and create sets. We treat "IN" as "ignoreExceptNull" function.

                    actions_stack.addAction(ExpressionAction::applyFunction(
                            FunctionFactory::instance().get("ignoreExceptNull", context),
                            { node->arguments->children.at(0)->getColumnName() },
                            getColumnName()));
                }
                return;
            }
        }

        /// A special function `indexHint`. Everything that is inside it is not calculated
        /// (and is used only for index analysis, see KeyCondition).
        if (node->name == "indexHint")
        {
            actions_stack.addAction(ExpressionAction::addColumn(ColumnWithTypeAndName(
                ColumnConst::create(ColumnUInt8::create(1, 1), 1), std::make_shared<DataTypeUInt8>(),
                    getColumnName())));
            return;
        }

        if (AggregateFunctionFactory::instance().isAggregateFunctionName(node->name))
            return;

        /// Context object that we pass to function should live during query.
        const Context & function_context = context.hasQueryContext()
            ? context.getQueryContext()
            : context;

        FunctionBuilderPtr function_builder;
        try
        {
            function_builder = FunctionFactory::instance().get(node->name, function_context);
        }
        catch (DB::Exception & e)
        {
            auto hints = AggregateFunctionFactory::instance().getHints(node->name);
            if (!hints.empty())
                e.addMessage("Or unknown aggregate function " + node->name + ". Maybe you meant: " + toString(hints));
            e.rethrow();
        }

        Names argument_names;
        DataTypes argument_types;
        bool arguments_present = true;

        /// If the function has an argument-lambda expression, you need to determine its type before the recursive call.
        bool has_lambda_arguments = false;

        for (size_t arg = 0; arg < node->arguments->children.size(); ++arg)
        {
            auto & child = node->arguments->children[arg];
            auto child_column_name = child->getColumnName();

            const auto * lambda = child->as<ASTFunction>();
            if (lambda && lambda->name == "lambda")
            {
                /// If the argument is a lambda expression, just remember its approximate type.
                if (lambda->arguments->children.size() != 2)
                    throw Exception("lambda requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

                const auto * lambda_args_tuple = lambda->arguments->children.at(0)->as<ASTFunction>();

                if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
                    throw Exception("First argument of lambda must be a tuple", ErrorCodes::TYPE_MISMATCH);

                has_lambda_arguments = true;
                argument_types.emplace_back(std::make_shared<DataTypeFunction>(DataTypes(lambda_args_tuple->arguments->children.size())));
                /// Select the name in the next cycle.
                argument_names.emplace_back();
            }
            else if (functionIsInOrGlobalInOperator(node->name) && arg == 1 && prepared_set)
            {
                ColumnWithTypeAndName column;
                column.type = std::make_shared<DataTypeSet>();

                /// If the argument is a set given by an enumeration of values (so, the set was already built), give it a unique name,
                ///  so that sets with the same literal representation do not fuse together (they can have different types).
                if (!prepared_set->empty())
                    column.name = getUniqueName(actions_stack.getSampleBlock(), "__set");
                else
                    column.name = child_column_name;

                if (!actions_stack.getSampleBlock().has(column.name))
                {
                    column.column = ColumnSet::create(1, prepared_set);

                    actions_stack.addAction(ExpressionAction::addColumn(column));
                }

                argument_types.push_back(column.type);
                argument_names.push_back(column.name);
            }
            else
            {
                /// If the argument is not a lambda expression, call it recursively and find out its type.
                visit(child);
                std::string name = child_column_name;
                if (actions_stack.getSampleBlock().has(name))
                {
                    argument_types.push_back(actions_stack.getSampleBlock().getByName(name).type);
                    argument_names.push_back(name);
                }
                else
                {
                    if (only_consts)
                        arguments_present = false;
                    else
                        throw Exception("Unknown identifier: " + name, ErrorCodes::UNKNOWN_IDENTIFIER);
                }
            }
        }

        if (only_consts && !arguments_present)
            return;

        if (has_lambda_arguments && !only_consts)
        {
            function_builder->getLambdaArgumentTypes(argument_types);

            /// Call recursively for lambda expressions.
            for (size_t i = 0; i < node->arguments->children.size(); ++i)
            {
                ASTPtr child = node->arguments->children[i];

                const auto * lambda = child->as<ASTFunction>();
                if (lambda && lambda->name == "lambda")
                {
                    const DataTypeFunction * lambda_type = typeid_cast<const DataTypeFunction *>(argument_types[i].get());
                    const auto * lambda_args_tuple = lambda->arguments->children.at(0)->as<ASTFunction>();
                    const ASTs & lambda_arg_asts = lambda_args_tuple->arguments->children;
                    NamesAndTypesList lambda_arguments;

                    for (size_t j = 0; j < lambda_arg_asts.size(); ++j)
                    {
                        auto opt_arg_name = getIdentifierName(lambda_arg_asts[j]);
                        if (!opt_arg_name)
                            throw Exception("lambda argument declarations must be identifiers", ErrorCodes::TYPE_MISMATCH);

                        lambda_arguments.emplace_back(*opt_arg_name, lambda_type->getArgumentTypes()[j]);
                    }

                    actions_stack.pushLevel(lambda_arguments);
                    visit(lambda->arguments->children.at(1));
                    ExpressionActionsPtr lambda_actions = actions_stack.popLevel();

                    String result_name = lambda->arguments->children.at(1)->getColumnName();
                    lambda_actions->finalize(Names(1, result_name));
                    DataTypePtr result_type = lambda_actions->getSampleBlock().getByName(result_name).type;

                    Names captured;
                    Names required = lambda_actions->getRequiredColumns();
                    for (const auto & required_arg : required)
                        if (findColumn(required_arg, lambda_arguments) == lambda_arguments.end())
                            captured.push_back(required_arg);

                    /// We can not name `getColumnName()`,
                    ///  because it does not uniquely define the expression (the types of arguments can be different).
                    String lambda_name = getUniqueName(actions_stack.getSampleBlock(), "__lambda");

                    auto function_capture = std::make_shared<FunctionCapture>(
                            lambda_actions, captured, lambda_arguments, result_type, result_name);
                    actions_stack.addAction(ExpressionAction::applyFunction(function_capture, captured, lambda_name));

                    argument_types[i] = std::make_shared<DataTypeFunction>(lambda_type->getArgumentTypes(), result_type);
                    argument_names[i] = lambda_name;
                }
            }
        }

        if (only_consts)
        {
            for (const auto & argument_name : argument_names)
            {
                if (!actions_stack.getSampleBlock().has(argument_name))
                {
                    arguments_present = false;
                    break;
                }
            }
        }

        if (arguments_present)
        {
            actions_stack.addAction(
                ExpressionAction::applyFunction(function_builder, argument_names, getColumnName()));
        }
    }
    else if (const auto * literal = ast->as<ASTLiteral>())
    {
        DataTypePtr type = applyVisitor(FieldToDataType(), literal->value);

        ColumnWithTypeAndName column;
        column.column = type->createColumnConst(1, convertFieldToType(literal->value, *type));
        column.type = type;
        column.name = getColumnName();

        actions_stack.addAction(ExpressionAction::addColumn(column));
    }
    else
    {
        for (auto & child : ast->children)
        {
            /// Do not go to FROM, JOIN, UNION.
            if (!child->as<ASTTableExpression>() && !child->as<ASTSelectQuery>())
                visit(child);
        }
    }
}

SetPtr ActionsVisitor::makeSet(const ASTFunction * node, const Block & sample_block)
{
    /** You need to convert the right argument to a set.
      * This can be a table name, a value, a value enumeration, or a subquery.
      * The enumeration of values is parsed as a function `tuple`.
      */
    const IAST & args = *node->arguments;
    const ASTPtr & arg = args.children.at(1);

    /// If the subquery or table name for SELECT.
    const auto * identifier = arg->as<ASTIdentifier>();
    if (arg->as<ASTSubquery>() || identifier)
    {
        auto set_key = PreparedSetKey::forSubquery(*arg);
        if (prepared_sets.count(set_key))
            return prepared_sets.at(set_key);

        /// A special case is if the name of the table is specified on the right side of the IN statement,
        ///  and the table has the type Set (a previously prepared set).
        if (identifier)
        {
            DatabaseAndTableWithAlias database_table(*identifier);
            StoragePtr table = context.tryGetTable(database_table.database, database_table.table);

            if (table)
            {
                StorageSet * storage_set = dynamic_cast<StorageSet *>(table.get());
                if (storage_set)
                {
                    prepared_sets[set_key] = storage_set->getSet();
                    return storage_set->getSet();
                }
            }
        }

        /// We get the stream of blocks for the subquery. Create Set and put it in place of the subquery.
        String set_id = arg->getColumnName();

        SubqueryForSet & subquery_for_set = subqueries_for_sets[set_id];

        /// If you already created a Set with the same subquery / table.
        if (subquery_for_set.set)
        {
            prepared_sets[set_key] = subquery_for_set.set;
            return subquery_for_set.set;
        }

        SetPtr set = std::make_shared<Set>(set_size_limit, false);

        /** The following happens for GLOBAL INs:
          * - in the addExternalStorage function, the IN (SELECT ...) subquery is replaced with IN _data1,
          *   in the subquery_for_set object, this subquery is set as source and the temporary table _data1 as the table.
          * - this function shows the expression IN_data1.
          */
        if (!subquery_for_set.source && no_storage_or_local)
        {
            auto interpreter = interpretSubquery(arg, context, subquery_depth, {});
            subquery_for_set.source = std::make_shared<LazyBlockInputStream>(
                interpreter->getSampleBlock(), [interpreter]() mutable { return interpreter->execute().in; });

            /** Why is LazyBlockInputStream used?
              *
              * The fact is that when processing a query of the form
              *  SELECT ... FROM remote_test WHERE column GLOBAL IN (subquery),
              *  if the distributed remote_test table contains localhost as one of the servers,
              *  the query will be interpreted locally again (and not sent over TCP, as in the case of a remote server).
              *
              * The query execution pipeline will be:
              * CreatingSets
              *  subquery execution, filling the temporary table with _data1 (1)
              *  CreatingSets
              *   reading from the table _data1, creating the set (2)
              *   read from the table subordinate to remote_test.
              *
              * (The second part of the pipeline under CreateSets is a reinterpretation of the query inside StorageDistributed,
              *  the query differs in that the database name and tables are replaced with subordinates, and the subquery is replaced with _data1.)
              *
              * But when creating the pipeline, when creating the source (2), it will be found that the _data1 table is empty
              *  (because the query has not started yet), and empty source will be returned as the source.
              * And then, when the query is executed, an empty set will be created in step (2).
              *
              * Therefore, we make the initialization of step (2) lazy
              *  - so that it does not occur until step (1) is completed, on which the table will be populated.
              *
              * Note: this solution is not very good, you need to think better.
              */
        }

        subquery_for_set.set = set;
        prepared_sets[set_key] = set;
        return set;
    }
    else
    {
        /// An explicit enumeration of values in parentheses.
        return makeExplicitSet(node, sample_block, false, context, set_size_limit, prepared_sets);
    }
}

}
