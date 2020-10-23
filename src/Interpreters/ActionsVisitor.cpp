#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Core/Row.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsMiscellaneous.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/FieldToDataType.h>

#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>

#include <Storages/StorageSet.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/misc.h>
#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Set.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/interpretSubquery.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/IdentifierSemantic.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int NOT_AN_AGGREGATE;
    extern const int UNEXPECTED_EXPRESSION;
    extern const int TYPE_MISMATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_ELEMENT_OF_SET;
    extern const int BAD_ARGUMENTS;
}

static NamesAndTypesList::iterator findColumn(const String & name, NamesAndTypesList & cols)
{
    return std::find_if(cols.begin(), cols.end(),
                        [&](const NamesAndTypesList::value_type & val) { return val.name == name; });
}

/// Recursion is limited in query parser and we did not check for too large depth here.
static size_t getTypeDepth(const DataTypePtr & type)
{
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        return 1 + getTypeDepth(array_type->getNestedType());
    else if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
        return 1 + (tuple_type->getElements().empty() ? 0 : getTypeDepth(tuple_type->getElements().at(0)));

    return 0;
}

template<typename Collection>
static Block createBlockFromCollection(const Collection & collection, const DataTypes & types, bool transform_null_in)
{
    size_t columns_num = types.size();
    MutableColumns columns(columns_num);
    for (size_t i = 0; i < columns_num; ++i)
        columns[i] = types[i]->createColumn();

    Row tuple_values;
    for (const auto & value : collection)
    {
        if (columns_num == 1)
        {
            auto field = convertFieldToType(value, *types[0]);
            bool need_insert_null = transform_null_in && types[0]->isNullable();
            if (!field.isNull() || need_insert_null)
                columns[0]->insert(std::move(field));
        }
        else
        {
            if (value.getType() != Field::Types::Tuple)
                throw Exception("Invalid type in set. Expected tuple, got "
                    + String(value.getTypeName()), ErrorCodes::INCORRECT_ELEMENT_OF_SET);

            const auto & tuple = DB::get<const Tuple &>(value);
            size_t tuple_size = tuple.size();

            if (tuple_size != columns_num)
                throw Exception("Incorrect size of tuple in set: " + toString(tuple_size)
                    + " instead of " + toString(columns_num), ErrorCodes::INCORRECT_ELEMENT_OF_SET);

            if (tuple_values.empty())
                tuple_values.resize(tuple_size);

            size_t i = 0;
            for (; i < tuple_size; ++i)
            {
                tuple_values[i] = convertFieldToType(tuple[i], *types[i]);
                bool need_insert_null = transform_null_in && types[i]->isNullable();
                if (tuple_values[i].isNull() && !need_insert_null)
                    break;
            }

            if (i == tuple_size)
                for (i = 0; i < tuple_size; ++i)
                    columns[i]->insert(std::move(tuple_values[i]));
        }
    }

    Block res;
    for (size_t i = 0; i < columns_num; ++i)
        res.insert(ColumnWithTypeAndName{std::move(columns[i]), types[i], "_" + toString(i)});
    return res;
}

static Field extractValueFromNode(const ASTPtr & node, const IDataType & type, const Context & context)
{
    if (const auto * lit = node->as<ASTLiteral>())
    {
        return convertFieldToType(lit->value, type);
    }
    else if (node->as<ASTFunction>())
    {
        std::pair<Field, DataTypePtr> value_raw = evaluateConstantExpression(node, context);
        return convertFieldToType(value_raw.first, type, value_raw.second.get());
    }
    else
        throw Exception("Incorrect element of set. Must be literal or constant expression.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
}

static Block createBlockFromAST(const ASTPtr & node, const DataTypes & types, const Context & context)
{
    /// Will form a block with values from the set.

    Block header;
    size_t num_columns = types.size();
    for (size_t i = 0; i < num_columns; ++i)
        header.insert(ColumnWithTypeAndName(types[i]->createColumn(), types[i], "_" + toString(i)));

    MutableColumns columns = header.cloneEmptyColumns();

    DataTypePtr tuple_type;
    Row tuple_values;
    const auto & list = node->as<ASTExpressionList &>();
    bool transform_null_in = context.getSettingsRef().transform_null_in;
    for (const auto & elem : list.children)
    {
        if (num_columns == 1)
        {
            /// One column at the left of IN.

            Field value = extractValueFromNode(elem, *types[0], context);
            bool need_insert_null = transform_null_in && types[0]->isNullable();

            if (!value.isNull() || need_insert_null)
                columns[0]->insert(value);
        }
        else if (elem->as<ASTFunction>() || elem->as<ASTLiteral>())
        {
            /// Multiple columns at the left of IN.
            /// The right hand side of in should be a set of tuples.

            Field function_result;
            const Tuple * tuple = nullptr;

            /// Tuple can be represented as a function in AST.
            auto * func = elem->as<ASTFunction>();
            if (func && func->name != "tuple")
            {
                if (!tuple_type)
                    tuple_type = std::make_shared<DataTypeTuple>(types);

                /// If the function is not a tuple, treat it as a constant expression that returns tuple and extract it.
                function_result = extractValueFromNode(elem, *tuple_type, context);
                if (function_result.getType() != Field::Types::Tuple)
                    throw Exception("Invalid type of set. Expected tuple, got " + String(function_result.getTypeName()),
                                    ErrorCodes::INCORRECT_ELEMENT_OF_SET);

                tuple = &function_result.get<Tuple>();
            }

            /// Tuple can be represented as a literal in AST.
            auto * literal = elem->as<ASTLiteral>();
            if (literal)
            {
                /// The literal must be tuple.
                if (literal->value.getType() != Field::Types::Tuple)
                    throw Exception("Invalid type in set. Expected tuple, got "
                        + String(literal->value.getTypeName()), ErrorCodes::INCORRECT_ELEMENT_OF_SET);

                tuple = &literal->value.get<Tuple>();
            }

            size_t tuple_size = tuple ? tuple->size() : func->arguments->children.size();
            if (tuple_size != num_columns)
                throw Exception("Incorrect size of tuple in set: " + toString(tuple_size) + " instead of " + toString(num_columns),
                    ErrorCodes::INCORRECT_ELEMENT_OF_SET);

            if (tuple_values.empty())
                tuple_values.resize(tuple_size);

            /// Fill tuple values by evaluation of constant expressions.
            size_t i = 0;
            for (; i < tuple_size; ++i)
            {
                Field value = tuple ? convertFieldToType((*tuple)[i], *types[i])
                                    : extractValueFromNode(func->arguments->children[i], *types[i], context);

                bool need_insert_null = transform_null_in && types[i]->isNullable();

                /// If at least one of the elements of the tuple has an impossible (outside the range of the type) value,
                ///  then the entire tuple too.
                if (value.isNull() && !need_insert_null)
                    break;

                tuple_values[i] = value;
            }

            if (i == tuple_size)
                for (i = 0; i < tuple_size; ++i)
                    columns[i]->insert(tuple_values[i]);
        }
        else
            throw Exception("Incorrect element of set", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
    }

    return header.cloneWithColumns(std::move(columns));
}

Block createBlockForSet(
    const DataTypePtr & left_arg_type,
    const ASTPtr & right_arg,
    const DataTypes & set_element_types,
    const Context & context)
{
    auto [right_arg_value, right_arg_type] = evaluateConstantExpression(right_arg, context);

    const size_t left_type_depth = getTypeDepth(left_arg_type);
    const size_t right_type_depth = getTypeDepth(right_arg_type);

    auto throw_unsupported_type = [](const auto & type)
    {
        throw Exception("Unsupported value type at the right-side of IN: "
            + type->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    };

    Block block;
    bool tranform_null_in = context.getSettingsRef().transform_null_in;

    /// 1 in 1; (1, 2) in (1, 2); identity(tuple(tuple(tuple(1)))) in tuple(tuple(tuple(1))); etc.
    if (left_type_depth == right_type_depth)
    {
        Array array{right_arg_value};
        block = createBlockFromCollection(array, set_element_types, tranform_null_in);
    }
    /// 1 in (1, 2); (1, 2) in ((1, 2), (3, 4)); etc.
    else if (left_type_depth + 1 == right_type_depth)
    {
        auto type_index = right_arg_type->getTypeId();
        if (type_index == TypeIndex::Tuple)
            block = createBlockFromCollection(DB::get<const Tuple &>(right_arg_value), set_element_types, tranform_null_in);
        else if (type_index == TypeIndex::Array)
            block = createBlockFromCollection(DB::get<const Array &>(right_arg_value), set_element_types, tranform_null_in);
        else
            throw_unsupported_type(right_arg_type);
    }
    else
        throw_unsupported_type(right_arg_type);

    return block;
}

Block createBlockForSet(
    const DataTypePtr & left_arg_type,
    const std::shared_ptr<ASTFunction> & right_arg,
    const DataTypes & set_element_types,
    const Context & context)
{
    auto get_tuple_type_from_ast = [&context](const auto & func) -> DataTypePtr
    {
        if (func && (func->name == "tuple" || func->name == "array") && !func->arguments->children.empty())
        {
            /// Won't parse all values of outer tuple.
            auto element = func->arguments->children.at(0);
            std::pair<Field, DataTypePtr> value_raw = evaluateConstantExpression(element, context);
            return std::make_shared<DataTypeTuple>(DataTypes({value_raw.second}));
        }

        return evaluateConstantExpression(func, context).second;
    };

    const DataTypePtr & right_arg_type = get_tuple_type_from_ast(right_arg);

    size_t left_tuple_depth = getTypeDepth(left_arg_type);
    size_t right_tuple_depth = getTypeDepth(right_arg_type);
    ASTPtr elements_ast;

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
        if (!set_func || (set_func->name != "tuple" && set_func->name != "array"))
            throw Exception("Incorrect type of 2nd argument for function 'in'"
                            ". Must be subquery or set of elements with type " + left_arg_type->getName() + ".",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        elements_ast = set_func->arguments;
    }
    else
        throw Exception("Invalid types for IN function: "
                        + left_arg_type->getName() + " and " + right_arg_type->getName() + ".",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return createBlockFromAST(elements_ast, set_element_types, context);
}

SetPtr makeExplicitSet(
    const ASTFunction * node, const ActionsDAG & actions, bool create_ordered_set,
    const Context & context, const SizeLimits & size_limits, PreparedSets & prepared_sets)
{
    const IAST & args = *node->arguments;

    if (args.children.size() != 2)
        throw Exception("Wrong number of arguments passed to function in", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const ASTPtr & left_arg = args.children.at(0);
    const ASTPtr & right_arg = args.children.at(1);

    const auto & index = actions.getIndex();
    auto it = index.find(left_arg->getColumnName());
    if (it == index.end())
        throw Exception("Unknown identifier: '" + left_arg->getColumnName() + "'", ErrorCodes::UNKNOWN_IDENTIFIER);
    const DataTypePtr & left_arg_type = it->second->result_type;

    DataTypes set_element_types = {left_arg_type};
    const auto * left_tuple_type = typeid_cast<const DataTypeTuple *>(left_arg_type.get());
    if (left_tuple_type && left_tuple_type->getElements().size() != 1)
        set_element_types = left_tuple_type->getElements();

    for (auto & element_type : set_element_types)
        if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(element_type.get()))
            element_type = low_cardinality_type->getDictionaryType();

    auto set_key = PreparedSetKey::forLiteral(*right_arg, set_element_types);
    if (prepared_sets.count(set_key))
        return prepared_sets.at(set_key); /// Already prepared.

    Block block;
    const auto & right_arg_func = std::dynamic_pointer_cast<ASTFunction>(right_arg);
    if (right_arg_func && (right_arg_func->name == "tuple" || right_arg_func->name == "array"))
        block = createBlockForSet(left_arg_type, right_arg_func, set_element_types, context);
    else
        block = createBlockForSet(left_arg_type, right_arg, set_element_types, context);

    SetPtr set = std::make_shared<Set>(size_limits, create_ordered_set, context.getSettingsRef().transform_null_in);
    set->setHeader(block.cloneEmpty());
    set->insertFromBlock(block);
    set->finishInsert();

    prepared_sets[set_key] = set;
    return set;
}

ActionsMatcher::Data::Data(
    const Context & context_, SizeLimits set_size_limit_, size_t subquery_depth_,
    const NamesAndTypesList & source_columns_, ActionsDAGPtr actions,
    PreparedSets & prepared_sets_, SubqueriesForSets & subqueries_for_sets_,
    bool no_subqueries_, bool no_makeset_, bool only_consts_, bool no_storage_or_local_)
    : context(context_)
    , set_size_limit(set_size_limit_)
    , subquery_depth(subquery_depth_)
    , source_columns(source_columns_)
    , prepared_sets(prepared_sets_)
    , subqueries_for_sets(subqueries_for_sets_)
    , no_subqueries(no_subqueries_)
    , no_makeset(no_makeset_)
    , only_consts(only_consts_)
    , no_storage_or_local(no_storage_or_local_)
    , visit_depth(0)
    , actions_stack(std::move(actions), context)
    , next_unique_suffix(actions_stack.getLastActions().getIndex().size() + 1)
{
}

bool ActionsMatcher::Data::hasColumn(const String & column_name) const
{
    return actions_stack.getLastActions().getIndex().count(column_name) != 0;
}

ScopeStack::ScopeStack(ActionsDAGPtr actions, const Context & context_)
    : context(context_)
{
    auto & level = stack.emplace_back();
    level.actions = std::move(actions);

    for (const auto & [name, node] : level.actions->getIndex())
        if (node->type == ActionsDAG::Type::INPUT)
            level.inputs.emplace(name);
}

void ScopeStack::pushLevel(const NamesAndTypesList & input_columns)
{
    auto & level = stack.emplace_back();
    level.actions = std::make_shared<ActionsDAG>();
    const auto & prev = stack[stack.size() - 2];

    for (const auto & input_column : input_columns)
    {
        level.actions->addInput(input_column.name, input_column.type);
        level.inputs.emplace(input_column.name);
    }

    const auto & index = level.actions->getIndex();

    for (const auto & [name, node] : prev.actions->getIndex())
    {
        if (index.count(name) == 0)
            level.actions->addInput({node->column, node->result_type, node->result_name});
    }
}

size_t ScopeStack::getColumnLevel(const std::string & name)
{
    for (size_t i = stack.size(); i > 0;)
    {
        --i;

        if (stack[i].inputs.count(name))
            return i;

        const auto & index = stack[i].actions->getIndex();
        auto it = index.find(name);

        if (it != index.end() && it->second->type != ActionsDAG::Type::INPUT)
            return i;
    }

    throw Exception("Unknown identifier: " + name, ErrorCodes::UNKNOWN_IDENTIFIER);
}

void ScopeStack::addColumn(ColumnWithTypeAndName column)
{
    const auto & node = stack[0].actions->addColumn(std::move(column));

    for (size_t j = 1; j < stack.size(); ++j)
        stack[j].actions->addInput({node.column, node.result_type, node.result_name});
}

void ScopeStack::addAlias(const std::string & name, std::string alias)
{
    auto level = getColumnLevel(name);
    const auto & node = stack[level].actions->addAlias(name, std::move(alias));

    for (size_t j = level + 1; j < stack.size(); ++j)
        stack[j].actions->addInput({node.column, node.result_type, node.result_name});
}

void ScopeStack::addArrayJoin(const std::string & source_name, std::string result_name, std::string unique_column_name)
{
    getColumnLevel(source_name);

    if (stack.front().actions->getIndex().count(source_name) == 0)
        throw Exception("Expression with arrayJoin cannot depend on lambda argument: " + source_name,
                        ErrorCodes::BAD_ARGUMENTS);

    const auto & node = stack.front().actions->addArrayJoin(source_name, std::move(result_name), std::move(unique_column_name));

    for (size_t j = 1; j < stack.size(); ++j)
        stack[j].actions->addInput({node.column, node.result_type, node.result_name});
}

void ScopeStack::addFunction(
    const FunctionOverloadResolverPtr & function,
    const Names & argument_names,
    std::string result_name,
    bool compile_expressions)
{
    size_t level = 0;
    for (const auto & argument : argument_names)
        level = std::max(level, getColumnLevel(argument));

    const auto & node = stack[level].actions->addFunction(function, argument_names, std::move(result_name), compile_expressions);

    for (size_t j = level + 1; j < stack.size(); ++j)
        stack[j].actions->addInput({node.column, node.result_type, node.result_name});
}

ActionsDAGPtr ScopeStack::popLevel()
{
    auto res = std::move(stack.back());
    stack.pop_back();
    return res.actions;
}

std::string ScopeStack::dumpNames() const
{
    return stack.back().actions->dumpNames();
}

const ActionsDAG & ScopeStack::getLastActions() const
{
    return *stack.back().actions;
}

struct CachedColumnName
{
    String cached;

    const String & get(const ASTPtr & ast)
    {
        if (cached.empty())
            cached = ast->getColumnName();
        return cached;
    }
};

bool ActionsMatcher::needChildVisit(const ASTPtr & node, const ASTPtr & child)
{
    /// Visit children themself
    if (node->as<ASTIdentifier>() ||
        node->as<ASTFunction>() ||
        node->as<ASTLiteral>())
        return false;

    /// Do not go to FROM, JOIN, UNION.
    if (child->as<ASTTableExpression>() ||
        child->as<ASTSelectQuery>())
        return false;

    return true;
}

void ActionsMatcher::visit(const ASTPtr & ast, Data & data)
{
    if (const auto * identifier = ast->as<ASTIdentifier>())
        visit(*identifier, ast, data);
    else if (const auto * node = ast->as<ASTFunction>())
        visit(*node, ast, data);
    else if (const auto * literal = ast->as<ASTLiteral>())
        visit(*literal, ast, data);
}

void ActionsMatcher::visit(const ASTIdentifier & identifier, const ASTPtr & ast, Data & data)
{
    CachedColumnName column_name;
    if (data.hasColumn(column_name.get(ast)))
        return;

    if (!data.only_consts)
    {
        /// The requested column is not in the block.
        /// If such a column exists in the table, then the user probably forgot to surround it with an aggregate function or add it to GROUP BY.

        for (const auto & column_name_type : data.source_columns)
        {
            if (column_name_type.name == column_name.get(ast))
            {
                throw Exception("Column " + backQuote(column_name.get(ast)) + " is not under aggregate function and not in GROUP BY",
                ErrorCodes::NOT_AN_AGGREGATE);
            }
        }

        /// Special check for WITH statement alias. Add alias action to be able to use this alias.
        if (identifier.prefer_alias_to_column_name && !identifier.alias.empty())
            data.addAlias(identifier.name, identifier.alias);
    }
}

void ActionsMatcher::visit(const ASTFunction & node, const ASTPtr & ast, Data & data)
{
    CachedColumnName column_name;
    if (data.hasColumn(column_name.get(ast)))
        return;

    if (node.name == "lambda")
        throw Exception("Unexpected lambda expression", ErrorCodes::UNEXPECTED_EXPRESSION);

    /// Function arrayJoin.
    if (node.name == "arrayJoin")
    {
        if (node.arguments->children.size() != 1)
            throw Exception("arrayJoin requires exactly 1 argument", ErrorCodes::TYPE_MISMATCH);

        ASTPtr arg = node.arguments->children.at(0);
        visit(arg, data);
        if (!data.only_consts)
        {
            String result_name = column_name.get(ast);
            data.addArrayJoin(arg->getColumnName(), result_name);
        }

        return;
    }

    SetPtr prepared_set;
    if (checkFunctionIsInOrGlobalInOperator(node))
    {
        /// Let's find the type of the first argument (then getActionsImpl will be called again and will not affect anything).
        visit(node.arguments->children.at(0), data);

        if (!data.no_makeset && (prepared_set = makeSet(node, data, data.no_subqueries)))
        {
            /// Transform tuple or subquery into a set.
        }
        else
        {
            if (!data.only_consts)
            {
                /// We are in the part of the tree that we are not going to compute. You just need to define types.
                /// Do not subquery and create sets. We replace "in*" function to "in*IgnoreSet".

                auto argument_name = node.arguments->children.at(0)->getColumnName();

                data.addFunction(
                        FunctionFactory::instance().get(node.name + "IgnoreSet", data.context),
                        { argument_name, argument_name },
                        column_name.get(ast));
            }
            return;
        }
    }

    if (AggregateFunctionFactory::instance().isAggregateFunctionName(node.name))
        return;

    FunctionOverloadResolverPtr function_builder;
    try
    {
        function_builder = FunctionFactory::instance().get(node.name, data.context);
    }
    catch (DB::Exception & e)
    {
        auto hints = AggregateFunctionFactory::instance().getHints(node.name);
        if (!hints.empty())
            e.addMessage("Or unknown aggregate function " + node.name + ". Maybe you meant: " + toString(hints));
        e.rethrow();
    }

    Names argument_names;
    DataTypes argument_types;
    bool arguments_present = true;

    /// If the function has an argument-lambda expression, you need to determine its type before the recursive call.
    bool has_lambda_arguments = false;

    for (size_t arg = 0; arg < node.arguments->children.size(); ++arg)
    {
        auto & child = node.arguments->children[arg];

        const auto * lambda = child->as<ASTFunction>();
        const auto * identifier = child->as<ASTIdentifier>();
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
        else if (checkFunctionIsInOrGlobalInOperator(node) && arg == 1 && prepared_set)
        {
            ColumnWithTypeAndName column;
            column.type = std::make_shared<DataTypeSet>();

            /// If the argument is a set given by an enumeration of values (so, the set was already built), give it a unique name,
            ///  so that sets with the same literal representation do not fuse together (they can have different types).
            if (!prepared_set->empty())
                column.name = data.getUniqueName("__set");
            else
                column.name = child->getColumnName();

            if (!data.hasColumn(column.name))
            {
                auto column_set = ColumnSet::create(1, prepared_set);
                /// If prepared_set is not empty, we have a set made with literals.
                /// Create a const ColumnSet to make constant folding work
                if (!prepared_set->empty())
                    column.column = ColumnConst::create(std::move(column_set), 1);
                else
                    column.column = std::move(column_set);
                data.addColumn(column);
            }

            argument_types.push_back(column.type);
            argument_names.push_back(column.name);
        }
        else if (identifier && (functionIsJoinGet(node.name) || functionIsDictGet(node.name)) && arg == 0)
        {
            auto table_id = IdentifierSemantic::extractDatabaseAndTable(*identifier);
            table_id = data.context.resolveStorageID(table_id, Context::ResolveOrdinary);
            auto column_string = ColumnString::create();
            column_string->insert(table_id.getDatabaseName() + "." + table_id.getTableName());
            ColumnWithTypeAndName column(
                ColumnConst::create(std::move(column_string), 1),
                std::make_shared<DataTypeString>(),
                data.getUniqueName("__" + node.name));
            data.addColumn(column);
            argument_types.push_back(column.type);
            argument_names.push_back(column.name);
        }
        else
        {
            /// If the argument is not a lambda expression, call it recursively and find out its type.
            visit(child, data);

            // In the above visit() call, if the argument is a literal, we
            // generated a unique column name for it. Use it instead of a generic
            // display name.
            auto child_column_name = child->getColumnName();
            const auto * as_literal = child->as<ASTLiteral>();
            if (as_literal)
            {
                assert(!as_literal->unique_column_name.empty());
                child_column_name = as_literal->unique_column_name;
            }

            const auto & index = data.actions_stack.getLastActions().getIndex();
            auto it = index.find(child_column_name);
            if (it != index.end())
            {
                argument_types.push_back(it->second->result_type);
                argument_names.push_back(child_column_name);
            }
            else
            {
                if (data.only_consts)
                    arguments_present = false;
                else
                    throw Exception("Unknown identifier: " + child_column_name + " there are columns: " + data.actions_stack.dumpNames(),
                                    ErrorCodes::UNKNOWN_IDENTIFIER);
            }
        }
    }

    if (data.only_consts && !arguments_present)
        return;

    if (has_lambda_arguments && !data.only_consts)
    {
        function_builder->getLambdaArgumentTypes(argument_types);

        /// Call recursively for lambda expressions.
        for (size_t i = 0; i < node.arguments->children.size(); ++i)
        {
            ASTPtr child = node.arguments->children[i];

            const auto * lambda = child->as<ASTFunction>();
            if (lambda && lambda->name == "lambda")
            {
                const DataTypeFunction * lambda_type = typeid_cast<const DataTypeFunction *>(argument_types[i].get());
                const auto * lambda_args_tuple = lambda->arguments->children.at(0)->as<ASTFunction>();
                const ASTs & lambda_arg_asts = lambda_args_tuple->arguments->children;
                NamesAndTypesList lambda_arguments;

                for (size_t j = 0; j < lambda_arg_asts.size(); ++j)
                {
                    auto opt_arg_name = tryGetIdentifierName(lambda_arg_asts[j]);
                    if (!opt_arg_name)
                        throw Exception("lambda argument declarations must be identifiers", ErrorCodes::TYPE_MISMATCH);

                    lambda_arguments.emplace_back(*opt_arg_name, lambda_type->getArgumentTypes()[j]);
                }

                data.actions_stack.pushLevel(lambda_arguments);
                visit(lambda->arguments->children.at(1), data);
                auto lambda_dag = data.actions_stack.popLevel();
                auto lambda_actions = lambda_dag->buildExpressions(data.context);

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
                String lambda_name = data.getUniqueName("__lambda");

                auto function_capture = std::make_unique<FunctionCaptureOverloadResolver>(
                        lambda_actions, captured, lambda_arguments, result_type, result_name);
                auto function_capture_adapter = std::make_shared<FunctionOverloadResolverAdaptor>(std::move(function_capture));
                data.addFunction(function_capture_adapter, captured, lambda_name);

                argument_types[i] = std::make_shared<DataTypeFunction>(lambda_type->getArgumentTypes(), result_type);
                argument_names[i] = lambda_name;
            }
        }
    }

    if (data.only_consts)
    {
        for (const auto & argument_name : argument_names)
        {
            if (!data.hasColumn(argument_name))
            {
                arguments_present = false;
                break;
            }
        }
    }

    if (arguments_present)
    {
        data.addFunction(function_builder, argument_names, column_name.get(ast));
    }
}

void ActionsMatcher::visit(const ASTLiteral & literal, const ASTPtr & /* ast */,
    Data & data)
{
    DataTypePtr type = applyVisitor(FieldToDataType(), literal.value);
    const auto value = convertFieldToType(literal.value, *type);

    // FIXME why do we have a second pass with a clean sample block over the same
    // AST here? Anyway, do not modify the column name if it is set already.
    if (literal.unique_column_name.empty())
    {
        const auto default_name = literal.getColumnName();
        const auto & index = data.actions_stack.getLastActions().getIndex();
        const ActionsDAG::Node * existing_column = nullptr;

        auto it = index.find(default_name);
        if (it != index.end())
            existing_column = it->second;

        /*
         * To approximate CSE, bind all identical literals to a single temporary
         * columns. We try to find the column by its default name, but after that
         * we have to check that it contains the correct data. This might not be
         * the case if it is a user-supplied column, or it is from under a join,
         * etc.
         * Overall, this is a hack around a generally poor name-based notion of
         * column identity we currently use.
         */
        if (existing_column
            && existing_column->column
            && isColumnConst(*existing_column->column)
            && existing_column->column->size() == 1
            && existing_column->column->operator[](0) == value)
        {
            const_cast<ASTLiteral &>(literal).unique_column_name = default_name;
        }
        else
        {
            const_cast<ASTLiteral &>(literal).unique_column_name
                = data.getUniqueName(default_name);
        }
    }

    if (data.hasColumn(literal.unique_column_name))
    {
        return;
    }

    ColumnWithTypeAndName column;
    column.name = literal.unique_column_name;
    column.column = type->createColumnConst(1, value);
    column.type = type;

    data.addColumn(std::move(column));
}

SetPtr ActionsMatcher::makeSet(const ASTFunction & node, Data & data, bool no_subqueries)
{
    /** You need to convert the right argument to a set.
      * This can be a table name, a value, a value enumeration, or a subquery.
      * The enumeration of values is parsed as a function `tuple`.
      */
    const IAST & args = *node.arguments;
    const ASTPtr & left_in_operand = args.children.at(0);
    const ASTPtr & right_in_operand = args.children.at(1);

    /// If the subquery or table name for SELECT.
    const auto * identifier = right_in_operand->as<ASTIdentifier>();
    if (right_in_operand->as<ASTSubquery>() || identifier)
    {
        if (no_subqueries)
            return {};
        auto set_key = PreparedSetKey::forSubquery(*right_in_operand);
        if (data.prepared_sets.count(set_key))
            return data.prepared_sets.at(set_key);

        /// A special case is if the name of the table is specified on the right side of the IN statement,
        ///  and the table has the type Set (a previously prepared set).
        if (identifier)
        {
            auto table_id = data.context.resolveStorageID(right_in_operand);
            StoragePtr table = DatabaseCatalog::instance().tryGetTable(table_id, data.context);

            if (table)
            {
                StorageSet * storage_set = dynamic_cast<StorageSet *>(table.get());
                if (storage_set)
                {
                    data.prepared_sets[set_key] = storage_set->getSet();
                    return storage_set->getSet();
                }
            }
        }

        /// We get the stream of blocks for the subquery. Create Set and put it in place of the subquery.
        String set_id = right_in_operand->getColumnName();

        SubqueryForSet & subquery_for_set = data.subqueries_for_sets[set_id];

        /// If you already created a Set with the same subquery / table.
        if (subquery_for_set.set)
        {
            data.prepared_sets[set_key] = subquery_for_set.set;
            return subquery_for_set.set;
        }

        SetPtr set = std::make_shared<Set>(data.set_size_limit, false, data.context.getSettingsRef().transform_null_in);

        /** The following happens for GLOBAL INs:
          * - in the addExternalStorage function, the IN (SELECT ...) subquery is replaced with IN _data1,
          *   in the subquery_for_set object, this subquery is set as source and the temporary table _data1 as the table.
          * - this function shows the expression IN_data1.
          */
        if (!subquery_for_set.source && data.no_storage_or_local)
        {
            auto interpreter = interpretSubquery(right_in_operand, data.context, data.subquery_depth, {});
            subquery_for_set.source = std::make_unique<QueryPlan>();
            interpreter->buildQueryPlan(*subquery_for_set.source);
        }

        subquery_for_set.set = set;
        data.prepared_sets[set_key] = set;
        return set;
    }
    else
    {
        const auto & last_actions = data.actions_stack.getLastActions();
        const auto & index = last_actions.getIndex();
        if (index.count(left_in_operand->getColumnName()) != 0)
            /// An explicit enumeration of values in parentheses.
            return makeExplicitSet(&node, last_actions, false, data.context, data.set_size_limit, data.prepared_sets);
        else
            return {};
    }
}

}
