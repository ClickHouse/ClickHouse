#include <memory>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Core/ColumnNumbers.h>
#include <Core/ColumnWithTypeAndName.h>

#include <Functions/grouping.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsMiscellaneous.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
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

#include <Processors/QueryPlan/QueryPlan.h>

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
#include <Interpreters/UserDefinedExecutableFunctionFactory.h>


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
    extern const int DUPLICATE_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
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
    {
        columns[i] = types[i]->createColumn();
        columns[i]->reserve(collection.size());
    }

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
                    columns[i]->insert(tuple_values[i]);
        }
    }

    Block res;
    for (size_t i = 0; i < columns_num; ++i)
        res.insert(ColumnWithTypeAndName{std::move(columns[i]), types[i], "_" + toString(i)});
    return res;
}

static Field extractValueFromNode(const ASTPtr & node, const IDataType & type, ContextPtr context)
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

static Block createBlockFromAST(const ASTPtr & node, const DataTypes & types, ContextPtr context)
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
    bool transform_null_in = context->getSettingsRef().transform_null_in;
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
                    throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET,
                        "Invalid type of set. Expected tuple, got {}",
                        function_result.getTypeName());

                tuple = &function_result.get<Tuple>();
            }

            /// Tuple can be represented as a literal in AST.
            auto * literal = elem->as<ASTLiteral>();
            if (literal)
            {
                /// The literal must be tuple.
                if (literal->value.getType() != Field::Types::Tuple)
                    throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET,
                        "Invalid type in set. Expected tuple, got {}",
                        literal->value.getTypeName());

                tuple = &literal->value.get<Tuple>();
            }

            assert(tuple || func);

            size_t tuple_size = tuple ? tuple->size() : func->arguments->children.size(); //-V1004
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


namespace
{

/** Create a block for set from expression.
  * 'set_element_types' - types of what are on the left hand side of IN.
  * 'right_arg' - list of values: 1, 2, 3 or list of tuples: (1, 2), (3, 4), (5, 6).
  *
  *  We need special implementation for ASTFunction, because in case, when we interpret
  *  large tuple or array as function, `evaluateConstantExpression` works extremely slow.
  */
Block createBlockForSet(
    const DataTypePtr & left_arg_type,
    const ASTPtr & right_arg,
    const DataTypes & set_element_types,
    ContextPtr context)
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
    bool tranform_null_in = context->getSettingsRef().transform_null_in;

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

/** Create a block for set from literal.
  * 'set_element_types' - types of what are on the left hand side of IN.
  * 'right_arg' - Literal - Tuple or Array.
  */
Block createBlockForSet(
    const DataTypePtr & left_arg_type,
    const std::shared_ptr<ASTFunction> & right_arg,
    const DataTypes & set_element_types,
    ContextPtr context)
{
    auto get_tuple_type_from_ast = [context](const auto & func) -> DataTypePtr
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

}


SetPtr makeExplicitSet(
    const ASTFunction * node, const ActionsDAG & actions, bool create_ordered_set,
    ContextPtr context, const SizeLimits & size_limits, PreparedSets & prepared_sets)
{
    const IAST & args = *node->arguments;

    if (args.children.size() != 2)
        throw Exception("Wrong number of arguments passed to function in", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const ASTPtr & left_arg = args.children.at(0);
    const ASTPtr & right_arg = args.children.at(1);

    auto column_name = left_arg->getColumnName();
    const auto & dag_node = actions.findInIndex(column_name);
    const DataTypePtr & left_arg_type = dag_node.result_type;

    DataTypes set_element_types = {left_arg_type};
    const auto * left_tuple_type = typeid_cast<const DataTypeTuple *>(left_arg_type.get());
    if (left_tuple_type && left_tuple_type->getElements().size() != 1)
        set_element_types = left_tuple_type->getElements();

    for (auto & element_type : set_element_types)
        if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(element_type.get()))
            element_type = low_cardinality_type->getDictionaryType();

    auto set_key = PreparedSetKey::forLiteral(*right_arg, set_element_types);
    if (auto it = prepared_sets.find(set_key); it != prepared_sets.end())
        return it->second; /// Already prepared.

    Block block;
    const auto & right_arg_func = std::dynamic_pointer_cast<ASTFunction>(right_arg);
    if (right_arg_func && (right_arg_func->name == "tuple" || right_arg_func->name == "array"))
        block = createBlockForSet(left_arg_type, right_arg_func, set_element_types, context);
    else
        block = createBlockForSet(left_arg_type, right_arg, set_element_types, context);

    SetPtr set
        = std::make_shared<Set>(size_limits, create_ordered_set, context->getSettingsRef().transform_null_in);
    set->setHeader(block.cloneEmpty().getColumnsWithTypeAndName());
    set->insertFromBlock(block.getColumnsWithTypeAndName());
    set->finishInsert();

    prepared_sets.emplace(set_key, set);
    return set;
}

ScopeStack::Level::~Level() = default;
ScopeStack::Level::Level() = default;
ScopeStack::Level::Level(Level &&) noexcept = default;

class ScopeStack::Index
{
    /// Map column name -> Node.
    /// Use string_view as key which always points to Node::result_name.
    std::unordered_map<std::string_view, const ActionsDAG::Node *> map;
    ActionsDAG::NodeRawConstPtrs & index;

public:
    explicit Index(ActionsDAG::NodeRawConstPtrs & index_) : index(index_)
    {
        for (const auto * node : index)
            map.emplace(node->result_name, node);
    }

    void addNode(const ActionsDAG::Node * node)
    {
        bool inserted = map.emplace(node->result_name, node).second;
        if (!inserted)
            throw Exception("Column '" + node->result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);

        index.push_back(node);
    }

    const ActionsDAG::Node * tryGetNode(const std::string & name) const
    {
        auto it = map.find(name);
        if (it == map.end())
            return nullptr;

        return it->second;
    }

    const ActionsDAG::Node & getNode(const std::string & name) const
    {
        const auto * node = tryGetNode(name);
        if (!node)
            throw Exception("Unknown identifier: '" + name + "'", ErrorCodes::UNKNOWN_IDENTIFIER);

        return *node;
    }

    bool contains(const std::string & name) const { return map.contains(name); }

    std::vector<std::string_view> getAllNames() const
    {
        std::vector<std::string_view> result;
        result.reserve(map.size());
        for (auto const & e : map)
            result.emplace_back(e.first);
        return result;
    }
};

ActionsMatcher::Data::Data(
    ContextPtr context_,
    SizeLimits set_size_limit_,
    size_t subquery_depth_,
    std::reference_wrapper<const NamesAndTypesList> source_columns_,
    ActionsDAGPtr actions_dag,
    PreparedSets & prepared_sets_,
    SubqueriesForSets & subqueries_for_sets_,
    bool no_subqueries_,
    bool no_makeset_,
    bool only_consts_,
    bool create_source_for_in_,
    AggregationKeysInfo aggregation_keys_info_,
    bool build_expression_with_window_functions_)
    : WithContext(context_)
    , set_size_limit(set_size_limit_)
    , subquery_depth(subquery_depth_)
    , source_columns(source_columns_)
    , prepared_sets(prepared_sets_)
    , subqueries_for_sets(subqueries_for_sets_)
    , no_subqueries(no_subqueries_)
    , no_makeset(no_makeset_)
    , only_consts(only_consts_)
    , create_source_for_in(create_source_for_in_)
    , visit_depth(0)
    , actions_stack(std::move(actions_dag), context_)
    , aggregation_keys_info(aggregation_keys_info_)
    , build_expression_with_window_functions(build_expression_with_window_functions_)
    , next_unique_suffix(actions_stack.getLastActions().getIndex().size() + 1)
{
}

bool ActionsMatcher::Data::hasColumn(const String & column_name) const
{
    return actions_stack.getLastActionsIndex().contains(column_name);
}

std::vector<std::string_view> ActionsMatcher::Data::getAllColumnNames() const
{
    const auto & index = actions_stack.getLastActionsIndex();
    return index.getAllNames();
}

ScopeStack::ScopeStack(ActionsDAGPtr actions_dag, ContextPtr context_) : WithContext(context_)
{
    auto & level = stack.emplace_back();
    level.actions_dag = std::move(actions_dag);
    level.index = std::make_unique<ScopeStack::Index>(level.actions_dag->getIndex());

    for (const auto & node : level.actions_dag->getIndex())
        if (node->type == ActionsDAG::ActionType::INPUT)
            level.inputs.emplace(node->result_name);
}

void ScopeStack::pushLevel(const NamesAndTypesList & input_columns)
{
    auto & level = stack.emplace_back();
    level.actions_dag = std::make_shared<ActionsDAG>();
    level.index = std::make_unique<ScopeStack::Index>(level.actions_dag->getIndex());
    const auto & prev = stack[stack.size() - 2];

    for (const auto & input_column : input_columns)
    {
        const auto & node = level.actions_dag->addInput(input_column.name, input_column.type);
        level.index->addNode(&node);
        level.inputs.emplace(input_column.name);
    }

    for (const auto & node : prev.actions_dag->getIndex())
    {
        if (!level.index->contains(node->result_name))
        {
            const auto & input = level.actions_dag->addInput({node->column, node->result_type, node->result_name});
            level.index->addNode(&input);
        }
    }
}

size_t ScopeStack::getColumnLevel(const std::string & name)
{
    for (size_t i = stack.size(); i > 0;)
    {
        --i;

        if (stack[i].inputs.contains(name))
            return i;

        const auto * node = stack[i].index->tryGetNode(name);
        if (node && node->type != ActionsDAG::ActionType::INPUT)
            return i;
    }

    throw Exception("Unknown identifier: " + name, ErrorCodes::UNKNOWN_IDENTIFIER);
}

void ScopeStack::addColumn(ColumnWithTypeAndName column)
{
    const auto & node = stack[0].actions_dag->addColumn(std::move(column));
    stack[0].index->addNode(&node);

    for (size_t j = 1; j < stack.size(); ++j)
    {
        const auto & input = stack[j].actions_dag->addInput({node.column, node.result_type, node.result_name});
        stack[j].index->addNode(&input);
    }
}

void ScopeStack::addAlias(const std::string & name, std::string alias)
{
    auto level = getColumnLevel(name);
    const auto & source = stack[level].index->getNode(name);
    const auto & node = stack[level].actions_dag->addAlias(source, std::move(alias));
    stack[level].index->addNode(&node);

    for (size_t j = level + 1; j < stack.size(); ++j)
    {
        const auto & input = stack[j].actions_dag->addInput({node.column, node.result_type, node.result_name});
        stack[j].index->addNode(&input);
    }
}

void ScopeStack::addArrayJoin(const std::string & source_name, std::string result_name)
{
    getColumnLevel(source_name);

    const auto * source_node = stack.front().index->tryGetNode(source_name);
    if (!source_node)
        throw Exception("Expression with arrayJoin cannot depend on lambda argument: " + source_name,
                        ErrorCodes::BAD_ARGUMENTS);

    const auto & node = stack.front().actions_dag->addArrayJoin(*source_node, std::move(result_name));
    stack.front().index->addNode(&node);

    for (size_t j = 1; j < stack.size(); ++j)
    {
        const auto & input = stack[j].actions_dag->addInput({node.column, node.result_type, node.result_name});
        stack[j].index->addNode(&input);
    }
}

void ScopeStack::addFunction(
    const FunctionOverloadResolverPtr & function,
    const Names & argument_names,
    std::string result_name)
{
    size_t level = 0;
    for (const auto & argument : argument_names)
        level = std::max(level, getColumnLevel(argument));

    ActionsDAG::NodeRawConstPtrs children;
    children.reserve(argument_names.size());
    for (const auto & argument : argument_names)
        children.push_back(&stack[level].index->getNode(argument));

    const auto & node = stack[level].actions_dag->addFunction(function, std::move(children), std::move(result_name));
    stack[level].index->addNode(&node);

    for (size_t j = level + 1; j < stack.size(); ++j)
    {
        const auto & input = stack[j].actions_dag->addInput({node.column, node.result_type, node.result_name});
        stack[j].index->addNode(&input);
    }
}

ActionsDAGPtr ScopeStack::popLevel()
{
    auto res = std::move(stack.back().actions_dag);
    stack.pop_back();
    return res;
}

std::string ScopeStack::dumpNames() const
{
    return stack.back().actions_dag->dumpNames();
}

const ActionsDAG & ScopeStack::getLastActions() const
{
    return *stack.back().actions_dag;
}

const ScopeStack::Index & ScopeStack::getLastActionsIndex() const
{
    return *stack.back().index;
}

bool ActionsMatcher::needChildVisit(const ASTPtr & node, const ASTPtr & child)
{
    /// Visit children themself
    if (node->as<ASTIdentifier>() ||
        node->as<ASTTableIdentifier>() ||
        node->as<ASTFunction>() ||
        node->as<ASTLiteral>() ||
        node->as<ASTExpressionList>())
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
    else if (const auto * table = ast->as<ASTTableIdentifier>())
        visit(*table, ast, data);
    else if (const auto * node = ast->as<ASTFunction>())
        visit(*node, ast, data);
    else if (const auto * literal = ast->as<ASTLiteral>())
        visit(*literal, ast, data);
    else if (auto * expression_list = ast->as<ASTExpressionList>())
        visit(*expression_list, ast, data);
    else
    {
        for (auto & child : ast->children)
            if (needChildVisit(ast, child))
                visit(child, data);
    }
}

std::optional<NameAndTypePair> ActionsMatcher::getNameAndTypeFromAST(const ASTPtr & ast, Data & data)
{
    // If the argument is a literal, we generated a unique column name for it.
    // Use it instead of a generic display name.
    auto child_column_name = ast->getColumnName();
    const auto * as_literal = ast->as<ASTLiteral>();
    if (as_literal)
    {
        assert(!as_literal->unique_column_name.empty());
        child_column_name = as_literal->unique_column_name;
    }

    const auto & index = data.actions_stack.getLastActionsIndex();
    if (const auto * node = index.tryGetNode(child_column_name))
        return NameAndTypePair(child_column_name, node->result_type);

    if (!data.only_consts)
        throw Exception("Unknown identifier: " + child_column_name + "; there are columns: " + data.actions_stack.dumpNames(),
                        ErrorCodes::UNKNOWN_IDENTIFIER);

    return {};
}

ASTs ActionsMatcher::doUntuple(const ASTFunction * function, ActionsMatcher::Data & data)
{
    if (function->arguments->children.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Number of arguments for function untuple doesn't match. Passed {}, should be 1",
                        function->arguments->children.size());

    auto & child = function->arguments->children[0];

    /// Calculate nested function.
    visit(child, data);

    /// Get type and name for tuple argument
    auto tuple_name_type = getNameAndTypeFromAST(child, data);
    if (!tuple_name_type)
        return {};

    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(tuple_name_type->type.get());

    if (!tuple_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Function untuple expect tuple argument, got {}",
                        tuple_name_type->type->getName());

    ASTs columns;
    size_t tid = 0;
    auto func_alias = function->tryGetAlias();
    for (const auto & name [[maybe_unused]] : tuple_type->getElementNames())
    {
        auto tuple_ast = function->arguments->children[0];

        /// This transformation can lead to exponential growth of AST size, let's check it.
        tuple_ast->checkSize(data.getContext()->getSettingsRef().max_ast_elements);

        if (tid != 0)
            tuple_ast = tuple_ast->clone();

        auto literal = std::make_shared<ASTLiteral>(UInt64{++tid});
        visit(*literal, literal, data);

        auto func = makeASTFunction("tupleElement", tuple_ast, literal);
        if (!func_alias.empty())
            func->setAlias(func_alias + "." + toString(tid));
        auto function_builder = FunctionFactory::instance().get(func->name, data.getContext());
        data.addFunction(function_builder, {tuple_name_type->name, literal->getColumnName()}, func->getColumnName());

        columns.push_back(std::move(func));
    }

    return columns;
}

void ActionsMatcher::visit(ASTExpressionList & expression_list, const ASTPtr &, Data & data)
{
    size_t num_children = expression_list.children.size();
    for (size_t i = 0; i < num_children; ++i)
    {
        if (const auto * function = expression_list.children[i]->as<ASTFunction>())
        {
            if (function->name == "untuple")
            {
                auto columns = doUntuple(function, data);

                if (columns.empty())
                    continue;

                expression_list.children.erase(expression_list.children.begin() + i);
                expression_list.children.insert(expression_list.children.begin() + i, columns.begin(), columns.end());
                num_children += columns.size() - 1;
                i += columns.size() - 1;
            }
            else
                visit(expression_list.children[i], data);
        }
        else
            visit(expression_list.children[i], data);
    }
}

void ActionsMatcher::visit(const ASTIdentifier & identifier, const ASTPtr &, Data & data)
{

    auto column_name = identifier.getColumnName();
    if (data.hasColumn(column_name))
        return;

    if (!data.only_consts)
    {
        /// The requested column is not in the block.
        /// If such a column exists in the table, then the user probably forgot to surround it with an aggregate function or add it to GROUP BY.

        for (const auto & column_name_type : data.source_columns)
        {
            if (column_name_type.name == column_name)
            {
                throw Exception(ErrorCodes::NOT_AN_AGGREGATE,
                    "Column {} is not under aggregate function and not in GROUP BY. Have columns: {}",
                    backQuote(column_name), toString(data.getAllColumnNames()));
            }
        }

        /// Special check for WITH statement alias. Add alias action to be able to use this alias.
        if (identifier.prefer_alias_to_column_name && !identifier.alias.empty())
            data.addAlias(identifier.name(), identifier.alias);
    }
}

void ActionsMatcher::visit(const ASTFunction & node, const ASTPtr & ast, Data & data)
{
    auto column_name = ast->getColumnName();
    if (data.hasColumn(column_name))
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
            data.addArrayJoin(arg->getColumnName(), column_name);

        return;
    }

    if (node.name == "grouping")
    {
        if (data.only_consts)
            return; // Can not perform constant folding, because this function can be executed only after GROUP BY

        size_t arguments_size = node.arguments->children.size();
        if (arguments_size == 0)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function GROUPING expects at least one argument");
        if (arguments_size > 64)
            throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION, "Function GROUPING can have up to 64 arguments, but {} provided", arguments_size);
        auto keys_info = data.aggregation_keys_info;
        auto aggregation_keys_number = keys_info.aggregation_keys.size();

        ColumnNumbers arguments_indexes;
        for (auto const & arg : node.arguments->children)
        {
            size_t pos = keys_info.aggregation_keys.getPosByName(arg->getColumnName());
            if (pos == aggregation_keys_number)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument of GROUPING function {} is not a part of GROUP BY clause", arg->getColumnName());
            arguments_indexes.push_back(pos);
        }

        switch (keys_info.group_by_kind)
        {
            case GroupByKind::GROUPING_SETS:
            {
                data.addFunction(std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionGroupingForGroupingSets>(std::move(arguments_indexes), keys_info.grouping_set_keys)), { "__grouping_set" }, column_name);
                break;
            }
            case GroupByKind::ROLLUP:
                data.addFunction(std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionGroupingForRollup>(std::move(arguments_indexes), aggregation_keys_number)), { "__grouping_set" }, column_name);
                break;
            case GroupByKind::CUBE:
            {
                data.addFunction(std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionGroupingForCube>(std::move(arguments_indexes), aggregation_keys_number)), { "__grouping_set" }, column_name);
                break;
            }
            case GroupByKind::ORDINARY:
            {
                data.addFunction(std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionGroupingOrdinary>(std::move(arguments_indexes))), {}, column_name);
                break;
            }
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Unexpected kind of GROUP BY clause for GROUPING function: {}", keys_info.group_by_kind);
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
                /// Do not evaluate subquery and create sets. We replace "in*" function to "in*IgnoreSet".

                auto argument_name = node.arguments->children.at(0)->getColumnName();
                data.addFunction(
                    FunctionFactory::instance().get(node.name + "IgnoreSet", data.getContext()),
                    {argument_name, argument_name},
                    column_name);
            }
            return;
        }
    }

    /// A special function `indexHint`. Everything that is inside it is not calculated
    if (node.name == "indexHint")
    {
        // Arguments are removed. We add function instead of constant column to avoid constant folding.
        data.addFunction(FunctionFactory::instance().get("indexHint", data.getContext()), {}, column_name);
        return;
    }

    // Now we need to correctly process window functions and any expression which depend on them.
    if (node.is_window_function)
    {
        // Also add columns from PARTITION BY and ORDER BY of window functions.
        if (node.window_definition)
        {
            visit(node.window_definition, data);
        }
        // Also manually add columns for arguments of the window function itself.
        // ActionVisitor is written in such a way that this method must itself
        // descend into all needed function children. Window functions can't have
        // any special functions as argument, so the code below that handles
        // special arguments is not needed. This is analogous to the
        // appendWindowFunctionsArguments() in SelectQueryExpressionAnalyzer and
        // partially duplicates its code. Probably we can remove most of the
        // logic from that function, but I don't yet have it all figured out...
        for (const auto & arg : node.arguments->children)
        {
            visit(arg, data);
        }

        // Don't need to do anything more for window functions here -- the
        // resulting column is added in ExpressionAnalyzer, similar to the
        // aggregate functions.
        return;
    }
    else if (node.compute_after_window_functions)
    {
        if (!data.build_expression_with_window_functions)
        {
            for (const auto & arg : node.arguments->children)
            {
                if (auto const * function = arg->as<ASTFunction>();
                    function && function->name == "lambda")
                {
                    // Lambda function is a special case. It shouldn't be visited here.
                    continue;
                }
                visit(arg, data);
            }
            return;
        }
    }

    // An aggregate function can also be calculated as a window function, but we
    // checked for it above, so no need to do anything more.
    if (AggregateUtils::isAggregateFunction(node))
        return;

    FunctionOverloadResolverPtr function_builder;

    auto current_context = data.getContext();

    if (UserDefinedExecutableFunctionFactory::instance().has(node.name, current_context))
    {
        Array parameters;
        if (node.parameters)
        {
            auto & node_parameters = node.parameters->children;
            size_t parameters_size = node_parameters.size();
            parameters.resize(parameters_size);

            for (size_t i = 0; i < parameters_size; ++i)
            {
                ASTPtr literal = evaluateConstantExpressionAsLiteral(node_parameters[i], current_context);
                parameters[i] = literal->as<ASTLiteral>()->value;
            }
        }

        function_builder = UserDefinedExecutableFunctionFactory::instance().tryGet(node.name, current_context, parameters);
    }

    if (!function_builder)
    {
        try
        {
            function_builder = FunctionFactory::instance().get(node.name, current_context);
        }
        catch (Exception & e)
        {
            auto hints = AggregateFunctionFactory::instance().getHints(node.name);
            if (!hints.empty())
                e.addMessage("Or unknown aggregate function " + node.name + ". Maybe you meant: " + toString(hints));
            throw;
        }
    }

    Names argument_names;
    DataTypes argument_types;
    bool arguments_present = true;

    /// If the function has an argument-lambda expression, you need to determine its type before the recursive call.
    bool has_lambda_arguments = false;

    if (node.arguments)
    {
        size_t num_arguments = node.arguments->children.size();
        for (size_t arg = 0; arg < num_arguments; ++arg)
        {
            auto & child = node.arguments->children[arg];

            const auto * function = child->as<ASTFunction>();
            const auto * identifier = child->as<ASTTableIdentifier>();
            if (function && function->name == "lambda")
            {
                /// If the argument is a lambda expression, just remember its approximate type.
                if (function->arguments->children.size() != 2)
                    throw Exception("lambda requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

                const auto * lambda_args_tuple = function->arguments->children.at(0)->as<ASTFunction>();

                if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
                    throw Exception("First argument of lambda must be a tuple", ErrorCodes::TYPE_MISMATCH);

                has_lambda_arguments = true;
                argument_types.emplace_back(std::make_shared<DataTypeFunction>(DataTypes(lambda_args_tuple->arguments->children.size())));
                /// Select the name in the next cycle.
                argument_names.emplace_back();
            }
            else if (function && function->name == "untuple")
            {
                auto columns = doUntuple(function, data);

                if (columns.empty())
                    continue;

                for (const auto & column : columns)
                {
                    if (auto name_type = getNameAndTypeFromAST(column, data))
                    {
                        argument_types.push_back(name_type->type);
                        argument_names.push_back(name_type->name);
                    }
                    else
                        arguments_present = false;
                }

                node.arguments->children.erase(node.arguments->children.begin() + arg);
                node.arguments->children.insert(node.arguments->children.begin() + arg, columns.begin(), columns.end());
                num_arguments += columns.size() - 1;
                arg += columns.size() - 1;
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
                auto table_id = identifier->getTableId();
                table_id = data.getContext()->resolveStorageID(table_id, Context::ResolveOrdinary);
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

                if (auto name_type = getNameAndTypeFromAST(child, data))
                {
                    argument_types.push_back(name_type->type);
                    argument_names.push_back(name_type->name);
                }
                else
                    arguments_present = false;
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

                    String result_name = lambda->arguments->children.at(1)->getColumnName();
                    lambda_dag->removeUnusedActions(Names(1, result_name));

                    auto lambda_actions = std::make_shared<ExpressionActions>(
                        lambda_dag,
                        ExpressionActionsSettings::fromContext(data.getContext(), CompileExpressions::yes));

                    DataTypePtr result_type = lambda_actions->getSampleBlock().getByName(result_name).type;

                    Names captured;
                    Names required = lambda_actions->getRequiredColumns();
                    for (const auto & required_arg : required)
                        if (findColumn(required_arg, lambda_arguments) == lambda_arguments.end())
                            captured.push_back(required_arg);

                    /// We can not name `getColumnName()`,
                    ///  because it does not uniquely define the expression (the types of arguments can be different).
                    String lambda_name = data.getUniqueName("__lambda");

                    auto function_capture = std::make_shared<FunctionCaptureOverloadResolver>(
                            lambda_actions, captured, lambda_arguments, result_type, result_name);
                    data.addFunction(function_capture, captured, lambda_name);

                    argument_types[i] = std::make_shared<DataTypeFunction>(lambda_type->getArgumentTypes(), result_type);
                    argument_names[i] = lambda_name;
                }
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
        /// Calculate column name here again, because AST may be changed here (in case of untuple).
        data.addFunction(function_builder, argument_names, ast->getColumnName());
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
        const auto & index = data.actions_stack.getLastActionsIndex();
        const auto * existing_column = index.tryGetNode(default_name);

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
    const auto * identifier = right_in_operand->as<ASTTableIdentifier>();
    if (right_in_operand->as<ASTSubquery>() || identifier)
    {
        if (no_subqueries)
            return {};
        auto set_key = PreparedSetKey::forSubquery(*right_in_operand);
        if (auto it = data.prepared_sets.find(set_key); it != data.prepared_sets.end())
            return it->second;

        /// A special case is if the name of the table is specified on the right side of the IN statement,
        ///  and the table has the type Set (a previously prepared set).
        if (identifier)
        {
            auto table_id = data.getContext()->resolveStorageID(right_in_operand);
            StoragePtr table = DatabaseCatalog::instance().tryGetTable(table_id, data.getContext());

            if (table)
            {
                StorageSet * storage_set = dynamic_cast<StorageSet *>(table.get());
                if (storage_set)
                {
                    data.prepared_sets.emplace(set_key, storage_set->getSet());
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
            data.prepared_sets.emplace(set_key, subquery_for_set.set);
            return subquery_for_set.set;
        }

        SetPtr set = std::make_shared<Set>(data.set_size_limit, false, data.getContext()->getSettingsRef().transform_null_in);

        /** The following happens for GLOBAL INs or INs:
          * - in the addExternalStorage function, the IN (SELECT ...) subquery is replaced with IN _data1,
          *   in the subquery_for_set object, this subquery is set as source and the temporary table _data1 as the table.
          * - this function shows the expression IN_data1.
          *
          * In case that we have HAVING with IN subquery, we have to force creating set for it.
          * Also it doesn't make sense if it is GLOBAL IN or ordinary IN.
          */
        if (!subquery_for_set.source && data.create_source_for_in)
        {
            auto interpreter = interpretSubquery(right_in_operand, data.getContext(), data.subquery_depth, {});
            subquery_for_set.source = std::make_unique<QueryPlan>();
            interpreter->buildQueryPlan(*subquery_for_set.source);
        }

        subquery_for_set.set = set;
        data.prepared_sets.emplace(set_key, set);
        return set;
    }
    else
    {
        const auto & last_actions = data.actions_stack.getLastActions();
        const auto & index = data.actions_stack.getLastActionsIndex();
        if (index.contains(left_in_operand->getColumnName()))
            /// An explicit enumeration of values in parentheses.
            return makeExplicitSet(&node, last_actions, false, data.getContext(), data.set_size_limit, data.prepared_sets);
        else
            return {};
    }
}

}
