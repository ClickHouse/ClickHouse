#include <memory>
#include <utility>
#include <vector>

#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Common/checkStackSize.h>
#include <Common/assert_cast.h>
#include <Core/Field.h>

#include <Core/ColumnNumbers.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Settings.h>

#include <Functions/grouping.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/indexHint.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeFactory.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>

#include <Storages/StorageSet.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTQueryParameter.h>

#include <Processors/QueryPlan/QueryPlan.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/misc.h>
#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Set.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/interpretSubquery.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Parsers/QueryParameterVisitor.h>

#include <Analyzer/QueryNode.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Parsers/queryToString.h>


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
    extern const int FUNCTION_CANNOT_HAVE_PARAMETERS;
    extern const int SYNTAX_ERROR;
}

static NamesAndTypesList::iterator findColumn(const String & name, NamesAndTypesList & cols)
{
    return std::find_if(cols.begin(), cols.end(),
                        [&](const NamesAndTypesList::value_type & val) { return val.name == name; });
}

namespace
{

size_t getCompoundTypeDepth(const IDataType & type)
{
    size_t result = 0;

    const IDataType * current_type = &type;

    while (true)
    {
        WhichDataType which_type(*current_type);

        if (which_type.isArray())
        {
            current_type = assert_cast<const DataTypeArray &>(*current_type).getNestedType().get();
            ++result;
        }
        else if (which_type.isTuple())
        {
            const auto & tuple_elements = assert_cast<const DataTypeTuple &>(*current_type).getElements();
            if (!tuple_elements.empty())
                current_type = tuple_elements.at(0).get();

            ++result;
        }
        else
        {
            break;
        }
    }

    return result;
}

/** Check whether the Tuple elements types (value_types here) meet the requirements of IN Operator and obtain the types of Block to be constructed.
  * This function is executed according to the following logic:
  * - if transform_null_in == false, just use left arg's types as Block type. (the Tuple elements types will be checked in createBlockFromCollection)
  * - distinguish situations where the number of left arg is one or more:
  *   - one : xxx IN (xxx, xxx, xxx, ...)
  *     * if left arg's type is not NULL, just use it as Block type.
  *     * select the first non-null-type in Tuple as the type of Block, and count whether there is a NULL type in Tuple.
  *     * if such a type does not exist, it means that all types in Tuple are NULL, then select NULL as the type of Block.
  *     * if the selected type is not a Nullable type and there is a NULL type in the Tuple, the selected type will be converted to the corresponding Nullable type. like:
  *       NULL in (1, 2, NULL), we need to choose Nullable(UInt8) as Block type.
  *     * if the selected type does not have a corresponding Nullable type, an exception is thrown.
  *   - more : (xxx, yyy, ...) IN ((xxx, yyy, ...), ...)
  *     extract the types from Tuple one by one, handle them according to the first situation, and finally merge them back into Tuple.
  */
DataTypes CheckAndGetBlockTypes(const DataTypes & block_types, const DataTypes & value_types, bool transform_null_in)
{
    if (transform_null_in == false)
    {
        return block_types;
    }
    DataTypes result;
    size_t columns_size = block_types.size();
    if (columns_size == 1)
    {
        if (!block_types[0]->isNullableNothing())
        {
            result.push_back(block_types[0]);
        }
        else
        {
            bool value_types_has_null_literal = false;
            for (const auto & value_type : value_types)
            {
                // select the first not-nullable-nothing type in set as block type.
                if (!value_type->isNullableNothing())
                {
                    if (result.empty())
                        result.push_back(value_type);
                }
                else
                {
                    value_types_has_null_literal = true;
                }
            }
            if (result.empty())
            {
                assert(value_types_has_null_literal == true);
                result.push_back(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>()));
            }
            // e.g. NULL in (1, 2, NULL), select UInt8 as block type now.
            // But this is not correct because we will insert NULL into the UInt8 type column
            if (value_types_has_null_literal == true && !result[0]->isNullable())
            {
                if (!result[0]->canBeInsideNullable())
                {
                    throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET, "DataType {} can not be inside nullable with NULL set element", result[0]->getName());
                }
                result[0] = std::make_shared<DataTypeNullable>(result[0]);
            }
        }
    }
    else
    {
        std::vector<DataTypes> value_tuple_types(columns_size);
        for (const auto & value_type : value_types)
        {
            if (value_type->getTypeId() != TypeIndex::Tuple)
            {
                throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET,
                    "Invalid type in set. Expected tuple, got {}",
                    value_type->getName());
            }
            const auto * tuple_type = assert_cast<const DataTypeTuple *>(value_type.get());
            const auto & tuple_element_types = tuple_type->getElements();
            if (tuple_element_types.size() != columns_size)
            {
                throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET,
                    "Incorrect size of tuple in set: {} instead of {}",
                    tuple_element_types.size(),
                    columns_size);
            }
            for (size_t i = 0; i < columns_size; ++i)
            {
                value_tuple_types[i].push_back(tuple_element_types[i]);
            }
        }
        for (size_t i = 0; i < columns_size; ++i)
        {
            DataTypes tuple_element_block_type = CheckAndGetBlockTypes({block_types[i]}, value_tuple_types[i], transform_null_in);
            result.push_back(tuple_element_block_type[0]);
        }
    }
    return result;
}

/// The `convertFieldToTypeStrict` is used to prevent unexpected results in case of conversion with loss of precision.
/// Example: `SELECT 33.3 :: Decimal(9, 1) AS a WHERE a IN (33.33 :: Decimal(9, 2))`
/// 33.33 in the set is converted to 33.3, but it is not equal to 33.3 in the column, so the result should still be empty.
/// We can not include values that don't represent any possible value from the type of filtered column to the set.
template<typename Collection>
Block createBlockFromCollection(const Collection & collection, const DataTypes & value_types, const DataTypes & types, bool transform_null_in)
{
    size_t columns_num = types.size();
    MutableColumns columns(columns_num);

    DataTypes ret_types = CheckAndGetBlockTypes(types, value_types, transform_null_in);
    for (size_t i = 0; i < columns_num; ++i)
    {
        columns[i] = ret_types[i]->createColumn();
        columns[i]->reserve(collection.size());
    }

    Row tuple_values;
    for (size_t collection_index = 0; collection_index < collection.size(); ++collection_index)
    {
        const auto& value = collection[collection_index];
        if (columns_num == 1)
        {
            auto field = convertFieldToTypeStrictWithTransformNullIn(value, *value_types[collection_index], *types[0], transform_null_in);
            bool need_insert_null = transform_null_in && types[0]->isNullable();
            if (field && (!field->isNull() || need_insert_null))
                columns[0]->insert(*field);
        }
        else
        {
            if (value.getType() != Field::Types::Tuple)
                throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET, "Invalid type in set. Expected tuple, got {}",
                    String(value.getTypeName()));

            const auto & tuple = value.template get<const Tuple &>();
            size_t tuple_size = tuple.size();
            if (tuple_size != columns_num)
                throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET, "Incorrect size of tuple in set: {} instead of {}",
                    tuple_size, columns_num);

            if (tuple_values.empty())
                tuple_values.resize(tuple_size);

            const DataTypePtr & value_type = value_types[collection_index];
            const DataTypes & tuple_value_type = typeid_cast<const DataTypeTuple *>(value_type.get())->getElements();

            size_t i = 0;
            for (; i < tuple_size; ++i)
            {
                auto converted_field = convertFieldToTypeStrictWithTransformNullIn(tuple[i], *tuple_value_type[i], *types[i], transform_null_in);
                if (!converted_field)
                    break;
                tuple_values[i] = std::move(*converted_field);

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
        res.insert(ColumnWithTypeAndName{std::move(columns[i]), ret_types[i], "_" + toString(i)});
    return res;
}

Block createBlockForSetImpl(
    const DataTypePtr & expression_type,
    const Field & value,
    const DataTypePtr & value_type,
    bool transform_null_in,
    bool & expression_type_has_nullable_nothing
);

/** Create a block for set from expression.
  * 'right_arg' - list of values: 1, 2, 3 or list of tuples: (1, 2), (3, 4), (5, 6).
  */
Block createBlockForSet(
    const DataTypePtr & left_arg_type,
    const ASTPtr & right_arg,
    ContextPtr context,
    bool & expression_type_has_nullable_nothing)
{
    auto [right_arg_value, right_arg_type] = evaluateConstantExpression(right_arg, context);

    return createBlockForSetImpl(left_arg_type, right_arg_value, right_arg_type, context->getSettingsRef().transform_null_in, expression_type_has_nullable_nothing);
}

/** Create a block for set from literal.
  * 'right_arg' - Literal - Tuple or Array.
  */
Block createBlockForSet(
    const DataTypePtr & left_arg_type,
    const std::shared_ptr<ASTFunction> & right_arg,
    ContextPtr context,
    bool & expression_type_has_nullable_nothing)
{
    auto get_type_and_value_from_ast = [context](const auto & func) -> std::pair<Field, DataTypePtr>
    {
        if ((func->name == "tuple" || func->name == "array") && !func->arguments->children.empty())
        {
            DataTypes types;
            auto get_type_and_value = [&](auto& fields) -> void
            {
                for (const auto & element : func->arguments->children)
                {
                    auto [element_field, element_type] = evaluateConstantExpression(element, context);
                    fields.push_back(std::move(element_field));
                    types.push_back(std::move(element_type));
                }
            };
            if (func->name == "tuple")
            {
                Tuple tuple;
                get_type_and_value(tuple);
                return {Field(std::move(tuple)), std::make_shared<DataTypeTuple>(std::move(types))};
            }
            else
            {
                Array array;
                get_type_and_value(array);
                return {Field(std::move(array)), std::make_shared<DataTypeArray>(std::move(types[0]))};
            }
        }

        return evaluateConstantExpression(func, context);
    };

    assert(right_arg);
    auto [right_arg_value, right_arg_type] = get_type_and_value_from_ast(right_arg);

    return createBlockForSetImpl(left_arg_type, right_arg_value, right_arg_type, context->getSettingsRef().transform_null_in, expression_type_has_nullable_nothing);
}

Block createBlockForSetImpl(
    const DataTypePtr & expression_type,
    const Field & value,
    const DataTypePtr & value_type,
    bool transform_null_in,
    bool & expression_type_has_nullable_nothing
)
{
    DataTypes set_element_types = {expression_type};
    const auto * lhs_tuple_type = typeid_cast<const DataTypeTuple *>(expression_type.get());

    if (lhs_tuple_type && lhs_tuple_type->getElements().size() != 1)
        set_element_types = lhs_tuple_type->getElements();

    for (auto & set_element_type : set_element_types)
    {
        if (const auto * set_element_low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(set_element_type.get()))
            set_element_type = set_element_low_cardinality_type->getDictionaryType();
        if (set_element_type->isNullableNothing())
        {
            expression_type_has_nullable_nothing = true;
        }
    }

    size_t lhs_type_depth = getCompoundTypeDepth(*expression_type);
    size_t rhs_type_depth = getCompoundTypeDepth(*value_type);

    Block result_block;

    if (lhs_type_depth == rhs_type_depth)
    {
        /// 1 in 1; (1, 2) in (1, 2); identity(tuple(tuple(tuple(1)))) in tuple(tuple(tuple(1))); etc.
        Array array{value};
        DataTypes value_types{value_type};
        result_block = createBlockFromCollection(array, value_types, set_element_types, transform_null_in);
    }
    else if (lhs_type_depth + 1 == rhs_type_depth)
    {
        /// 1 in (1, 2); (1, 2) in ((1, 2), (3, 4))
        WhichDataType rhs_which_type(value_type);

        if (rhs_which_type.isArray())
        {
            const DataTypeArray * value_array_type = assert_cast<const DataTypeArray *>(value_type.get());
            size_t value_array_size = value.get<const Array &>().size();
            DataTypes value_types(value_array_size, value_array_type->getNestedType());
            result_block = createBlockFromCollection(value.get<const Array &>(), value_types, set_element_types, transform_null_in);
        }
        else if (rhs_which_type.isTuple())
        {
            const DataTypeTuple * value_tuple_type = assert_cast<const DataTypeTuple *>(value_type.get());
            const DataTypes & value_types = value_tuple_type->getElements();
            result_block = createBlockFromCollection(value.get<const Tuple &>(), value_types, set_element_types, transform_null_in);
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Unsupported type at the right-side of IN. Expected Array or Tuple. Actual {}",
                value_type->getName());
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Unsupported types for IN. First argument type {}. Second argument type {}",
            expression_type->getName(),
            value_type->getName());
    }

    return result_block;
}

}

FutureSetPtr makeExplicitSet(
    const ASTFunction * node, const ActionsDAG & actions, ContextPtr context, PreparedSets & prepared_sets)
{
    const IAST & args = *node->arguments;

    if (args.children.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Wrong number of arguments passed to function in");

    const ASTPtr & left_arg = args.children.at(0);
    const ASTPtr & right_arg = args.children.at(1);

    auto column_name = left_arg->getColumnName();
    const auto & dag_node = actions.findInOutputs(column_name);
    const DataTypePtr & left_arg_type = dag_node.result_type;

    DataTypes left_arg_types = {left_arg_type};
    const auto * left_tuple_type = typeid_cast<const DataTypeTuple *>(left_arg_type.get());
    if (left_tuple_type && left_tuple_type->getElements().size() != 1)
        left_arg_types = left_tuple_type->getElements();

    left_arg_types = Set::getElementTypes(left_arg_types, context->getSettingsRef().transform_null_in);

    auto set_key = right_arg->getTreeHash(/*ignore_aliases=*/ true);
    if (auto set = prepared_sets.findTuple(set_key, left_arg_types))
        return set; /// Already prepared.

    Block block;
    bool first_argument_has_nullable_nothing = false;
    const auto & right_arg_func = std::dynamic_pointer_cast<ASTFunction>(right_arg);
    if (right_arg_func && (right_arg_func->name == "tuple" || right_arg_func->name == "array"))
        block = createBlockForSet(left_arg_type, right_arg_func, context, first_argument_has_nullable_nothing);
    else
        block = createBlockForSet(left_arg_type, right_arg, context, first_argument_has_nullable_nothing);

    return prepared_sets.addFromTuple(set_key, block, context->getSettingsRef(), left_arg_types, first_argument_has_nullable_nothing);
}

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
    ~Index() = default;

    void addNode(const ActionsDAG::Node * node)
    {
        bool inserted = map.emplace(node->result_name, node).second;
        if (!inserted)
            throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Column '{}' already exists", node->result_name);

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
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown identifier: '{}'", name);

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

ScopeStack::Level::Level() = default;
ScopeStack::Level::~Level() = default;
ScopeStack::Level::Level(Level &&) noexcept = default;

ActionsMatcher::Data::Data(
    ContextPtr context_,
    SizeLimits set_size_limit_,
    size_t subquery_depth_,
    std::reference_wrapper<const NamesAndTypesList> source_columns_,
    ActionsDAG actions_dag,
    PreparedSetsPtr prepared_sets_,
    bool no_subqueries_,
    bool no_makeset_,
    bool only_consts_,
    AggregationKeysInfo aggregation_keys_info_,
    bool build_expression_with_window_functions_,
    bool is_create_parameterized_view_)
    : WithContext(context_)
    , set_size_limit(set_size_limit_)
    , subquery_depth(subquery_depth_)
    , source_columns(source_columns_)
    , prepared_sets(prepared_sets_)
    , no_subqueries(no_subqueries_)
    , no_makeset(no_makeset_)
    , only_consts(only_consts_)
    , visit_depth(0)
    , actions_stack(std::move(actions_dag), context_)
    , aggregation_keys_info(aggregation_keys_info_)
    , build_expression_with_window_functions(build_expression_with_window_functions_)
    , is_create_parameterized_view(is_create_parameterized_view_)
    , next_unique_suffix(actions_stack.getLastActions().getOutputs().size() + 1)
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

ScopeStack::ScopeStack(ActionsDAG actions_dag, ContextPtr context_) : WithContext(context_)
{
    auto & level = stack.emplace_back();
    level.actions_dag = std::move(actions_dag);
    level.index = std::make_unique<ScopeStack::Index>(level.actions_dag.getOutputs());

    for (const auto & node : level.actions_dag.getOutputs())
        if (node->type == ActionsDAG::ActionType::INPUT)
            level.inputs.emplace(node->result_name);
}

void ScopeStack::pushLevel(const NamesAndTypesList & input_columns)
{
    auto & level = stack.emplace_back();
    level.index = std::make_unique<ScopeStack::Index>(level.actions_dag.getOutputs());
    const auto & prev = stack[stack.size() - 2];

    for (const auto & input_column : input_columns)
    {
        const auto & node = level.actions_dag.addInput(input_column.name, input_column.type);
        level.index->addNode(&node);
        level.inputs.emplace(input_column.name);
    }

    for (const auto & node : prev.actions_dag.getOutputs())
    {
        if (!level.index->contains(node->result_name))
        {
            const auto & input = level.actions_dag.addInput({node->column, node->result_type, node->result_name});
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

    throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown identifier: {}", name);
}

void ScopeStack::addColumn(ColumnWithTypeAndName column)
{
    const auto & node = stack[0].actions_dag.addColumn(std::move(column));
    stack[0].index->addNode(&node);

    for (size_t j = 1; j < stack.size(); ++j)
    {
        const auto & input = stack[j].actions_dag.addInput({node.column, node.result_type, node.result_name});
        stack[j].index->addNode(&input);
    }
}

void ScopeStack::addAlias(const std::string & name, std::string alias)
{
    auto level = getColumnLevel(name);
    const auto & source = stack[level].index->getNode(name);
    const auto & node = stack[level].actions_dag.addAlias(source, std::move(alias));
    stack[level].index->addNode(&node);

    for (size_t j = level + 1; j < stack.size(); ++j)
    {
        const auto & input = stack[j].actions_dag.addInput({node.column, node.result_type, node.result_name});
        stack[j].index->addNode(&input);
    }
}

void ScopeStack::addArrayJoin(const std::string & source_name, std::string result_name)
{
    getColumnLevel(source_name);

    const auto * source_node = stack.front().index->tryGetNode(source_name);
    if (!source_node)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expression with arrayJoin cannot depend on lambda argument: {}",
                        source_name);

    const auto & node = stack.front().actions_dag.addArrayJoin(*source_node, std::move(result_name));
    stack.front().index->addNode(&node);

    for (size_t j = 1; j < stack.size(); ++j)
    {
        const auto & input = stack[j].actions_dag.addInput({node.column, node.result_type, node.result_name});
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

    const auto & node = stack[level].actions_dag.addFunction(function, std::move(children), std::move(result_name));
    stack[level].index->addNode(&node);

    for (size_t j = level + 1; j < stack.size(); ++j)
    {
        const auto & input = stack[j].actions_dag.addInput({node.column, node.result_type, node.result_name});
        stack[j].index->addNode(&input);
    }
}

ActionsDAG ScopeStack::popLevel()
{
    auto res = std::move(stack.back().actions_dag);
    stack.pop_back();
    return res;
}

std::string ScopeStack::dumpNames() const
{
    return stack.back().actions_dag.dumpNames();
}

const ActionsDAG & ScopeStack::getLastActions() const
{
    return stack.back().actions_dag;
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

    /// Do not go to FROM, JOIN, UNION
    if (child->as<ASTTableExpression>() ||
        child->as<ASTSelectQuery>())
        return false;

    return true;
}

void ActionsMatcher::visit(const ASTPtr & ast, Data & data)
{
    checkStackSize();

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
        throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown identifier: {}; there are columns: {}",
            child_column_name, data.actions_stack.dumpNames());

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
    auto untuple_alias = function->tryGetAlias();
    for (const auto & element_name : tuple_type->getElementNames())
    {
        auto tuple_ast = function->arguments->children[0];

        /// This transformation can lead to exponential growth of AST size, let's check it.
        tuple_ast->checkSize(data.getContext()->getSettingsRef().max_ast_elements);

        if (tid != 0)
            tuple_ast = tuple_ast->clone();

        auto literal = std::make_shared<ASTLiteral>(UInt64{++tid});
        visit(*literal, literal, data);

        auto func = makeASTFunction("tupleElement", tuple_ast, literal);
        if (!untuple_alias.empty())
        {
            auto element_alias = tuple_type->haveExplicitNames() ? element_name : toString(tid);
            func->setAlias(untuple_alias + "." + element_alias);
        }

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

namespace
{
void checkFunctionHasEmptyNullsAction(const ASTFunction & node)
{
    if (node.nulls_action != NullsAction::EMPTY)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Function {} cannot use {} NULLS",
            node.name,
            node.nulls_action == NullsAction::IGNORE_NULLS ? "IGNORE" : "RESPECT");
}
}

void ActionsMatcher::visit(const ASTFunction & node, const ASTPtr & ast, Data & data)
{
    auto column_name = ast->getColumnName();
    if (data.hasColumn(column_name))
        return;

    if (node.name == "lambda")
        throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION, "Unexpected lambda expression");

    /// Function arrayJoin.
    if (node.name == "arrayJoin")
    {
        if (node.arguments->children.size() != 1)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "arrayJoin requires exactly 1 argument");
        checkFunctionHasEmptyNullsAction(node);

        ASTPtr arg = node.arguments->children.at(0);
        visit(arg, data);
        if (!data.only_consts)
            data.addArrayJoin(arg->getColumnName(), column_name);

        return;
    }

    if (node.name == "grouping")
    {
        checkFunctionHasEmptyNullsAction(node);
        if (data.only_consts)
            return; // Can not perform constant folding, because this function can be executed only after GROUP BY

        size_t arguments_size = node.arguments->children.size();
        if (arguments_size == 0)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function GROUPING expects at least one argument");
        if (arguments_size > 64)
            throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                            "Function GROUPING can have up to 64 arguments, but {} provided", arguments_size);
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
                data.addFunction(std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionGroupingForGroupingSets>(std::move(arguments_indexes), keys_info.grouping_set_keys, data.getContext()->getSettingsRef().force_grouping_standard_compatibility)), { "__grouping_set" }, column_name);
                break;
            }
            case GroupByKind::ROLLUP:
                data.addFunction(std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionGroupingForRollup>(std::move(arguments_indexes), aggregation_keys_number, data.getContext()->getSettingsRef().force_grouping_standard_compatibility)), { "__grouping_set" }, column_name);
                break;
            case GroupByKind::CUBE:
            {
                data.addFunction(std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionGroupingForCube>(std::move(arguments_indexes), aggregation_keys_number, data.getContext()->getSettingsRef().force_grouping_standard_compatibility)), { "__grouping_set" }, column_name);
                break;
            }
            case GroupByKind::ORDINARY:
            {
                data.addFunction(std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionGroupingOrdinary>(std::move(arguments_indexes), data.getContext()->getSettingsRef().force_grouping_standard_compatibility)), {}, column_name);
                break;
            }
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Unexpected kind of GROUP BY clause for GROUPING function: {}", keys_info.group_by_kind);
        }
        return;
    }

    FutureSetPtr prepared_set;
    if (checkFunctionIsInOrGlobalInOperator(node))
    {
        checkFunctionHasEmptyNullsAction(node);
        /// Let's find the type of the first argument (then getActionsImpl will be called again and will not affect anything).
        visit(node.arguments->children.at(0), data);

        if (!data.no_makeset && !(data.is_create_parameterized_view && !analyzeReceiveQueryParams(ast).empty()))
            prepared_set = makeSet(node, data, data.no_subqueries);

        if (prepared_set)
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
        checkFunctionHasEmptyNullsAction(node);
        if (data.only_consts)
        {
            /// We need to collect constants inside `indexHint` for index analysis.
            if (node.arguments)
            {
                for (const auto & arg : node.arguments->children)
                    visit(arg, data);
            }
            return;
        }

        /// Here we create a separate DAG for indexHint condition.
        /// It will be used only for index analysis.
        Data index_hint_data(
            data.getContext(),
            data.set_size_limit,
            data.subquery_depth,
            data.source_columns,
            ActionsDAG(data.source_columns),
            data.prepared_sets,
            data.no_subqueries,
            data.no_makeset,
            data.only_consts,
            data.aggregation_keys_info);

        NamesWithAliases args;

        if (node.arguments)
        {
            for (const auto & arg : node.arguments->children)
            {
                visit(arg, index_hint_data);
                args.push_back({arg->getColumnNameWithoutAlias(), {}});
            }
        }

        auto dag = index_hint_data.getActions();
        dag.project(args);

        auto index_hint = std::make_shared<FunctionIndexHint>();
        index_hint->setActions(std::move(dag));

        // Arguments are removed. We add function instead of constant column to avoid constant folding.
        data.addFunction(std::make_unique<FunctionToOverloadResolverAdaptor>(index_hint), {}, column_name);
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

    if (UserDefinedExecutableFunctionFactory::instance().has(node.name, current_context)) /// NOLINT(readability-static-accessed-through-instance)
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

        function_builder = UserDefinedExecutableFunctionFactory::instance().tryGet(node.name, current_context, parameters); /// NOLINT(readability-static-accessed-through-instance)
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

        /// Normal functions are not parametric for now.
        if (node.parameters)
            throw Exception(ErrorCodes::FUNCTION_CANNOT_HAVE_PARAMETERS, "Function {} is not parametric", node.name);
    }

    checkFunctionHasEmptyNullsAction(node);

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
            const auto * query_parameter = child->as<ASTQueryParameter>();
            if (function && function->name == "lambda")
            {
                if (!isASTLambdaFunction(*function))
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "Lambda function definition expects two arguments, first argument must be a tuple of arguments");

                /// If the argument is a lambda expression, just remember its approximate type.
                const auto * lambda_args_tuple = function->arguments->children.at(0)->as<ASTFunction>();
                if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "First argument of lambda must be a tuple");

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
                const bool is_constant_set = typeid_cast<const FutureSetFromSubquery *>(prepared_set.get()) == nullptr;
                if (is_constant_set)
                    column.name = data.getUniqueName("__set");
                else
                    column.name = child->getColumnName();

                if (!data.hasColumn(column.name))
                {
                    auto column_set = ColumnSet::create(1, prepared_set);
                    /// If prepared_set is not empty, we have a set made with literals.
                    /// Create a const ColumnSet to make constant folding work
                    if (is_constant_set)
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
            else if (data.is_create_parameterized_view && query_parameter)
            {
                const auto data_type = DataTypeFactory::instance().get(query_parameter->type);
                /// During analysis for CREATE VIEW of a parameterized view, if parameter is
                /// used multiple times, column is only added once
                if (!data.hasColumn(query_parameter->name))
                {
                    ColumnWithTypeAndName column(data_type, query_parameter->name);
                    data.addColumn(column);
                }

                argument_types.push_back(data_type);
                argument_names.push_back(query_parameter->name);
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
                            throw Exception(ErrorCodes::TYPE_MISMATCH, "lambda argument declarations must be identifiers");

                        lambda_arguments.emplace_back(*opt_arg_name, lambda_type->getArgumentTypes()[j]);
                    }

                    data.actions_stack.pushLevel(lambda_arguments);
                    visit(lambda->arguments->children.at(1), data);
                    auto lambda_dag = data.actions_stack.popLevel();

                    String result_name = lambda->arguments->children.at(1)->getColumnName();
                    lambda_dag.removeUnusedActions(Names(1, result_name));

                    auto lambda_actions = std::make_shared<ExpressionActions>(
                        std::move(lambda_dag),
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
    DataTypePtr type;
    if (literal.custom_type)
        type = literal.custom_type;
    else if (data.getContext()->getSettingsRef().allow_experimental_variant_type && data.getContext()->getSettingsRef().use_variant_as_common_type)
        type = applyVisitor(FieldToDataType<LeastSupertypeOnError::Variant>(), literal.value);
    else
        type = applyVisitor(FieldToDataType(), literal.value);

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

FutureSetPtr ActionsMatcher::makeSet(const ASTFunction & node, Data & data, bool no_subqueries)
{
    if (!data.prepared_sets)
        return {};

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

        PreparedSets::Hash set_key;
        if (data.getContext()->getSettingsRef().allow_experimental_analyzer && !identifier)
        {
            /// Here we can be only from mutation interpreter. Normal selects with analyzed use other interpreter.
            /// This is a hacky way to allow reusing cache for prepared sets.
            ///
            /// Mutation is executed in two stages:
            /// * first, query 'SELECT count() FROM table WHERE ...' is executed to get the set of affected parts (using analyzer)
            /// * second, every part is mutated separately, where plan is build "manually", using this code as well
            /// To share the Set in between first and second stage, we should use the same hash.
            /// New analyzer is uses a hash from query tree, so here we also build a query tree.
            ///
            /// Note : this code can be safely removed, but the test 02581_share_big_sets will be too slow (and fail by timeout).
            /// Note : we should use new analyzer for mutations and remove this hack.
            InterpreterSelectQueryAnalyzer interpreter(right_in_operand, data.getContext(), SelectQueryOptions().analyze(true).subquery());
            const auto & query_tree = interpreter.getQueryTree();
            if (auto * query_node = query_tree->as<QueryNode>())
                query_node->setIsSubquery(true);
            set_key = query_tree->getTreeHash();
        }
        else
            set_key = right_in_operand->getTreeHash(/*ignore_aliases=*/ true);

        if (auto set = data.prepared_sets->findSubquery(set_key))
            return set;

        FutureSetFromSubqueryPtr external_table_set;

        /// A special case is if the name of the table is specified on the right side of the IN statement,
        ///  and the table has the type Set (a previously prepared set).
        if (identifier)
        {
            auto table_id = data.getContext()->resolveStorageID(right_in_operand);
            StoragePtr table = DatabaseCatalog::instance().tryGetTable(table_id, data.getContext());

            if (table)
            {
                if (auto set = data.prepared_sets->findStorage(set_key))
                    return set;

                if (StorageSet * storage_set = dynamic_cast<StorageSet *>(table.get()))
                    return data.prepared_sets->addFromStorage(set_key, storage_set->getSet());
            }

            if (!data.getContext()->isGlobalContext())
            {
                /// If we are reading from storage, it can be an external table which is used for GLOBAL IN.
                /// Here, we take FutureSet which is used to build external table.
                /// It will be used if set is useful for primary key. During PK analysis
                /// temporary table is not filled yet, so we need to fill it first.
                if (auto tmp_table = data.getContext()->findExternalTable(identifier->getColumnName()))
                    external_table_set = tmp_table->future_set;
            }
        }

        std::unique_ptr<QueryPlan> source = std::make_unique<QueryPlan>();

        /** The following happens for GLOBAL INs or INs:
          * - in the addExternalStorage function, the IN (SELECT ...) subquery is replaced with IN _data1,
          *   in the subquery_for_set object, this subquery is set as source and the temporary table _data1 as the table.
          * - this function shows the expression IN_data1.
          *
          * In case that we have HAVING with IN subquery, we have to force creating set for it.
          * Also it doesn't make sense if it is GLOBAL IN or ordinary IN.
          */
        {
            auto interpreter = interpretSubquery(right_in_operand, data.getContext(), data.subquery_depth, {});
            interpreter->buildQueryPlan(*source);
        }

        return data.prepared_sets->addFromSubquery(
            set_key, std::move(source), nullptr, std::move(external_table_set), data.getContext()->getSettingsRef());
    }
    else
    {
        const auto & last_actions = data.actions_stack.getLastActions();
        const auto & index = data.actions_stack.getLastActionsIndex();
        if (data.prepared_sets && index.contains(left_in_operand->getColumnName()))
            /// An explicit enumeration of values in parentheses.
            return makeExplicitSet(&node, last_actions, data.getContext(), *data.prepared_sets);
        else
            return {};
    }
}

}
