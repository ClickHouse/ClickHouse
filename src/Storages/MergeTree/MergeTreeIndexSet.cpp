#include <Storages/MergeTree/MergeTreeIndexSet.h>

#include <DataTypes/IDataType.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>

#include <Functions/FunctionFactory.h>
#include <Planner/PlannerActionsVisitor.h>

#include <Storages/MergeTree/MergeTreeIndexUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

/// 0b11 -- can be true and false at the same time
static const Field UNKNOWN_FIELD(3u);


MergeTreeIndexGranuleSet::MergeTreeIndexGranuleSet(
    const String & index_name_,
    const Block & index_sample_block_,
    size_t max_rows_)
    : index_name(index_name_)
    , max_rows(max_rows_)
    , index_sample_block(index_sample_block_)
    , block(index_sample_block)
{
}

MergeTreeIndexGranuleSet::MergeTreeIndexGranuleSet(
    const String & index_name_,
    const Block & index_sample_block_,
    size_t max_rows_,
    MutableColumns && mutable_columns_)
    : index_name(index_name_)
    , max_rows(max_rows_)
    , index_sample_block(index_sample_block_)
    , block(index_sample_block.cloneWithColumns(std::move(mutable_columns_)))
{
}

void MergeTreeIndexGranuleSet::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty set index {}.", backQuote(index_name));

    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();

    if (max_rows != 0 && size() > max_rows)
    {
        size_serialization->serializeBinary(0, ostr, {});
        return;
    }

    size_serialization->serializeBinary(size(), ostr, {});

    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        const auto & type = index_sample_block.getByPosition(i).type;

        ISerialization::SerializeBinaryBulkSettings settings;
        settings.getter = [&ostr](ISerialization::SubstreamPath) -> WriteBuffer * { return &ostr; };
        settings.position_independent_encoding = false;
        settings.low_cardinality_max_dictionary_size = 0;

        auto serialization = type->getDefaultSerialization();
        ISerialization::SerializeBinaryBulkStatePtr state;

        const auto & column = *block.getByPosition(i).column;
        serialization->serializeBinaryBulkStatePrefix(column, settings, state);
        serialization->serializeBinaryBulkWithMultipleStreams(column, 0, size(), settings, state);
        serialization->serializeBinaryBulkStateSuffix(settings, state);
    }
}

void MergeTreeIndexGranuleSet::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    block.clear();

    Field field_rows;
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    size_type->getDefaultSerialization()->deserializeBinary(field_rows, istr, {});
    size_t rows_to_read = field_rows.get<size_t>();

    if (rows_to_read == 0)
        return;

    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        const auto & column = index_sample_block.getByPosition(i);
        const auto & type = column.type;
        ColumnPtr new_column = type->createColumn();


        ISerialization::DeserializeBinaryBulkSettings settings;
        settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &istr; };
        settings.position_independent_encoding = false;

        ISerialization::DeserializeBinaryBulkStatePtr state;
        auto serialization = type->getDefaultSerialization();

        serialization->deserializeBinaryBulkStatePrefix(settings, state);
        serialization->deserializeBinaryBulkWithMultipleStreams(new_column, rows_to_read, settings, state, nullptr);

        block.insert(ColumnWithTypeAndName(new_column, type, column.name));
    }
}


MergeTreeIndexAggregatorSet::MergeTreeIndexAggregatorSet(const String & index_name_, const Block & index_sample_block_, size_t max_rows_)
    : index_name(index_name_)
    , max_rows(max_rows_)
    , index_sample_block(index_sample_block_)
    , columns(index_sample_block_.cloneEmptyColumns())
{
    ColumnRawPtrs column_ptrs;
    column_ptrs.reserve(index_sample_block.columns());
    Columns materialized_columns;
    for (const auto & column : index_sample_block.getColumns())
    {
        materialized_columns.emplace_back(column->convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality());
        column_ptrs.emplace_back(materialized_columns.back().get());
    }

    data.init(ClearableSetVariants::chooseMethod(column_ptrs, key_sizes));

    columns = index_sample_block.cloneEmptyColumns();
}

void MergeTreeIndexAggregatorSet::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The provided position is not less than the number of block rows. "
                "Position: {}, Block rows: {}.", *pos, block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);

    if (max_rows && size() > max_rows)
    {
        *pos += rows_read;
        return;
    }

    ColumnRawPtrs index_column_ptrs;
    index_column_ptrs.reserve(index_sample_block.columns());
    Columns materialized_columns;
    const Names index_columns = index_sample_block.getNames();
    for (const auto & column_name : index_columns)
    {
        materialized_columns.emplace_back(
                block.getByName(column_name).column->convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality());
        index_column_ptrs.emplace_back(materialized_columns.back().get());
    }

    IColumn::Filter filter(block.rows(), 0);

    bool has_new_data = false;
    switch (data.type)
    {
        case ClearableSetVariants::Type::EMPTY:
            break;
#define M(NAME) \
        case ClearableSetVariants::Type::NAME: \
            has_new_data = buildFilter(*data.NAME, index_column_ptrs, filter, *pos, rows_read, data); \
            break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    if (has_new_data)
    {
        for (size_t i = 0; i < columns.size(); ++i)
        {
            auto filtered_column = block.getByName(index_columns[i]).column->filter(filter, block.rows());
            columns[i]->insertRangeFrom(*filtered_column, 0, filtered_column->size());
        }
    }

    *pos += rows_read;
}

template <typename Method>
bool MergeTreeIndexAggregatorSet::buildFilter(
    Method & method,
    const ColumnRawPtrs & column_ptrs,
    IColumn::Filter & filter,
    size_t pos,
    size_t limit,
    ClearableSetVariants & variants) const
{
    /// Like DistinctSortedTransform.
    typename Method::State state(column_ptrs, key_sizes, nullptr);

    bool has_new_data = false;
    for (size_t i = 0; i < limit; ++i)
    {
        auto emplace_result = state.emplaceKey(method.data, pos + i, variants.string_pool);

        if (emplace_result.isInserted())
            has_new_data = true;

        /// Emit the record if there is no such key in the current set yet.
        /// Skip it otherwise.
        filter[pos + i] = emplace_result.isInserted();
    }
    return has_new_data;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorSet::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleSet>(index_name, index_sample_block, max_rows, std::move(columns));

    switch (data.type)
    {
        case ClearableSetVariants::Type::EMPTY:
            break;
#define M(NAME) \
        case ClearableSetVariants::Type::NAME: \
            data.NAME->data.clear(); \
            break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    columns = index_sample_block.cloneEmptyColumns();

    return granule;
}


MergeTreeIndexConditionSet::MergeTreeIndexConditionSet(
    const String & index_name_,
    const Block & index_sample_block,
    size_t max_rows_,
    const ActionsDAGPtr & filter_dag,
    ContextPtr context)
    : index_name(index_name_)
    , max_rows(max_rows_)
{
    for (const auto & name : index_sample_block.getNames())
        if (!key_columns.contains(name))
            key_columns.insert(name);

    if (!filter_dag)
        return;

    if (checkDAGUseless(*filter_dag->getOutputs().at(0), context))
        return;

    auto filter_actions_dag = filter_dag->clone();
    const auto * filter_actions_dag_node = filter_actions_dag->getOutputs().at(0);

    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> node_to_result_node;
    filter_actions_dag->getOutputs()[0] = &traverseDAG(*filter_actions_dag_node, filter_actions_dag, context, node_to_result_node);

    filter_actions_dag->removeUnusedActions();
    actions = std::make_shared<ExpressionActions>(filter_actions_dag);
}

bool MergeTreeIndexConditionSet::alwaysUnknownOrTrue() const
{
    return isUseless();
}

bool MergeTreeIndexConditionSet::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    if (isUseless())
        return true;

    auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleSet>(idx_granule);
    if (!granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Set index condition got a granule with the wrong type");

    if (isUseless() || granule->empty() || (max_rows != 0 && granule->size() > max_rows))
        return true;

    Block result = granule->block;
    actions->execute(result);

    const auto & filter_node_name = actions->getActionsDAG().getOutputs().at(0)->result_name;
    auto column = result.getByName(filter_node_name).column->convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();

    if (column->onlyNull())
        return false;

    const auto * col_uint8 = typeid_cast<const ColumnUInt8 *>(column.get());

    const NullMap * null_map = nullptr;

    if (const auto * col_nullable = checkAndGetColumn<ColumnNullable>(&*column))
    {
        col_uint8 = typeid_cast<const ColumnUInt8 *>(&col_nullable->getNestedColumn());
        null_map = &col_nullable->getNullMapData();
    }

    if (!col_uint8)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ColumnUInt8 expected as Set index condition result");

    const auto & condition = col_uint8->getData();
    size_t column_size = column->size();

    for (size_t i = 0; i < column_size; ++i)
        if ((!null_map || (*null_map)[i] == 0) && condition[i] & 1)
            return true;

    return false;
}


const ActionsDAG::Node & MergeTreeIndexConditionSet::traverseDAG(const ActionsDAG::Node & node,
    ActionsDAGPtr & result_dag,
    const ContextPtr & context,
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> & node_to_result_node) const
{
    auto result_node_it = node_to_result_node.find(&node);
    if (result_node_it != node_to_result_node.end())
        return *result_node_it->second;

    const ActionsDAG::Node * result_node = nullptr;

    if (const auto * operator_node_ptr = operatorFromDAG(node, result_dag, context, node_to_result_node))
    {
        result_node = operator_node_ptr;
    }
    else if (const auto * atom_node_ptr = atomFromDAG(node, result_dag, context))
    {
        result_node = atom_node_ptr;

        if (atom_node_ptr->type == ActionsDAG::ActionType::INPUT ||
            atom_node_ptr->type == ActionsDAG::ActionType::FUNCTION)
        {
            auto bit_wrapper_function = FunctionFactory::instance().get("__bitWrapperFunc", context);
            result_node = &result_dag->addFunction(bit_wrapper_function, {atom_node_ptr}, {});
        }
    }
    else
    {
        ColumnWithTypeAndName unknown_field_column_with_type;

        unknown_field_column_with_type.name = calculateConstantActionNodeName(UNKNOWN_FIELD);
        unknown_field_column_with_type.type = std::make_shared<DataTypeUInt8>();
        unknown_field_column_with_type.column = unknown_field_column_with_type.type->createColumnConst(1, UNKNOWN_FIELD);

        result_node = &result_dag->addColumn(unknown_field_column_with_type);
    }

    node_to_result_node.emplace(&node, result_node);
    return *result_node;
}

const ActionsDAG::Node * MergeTreeIndexConditionSet::atomFromDAG(const ActionsDAG::Node & node, ActionsDAGPtr & result_dag, const ContextPtr & context) const
{
    /// Function, literal or column

    const auto * node_to_check = &node;
    while (node_to_check->type == ActionsDAG::ActionType::ALIAS)
        node_to_check = node_to_check->children[0];

    if (node_to_check->column && isColumnConst(*node_to_check->column))
        return &node;

    RPNBuilderTreeContext tree_context(context);
    RPNBuilderTreeNode tree_node(node_to_check, tree_context);

    auto column_name = tree_node.getColumnName();
    if (key_columns.contains(column_name))
    {
        const auto * result_node = node_to_check;

        if (node.type != ActionsDAG::ActionType::INPUT)
            result_node = &result_dag->addInput(column_name, node.result_type);

        return result_node;
    }

    if (node.type != ActionsDAG::ActionType::FUNCTION)
        return nullptr;

    const auto & arguments = node.children;
    size_t arguments_size = arguments.size();

    ActionsDAG::NodeRawConstPtrs children(arguments_size);

    for (size_t i = 0; i < arguments_size; ++i)
    {
        children[i] = atomFromDAG(*arguments[i], result_dag, context);

        if (!children[i])
            return nullptr;
    }

    return &result_dag->addFunction(node.function_base, children, {});
}

const ActionsDAG::Node * MergeTreeIndexConditionSet::operatorFromDAG(const ActionsDAG::Node & node,
    ActionsDAGPtr & result_dag,
    const ContextPtr & context,
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> & node_to_result_node) const
{
    /// Functions AND, OR, NOT. Replace with bit*.

    const auto * node_to_check = &node;
    while (node_to_check->type == ActionsDAG::ActionType::ALIAS)
        node_to_check = node_to_check->children[0];

    if (node_to_check->column && isColumnConst(*node_to_check->column))
        return nullptr;

    if (node_to_check->type != ActionsDAG::ActionType::FUNCTION)
        return nullptr;

    auto function_name = node_to_check->function->getName();
    const auto & arguments = node_to_check->children;
    size_t arguments_size = arguments.size();

    if (function_name == "not")
    {
        if (arguments_size != 1)
            return nullptr;

        const ActionsDAG::Node * argument = &traverseDAG(*arguments[0], result_dag, context, node_to_result_node);

        auto bit_swap_last_two_function = FunctionFactory::instance().get("__bitSwapLastTwo", context);
        return &result_dag->addFunction(bit_swap_last_two_function, {argument}, {});
    }
    else if (function_name == "and" || function_name == "indexHint" || function_name == "or")
    {
        if (arguments_size < 2)
            return nullptr;

        ActionsDAG::NodeRawConstPtrs children;
        children.resize(arguments_size);

        for (size_t i = 0; i < arguments_size; ++i)
            children[i] = &traverseDAG(*arguments[i], result_dag, context, node_to_result_node);

        FunctionOverloadResolverPtr function;

        if (function_name == "and" || function_name == "indexHint")
            function = FunctionFactory::instance().get("__bitBoolMaskAnd", context);
        else
            function = FunctionFactory::instance().get("__bitBoolMaskOr", context);

        const auto * last_argument = children.back();
        children.pop_back();

        const auto * before_last_argument = children.back();
        children.pop_back();

        while (true)
        {
            last_argument = &result_dag->addFunction(function, {before_last_argument, last_argument}, {});

            if (children.empty())
                break;

            before_last_argument = children.back();
            children.pop_back();
        }

        return last_argument;
    }

    return nullptr;
}

bool MergeTreeIndexConditionSet::checkDAGUseless(const ActionsDAG::Node & node, const ContextPtr & context, bool atomic) const
{
    const auto * node_to_check = &node;
    while (node_to_check->type == ActionsDAG::ActionType::ALIAS)
        node_to_check = node_to_check->children[0];

    RPNBuilderTreeContext tree_context(context);
    RPNBuilderTreeNode tree_node(node_to_check, tree_context);

    if (node.column && isColumnConst(*node.column)
        && !WhichDataType(node.result_type).isSet())
    {
        Field literal;
        node.column->get(0, literal);
        return !atomic && literal.safeGet<bool>();
    }
    else if (node.type == ActionsDAG::ActionType::FUNCTION)
    {
        auto column_name = tree_node.getColumnName();
        if (key_columns.contains(column_name))
            return false;

        auto function_name = node.function_base->getName();
        const auto & arguments = node.children;

        if (function_name == "and" || function_name == "indexHint")
            return std::all_of(arguments.begin(), arguments.end(), [&, atomic](const auto & arg) { return checkDAGUseless(*arg, context, atomic); });
        else if (function_name == "or")
            return std::any_of(arguments.begin(), arguments.end(), [&, atomic](const auto & arg) { return checkDAGUseless(*arg, context, atomic); });
        else if (function_name == "not")
            return checkDAGUseless(*arguments.at(0), context, atomic);
        else
            return std::any_of(arguments.begin(), arguments.end(),
                [&](const auto & arg) { return checkDAGUseless(*arg, context, true /*atomic*/); });
    }

    auto column_name = tree_node.getColumnName();
    return !key_columns.contains(column_name);
}

void MergeTreeIndexConditionSet::traverseAST(ASTPtr & node) const
{
    if (operatorFromAST(node))
    {
        auto & args = node->as<ASTFunction>()->arguments->children;

        for (auto & arg : args)
            traverseAST(arg);
        return;
    }

    if (atomFromAST(node))
    {
        if (node->as<ASTIdentifier>() || node->as<ASTFunction>())
            /// __bitWrapperFunc* uses default implementation for Nullable types
            /// Here we additionally convert Null to 0,
            /// otherwise condition 'something OR NULL' will always return Null and filter everything.
            node = makeASTFunction("__bitWrapperFunc", makeASTFunction("ifNull", node, std::make_shared<ASTLiteral>(Field(0))));
    }
    else
        node = std::make_shared<ASTLiteral>(UNKNOWN_FIELD);
}

bool MergeTreeIndexConditionSet::atomFromAST(ASTPtr & node) const
{
    /// Function, literal or column

    if (node->as<ASTLiteral>())
        return true;

    if (const auto * identifier = node->as<ASTIdentifier>())
        return key_columns.contains(identifier->getColumnName());

    if (auto * func = node->as<ASTFunction>())
    {
        if (key_columns.contains(func->getColumnName()))
        {
            /// Function is already calculated.
            node = std::make_shared<ASTIdentifier>(func->getColumnName());
            return true;
        }

        auto & args = func->arguments->children;

        for (auto & arg : args)
            if (!atomFromAST(arg))
                return false;

        return true;
    }

    return false;
}

bool MergeTreeIndexConditionSet::operatorFromAST(ASTPtr & node)
{
    /// Functions AND, OR, NOT. Replace with bit*.
    auto * func = node->as<ASTFunction>();
    if (!func)
        return false;

    auto & args = func->arguments->children;

    if (func->name == "not")
    {
        if (args.size() != 1)
            return false;

        func->name = "__bitSwapLastTwo";
    }
    else if (func->name == "and" || func->name == "indexHint")
    {
        if (args.size() < 2)
            return false;

        auto last_arg = args.back();
        args.pop_back();

        ASTPtr new_func;
        if (args.size() > 1)
            new_func = makeASTFunction(
                    "__bitBoolMaskAnd",
                    node,
                    last_arg);
        else
            new_func = makeASTFunction(
                    "__bitBoolMaskAnd",
                    args.back(),
                    last_arg);

        node = new_func;
    }
    else if (func->name == "or")
    {
        if (args.size() < 2)
            return false;

        auto last_arg = args.back();
        args.pop_back();

        ASTPtr new_func;
        if (args.size() > 1)
            new_func = makeASTFunction(
                    "__bitBoolMaskOr",
                    node,
                    last_arg);
        else
            new_func = makeASTFunction(
                    "__bitBoolMaskOr",
                    args.back(),
                    last_arg);

        node = new_func;
    }
    else
        return false;

    return true;
}

bool MergeTreeIndexConditionSet::checkASTUseless(const ASTPtr & node, bool atomic) const
{
    if (!node)
        return true;

    if (const auto * func = node->as<ASTFunction>())
    {
        if (key_columns.contains(func->getColumnName()))
            return false;

        const ASTs & args = func->arguments->children;

        if (func->name == "and" || func->name == "indexHint")
            return std::all_of(args.begin(), args.end(), [this, atomic](const auto & arg) { return checkASTUseless(arg, atomic); });
        else if (func->name == "or")
            return std::any_of(args.begin(), args.end(), [this, atomic](const auto & arg) { return checkASTUseless(arg, atomic); });
        else if (func->name == "not")
            return checkASTUseless(args[0], atomic);
        else
            return std::any_of(args.begin(), args.end(),
                [this](const auto & arg) { return checkASTUseless(arg, true); });
    }
    else if (const auto * literal = node->as<ASTLiteral>())
        return !atomic && literal->value.safeGet<bool>();
    else if (const auto * identifier = node->as<ASTIdentifier>())
        return !key_columns.contains(identifier->getColumnName());
    else
        return true;
}


MergeTreeIndexGranulePtr MergeTreeIndexSet::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleSet>(index.name, index.sample_block, max_rows);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexSet::createIndexAggregator(const MergeTreeWriterSettings & /*settings*/) const
{
    return std::make_shared<MergeTreeIndexAggregatorSet>(index.name, index.sample_block, max_rows);
}

MergeTreeIndexConditionPtr MergeTreeIndexSet::createIndexCondition(
    const ActionsDAGPtr & filter_actions_dag, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionSet>(index.name, index.sample_block, max_rows, filter_actions_dag, context);
}

MergeTreeIndexPtr setIndexCreator(const IndexDescription & index)
{
    size_t max_rows = index.arguments[0].get<size_t>();
    return std::make_shared<MergeTreeIndexSet>(index, max_rows);
}

void setIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    if (index.arguments.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Set index must have exactly one argument.");
    else if (index.arguments[0].getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Set index argument must be positive integer.");
}

}
