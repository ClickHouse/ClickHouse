#include <Storages/MergeTree/MergeTreeIndexSet.h>

#include <Common/FieldAccurateComparison.h>
#include <Common/quoteString.h>

#include <DataTypes/IDataType.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/PreparedSets.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/indexHint.h>
#include <Planner/PlannerActionsVisitor.h>

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
    , block(index_sample_block_.cloneEmpty())
{
    size_t num_columns = block.columns();
    serializations.resize(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        serializations[i] = block.getByPosition(i).type->getDefaultSerialization();
}

MergeTreeIndexGranuleSet::MergeTreeIndexGranuleSet(
    const String & index_name_,
    const Block & index_sample_block_,
    size_t max_rows_,
    MutableColumns && mutable_columns_,
    std::vector<Range> && set_hyperrectangle_)
    : index_name(index_name_)
    , max_rows(max_rows_)
    , block(index_sample_block_.cloneWithColumns(std::move(mutable_columns_)))
    , set_hyperrectangle(std::move(set_hyperrectangle_))
{
    size_t num_columns = block.columns();
    serializations.resize(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        serializations[i] = block.getByPosition(i).type->getDefaultSerialization();
}

void MergeTreeIndexGranuleSet::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty set index {}.", backQuote(index_name));

    if (max_rows != 0 && size() > max_rows)
    {
        UInt64 rows = 0;
        writeBinary(rows, ostr);
        return;
    }

    UInt64 rows = size();
    writeBinary(rows, ostr);
    size_t num_columns = block.columns();

    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & elem = block.getByPosition(i);

        ISerialization::SerializeBinaryBulkSettings settings;
        settings.getter = [&ostr](ISerialization::SubstreamPath) -> WriteBuffer * { return &ostr; };
        settings.position_independent_encoding = false;
        settings.low_cardinality_max_dictionary_size = 0;

        ISerialization::SerializeBinaryBulkStatePtr state;

        const auto & column = *elem.column;
        serializations[i]->serializeBinaryBulkStatePrefix(column, settings, state);
        serializations[i]->serializeBinaryBulkWithMultipleStreams(column, 0, rows, settings, state);
        serializations[i]->serializeBinaryBulkStateSuffix(settings, state);
    }
}

void MergeTreeIndexGranuleSet::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    UInt64 rows_to_read = 0;
    readBinary(rows_to_read, istr);
    if (rows_to_read == 0)
        return;

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &istr; };
    settings.position_independent_encoding = false;

    size_t num_columns = block.columns();
    set_hyperrectangle.clear();
    Field min_val;
    Field max_val;

    for (size_t i = 0; i < num_columns; ++i)
    {
        auto & elem = block.getByPosition(i);
        elem.column = elem.column->cloneEmpty();

        ISerialization::DeserializeBinaryBulkStatePtr state;

        serializations[i]->deserializeBinaryBulkStatePrefix(settings, state, nullptr);
        serializations[i]->deserializeBinaryBulkWithMultipleStreams(elem.column, 0, rows_to_read, settings, state, nullptr);

        if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(elem.column.get()))
            column_nullable->getExtremesNullLast(min_val, max_val);
        else
            elem.column->getExtremes(min_val, max_val);

        set_hyperrectangle.emplace_back(min_val, true, max_val, true);
    }
}


MergeTreeIndexBulkGranulesSet::MergeTreeIndexBulkGranulesSet(
    const Block & index_sample_block_)
    : block(index_sample_block_.cloneEmpty()),
    block_for_reading(index_sample_block_.cloneEmpty())
{
    size_t num_columns = block.columns();
    serializations.resize(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        serializations[i] = block.getByPosition(i).type->getDefaultSerialization();

    block.insert(ColumnWithTypeAndName
    {
        ColumnUInt64::create(),
        std::make_shared<DataTypeUInt64>(),
        "_granule_num"
    });
}


void MergeTreeIndexBulkGranulesSet::deserializeBinary(size_t granule_num, ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    if (empty)
    {
        min_granule = granule_num;
        empty = false;
    }
    max_granule = granule_num;

    UInt64 rows_to_read;
    readBinary(rows_to_read, istr);
    if (rows_to_read == 0)
        return;

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &istr; };
    settings.position_independent_encoding = false;

    size_t num_columns = block.columns() - 1;

    /// Due to using of position-dependent encoding, we have to read into a temporary block and then move to the accumulating block.
    for (size_t i = 0; i < num_columns; ++i)
    {
        auto column = block_for_reading.getByPosition(i).column;
        ISerialization::DeserializeBinaryBulkStatePtr state;

        serializations[i]->deserializeBinaryBulkStatePrefix(settings, state, nullptr);
        serializations[i]->deserializeBinaryBulkWithMultipleStreams(column, 0, rows_to_read, settings, state, nullptr);

        block.getByPosition(i).column->assumeMutableRef().insertRangeFrom(*column, 0, rows_to_read);
        column->assumeMutableRef().popBack(rows_to_read);
    }

    /// The last column is designating the granule
    auto & elem = block.getByPosition(num_columns);
    MutableColumnPtr granule_num_column = elem.column->assumeMutable();

    auto & data = assert_cast<ColumnUInt64 &>(*granule_num_column).getData();
    for (size_t i = 0; i < rows_to_read; ++i)
        data.push_back(granule_num);
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
        FieldRef field_min;
        FieldRef field_max;
        for (size_t i = 0; i < columns.size(); ++i)
        {
            auto filtered_column = block.getByName(index_columns[i]).column->filter(filter, block.rows());
            columns[i]->insertRangeFrom(*filtered_column, 0, filtered_column->size());

            if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(filtered_column.get()))
                column_nullable->getExtremesNullLast(field_min, field_max);
            else
                filtered_column->getExtremes(field_min, field_max);

            if (set_hyperrectangle.size() <= i)
            {
                set_hyperrectangle.emplace_back(field_min, true, field_max, true);
            }
            else
            {
                set_hyperrectangle[i].left
                    = accurateLess(set_hyperrectangle[i].left, field_min) ? set_hyperrectangle[i].left : field_min;
                set_hyperrectangle[i].right
                    = accurateLess(set_hyperrectangle[i].right, field_max) ? field_max : set_hyperrectangle[i].right;
            }
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
    auto granule = std::make_shared<MergeTreeIndexGranuleSet>(index_name, index_sample_block, max_rows, std::move(columns), std::move(set_hyperrectangle));

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

KeyCondition buildCondition(const IndexDescription & index, const ActionsDAGWithInversionPushDown & filter_dag, ContextPtr context)
{
    return KeyCondition{filter_dag, context, index.column_names, index.expression};
}

MergeTreeIndexConditionSet::MergeTreeIndexConditionSet(
    size_t max_rows_,
    const ActionsDAGWithInversionPushDown & filter_dag,
    ContextPtr context,
    const IndexDescription & index_description)
    : index_name(index_description.name)
    , max_rows(max_rows_)
    , index_data_types(index_description.data_types)
    , condition(buildCondition(index_description, filter_dag, context))
{
    for (const auto & name : index_description.sample_block.getNames())
        if (!key_columns.contains(name))
            key_columns.insert(name);

    if (!filter_dag.predicate)
        return;

    std::vector<FutureSetPtr> sets_to_prepare;
    if (checkDAGUseless(*filter_dag.predicate, context, sets_to_prepare))
        return;
    /// Try to run subqueries, don't use index if failed (e.g. if use_index_for_in_with_subqueries is disabled).
    for (auto & set : sets_to_prepare)
        if (!set->buildOrderedSetInplace(context))
            return;

    auto filter_actions_dag = filter_dag.dag->clone();
    const auto * filter_actions_dag_node = filter_actions_dag.getOutputs().at(0);

    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> node_to_result_node;
    const auto & predicate_node = traverseDAG(*filter_actions_dag_node, filter_actions_dag, context, node_to_result_node);

    auto sub_dag = ActionsDAG::cloneSubDAG({&predicate_node}, false);

    actions_output_column_name = sub_dag.getOutputs().at(0)->result_name;
    actions = std::make_shared<ExpressionActions>(std::move(sub_dag));
}

bool MergeTreeIndexConditionSet::alwaysUnknownOrTrue() const
{
    return isUseless();
}

bool MergeTreeIndexConditionSet::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    if (isUseless())
        return true;

    const MergeTreeIndexGranuleSet & granule = assert_cast<const MergeTreeIndexGranuleSet &>(*idx_granule);

    size_t size = granule.size();
    if (size == 0 || (max_rows != 0 && size > max_rows))
        return true;

    if (!condition.checkInHyperrectangle(granule.set_hyperrectangle, index_data_types).can_be_true)
        return false;

    Block result = granule.block;
    actions->execute(result);

    const auto & column = result.getByName(actions_output_column_name).column;

    for (size_t i = 0; i < size; ++i)
        if (!column->isNullAt(i) && (column->get64(i) & 1))
            return true;

    return false;
}

MergeTreeIndexConditionSet::FilteredGranules MergeTreeIndexConditionSet::getPossibleGranules(const MergeTreeIndexBulkGranulesPtr & idx_granules) const
{
    FilteredGranules res;
    const MergeTreeIndexBulkGranulesSet & granules = assert_cast<const MergeTreeIndexBulkGranulesSet &>(*idx_granules);

    /// This is the range of granules we will analyze.
    /// We will return a subset of granules from this range.
    size_t total_granules = 1 + granules.max_granule - granules.min_granule;

    /// This block contains values of all set elements of all granules,
    /// along with the corresponding granule number for them.
    /// Some granules may not have any info, so they are not present in the block.
    Block block = granules.block;
    size_t block_size = block.rows();

    /// Condition is useless (does not allow using the index)
    /// Or index granules have no set elements.
    auto whole_range = [total_granules, min_granule = granules.min_granule]
    {
        FilteredGranules whole;
        whole.resize(total_granules);
        for (size_t i = 0; i < total_granules; ++i)
            whole[i] = min_granule + i;
        return whole;
    };

    if (block_size == 0 || isUseless())
        return whole_range();

    /// Calculate the condition expression on top of every element of every set.
    actions->execute(block);

    /// This is the UInt8 mask of elements that passed the condition.
    const auto & column = block.getByName(actions_output_column_name).column->convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();
    if (column->onlyNull())
        return whole_range();

    const auto * col_uint8 = typeid_cast<const ColumnUInt8 *>(column.get());
    const NullMap * null_map = nullptr;

    if (const auto * col_nullable = checkAndGetColumn<ColumnNullable>(&*column))
    {
        col_uint8 = typeid_cast<const ColumnUInt8 *>(&col_nullable->getNestedColumn());
        null_map = &col_nullable->getNullMapData();
    }

    if (!col_uint8)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ColumnUInt8 is expected as a Set index condition result");

    const auto & filter_result = col_uint8->getData();
    /// The column containing the corresponding granule nums. We will take the granules where the filter is true.
    const auto & granule_nums = assert_cast<const ColumnUInt64 &>(*block.getByName("_granule_num").column).getData();

    /// We will iterate over the range of granules and check if there is at least one elemnent from the sets passing the filter.
    /// But if there are no set elements at all, it means no info, and the granule will also pass.
    size_t block_pos = 0;
    for (size_t i = 0; i < total_granules; ++i)
    {
        size_t current_granule = granules.min_granule + i;
        size_t current_granule_in_block = block_pos < block_size ? granule_nums[block_pos] : 0;

        if (block_pos >= block_size || current_granule < current_granule_in_block)
        {
            /// This granule does not have any info in the index - it passes.
            res.push_back(current_granule);
        }
        else
        {
            /// This granule has set elements - check if any of them pass the filter.
            chassert(current_granule == current_granule_in_block);
            bool passed = false;
            while (block_pos < block_size && current_granule_in_block == granule_nums[block_pos])
            {
                if (!passed)
                {
                    passed = (!null_map || !(*null_map)[block_pos]) && (filter_result[block_pos] & 1);
                    if (passed)
                        res.push_back(current_granule);
                    /// After we found that it passes, we just iterate over the block to the next granule or the end.
                }
                ++block_pos;
            }
        }
    }

    return res;
}


static const ActionsDAG::NodeRawConstPtrs & getArguments(const ActionsDAG::Node & node, ActionsDAG * result_dag_or_null, ActionsDAG::NodeRawConstPtrs * storage)
{
    chassert(node.type == ActionsDAG::ActionType::FUNCTION);
    if (node.function_base->getName() != "indexHint")
        return node.children;

    /// indexHint arguments are stored inside of `FunctionIndexHint` class.
    const auto & adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor &>(*node.function_base);
    const auto & index_hint = typeid_cast<const FunctionIndexHint &>(*adaptor.getFunction());
    if (!result_dag_or_null)
        return index_hint.getActions().getOutputs();

    /// Import the DAG and map argument pointers.
    auto actions_clone = index_hint.getActions().clone();
    chassert(storage);
    result_dag_or_null->mergeNodes(std::move(actions_clone), storage);
    return *storage;
}

const ActionsDAG::Node & MergeTreeIndexConditionSet::traverseDAG(const ActionsDAG::Node & node,
    ActionsDAG & result_dag,
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
            result_node = &result_dag.addFunction(bit_wrapper_function, {atom_node_ptr}, {});
        }
    }
    else
    {
        ColumnWithTypeAndName unknown_field_column_with_type;

        unknown_field_column_with_type.name = calculateConstantActionNodeName(UNKNOWN_FIELD);
        unknown_field_column_with_type.type = std::make_shared<DataTypeUInt8>();
        unknown_field_column_with_type.column = unknown_field_column_with_type.type->createColumnConst(1, UNKNOWN_FIELD);

        result_node = &result_dag.addColumn(unknown_field_column_with_type);
    }

    node_to_result_node.emplace(&node, result_node);
    return *result_node;
}

const ActionsDAG::Node * MergeTreeIndexConditionSet::atomFromDAG(const ActionsDAG::Node & node, ActionsDAG & result_dag, const ContextPtr & context) const
{
    /// Function, literal or column

    const auto * node_to_check = &node;
    while (node_to_check->type == ActionsDAG::ActionType::ALIAS)
        node_to_check = node_to_check->children[0];

    if (node_to_check->column && (isColumnConst(*node_to_check->column) || WhichDataType(node.result_type).isSet()))
        return &node;

    RPNBuilderTreeContext tree_context(context);
    RPNBuilderTreeNode tree_node(node_to_check, tree_context);

    auto column_name = tree_node.getColumnName();
    if (key_columns.contains(column_name))
    {
        const auto * result_node = node_to_check;

        if (node.type != ActionsDAG::ActionType::INPUT)
            result_node = &result_dag.addInput(column_name, node.result_type);

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

    return &result_dag.addFunction(node.function_base, children, {});
}

const ActionsDAG::Node * MergeTreeIndexConditionSet::operatorFromDAG(const ActionsDAG::Node & node,
    ActionsDAG & result_dag,
    const ContextPtr & context,
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> & node_to_result_node) const
{
    /// Functions AND, OR, NOT. Replace with bit*.

    const auto * node_to_check = &node;
    while (node_to_check->type == ActionsDAG::ActionType::ALIAS)
        node_to_check = node_to_check->children[0];

    if (node_to_check->column && (isColumnConst(*node_to_check->column) || WhichDataType(node.result_type).isSet()))
        return nullptr;

    if (node_to_check->type != ActionsDAG::ActionType::FUNCTION)
        return nullptr;

    auto function_name = node_to_check->function->getName();
    ActionsDAG::NodeRawConstPtrs temp_ptrs_to_argument;
    const auto & arguments = getArguments(*node_to_check, &result_dag, &temp_ptrs_to_argument);
    size_t arguments_size = arguments.size();

    if (function_name == "not")
    {
        if (arguments_size != 1)
            return nullptr;

        const ActionsDAG::Node * argument = &traverseDAG(*arguments[0], result_dag, context, node_to_result_node);

        auto bit_swap_last_two_function = FunctionFactory::instance().get("__bitSwapLastTwo", context);
        return &result_dag.addFunction(bit_swap_last_two_function, {argument}, {});
    }
    if (function_name == "and" || function_name == "indexHint" || function_name == "or")
    {
        if (arguments_size < 1)
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

        while (!children.empty())
        {
            const auto * before_last_argument = children.back();
            children.pop_back();

            last_argument = &result_dag.addFunction(function, {before_last_argument, last_argument}, {});
        }

        return last_argument;
    }

    return nullptr;
}

bool MergeTreeIndexConditionSet::checkDAGUseless(const ActionsDAG::Node & node, const ContextPtr & context, std::vector<FutureSetPtr> & sets_to_prepare, bool atomic) const
{
    const auto * node_to_check = &node;
    while (node_to_check->type == ActionsDAG::ActionType::ALIAS)
        node_to_check = node_to_check->children[0];

    RPNBuilderTreeContext tree_context(context);
    RPNBuilderTreeNode tree_node(node_to_check, tree_context);

    if (WhichDataType(node.result_type).isSet())
    {
        if (auto set = tree_node.tryGetPreparedSet())
            sets_to_prepare.push_back(set);
        return false;
    }
    if (node.column && isColumnConst(*node.column))
    {
        Field literal;
        node.column->get(0, literal);
        return !atomic && literal.safeGet<bool>();
    }
    if (node.type == ActionsDAG::ActionType::FUNCTION)
    {
        auto column_name = tree_node.getColumnName();
        if (key_columns.contains(column_name))
            return false;

        auto function_name = node.function_base->getName();
        const auto & arguments = getArguments(node, nullptr, nullptr);

        if (function_name == "and" || function_name == "indexHint")
        {
            /// Can't use std::all_of() because we have to call checkDAGUseless() for all arguments
            /// to populate sets_to_prepare.
            bool all_useless = true;
            for (const auto & arg : arguments)
            {
                bool u = checkDAGUseless(*arg, context, sets_to_prepare, atomic);
                all_useless = all_useless && u;
            }
            return all_useless;
        }
        if (function_name == "or")
            return std::any_of(
                arguments.begin(),
                arguments.end(),
                [&, atomic](const auto & arg) { return checkDAGUseless(*arg, context, sets_to_prepare, atomic); });
        if (function_name == "not")
            return checkDAGUseless(*arguments.at(0), context, sets_to_prepare, atomic);
        return std::any_of(
            arguments.begin(),
            arguments.end(),
            [&](const auto & arg) { return checkDAGUseless(*arg, context, sets_to_prepare, true /*atomic*/); });
    }

    auto column_name = tree_node.getColumnName();
    return !key_columns.contains(column_name);
}


MergeTreeIndexGranulePtr MergeTreeIndexSet::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleSet>(index.name, index.sample_block, max_rows);
}

MergeTreeIndexBulkGranulesPtr MergeTreeIndexSet::createIndexBulkGranules() const
{
    return std::make_shared<MergeTreeIndexBulkGranulesSet>(index.sample_block);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexSet::createIndexAggregator(const MergeTreeWriterSettings & /*settings*/) const
{
    return std::make_shared<MergeTreeIndexAggregatorSet>(index.name, index.sample_block, max_rows);
}

MergeTreeIndexConditionPtr MergeTreeIndexSet::createIndexCondition(
    const ActionsDAG::Node * predicate, ContextPtr context) const
{
    ActionsDAGWithInversionPushDown filter_dag(predicate, context);
    return std::make_shared<MergeTreeIndexConditionSet>(max_rows, filter_dag, context, index);
}

MergeTreeIndexPtr setIndexCreator(const IndexDescription & index)
{
    size_t max_rows = index.arguments[0].safeGet<size_t>();
    return std::make_shared<MergeTreeIndexSet>(index, max_rows);
}

void setIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    if (index.arguments.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Set index must have exactly one argument.");
    if (index.arguments[0].getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Set index argument must be positive integer.");
}

}
