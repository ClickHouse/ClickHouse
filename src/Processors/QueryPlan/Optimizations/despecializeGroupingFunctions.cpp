#include <bit>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/grouping.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/// One lookup-table entry per grouping set. ROLLUP produces keys + 1 sets and GROUPING SETS as
/// many as the query lists, so real tables are small; the cap is a backstop so a pathological
/// query cannot blow up the plan (the constant is serialized into every task of the stage).
/// CUBE is rewritten to bit arithmetic instead and needs no table.
constexpr UInt64 max_grouping_sets_to_despecialize = 1ULL << 16;

const FunctionGroupingBase * tryGetGroupingSpecialization(const ActionsDAG::Node & node)
{
    if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base)
        return nullptr;

    const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(node.function_base.get());
    if (!adaptor)
        return nullptr;

    /// dynamic_cast, not typeid_cast: the object is one of the final specializations and the cast
    /// target is their common base.
    const auto * grouping = dynamic_cast<const FunctionGroupingBase *>(adaptor->getFunction().get());
    /// Plain `grouping` is a placeholder that only exists before analysis resolves it; it cannot
    /// be executed, so it cannot appear in a plan.
    if (!grouping || grouping->getName() == "grouping")
        return nullptr;

    return grouping;
}

/// Compute the function's result for every possible `__grouping_set` value: a column of
/// `num_sets` masks, one per set index.
ColumnPtr computeGroupingMasks(const ActionsDAG::Node & node, UInt64 num_sets)
{
    auto set_indexes = ColumnUInt64::create();
    auto & set_indexes_data = set_indexes->getData();
    set_indexes_data.reserve(num_sets);
    for (UInt64 set_index = 0; set_index < num_sets; ++set_index)
        set_indexes_data.push_back(set_index);

    ColumnsWithTypeAndName arguments{{std::move(set_indexes), std::make_shared<DataTypeUInt64>(), "__grouping_set"}};

    const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(node.function_base.get());
    auto function_base = FunctionToOverloadResolverAdaptor(adaptor->getFunction()).build(arguments);
    return function_base->execute(arguments, function_base->getResultType(), num_sets, /* dry_run */ false);
}

/// The first argument is the `__grouping_set` column (see `GroupingFunctionsResolvePass`); the
/// rest only name the grouped columns and are not read. The planner may qualify the column name,
/// so check the suffix.
void checkFirstArgumentIsGroupingSet(const ActionsDAG::Node & node)
{
    if (node.children.empty() || !node.children[0]->result_name.ends_with("__grouping_set"))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "The first argument of grouping function specialization {} must be the __grouping_set column, got {}",
            node.result_name, node.children.empty() ? "no arguments" : node.children[0]->result_name);
}

/// CUBE produces 2^keys sets - too many to tabulate - but its `grouping` value is plain bit
/// arithmetic over the per-row `__grouping_set` column: `CubeTransform` numbers the sets so that
/// bit (keys - 1 - key_index) is set exactly when that key is aggregated over. For CUBE(a, b):
///
///   row produced by the set   __grouping_set   grouping(a)   grouping(b)
///   (a, b)       full group       0 = 0b00          0             0
///   (a, NULL)    subtotal         1 = 0b01          0             1
///   (NULL, b)    subtotal         2 = 0b10          1             0
///   (NULL, NULL) grand total      3 = 0b11          1             1
///
/// So the expression extracts one bit per `grouping` argument (inverted when
/// `force_grouping_standard_compatibility` is off) and packs them in argument order, first
/// argument highest.
const ActionsDAG::Node * buildCubeGroupingExpression(
    ActionsDAG & dag,
    const FunctionGroupingBase & grouping,
    UInt64 num_sets,
    const ActionsDAG::Node * grouping_set_node,
    const String & result_name)
{
    const auto & arguments_indexes = grouping.getArgumentsIndexes();
    const UInt64 keys_count = std::countr_zero(num_sets);

    auto uint64 = std::make_shared<DataTypeUInt64>();
    size_t const_counter = 0;
    auto add_const = [&](UInt64 value) -> const ActionsDAG::Node &
    {
        MutableColumnConstPtr column = uint64->createColumnConst(1, Field(value));
        return dag.addColumn(std::move(column), uint64, fmt::format("__grouping_c{}_of_{}", const_counter++, result_name));
    };

    auto bit_shift_right = FunctionFactory::instance().get("bitShiftRight", nullptr);
    auto bit_shift_left = FunctionFactory::instance().get("bitShiftLeft", nullptr);
    auto bit_and = FunctionFactory::instance().get("bitAnd", nullptr);
    auto bit_xor = FunctionFactory::instance().get("bitXor", nullptr);
    auto bit_or = FunctionFactory::instance().get("bitOr", nullptr);

    const size_t num_arguments = arguments_indexes.size();
    const ActionsDAG::Node & one_node = add_const(1);
    const ActionsDAG::Node * value = nullptr;
    for (size_t i = 0; i < num_arguments; ++i)
    {
        const ActionsDAG::Node * bit = grouping_set_node;
        if (const UInt64 shift = keys_count - 1 - arguments_indexes[i]; shift != 0)
            bit = &dag.addFunction(bit_shift_right, {bit, &add_const(shift)}, {});
        bit = &dag.addFunction(bit_and, {bit, &one_node}, {});
        if (!grouping.getForceCompatibility())
            bit = &dag.addFunction(bit_xor, {bit, &one_node}, {});

        const ActionsDAG::Node * term = bit;
        if (const UInt64 place = num_arguments - 1 - i; place != 0)
            term = &dag.addFunction(bit_shift_left, {bit, &add_const(place)}, {});
        value = value ? &dag.addFunction(bit_or, {value, term}, {}) : term;
    }
    return value;
}

using NodeReplacements = std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *>;

/// Rebuild the expression tree over the replaced nodes. Expression trees consist of FUNCTION and
/// ALIAS nodes over inputs and constants, so only those two need recursion.
const ActionsDAG::Node * replaceNodes(ActionsDAG & dag, const ActionsDAG::Node * node, const NodeReplacements & replacements)
{
    if (auto it = replacements.find(node); it != replacements.end())
        return it->second;

    if (node->type == ActionsDAG::ActionType::ALIAS)
    {
        const auto * new_child = replaceNodes(dag, node->children[0], replacements);
        if (new_child != node->children[0])
            return &dag.addAlias(*new_child, node->result_name);
    }
    else if (node->type == ActionsDAG::ActionType::FUNCTION)
    {
        ActionsDAG::NodeRawConstPtrs new_children;
        new_children.reserve(node->children.size());
        for (const auto * child : node->children)
            new_children.push_back(replaceNodes(dag, child, replacements));

        if (new_children != node->children)
            return &dag.addFunction(node->function_base, std::move(new_children), node->result_name);
    }

    return node;
}

bool despecializeGroupingFunctionsInDAG(ActionsDAG & dag)
{
    /// Collect first: the rewrite adds nodes, and new nodes must not be revisited.
    std::vector<const ActionsDAG::Node *> grouping_nodes;
    for (const auto & node : dag.getNodes())
        if (tryGetGroupingSpecialization(node))
            grouping_nodes.push_back(&node);

    if (grouping_nodes.empty())
        return false;

    NodeReplacements replacements;
    for (const auto * node : grouping_nodes)
    {
        const auto * grouping = tryGetGroupingSpecialization(*node);
        const UInt64 num_sets = grouping->getNumberOfGroupingSets();

        const ActionsDAG::Node * replacement = nullptr;
        if (grouping->getName() == "groupingOrdinary")
        {
            /// A single grouping set: the result is one value for every row. Materialize it so
            /// the column stays non-constant in the header, as the function's result was.
            auto masks = computeGroupingMasks(*node, num_sets);
            auto value_type = std::make_shared<DataTypeUInt64>();
            MutableColumnConstPtr value_column = value_type->createColumnConst(1, (*masks)[0]);
            const auto & value_node = dag.addColumn(
                std::move(value_column), value_type, fmt::format("__grouping_mask_of_{}", node->result_name));
            auto materialize = FunctionFactory::instance().get("materialize", nullptr);
            replacement = &dag.addFunction(materialize, {&value_node}, {});
        }
        else if (grouping->getName() == "groupingForCube")
        {
            checkFirstArgumentIsGroupingSet(*node);
            replacement = buildCubeGroupingExpression(dag, *grouping, num_sets, node->children[0], node->result_name);
        }
        else
        {
            checkFirstArgumentIsGroupingSet(*node);

            /// Throw rather than keep the original function, which no worker could deserialize.
            if (num_sets > max_grouping_sets_to_despecialize)
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                    "make_distributed_plan does not support `grouping` over {} grouping sets (maximum {})",
                    num_sets, max_grouping_sets_to_despecialize);

            auto masks = computeGroupingMasks(*node, num_sets);

            Array masks_array;
            masks_array.reserve(num_sets);
            for (UInt64 set_index = 0; set_index < num_sets; ++set_index)
                masks_array.push_back((*masks)[set_index]);

            auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
            MutableColumnConstPtr array_column = array_type->createColumnConst(1, Field(std::move(masks_array)));
            const auto & masks_node = dag.addColumn(
                std::move(array_column), array_type, fmt::format("__grouping_masks_of_{}", node->result_name));

            auto one_type = std::make_shared<DataTypeUInt8>();
            MutableColumnConstPtr one_column = one_type->createColumnConst(1, Field(UInt64(1)));
            const auto & one_node = dag.addColumn(
                std::move(one_column), one_type, fmt::format("__grouping_one_of_{}", node->result_name));

            /// `arrayElement` is 1-based, so the index is `__grouping_set + 1`.
            auto plus = FunctionFactory::instance().get("plus", nullptr);
            const auto & index_node = dag.addFunction(plus, {node->children[0], &one_node}, {});
            auto array_element = FunctionFactory::instance().get("arrayElement", nullptr);
            replacement = &dag.addFunction(array_element, {&masks_node, &index_node}, {});
        }

        if (!replacement->result_type->equals(*node->result_type))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Grouping function rewrite changed the result type from {} to {}",
                node->result_type->getName(), replacement->result_type->getName());

        replacements[node] = &dag.addAlias(*replacement, node->result_name);
    }

    if (replacements.empty())
        return false;

    for (auto & output : dag.getOutputs())
        output = replaceNodes(dag, output, replacements);

    /// Keep the inputs: the step's input header is fixed by the child step, and the rewrite drops
    /// the grouping function's unused argument columns, which may have been an input's only use.
    dag.removeUnusedActions(/* allow_remove_inputs */ false);
    return true;
}

}

namespace QueryPlanOptimizations
{

/// The `grouping` function specializations (`groupingForRollup` etc.) hold their parameters -
/// argument positions among the GROUP BY keys, the key count, the compatibility flag - inside the
/// function object, and a serialized plan carries functions by name only, so a worker cannot
/// rebuild them. Each one is a pure map from the `__grouping_set` value to a constant, so replace
/// it with a precomputed lookup: `arrayElement([...], __grouping_set + 1)` (or a plain constant
/// when there is a single grouping set). After the rewrite the plan contains only ordinary
/// functions and constant data.
void despecializeGroupingFunctions(QueryPlan::Node & root);
void despecializeGroupingFunctions(QueryPlan::Node & root)
{
    std::vector<QueryPlan::Node *> stack = {&root};
    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();

        if (auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get()))
            despecializeGroupingFunctionsInDAG(expression_step->getExpression());
        else if (auto * filter_step = typeid_cast<FilterStep *>(node->step.get()))
            despecializeGroupingFunctionsInDAG(filter_step->getExpression());

        for (auto * child : node->children)
            stack.push_back(child);
    }
}

}

}
