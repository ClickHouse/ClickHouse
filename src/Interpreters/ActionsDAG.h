#pragma once

#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <Core/Names.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

namespace DB
{

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

class IExecutableFunction;
using ExecutableFunctionPtr = std::shared_ptr<IExecutableFunction>;

class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<IFunctionBase>;

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

class CompiledExpressionCache;

/// Directed acyclic graph of expressions.
/// This is an intermediate representation of actions which is usually built from expression list AST.
/// Node of DAG describe calculation of a single column with known type, name, and constant value (if applicable).
///
/// DAG representation is useful in case we need to know explicit dependencies between actions.
/// It is helpful when it is needed to optimize actions, remove unused expressions, compile subexpressions,
/// split or merge parts of graph, calculate expressions on partial input.
///
/// Built DAG is used by ExpressionActions, which calculates expressions on block.
class ActionsDAG
{
public:

    enum class ActionType
    {
        /// Column which must be in input.
        INPUT,
        /// Constant column with known value.
        COLUMN,
        /// Another one name for column.
        ALIAS,
        /// Function arrayJoin. Specially separated because it changes the number of rows.
        ARRAY_JOIN,
        FUNCTION,
    };

    struct Node;
    using NodeRawPtrs = std::vector<Node *>;
    using NodeRawConstPtrs = std::vector<const Node *>;

    struct Node
    {
        NodeRawConstPtrs children;

        ActionType type;

        std::string result_name;
        DataTypePtr result_type;

        FunctionOverloadResolverPtr function_builder;
        /// Can be used after action was added to ExpressionActions if we want to get function signature or properties like monotonicity.
        FunctionBasePtr function_base;
        /// Prepared function which is used in function execution.
        ExecutableFunctionPtr function;
        /// If function is a compiled statement.
        bool is_function_compiled = false;

        /// For COLUMN node and propagated constants.
        ColumnPtr column;
        /// Some functions like `ignore()` always return constant but can't be replaced by constant it.
        /// We calculate such constants in order to avoid unnecessary materialization, but prohibit it's folding.
        bool allow_constant_folding = true;
    };

    /// NOTE: std::list is an implementation detail.
    /// It allows to add and remove new nodes inplace without reallocation.
    /// Raw pointers to nodes remain valid.
    using Nodes = std::list<Node>;

private:
    Nodes nodes;
    NodeRawConstPtrs index;
    NodeRawConstPtrs inputs;

    bool project_input = false;
    bool projected_output = false;

public:
    ActionsDAG() = default;
    ActionsDAG(ActionsDAG &&) = default;
    ActionsDAG(const ActionsDAG &) = delete;
    ActionsDAG & operator=(const ActionsDAG &) = delete;
    explicit ActionsDAG(const NamesAndTypesList & inputs_);
    explicit ActionsDAG(const ColumnsWithTypeAndName & inputs_);

    const Nodes & getNodes() const { return nodes; }
    const NodeRawConstPtrs & getIndex() const { return index; }
    const NodeRawConstPtrs & getInputs() const { return inputs; }

    NamesAndTypesList getRequiredColumns() const;
    ColumnsWithTypeAndName getResultColumns() const;
    NamesAndTypesList getNamesAndTypesList() const;

    Names getNames() const;
    std::string dumpNames() const;
    std::string dumpDAG() const;

    const Node & addInput(std::string name, DataTypePtr type);
    const Node & addInput(ColumnWithTypeAndName column);
    const Node & addColumn(ColumnWithTypeAndName column);
    const Node & addAlias(const Node & child, std::string alias);
    const Node & addArrayJoin(const Node & child, std::string result_name);
    const Node & addFunction(
            const FunctionOverloadResolverPtr & function,
            NodeRawConstPtrs children,
            std::string result_name);

    /// Index can contain any column returned from DAG.
    /// You may manually change it if needed.
    NodeRawConstPtrs & getIndex() { return index; }
    /// Find first column by name in index. This search is linear.
    const Node & findInIndex(const std::string & name) const;
    /// Same, but return nullptr if node not found.
    const Node * tryFindInIndex(const std::string & name) const;
    /// Find first node with the same name in index and replace it.
    /// If was not found, add node to index end.
    void addOrReplaceInIndex(const Node & node);

    /// Call addAlias several times.
    void addAliases(const NamesWithAliases & aliases);
    /// Add alias actions and remove unused columns from index. Also specify result columns order in index.
    void project(const NamesWithAliases & projection);

    /// If column is not in index, try to find it in nodes and insert back into index.
    bool tryRestoreColumn(const std::string & column_name);
    /// Find column in result. Remove it from index.
    /// If columns is in inputs and has no dependent nodes, remove it from inputs too.
    /// Return true if column was removed from inputs.
    bool removeUnusedResult(const std::string & column_name);

    void projectInput(bool project = true) { project_input = project; }
    bool isInputProjected() const { return project_input; }
    bool isOutputProjected() const { return projected_output; }

    void removeUnusedActions(const Names & required_names);
    void removeUnusedActions(const NameSet & required_names);

    bool hasArrayJoin() const;
    bool hasStatefulFunctions() const;
    bool trivial() const; /// If actions has no functions or array join.

#if USE_EMBEDDED_COMPILER
    void compileExpressions(size_t min_count_to_compile_expression);
#endif

    ActionsDAGPtr clone() const;

    /// For apply materialize() function for every output.
    /// Also add aliases so the result names remain unchanged.
    void addMaterializingOutputActions();

    /// Apply materialize() function to node. Result node has the same name.
    const Node & materializeNode(const Node & node);

    enum class MatchColumnsMode
    {
        /// Require same number of columns in source and result. Match columns by corresponding positions, regardless to names.
        Position,
        /// Find columns in source by their names. Allow excessive columns in source.
        Name,
    };

    /// Create ActionsDAG which converts block structure from source to result.
    /// It is needed to convert result from different sources to the same structure, e.g. for UNION query.
    /// Conversion should be possible with only usage of CAST function and renames.
    /// @param ignore_constant_values - Do not check that constants are same. Use value from result_header.
    /// @param add_casted_columns - Create new columns with converted values instead of replacing original.
    static ActionsDAGPtr makeConvertingActions(
        const ColumnsWithTypeAndName & source,
        const ColumnsWithTypeAndName & result,
        MatchColumnsMode mode,
        bool ignore_constant_values = false,
        bool add_casted_columns = false,
        NameToNameMap * new_names = nullptr);

    /// Create expression which add const column and then materialize it.
    static ActionsDAGPtr makeAddingColumnActions(ColumnWithTypeAndName column);

    /// Create ActionsDAG which represents expression equivalent to applying first and second actions consequently.
    /// Is used to replace `(first -> second)` expression chain to single `merge(first, second)` expression.
    /// If first.settings.project_input is set, then outputs of `first` must include inputs of `second`.
    /// Otherwise, any two actions may be combined.
    static ActionsDAGPtr merge(ActionsDAG && first, ActionsDAG && second);

    using SplitResult = std::pair<ActionsDAGPtr, ActionsDAGPtr>;

    /// Split ActionsDAG into two DAGs, where first part contains all nodes from split_nodes and their children.
    /// Execution of first then second parts on block is equivalent to execution of initial DAG.
    /// First DAG and initial DAG have equal inputs, second DAG and initial DAG has equal index (outputs).
    /// Second DAG inputs may contain less inputs then first DAG (but also include other columns).
    SplitResult split(std::unordered_set<const Node *> split_nodes) const;

    /// Splits actions into two parts. Returned first half may be swapped with ARRAY JOIN.
    SplitResult splitActionsBeforeArrayJoin(const NameSet & array_joined_columns) const;

    /// Splits actions into two parts. First part has minimal size sufficient for calculation of column_name.
    /// Index of initial actions must contain column_name.
    SplitResult splitActionsForFilter(const std::string & column_name) const;

    /// Create actions which may calculate part of filter using only available_inputs.
    /// If nothing may be calculated, returns nullptr.
    /// Otherwise, return actions which inputs are from available_inputs.
    /// Returned actions add single column which may be used for filter.
    /// Also, replace some nodes of current inputs to constant 1 in case they are filtered.
    ///
    /// @param all_inputs should contain inputs from previous step, which will be used for result actions.
    /// It is expected that all_inputs contain columns from available_inputs.
    /// This parameter is needed to enforce result actions save columns order in block.
    /// Otherwise for some queries, e.g. with GROUP BY, columns will be mixed.
    /// Example: SELECT sum(x), y, z FROM tab WHERE z > 0 and sum(x) > 0
    /// Pushed condition: z > 0
    /// GROUP BY step will transform columns `x, y, z` -> `sum(x), y, z`
    /// If we just add filter step with actions `z -> z > 0` before GROUP BY,
    /// columns will be transformed like `x, y, z` -> `z, z > 0, x, y` -(remove filter)-> `z, x, y`.
    /// To avoid it, add inputs from `all_inputs` list,
    /// so actions `x, y, z -> x, y, z, z > 0` -(remove filter)-> `x, y, z` will not change columns order.
    ActionsDAGPtr cloneActionsForFilterPushDown(
        const std::string & filter_name,
        bool can_remove_filter,
        const Names & available_inputs,
        const ColumnsWithTypeAndName & all_inputs);

private:
    Node & addNode(Node node);

    void removeUnusedActions(bool allow_remove_inputs = true);

#if USE_EMBEDDED_COMPILER
    void compileFunctions(size_t min_count_to_compile_expression);
#endif

    static ActionsDAGPtr cloneActionsForConjunction(NodeRawConstPtrs conjunction, const ColumnsWithTypeAndName & all_inputs);
};

}
