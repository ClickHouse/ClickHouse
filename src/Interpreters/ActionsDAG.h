#pragma once

#include <utility>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>

#include "config.h"

namespace DB
{

class IExecutableFunction;
using ExecutableFunctionPtr = std::shared_ptr<IExecutableFunction>;

class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<const IFunctionBase>;

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

class FunctionNode;

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

namespace JSONBuilder
{
    class JSONMap;

    class IItem;
    using ItemPtr = std::unique_ptr<IItem>;
}

class SortDescription;

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

    enum class ActionType : uint8_t
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

        ActionType type{};

        std::string result_name;
        DataTypePtr result_type;

        /// Can be used to get function signature or properties like monotonicity.
        FunctionBasePtr function_base;
        /// Prepared function which is used in function execution.
        ExecutableFunctionPtr function;
        /// If function is a compiled statement.
        bool is_function_compiled = false;

        /// It is a constant calculated from deterministic functions (See IFunction::isDeterministic).
        /// This property is kept after constant folding of non-deterministic functions like 'now', 'today'.
        bool is_deterministic_constant = true;
        /// For COLUMN node and propagated constants.
        ColumnPtr column;

        /// If result of this not is deterministic. Checks only this node, not a subtree.
        bool isDeterministic() const;
        void toTree(JSONBuilder::JSONMap & map) const;
    };

    /// NOTE: std::list is an implementation detail.
    /// It allows to add and remove new nodes inplace without reallocation.
    /// Raw pointers to nodes remain valid.
    using Nodes = std::list<Node>;

private:
    Nodes nodes;
    NodeRawConstPtrs inputs;
    NodeRawConstPtrs outputs;

public:
    ActionsDAG() = default;
    ActionsDAG(ActionsDAG &&) = default;
    ActionsDAG(const ActionsDAG &) = delete;
    ActionsDAG & operator=(ActionsDAG &&) = default;
    ActionsDAG & operator=(const ActionsDAG &) = delete;
    explicit ActionsDAG(const NamesAndTypesList & inputs_);
    explicit ActionsDAG(const ColumnsWithTypeAndName & inputs_);

    const Nodes & getNodes() const { return nodes; }
    static Nodes detachNodes(ActionsDAG && dag) { return std::move(dag.nodes); }
    const NodeRawConstPtrs & getOutputs() const { return outputs; }
    /** Output nodes can contain any column returned from DAG.
      * You may manually change it if needed.
      */
    NodeRawConstPtrs & getOutputs() { return outputs; }

    const NodeRawConstPtrs & getInputs() const { return inputs; }

    NamesAndTypesList getRequiredColumns() const;
    Names getRequiredColumnsNames() const;
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
    const Node & addFunction(
        const FunctionNode & function,
        NodeRawConstPtrs children,
        std::string result_name);
    const Node & addFunction(
        const FunctionBasePtr & function_base,
        NodeRawConstPtrs children,
        std::string result_name);
    const Node & addCast(const Node & node_to_cast, const DataTypePtr & cast_type, std::string result_name);

    /// Find first column by name in output nodes. This search is linear.
    const Node & findInOutputs(const std::string & name) const;

    /// Same, but return nullptr if node not found.
    const Node * tryFindInOutputs(const std::string & name) const;

    /// Same, but for the list of names.
    NodeRawConstPtrs findInOutpus(const Names & names) const;

    /// Find first node with the same name in output nodes and replace it.
    /// If was not found, add node to outputs end.
    void addOrReplaceInOutputs(const Node & node);

    /// Call addAlias several times.
    void addAliases(const NamesWithAliases & aliases);

    /// Add alias actions. Also specify result columns order in outputs.
    void project(const NamesWithAliases & projection);

    /// Add input for every column from sample_block which is not mapped to existing input.
    void appendInputsForUnusedColumns(const Block & sample_block);

    /// If column is not in outputs, try to find it in nodes and insert back into outputs.
    bool tryRestoreColumn(const std::string & column_name);

    /// Find column in result. Remove it from outputs.
    /// If columns is in inputs and has no dependent nodes, remove it from inputs too.
    /// Return true if column was removed from inputs.
    bool removeUnusedResult(const std::string & column_name);

    /// Remove actions that are not needed to compute output nodes
    void removeUnusedActions(bool allow_remove_inputs = true, bool allow_constant_folding = true);

    /// Remove actions that are not needed to compute output nodes. Keep inputs from used_inputs.
    void removeUnusedActions(const std::unordered_set<const Node *> & used_inputs, bool allow_constant_folding = true);

    /// Remove actions that are not needed to compute output nodes with required names
    void removeUnusedActions(const Names & required_names, bool allow_remove_inputs = true, bool allow_constant_folding = true);

    /// Remove actions that are not needed to compute output nodes with required names
    void removeUnusedActions(const NameSet & required_names, bool allow_remove_inputs = true, bool allow_constant_folding = true);

    void removeAliasesForFilter(const std::string & filter_name);

    /// Transform the current DAG in a way that leaf nodes get folded into their parents. It's done
    /// because each projection can provide some columns as inputs to substitute certain sub-DAGs
    /// (expressions). Consider the following example:
    /// CREATE TABLE tbl (dt DateTime, val UInt64,
    ///                   PROJECTION p_hour (SELECT sum(val) GROUP BY toStartOfHour(dt)));
    ///
    /// Query: SELECT toStartOfHour(dt), sum(val) FROM tbl GROUP BY toStartOfHour(dt);
    ///
    /// We will have an ActionsDAG like this:
    /// FUNCTION: toStartOfHour(dt)       sum(val)
    ///                 ^                   ^
    ///                 |                   |
    /// INPUT:          dt                  val
    ///
    /// Now we traverse the DAG and see if any FUNCTION node can be replaced by projection's INPUT node.
    /// The result DAG will be:
    /// INPUT:  toStartOfHour(dt)       sum(val)
    ///
    /// We don't need aggregate columns from projection because they are matched after DAG.
    /// Currently we use canonical names of each node to find matches. It can be improved after we
    /// have a full-featured name binding system.
    ///
    /// @param required_columns should contain columns which this DAG is required to produce after folding. It used for result actions.
    /// @param projection_block_for_keys contains all key columns of given projection.
    /// @param predicate_column_name means we need to produce the predicate column after folding.
    /// @param add_missing_keys means whether to add additional missing columns to input nodes from projection key columns directly.
    /// @return required columns for this folded DAG. It's expected to be fewer than the original ones if some projection is used.
    NameSet foldActionsByProjection(
        const NameSet & required_columns,
        const Block & projection_block_for_keys,
        const String & predicate_column_name = {},
        bool add_missing_keys = true);

    /// Get an ActionsDAG in a following way:
    /// * Traverse a tree starting from required_outputs
    /// * If there is a node from new_inputs keys, replace it to INPUT
    /// * INPUT name should be taken from new_inputs mapped node name
    /// * Mapped nodes may be the same nodes, and in this case there would be a single INPUT
    /// Here want to substitute some expressions to columns from projection.
    /// This function expects that all required_outputs can be calculated from nodes in new_inputs.
    /// If not, exception will happen.
    /// This function also expects that new_inputs and required_outputs are valid nodes from the same DAG.
    /// Example:
    /// DAG:                   new_inputs:                   Result DAG
    /// a      b               c * d -> "(a + b) * d"
    /// \     /                e     -> ""
    ///  a + b
    ///     \                  required_outputs:         =>  "(a + b) * d"    e
    ///   c (alias)   d        c * d - e                              \      /
    ///       \      /                                               c * d - e
    ///        c * d       e
    ///            \      /
    ///            c * d - e
    static ActionsDAG foldActionsByProjection(
        const std::unordered_map<const Node *, const Node *> & new_inputs,
        const NodeRawConstPtrs & required_outputs);

    bool hasArrayJoin() const;
    bool hasStatefulFunctions() const;
    bool trivial() const; /// If actions has no functions or array join.
    void assertDeterministic() const; /// Throw if not isDeterministic.
    bool hasNonDeterministic() const;

#if USE_EMBEDDED_COMPILER
    void compileExpressions(size_t min_count_to_compile_expression, const std::unordered_set<const Node *> & lazy_executed_nodes = {});
#endif

    ActionsDAG clone(std::unordered_map<const Node *, Node *> & old_to_new_nodes) const;
    ActionsDAG clone() const;

    static ActionsDAG cloneSubDAG(const NodeRawConstPtrs & outputs, bool remove_aliases);

    /// Execute actions for header. Input block must have empty columns.
    /// Result should be equal to the execution of ExpressionActions built from this DAG.
    /// Actions are not changed, no expressions are compiled.
    ///
    /// In addition, check that result constants are constants according to DAG.
    /// In case if function return constant, but arguments are not constant, materialize it.
    Block updateHeader(const Block & header) const;

    using IntermediateExecutionResult = std::unordered_map<const Node *, ColumnWithTypeAndName>;
    static ColumnsWithTypeAndName evaluatePartialResult(
        IntermediateExecutionResult & node_to_column,
        const NodeRawConstPtrs & outputs,
        size_t input_rows_count,
        bool throw_on_error);

    /// For apply materialize() function for every output.
    /// Also add aliases so the result names remain unchanged.
    void addMaterializingOutputActions();

    /// Apply materialize() function to node. Result node has the same name.
    const Node & materializeNode(const Node & node);

    enum class MatchColumnsMode : uint8_t
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
    /// @param new_names - Output parameter for new column names when add_casted_columns is used.
    static ActionsDAG makeConvertingActions(
        const ColumnsWithTypeAndName & source,
        const ColumnsWithTypeAndName & result,
        MatchColumnsMode mode,
        bool ignore_constant_values = false,
        bool add_casted_columns = false,
        NameToNameMap * new_names = nullptr);

    /// Create expression which add const column and then materialize it.
    static ActionsDAG makeAddingColumnActions(ColumnWithTypeAndName column);

    /// Create ActionsDAG which represents expression equivalent to applying first and second actions consequently.
    /// Is used to replace `(first -> second)` expression chain to single `merge(first, second)` expression.
    /// If first.settings.project_input is set, then outputs of `first` must include inputs of `second`.
    /// Otherwise, any two actions may be combined.
    static ActionsDAG merge(ActionsDAG && first, ActionsDAG && second);

    /// The result is similar to merge(*this, second);
    /// Invariant : no nodes are removed from the first (this) DAG.
    /// So that pointers to nodes are kept valid.
    void mergeInplace(ActionsDAG && second);

    /// Merge current nodes with specified dag nodes.
    /// *out_outputs is filled with pointers to the nodes corresponding to second.getOutputs().
    void mergeNodes(ActionsDAG && second, NodeRawConstPtrs * out_outputs = nullptr);

    struct SplitResult;

    /// Split ActionsDAG into two DAGs, where first part contains all nodes from split_nodes and their children.
    /// Execution of first then second parts on block is equivalent to execution of initial DAG.
    /// Inputs and outputs of original DAG are split between the first and the second DAGs.
    /// Intermediate result can apper in first outputs and second inputs.
    /// Example:
    ///   initial DAG    : (a, b, c, d, e) -> (w, x, y, z)  | 1 a 2 b 3 c 4 d 5 e 6      ->  1 2 3 4 5 6 w x y z
    ///   split (first)  : (a, c, d) -> (i, j, k, w, y)     | 1 a 2 b 3 c 4 d 5 e 6      ->  1 2 b 3 4 5 e 6 i j k w y
    ///   split (second) : (i, j, k, y, b, e) -> (x, y, z)  | 1 2 b 3 4 5 e 6 i j k w y  ->  1 2 3 4 5 6 w x y z
    SplitResult split(std::unordered_set<const Node *> split_nodes, bool create_split_nodes_mapping = false, bool avoid_duplicate_inputs = false) const;

    /// Splits actions into two parts. Returned first half may be swapped with ARRAY JOIN.
    SplitResult splitActionsBeforeArrayJoin(const NameSet & array_joined_columns) const;

    /// Splits actions into two parts. First part has minimal size sufficient for calculation of column_name.
    /// Outputs of initial actions must contain column_name.
    SplitResult splitActionsForFilter(const std::string & column_name) const;

    /// Splits actions into two parts. The first part contains all the calculations required to calculate sort_columns.
    /// The second contains the rest.
    SplitResult splitActionsBySortingDescription(const NameSet & sort_columns) const;

    /** Returns true if filter DAG is always false for inputs with default values.
      *
      * @param filter_name - name of filter node in current DAG.
      * @param input_stream_header - input stream header.
      */
    bool isFilterAlwaysFalseForDefaultValueInputs(const std::string & filter_name, const Block & input_stream_header) const;

    /// Create actions which may calculate part of filter using only available_inputs.
    /// If nothing may be calculated, returns nullptr.
    /// Otherwise, return actions which inputs are from available_inputs.
    /// Returned actions add single column which may be used for filter. Added column will be the first one.
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
    /// columns will be transformed like `x, y, z` -> `z > 0, z, x, y` -(remove filter)-> `z, x, y`.
    /// To avoid it, add inputs from `all_inputs` list,
    /// so actions `x, y, z -> z > 0, x, y, z` -(remove filter)-> `x, y, z` will not change columns order.
    std::optional<ActionsDAG> splitActionsForFilterPushDown(
        const std::string & filter_name,
        bool removes_filter,
        const Names & available_inputs,
        const ColumnsWithTypeAndName & all_inputs);

    struct ActionsForJOINFilterPushDown;

    /** Split actions for JOIN filter push down.
      *
      * @param filter_name - name of filter node in current DAG.
      * @param removes_filter - if filter is removed after it is applied.
      * @param left_stream_available_columns_to_push_down - columns from left stream that are safe to use in push down conditions
      * to left stream.
      * @param left_stream_header - left stream header.
      * @param right_stream_available_columns_to_push_down - columns from right stream that are safe to use in push down conditions
      * to right stream.
      * @param right_stream_header - right stream header.
      * @param equivalent_columns_to_push_down - columns from left and right streams that are safe to use in push down conditions
      * to left and right streams.
      * @param equivalent_left_stream_column_to_right_stream_column - equivalent left stream column name to right stream column map.
      * @param equivalent_right_stream_column_to_left_stream_column - equivalent right stream column name to left stream column map.
      */
    ActionsForJOINFilterPushDown splitActionsForJOINFilterPushDown(
        const std::string & filter_name,
        bool removes_filter,
        const Names & left_stream_available_columns_to_push_down,
        const Block & left_stream_header,
        const Names & right_stream_available_columns_to_push_down,
        const Block & right_stream_header,
        const Names & equivalent_columns_to_push_down,
        const std::unordered_map<std::string, ColumnWithTypeAndName> & equivalent_left_stream_column_to_right_stream_column,
        const std::unordered_map<std::string, ColumnWithTypeAndName> & equivalent_right_stream_column_to_left_stream_column);

    bool
    isSortingPreserved(const Block & input_header, const SortDescription & sort_description, const String & ignore_output_column = "") const;

    /** Build filter dag from multiple filter dags.
      *
      * If filter nodes are empty, result is nullptr.
      *
      * If filter nodes are not empty, nodes and their children are merged into single dag.
      *
      * Additionally during dag construction if node has name that exists in node_name_to_input_column map argument
      * in final dag this node is represented as INPUT node with specified column.
      *
      * If single_output_condition_node = true, result dag has single output node:
      * 1. If there is single filter node, result dag output will contain this node.
      * 2. If there are multiple filter nodes, result dag output will contain single `and` function node
      * and children of this node will be filter nodes.
      *
      * If single_output_condition_node = false, result dag has multiple output nodes.
      */
    static std::optional<ActionsDAG> buildFilterActionsDAG(
        const NodeRawConstPtrs & filter_nodes,
        const std::unordered_map<std::string, ColumnWithTypeAndName> & node_name_to_input_node_column = {},
        bool single_output_condition_node = true);

    /// Check if `predicate` is a combination of AND functions.
    /// Returns a list of nodes representing atomic predicates.
    static NodeRawConstPtrs extractConjunctionAtoms(const Node * predicate);

    /// Get a list of nodes. For every node, check if it can be compused using allowed subset of inputs.
    /// Returns only those nodes from the list which can be computed.
    static NodeRawConstPtrs filterNodesByAllowedInputs(
        NodeRawConstPtrs nodes,
        const std::unordered_set<const Node *> & allowed_inputs);

private:
    NodeRawConstPtrs getParents(const Node * target) const;

    Node & addNode(Node node);

    const Node & addFunctionImpl(
        const FunctionBasePtr & function_base,
        NodeRawConstPtrs children,
        ColumnsWithTypeAndName arguments,
        std::string result_name,
        DataTypePtr result_type,
        bool all_const);

#if USE_EMBEDDED_COMPILER
    void compileFunctions(size_t min_count_to_compile_expression, const std::unordered_set<const Node *> & lazy_executed_nodes = {});
#endif

    static std::optional<ActionsDAG> createActionsForConjunction(NodeRawConstPtrs conjunction, const ColumnsWithTypeAndName & all_inputs);

    void removeUnusedConjunctions(NodeRawConstPtrs rejected_conjunctions, Node * predicate, bool removes_filter);
};

struct ActionsDAG::SplitResult
{
    ActionsDAG first;
    ActionsDAG second;
    std::unordered_map<const Node *, const Node *> split_nodes_mapping;
};

struct ActionsDAG::ActionsForJOINFilterPushDown
{
    std::optional<ActionsDAG> left_stream_filter_to_push_down;
    bool left_stream_filter_removes_filter;
    std::optional<ActionsDAG> right_stream_filter_to_push_down;
    bool right_stream_filter_removes_filter;
};

class FindOriginalNodeForOutputName
{
    using NameToNodeIndex = std::unordered_map<std::string_view, const ActionsDAG::Node *>;

public:
    explicit FindOriginalNodeForOutputName(const ActionsDAG & actions);
    const ActionsDAG::Node * find(const String & output_name);

private:
    NameToNodeIndex index;
};

class FindAliasForInputName
{
    using NameToNodeIndex = std::unordered_map<std::string_view, const ActionsDAG::Node *>;

public:
    explicit FindAliasForInputName(const ActionsDAG & actions);
    const ActionsDAG::Node * find(const String & name);

private:
    NameToNodeIndex index;
};

/// This is an ugly way to bypass impossibility to forward declare ActionDAG::Node.
struct ActionDAGNodes
{
    ActionsDAG::NodeRawConstPtrs nodes;
};

/// Helper for query analysis.
/// If project_input is set, all columns not found in inputs should be removed.
/// Now, we do it before adding a step to query plan by calling appendInputsForUnusedColumns.
struct ActionsAndProjectInputsFlag
{
    ActionsDAG dag;
    bool project_input = false;
};

using ActionsAndProjectInputsFlagPtr = std::shared_ptr<ActionsAndProjectInputsFlag>;

}
