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

class Context;
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

    struct Node
    {
        std::vector<Node *> children;

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

    /// Index is used to:
    ///     * find Node by it's result_name
    ///     * specify order of columns in result
    /// It represents a set of available columns.
    /// Removing of column from index is equivalent to removing of column from final result.
    ///
    /// DAG allows actions with duplicating result names. In this case index will point to last added Node.
    /// It does not cause any problems as long as execution of actions does not depend on action names anymore.
    ///
    /// Index is a list of nodes + [map: name -> list::iterator].
    /// List is ordered, may contain nodes with same names, or one node several times.
    class Index
    {
    private:
        std::list<Node *> list;
        /// Map key is a string_view to Node::result_name for node from value.
        /// Map always point to existing node, so key always valid (nodes live longer then index).
        std::unordered_map<std::string_view, std::list<Node *>::iterator> map;

    public:
        auto size() const { return list.size(); }
        bool contains(std::string_view key) const { return map.count(key) != 0; }

        std::list<Node *>::iterator begin() { return list.begin(); }
        std::list<Node *>::iterator end() { return list.end(); }
        std::list<Node *>::const_iterator begin() const { return list.begin(); }
        std::list<Node *>::const_iterator end() const { return list.end(); }
        std::list<Node *>::const_reverse_iterator rbegin() const { return list.rbegin(); }
        std::list<Node *>::const_reverse_iterator rend() const { return list.rend(); }
        std::list<Node *>::const_iterator find(std::string_view key) const
        {
            auto it = map.find(key);
            if (it == map.end())
                return list.end();

            return it->second;
        }

        /// Insert method doesn't check if map already have node with the same name.
        /// If node with the same name exists, it is removed from map, but not list.
        /// It is expected and used for project(), when result may have several columns with the same name.
        void insert(Node * node)
        {
            auto it = list.emplace(list.end(), node);
            if (auto handle = map.extract(node->result_name))
            {
                handle.key() = node->result_name; /// Change string_view
                handle.mapped() = it;
                map.insert(std::move(handle));
            }
            else
                map[node->result_name] = it;
        }

        void prepend(Node * node)
        {
            auto it = list.emplace(list.begin(), node);
            if (auto handle = map.extract(node->result_name))
            {
                handle.key() = node->result_name; /// Change string_view
                handle.mapped() = it;
                map.insert(std::move(handle));
            }
            else
                map[node->result_name] = it;
        }

        /// If node with same name exists in index, replace it. Otherwise insert new node to index.
        void replace(Node * node)
        {
            if (auto handle = map.extract(node->result_name))
            {
                handle.key() = node->result_name; /// Change string_view
                *handle.mapped() = node;
                map.insert(std::move(handle));
            }
            else
                insert(node);
        }

        void replace(std::list<Node *>::iterator it, Node * node)
        {
            auto map_it = map.find((*it)->result_name);
            bool in_map = map_it != map.end() && map_it->second == it;
            if (in_map)
                map.erase(map_it);

            *it = node;

            if (in_map)
                map[node->result_name] = it;
        }

        void remove(std::list<Node *>::iterator it)
        {
            auto map_it = map.find((*it)->result_name);
            if (map_it != map.end() && map_it->second == it)
                map.erase(map_it);

            list.erase(it);
        }

        void swap(Index & other)
        {
            list.swap(other.list);
            map.swap(other.map);
        }
    };

    /// NOTE: std::list is an implementation detail.
    /// It allows to add and remove new nodes inplace without reallocation.
    /// Raw pointers to nodes remain valid.
    using Nodes = std::list<Node>;
    using Inputs = std::vector<Node *>;

    struct ActionsSettings
    {
        size_t max_temporary_columns = 0;
        size_t max_temporary_non_const_columns = 0;
        size_t min_count_to_compile_expression = 0;
        bool compile_expressions = false;
        bool project_input = false;
        bool projected_output = false;
    };

private:
    Nodes nodes;
    Index index;
    Inputs inputs;

    ActionsSettings settings;

#if USE_EMBEDDED_COMPILER
    std::shared_ptr<CompiledExpressionCache> compilation_cache;
#endif

public:
    ActionsDAG() = default;
    ActionsDAG(ActionsDAG &&) = default;
    ActionsDAG(const ActionsDAG &) = delete;
    ActionsDAG & operator=(const ActionsDAG &) = delete;
    explicit ActionsDAG(const NamesAndTypesList & inputs_);
    explicit ActionsDAG(const ColumnsWithTypeAndName & inputs_);

    const Nodes & getNodes() const { return nodes; }
    const Index & getIndex() const { return index; }
    const Inputs & getInputs() const { return inputs; }

    NamesAndTypesList getRequiredColumns() const;
    ColumnsWithTypeAndName getResultColumns() const;
    NamesAndTypesList getNamesAndTypesList() const;

    Names getNames() const;
    std::string dumpNames() const;
    std::string dumpDAG() const;

    const Node & addInput(std::string name, DataTypePtr type, bool can_replace = false, bool add_to_index = true);
    const Node & addInput(ColumnWithTypeAndName column, bool can_replace = false, bool add_to_index = true);
    const Node & addColumn(ColumnWithTypeAndName column, bool can_replace = false, bool materialize = false);
    const Node & addAlias(const std::string & name, std::string alias, bool can_replace = false);
    const Node & addArrayJoin(const std::string & source_name, std::string result_name);
    const Node & addFunction(
            const FunctionOverloadResolverPtr & function,
            const Names & argument_names,
            std::string result_name,
            const Context & context,
            bool can_replace = false);

    void addNodeToIndex(const Node * node) { index.insert(const_cast<Node *>(node)); }

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

    void projectInput(bool project = true) { settings.project_input = project; }
    void removeUnusedActions(const Names & required_names);

    bool hasArrayJoin() const;
    bool hasStatefulFunctions() const;
    bool trivial() const; /// If actions has no functions or array join.

    const ActionsSettings & getSettings() const { return settings; }

    void compileExpressions();

    ActionsDAGPtr clone() const;

    /// For apply materialize() function for every output.
    /// Also add aliases so the result names remain unchanged.
    void addMaterializingOutputActions();

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
    static ActionsDAGPtr makeConvertingActions(
        const ColumnsWithTypeAndName & source,
        const ColumnsWithTypeAndName & result,
        MatchColumnsMode mode,
        bool ignore_constant_values = false); /// Do not check that constants are same. Use value from result_header.

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
    ActionsDAGPtr cloneActionsForFilterPushDown(
        const std::string & filter_name,
        bool can_remove_filter,
        const Names & available_inputs,
        const ColumnsWithTypeAndName & all_inputs);

private:
    Node & addNode(Node node, bool can_replace = false, bool add_to_index = true);
    Node & getNode(const std::string & name);

    Node & addAlias(Node & child, std::string alias, bool can_replace);
    Node & addFunction(
            const FunctionOverloadResolverPtr & function,
            Inputs children,
            std::string result_name,
            bool can_replace,
            bool add_to_index = true);

    ActionsDAGPtr cloneEmpty() const
    {
        auto actions = std::make_shared<ActionsDAG>();
        actions->settings = settings;

#if USE_EMBEDDED_COMPILER
        actions->compilation_cache = compilation_cache;
#endif
        return actions;
    }

    void removeUnusedActions(const std::vector<Node *> & required_nodes);
    void removeUnusedActions(bool allow_remove_inputs = true);
    void addAliases(const NamesWithAliases & aliases, std::vector<Node *> & result_nodes);

    void compileFunctions();

    ActionsDAGPtr cloneActionsForConjunction(std::vector<Node *> conjunction, const ColumnsWithTypeAndName & all_inputs);
};


}
