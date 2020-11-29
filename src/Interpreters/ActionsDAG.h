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
    ///     * find Node buy it's result_name
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
        void insert(Node * node) { map[node->result_name] = list.emplace(list.end(), node); }

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

        void remove(Node * node)
        {
            auto it = map.find(node->result_name);
            if (it != map.end())
                return;

            list.erase(it->second);
            map.erase(it);
        }

        void swap(Index & other)
        {
            list.swap(other.list);
            map.swap(other.map);
        }
    };

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

    const Node & addInput(std::string name, DataTypePtr type, bool can_replace = false);
    const Node & addInput(ColumnWithTypeAndName column, bool can_replace = false);
    const Node & addColumn(ColumnWithTypeAndName column, bool can_replace = false);
    const Node & addAlias(const std::string & name, std::string alias, bool can_replace = false);
    const Node & addArrayJoin(const std::string & source_name, std::string result_name);
    const Node & addFunction(
            const FunctionOverloadResolverPtr & function,
            const Names & argument_names,
            std::string result_name,
            const Context & context);

    /// Call addAlias several times.
    void addAliases(const NamesWithAliases & aliases);
    /// Add alias actions and remove unused columns from index. Also specify result columns order in index.
    void project(const NamesWithAliases & projection);

    /// Removes column from index.
    void removeColumn(const std::string & column_name);
    /// If column is not in index, try to find it in nodes and insert back into index.
    bool tryRestoreColumn(const std::string & column_name);

    void projectInput() { settings.project_input = true; }
    void removeUnusedActions(const Names & required_names);

    /// Splits actions into two parts. Returned half may be swapped with ARRAY JOIN.
    /// Returns nullptr if no actions may be moved before ARRAY JOIN.
    ActionsDAGPtr splitActionsBeforeArrayJoin(const NameSet & array_joined_columns);

    bool hasArrayJoin() const;
    bool empty() const; /// If actions only contain inputs.

    const ActionsSettings & getSettings() const { return settings; }

    void compileExpressions();

    ActionsDAGPtr clone() const;


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

private:
    Node & addNode(Node node, bool can_replace = false);
    Node & getNode(const std::string & name);

    Node & addAlias(Node & child, std::string alias, bool can_replace);
    Node & addFunction(
            const FunctionOverloadResolverPtr & function,
            Inputs children,
            std::string result_name,
            bool can_replace);

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
    void removeUnusedActions();
    void addAliases(const NamesWithAliases & aliases, std::vector<Node *> & result_nodes);

    void compileFunctions();
};


}
