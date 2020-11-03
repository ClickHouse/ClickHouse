#pragma once

#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Names.h>
#include <Core/Settings.h>
#include <Core/ColumnNumbers.h>
#include <Common/SipHash.h>
#include <Common/UInt128.h>
#include <unordered_map>
#include <unordered_set>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <DataTypes/DataTypeArray.h>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/identity.hpp>

#include <variant>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class Context;
class TableJoin;
class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class IExecutableFunction;
using ExecutableFunctionPtr = std::shared_ptr<IExecutableFunction>;

class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<IFunctionBase>;

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

class ExpressionActions;
class CompiledExpressionCache;

class ArrayJoinAction;
using ArrayJoinActionPtr = std::shared_ptr<ArrayJoinAction>;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

class ActionsDAG
{
public:

    enum class Type
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

        Type type;

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

    class Index
    {
    public:
        Node *& operator[](std::string_view key)
        {
            auto res = map.emplace(key, list.end());
            if (res.second)
                res.first->second = list.emplace(list.end(), nullptr);

            return *res.first->second;
        }

        void swap(Index & other)
        {
            list.swap(other.list);
            map.swap(other.map);
        }

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
        void remove(Node * node)
        {
            auto it = map.find(node->result_name);
            if (it != map.end())
                return;

            list.erase(it->second);
            map.erase(it);
        }

    private:
        std::list<Node *> list;
        std::unordered_map<std::string_view, std::list<Node *>::iterator> map;
    };

    using Nodes = std::list<Node>;

private:
    Nodes nodes;
    Index index;

    size_t max_temporary_columns = 0;
    size_t max_temporary_non_const_columns = 0;

#if USE_EMBEDDED_COMPILER
    std::shared_ptr<CompiledExpressionCache> compilation_cache;
#endif

    bool project_input = false;
    bool projected_output = false;

public:
    ActionsDAG() = default;
    ActionsDAG(const ActionsDAG &) = delete;
    ActionsDAG & operator=(const ActionsDAG &) = delete;
    explicit ActionsDAG(const NamesAndTypesList & inputs);
    explicit ActionsDAG(const ColumnsWithTypeAndName & inputs);

    const Nodes & getNodes() const { return nodes; }
    const Index & getIndex() const { return index; }

    NamesAndTypesList getRequiredColumns() const;
    ColumnsWithTypeAndName getResultColumns() const;
    NamesAndTypesList getNamesAndTypesList() const;

    Names getNames() const;
    std::string dumpNames() const;
    std::string dump() const;
    std::string dumpDAG() const;

    const Node & addInput(std::string name, DataTypePtr type);
    const Node & addInput(ColumnWithTypeAndName column);
    const Node & addColumn(ColumnWithTypeAndName column);
    const Node & addAlias(const std::string & name, std::string alias, bool can_replace = false);
    const Node & addArrayJoin(const std::string & source_name, std::string result_name);
    const Node & addFunction(
            const FunctionOverloadResolverPtr & function,
            const Names & argument_names,
            std::string result_name,
            const Context & context);

    /// Call addAlias several times.
    void addAliases(const NamesWithAliases & aliases);
    /// Adds alias actions and removes unused columns from index.
    void project(const NamesWithAliases & projection);

    /// Removes column from index.
    void removeColumn(const std::string & column_name);
    /// If column is not in index, try to find it in nodes and insert back into index.
    bool tryRestoreColumn(const std::string & column_name);

    void projectInput() { project_input = true; }
    void removeUnusedActions(const Names & required_names);
    ExpressionActionsPtr buildExpressions();

    /// Splits actions into two parts. Returned half may be swapped with ARRAY JOIN.
    /// Returns nullptr if no actions may be moved before ARRAY JOIN.
    ActionsDAGPtr splitActionsBeforeArrayJoin(const NameSet & array_joined_columns);

    bool hasArrayJoin() const;
    bool empty() const;
    bool projectedOutput() const { return projected_output; }

    ActionsDAGPtr clone() const;

private:
    Node & addNode(Node node, bool can_replace = false);
    Node & getNode(const std::string & name);

    ActionsDAGPtr cloneEmpty() const
    {
        auto actions = std::make_shared<ActionsDAG>();
        actions->max_temporary_columns = max_temporary_columns;
        actions->max_temporary_non_const_columns = max_temporary_non_const_columns;

#if USE_EMBEDDED_COMPILER
        actions->compilation_cache = compilation_cache;
#endif
        return actions;
    }

    ExpressionActionsPtr linearizeActions() const;
    void removeUnusedActions(const std::vector<Node *> & required_nodes);
    void addAliases(const NamesWithAliases & aliases, std::vector<Node *> & result_nodes);
};


/** Contains a sequence of actions on the block.
  */
class ExpressionActions
{
public:
    using Node = ActionsDAG::Node;
    using Index = ActionsDAG::Index;

    struct Argument
    {
        size_t pos;
        bool remove;
    };

    using Arguments = std::vector<Argument>;

    struct Action
    {
        const Node * node;
        Arguments arguments;
        size_t result_position;

        std::string toString() const;
    };

    using Actions = std::vector<Action>;

private:
    struct ExecutionContext
    {
        ColumnsWithTypeAndName & inputs;
        ColumnsWithTypeAndName columns;
        std::vector<ssize_t> inputs_pos;
        size_t num_rows;
    };

    std::list<Node> nodes;
    Actions actions;
    size_t num_columns;

    NamesAndTypesList required_columns;
    ColumnNumbers result_positions;
    Block sample_block;

    /// This flag means that all columns except input will be removed from block before execution.
    bool project_input = false;

    size_t max_temporary_non_const_columns = 0;

    friend class ActionsDAG;

public:
    ~ExpressionActions();
    ExpressionActions() = default;
    ExpressionActions(const ExpressionActions &) = delete;
    ExpressionActions & operator=(const ExpressionActions &) = delete;

    const Actions & getActions() const { return actions; }

    /// Adds to the beginning the removal of all extra columns.
    void projectInput() { project_input = true; }

    /// - Adds actions to delete all but the specified columns.
    /// - Removes unused input columns.
    /// - Can somehow optimize the expression.
    /// - Does not reorder the columns.
    /// - Does not remove "unexpected" columns (for example, added by functions).
    /// - If output_columns is empty, leaves one arbitrary column (so that the number of rows in the block is not lost).
    // void finalize(const Names & output_columns);

    /// Get a list of input columns.
    Names getRequiredColumns() const;
    const NamesAndTypesList & getRequiredColumnsWithTypes() const { return required_columns; }

    /// Execute the expression on the block. The block must contain all the columns returned by getRequiredColumns.
    void execute(Block & block, bool dry_run = false) const;
    void execute(Block & block, size_t & num_rows, bool dry_run = false) const;

    bool hasArrayJoin() const;

    /// Obtain a sample block that contains the names and types of result columns.
    const Block & getSampleBlock() const { return sample_block; }

    std::string dumpActions() const;

    static std::string getSmallestColumn(const NamesAndTypesList & columns);

    /// Check if column is always zero. True if it's definite, false if we can't say for sure.
    /// Call it only after subqueries for sets were executed.
    bool checkColumnIsAlwaysFalse(const String & column_name) const;

    ExpressionActionsPtr clone() const;

private:

    void checkLimits(ExecutionContext & execution_context) const;

    static void executeAction(const Action & action, ExecutionContext & execution_context, bool dry_run);
};


/** The sequence of transformations over the block.
  * It is assumed that the result of each step is fed to the input of the next step.
  * Used to execute parts of the query individually.
  *
  * For example, you can create a chain of two steps:
  *     1) evaluate the expression in the WHERE clause,
  *     2) calculate the expression in the SELECT section,
  * and between the two steps do the filtering by value in the WHERE clause.
  */
struct ExpressionActionsChain
{
    explicit ExpressionActionsChain(const Context & context_) : context(context_) {}


    struct Step
    {
        virtual ~Step() = default;
        explicit Step(Names required_output_) : required_output(std::move(required_output_)) {}

        /// Columns were added to the block before current step in addition to prev step output.
        NameSet additional_input;
        /// Columns which are required in the result of current step.
        Names required_output;
        /// True if column from required_output is needed only for current step and not used in next actions
        /// (and can be removed from block). Example: filter column for where actions.
        /// If not empty, has the same size with required_output; is filled in finalize().
        std::vector<bool> can_remove_required_output;

        virtual NamesAndTypesList getRequiredColumns() const = 0;
        virtual ColumnsWithTypeAndName getResultColumns() const = 0;
        /// Remove unused result and update required columns
        virtual void finalize(const Names & required_output_) = 0;
        /// Add projections to expression
        virtual void prependProjectInput() const = 0;
        virtual std::string dump() const = 0;

        /// Only for ExpressionActionsStep
        ActionsDAGPtr & actions();
        const ActionsDAGPtr & actions() const;
    };

    struct ExpressionActionsStep : public Step
    {
        ActionsDAGPtr actions;

        explicit ExpressionActionsStep(ActionsDAGPtr actions_, Names required_output_ = Names())
            : Step(std::move(required_output_))
            , actions(std::move(actions_))
        {
        }

        NamesAndTypesList getRequiredColumns() const override
        {
            return actions->getRequiredColumns();
        }

        ColumnsWithTypeAndName getResultColumns() const override
        {
            return actions->getResultColumns();
        }

        void finalize(const Names & required_output_) override
        {
            if (!actions->projectedOutput())
                actions->removeUnusedActions(required_output_);
        }

        void prependProjectInput() const override
        {
            actions->projectInput();
        }

        std::string dump() const override
        {
            return actions->dump();
        }
    };

    struct ArrayJoinStep : public Step
    {
        ArrayJoinActionPtr array_join;
        NamesAndTypesList required_columns;
        ColumnsWithTypeAndName result_columns;

        ArrayJoinStep(ArrayJoinActionPtr array_join_, ColumnsWithTypeAndName required_columns_);

        NamesAndTypesList getRequiredColumns() const override { return required_columns; }
        ColumnsWithTypeAndName getResultColumns() const override { return result_columns; }
        void finalize(const Names & required_output_) override;
        void prependProjectInput() const override {} /// TODO: remove unused columns before ARRAY JOIN ?
        std::string dump() const override { return "ARRAY JOIN"; }
    };

    struct JoinStep : public Step
    {
        std::shared_ptr<TableJoin> analyzed_join;
        JoinPtr join;

        NamesAndTypesList required_columns;
        ColumnsWithTypeAndName result_columns;

        JoinStep(std::shared_ptr<TableJoin> analyzed_join_, JoinPtr join_, ColumnsWithTypeAndName required_columns_);
        NamesAndTypesList getRequiredColumns() const override { return required_columns; }
        ColumnsWithTypeAndName getResultColumns() const override { return result_columns; }
        void finalize(const Names & required_output_) override;
        void prependProjectInput() const override {} /// TODO: remove unused columns before JOIN ?
        std::string dump() const override { return "JOIN"; }
    };

    using StepPtr = std::unique_ptr<Step>;
    using Steps = std::vector<StepPtr>;

    const Context & context;
    Steps steps;

    void addStep(NameSet non_constant_inputs = {});

    void finalize();

    void clear()
    {
        steps.clear();
    }

    ActionsDAGPtr getLastActions(bool allow_empty = false)
    {
        if (steps.empty())
        {
            if (allow_empty)
                return {};
            throw Exception("Empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);
        }

        return typeid_cast<ExpressionActionsStep *>(steps.back().get())->actions;
    }

    Step & getLastStep()
    {
        if (steps.empty())
            throw Exception("Empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);

        return *steps.back();
    }

    Step & lastStep(const NamesAndTypesList & columns)
    {
        if (steps.empty())
            steps.emplace_back(std::make_unique<ExpressionActionsStep>(std::make_shared<ActionsDAG>(columns)));
        return *steps.back();
    }

    std::string dumpChain() const;
};

}
