#include "config.h"

#if USE_EMBEDDED_COMPILER

#include <optional>
#include <stack>

#include <Common/logger_useful.h>
#include <base/sort.h>
#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Native.h>

#include <Interpreters/JIT/CHJIT.h>
#include <Interpreters/JIT/CompileDAG.h>
#include <Interpreters/JIT/compileFunction.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Interpreters/ActionsDAG.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event CompiledFunctionRows;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    std::mutex expression_jit_mutex;
    /// See `aggregator_jit_instance` in `Aggregator.cpp` for the rationale of `shared_ptr` ownership.
    std::shared_ptr<CHJIT> expression_jit_instance;
}

static std::shared_ptr<CHJIT> getJITInstancePtr()
{
    std::lock_guard lock(expression_jit_mutex);
    if (!expression_jit_instance)
        expression_jit_instance = std::make_shared<CHJIT>();
    return expression_jit_instance;
}

void resetExpressionJITInstance()
{
    std::lock_guard lock(expression_jit_mutex);
    expression_jit_instance.reset();
}

static LoggerPtr getLogger()
{
    return ::getLogger("ExpressionJIT");
}

class CompiledFunctionHolder : public CompiledExpressionCacheEntry
{
public:

    explicit CompiledFunctionHolder(CompiledFunction compiled_function_, std::shared_ptr<CHJIT> jit_owner_)
        : CompiledExpressionCacheEntry(compiled_function_.compiled_module.size)
        , compiled_function(compiled_function_)
        , jit_owner(std::move(jit_owner_))
    {}

    ~CompiledFunctionHolder() override
    {
        try
        {
            /// Use the JIT instance that compiled this module (see `CompiledAggregateFunctionsHolder`).
            jit_owner->deleteCompiledModule(compiled_function.compiled_module);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    CompiledFunction compiled_function;

private:
    std::shared_ptr<CHJIT> jit_owner;
};

class LLVMExecutableFunction : public IExecutableFunction
{
public:

    explicit LLVMExecutableFunction(
        const std::string & name_,
        std::shared_ptr<CompiledFunctionHolder> compiled_function_holder_,
        size_t fused_functions_count_,
        std::shared_ptr<const std::vector<std::string>> node_names_)
        : name(name_)
        , compiled_function_holder(compiled_function_holder_)
        , fused_functions_count(fused_functions_count_)
        , node_names(std::move(node_names_))
    {
    }

    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (!canBeNativeType(*result_type))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "LLVMExecutableFunction unexpected result type in: {}", result_type->getName());

        auto result_column = result_type->createUninitializedColumnWithSize(input_rows_count);

        if (input_rows_count)
        {
            std::vector<ColumnData> columns(arguments.size() + 1);
            Columns columns_backup;

            for (size_t i = 0; i < arguments.size(); ++i)
            {
                auto column = arguments[i].column->convertToFullColumnIfConst();
                columns_backup.emplace_back(column);
                columns[i] = getColumnData(column.get());
            }

            columns[arguments.size()] = getColumnData(result_column.get());

            auto jit_compiled_function = compiled_function_holder->compiled_function.compiled_function;
            JITError error;
            callJITFunction(jit_compiled_function, input_rows_count, columns.data(), &error);
            if (error.code != JITErrorCode::None)
            {
                const auto & error_sites = compiled_function_holder->compiled_function.error_sites;
                if (error.site >= error_sites.size() || error_sites[error.site].node >= node_names->size())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "JIT-compiled expression {} reported an error for unknown site {}", name, error.site);
                const auto & site = error_sites[error.site];
                throwJITError(error, site, (*node_names)[site.node]);
            }
            ProfileEvents::increment(ProfileEvents::CompiledFunctionRows, input_rows_count * fused_functions_count);

            #if defined(MEMORY_SANITIZER)
            /// Memory sanitizer doesn't know about stores from JIT-ed code.
            /// But maybe we can generate this code with MSan instrumentation?

            if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(result_column.get()))
            {
                const auto & nested_column = nullable_column->getNestedColumn();
                const auto & null_map_column = nullable_column->getNullMapColumn();

                auto nested_column_raw_data = nested_column.getRawData();
                __msan_unpoison(nested_column_raw_data.data(), nested_column_raw_data.size());

                auto null_map_column_raw_data = null_map_column.getRawData();
                __msan_unpoison(null_map_column_raw_data.data(), null_map_column_raw_data.size());
            }
            else
            {
                __msan_unpoison(result_column->getRawData().data(), result_column->getRawData().size());
            }

            #endif
        }

        return result_column;
    }

private:
    std::string name;
    std::shared_ptr<CompiledFunctionHolder> compiled_function_holder;
    /// The number of functions of the original expression fused into this one, for `CompiledFunctionRows`.
    size_t fused_functions_count;
    /// The names of the nodes of the expression's `CompileDAG`, for the exception of a failed node.
    std::shared_ptr<const std::vector<std::string>> node_names;
};

class LLVMFunction : public IFunctionBase
{
public:

    explicit LLVMFunction(const CompileDAG & dag_)
        : name(dag_.dump())
        , dag(dag_)
    {
        /// The compiled code is shared by expressions that differ only in column names, so the name of a failed node
        /// is taken from this expression's DAG.
        auto names = std::make_shared<std::vector<std::string>>();
        names->reserve(dag.getNodesCount());

        for (size_t i = 0; i < dag.getNodesCount(); ++i)
        {
            const auto & node = dag[i];
            names->push_back(node.name);

            if (node.type == CompileDAG::CompileType::FUNCTION)
                nested_functions.emplace_back(node.function);
            else if (node.type == CompileDAG::CompileType::INPUT)
                argument_types.emplace_back(node.result_type);
        }

        node_names = std::move(names);
    }

    void setCompiledFunction(std::shared_ptr<CompiledFunctionHolder> compiled_function_holder_)
    {
        compiled_function_holder = compiled_function_holder_;
    }

    bool isCompilable() const override { return true; }

    llvm::Value * compile(llvm::IRBuilderBase & builder, const ValuesWithType & arguments) const override
    {
        return dag.compile(builder, arguments).value;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override
    {
        for (const auto & f : nested_functions)
            if (!f->isSuitableForShortCircuitArgumentsExecution(arguments))
                return false;

        return true;
    }

    String getName() const override { return name; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }

    const DataTypePtr & getResultType() const override { return dag.back().result_type; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        if (!compiled_function_holder)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Compiled function was not initialized {}", name);

        return std::make_unique<LLVMExecutableFunction>(name, compiled_function_holder, nested_functions.size(), node_names);
    }

    bool isDeterministic() const override
    {
        for (const auto & f : nested_functions)
            if (!f->isDeterministic())
                return false;

        return true;
    }

    bool isDeterministicInScopeOfQuery() const override
    {
        for (const auto & f : nested_functions)
            if (!f->isDeterministicInScopeOfQuery())
                return false;

        return true;
    }

    bool isSuitableForConstantFolding() const override
    {
        for (const auto & f : nested_functions)
            if (!f->isSuitableForConstantFolding())
                return false;

        return true;
    }

    bool isInjective(const ColumnsWithTypeAndName & sample_block) const override
    {
        for (const auto & f : nested_functions)
            if (!f->isInjective(sample_block))
                return false;

        return true;
    }

    bool hasInformationAboutMonotonicity() const override
    {
        for (const auto & f : nested_functions)
            if (!f->hasInformationAboutMonotonicity())
                return false;

        return true;
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        const IDataType * type_ptr = &type;
        Field left_mut = left;
        Field right_mut = right;

        Monotonicity result = { .is_monotonic = true, .is_positive = true, .is_always_monotonic = true };

        /// monotonicity is only defined for unary functions, so the chain must describe a sequence of nested calls
        for (size_t i = 0; i < nested_functions.size(); ++i)
        {
            Monotonicity m = nested_functions[i]->getMonotonicityForRange(*type_ptr, left_mut, right_mut);
            if (!m.is_monotonic)
                return m;
            result.is_positive ^= !m.is_positive;
            result.is_always_monotonic &= m.is_always_monotonic;
            if (i + 1 < nested_functions.size())
            {
                if (left_mut != Field())
                    applyFunction(*nested_functions[i], left_mut);
                if (right_mut != Field())
                    applyFunction(*nested_functions[i], right_mut);
                if (!m.is_positive)
                    std::swap(left_mut, right_mut);
                type_ptr = nested_functions[i]->getResultType().get();
            }
        }
        return result;
    }

    static void applyFunction(const IFunctionBase & function, Field & value)
    {
        const auto & type = function.getArgumentTypes().at(0);
        ColumnsWithTypeAndName args{{type->createColumnConst(1, value), type, "x" }};
        auto col = function.execute(args, function.getResultType(), 1, /* dry_run = */ false);
        col->get(0, value);
    }

private:
    std::string name;
    CompileDAG dag;
    DataTypes argument_types;
    std::vector<FunctionBasePtr> nested_functions;
    /// The name of each node of `dag`, shared with the executable functions.
    std::shared_ptr<const std::vector<std::string>> node_names;
    std::shared_ptr<CompiledFunctionHolder> compiled_function_holder;
};

static FunctionBasePtr compile(
    const CompileDAG & dag,
    size_t min_count_to_compile_expression)
{
    static std::unordered_map<UInt128, UInt64, UInt128Hash> counter;
    static std::mutex mutex;

    auto hash_key = dag.hash();
    {
        std::lock_guard lock(mutex);
        if (counter[hash_key]++ < min_count_to_compile_expression)
            return nullptr;
    }

    auto llvm_function = std::make_shared<LLVMFunction>(dag);

    if (auto * compilation_cache = CompiledExpressionCacheFactory::instance().tryGetCache())
    {
        auto [compiled_function_cache_entry, _] = compilation_cache->getOrSet(hash_key, [&] ()
        {
            LOG_TRACE(getLogger(), "Compile expression {}", llvm_function->getName());
            auto jit_owner = getJITInstancePtr();
            auto compiled_function = compileFunction(*jit_owner, *llvm_function);
            return std::make_shared<CompiledFunctionHolder>(compiled_function, std::move(jit_owner));
        });

        std::shared_ptr<CompiledFunctionHolder> compiled_function_holder = std::static_pointer_cast<CompiledFunctionHolder>(compiled_function_cache_entry);
        llvm_function->setCompiledFunction(std::move(compiled_function_holder));
    }
    else
    {
        auto jit_owner = getJITInstancePtr();
        auto compiled_function = compileFunction(*jit_owner, *llvm_function);
        auto compiled_function_holder = std::make_shared<CompiledFunctionHolder>(compiled_function, std::move(jit_owner));

        llvm_function->setCompiledFunction(std::move(compiled_function_holder));
    }

    return llvm_function;
}

static bool isCompilableConstant(const ActionsDAG::Node & node)
{
    return node.column && canBeNativeType(*node.result_type);
}

static const ActionsDAG::Node * removeAliasIfNecessary(const ActionsDAG::Node * node)
{
    const ActionsDAG::Node * node_no_alias = node;

    while (node_no_alias->type == ActionsDAG::ActionType::ALIAS)
        node_no_alias = node_no_alias->children[0];

    return node_no_alias;
}

/// The short-circuit functions whose lazy arguments can be compiled: `getCompilableDAG` knows on which rows the interpreter
/// evaluates each of their arguments (`shortCircuitGuardTerms`).
static bool isShortCircuitFunctionWithGuards(const IFunctionBase & function)
{
    const auto & name = function.getName();
    return name == "if" || name == "multiIf" || name == "and" || name == "or";
}

/// The results of `isLazySubtreeCompilable` by node, for one set of lazily executed nodes. Without it, every node above a
/// subtree would check the subtree again: exponential time in the nesting of short-circuit functions.
using LazySubtreeCompilableCache = std::unordered_map<const ActionsDAG::Node *, bool>;

static bool isCompilableFunction(
    const ActionsDAG::Node & node,
    const std::unordered_set<const ActionsDAG::Node *> & lazy_executed_nodes,
    LazySubtreeCompilableCache & lazy_subtree_cache);

/// Whether every lazily executed node below `node`, through lazy nodes, is a compilable function, so that the compiled
/// function computes them all and none becomes an input that the interpreter would evaluate on all rows.
static bool isLazySubtreeCompilable(
    const ActionsDAG::Node & node,
    const std::unordered_set<const ActionsDAG::Node *> & lazy_executed_nodes,
    LazySubtreeCompilableCache & lazy_subtree_cache)
{
    if (auto it = lazy_subtree_cache.find(&node); it != lazy_subtree_cache.end())
        return it->second;

    bool result = true;
    for (const auto & child : node.children)
    {
        const auto * child_no_alias = removeAliasIfNecessary(child);
        if (!lazy_executed_nodes.contains(child_no_alias))
            continue;
        if (child_no_alias->type != ActionsDAG::ActionType::FUNCTION
            || !isCompilableFunction(*child_no_alias, lazy_executed_nodes, lazy_subtree_cache)
            || !isLazySubtreeCompilable(*child_no_alias, lazy_executed_nodes, lazy_subtree_cache))
        {
            result = false;
            break;
        }
    }
    lazy_subtree_cache.emplace(&node, result);
    return result;
}

static bool isCompilableFunction(
    const ActionsDAG::Node & node,
    const std::unordered_set<const ActionsDAG::Node *> & lazy_executed_nodes,
    LazySubtreeCompilableCache & lazy_subtree_cache)
{
    if (node.type != ActionsDAG::ActionType::FUNCTION)
        return false;

    const auto & function = *node.function_base;

    IFunction::ShortCircuitSettings settings;
    if (function.isShortCircuit(settings, node.children.size()))
    {
        bool has_lazy_child = false;
        for (const auto & child : node.children)
            has_lazy_child |= lazy_executed_nodes.contains(removeAliasIfNecessary(child));

        /// Lazy arguments are computed on every row, with their errors masked to the rows the interpreter evaluates them
        /// on (see `getCompilableDAG`). Not with a `Decimal` result: the scale lifts of `FunctionIfBase` are checked on
        /// every row of a branch, and a lazy branch holds defaults on the rows it does not take in the interpreter.
        if (has_lazy_child
            && (!isShortCircuitFunctionWithGuards(function) || isDecimal(removeNullable(function.getResultType()))
                || !isLazySubtreeCompilable(node, lazy_executed_nodes, lazy_subtree_cache)))
            return false;

        /// `and` and `or` may execute their eager arguments first (`short_circuit_function_evaluation_reorder_arguments`),
        /// so that a lazy argument followed by an eager one is evaluated on fewer rows than its position says. When all
        /// eager arguments come first, the order cannot change the rows on which an argument that can throw is evaluated.
        if (has_lazy_child && settings.arguments_are_commutative)
        {
            bool seen_lazy = false;
            for (const auto & child : node.children)
            {
                const bool is_lazy = lazy_executed_nodes.contains(removeAliasIfNecessary(child));
                if (!is_lazy && seen_lazy)
                    return false;
                seen_lazy |= is_lazy;
            }
        }
    }

    if (!canBeNativeType(*function.getResultType()))
    {
        return false;
    }

    const auto & argument_types = function.getArgumentTypes();
    auto skip_arguments = function.getArgumentsThatDontParticipateInCompilation(argument_types);
    for (size_t i = 0; i < argument_types.size(); ++i)
    {
        if (std::find(skip_arguments.begin(), skip_arguments.end(), i) != skip_arguments.end())
            continue;

        const auto & type = argument_types[i];
        if (!canBeNativeType(*type))
        {
            return false;
        }

        /// Declared argument types can disagree with the children: a DAG rewrite may repoint a child
        /// without updating `function_base`, and an alias carries its own copy of the type, which a
        /// rewrite can leave behind. Compilation lowers the type the argument actually holds.
        if (i < node.children.size() && !canBeNativeType(*removeAliasIfNecessary(node.children[i])->result_type))
        {
            return false;
        }
    }

    return function.isCompilable();
}

/// The guard terms of argument `argument` of the short-circuit function `node` of `dag`: on which rows the interpreter
/// evaluates the argument when it is lazy (`executeShortCircuitArguments` of `if` and `multiIf`, `executeShortCircuit` of
/// `and` and `or`). A NULL condition counts as false in `if` and `multiIf`, and decides nothing in `and` and `or`.
static std::vector<CompileDAG::GuardTerm> shortCircuitGuardTerms(const CompileDAG::Node & node, size_t argument)
{
    using Kind = CompileDAG::GuardTerm::Kind;
    std::vector<CompileDAG::GuardTerm> terms;
    const auto & name = node.function->getName();
    const auto & arguments = node.arguments;

    if (name == "if")
    {
        if (argument == 1)
            terms.push_back({.condition = arguments[0], .kind = Kind::True});
        else if (argument == 2)
            terms.push_back({.condition = arguments[0], .kind = Kind::NotTrue});
    }
    else if (name == "multiIf")
    {
        /// Conditions at even positions, their values at the next odd position; the last argument is the else branch.
        const bool is_else = argument + 1 == arguments.size();
        const bool is_value = argument % 2 == 1;
        const size_t earlier_conditions_end = is_else ? argument : (is_value ? argument - 1 : argument);
        for (size_t i = 0; i < earlier_conditions_end; i += 2)
            terms.push_back({.condition = arguments[i], .kind = Kind::NotTrue});
        if (is_value && !is_else)
            terms.push_back({.condition = arguments[argument - 1], .kind = Kind::True});
    }
    else if (name == "and" || name == "or")
    {
        for (size_t i = 0; i < argument; ++i)
            terms.push_back({.condition = arguments[i], .kind = name == "and" ? Kind::NotFalse : Kind::NotTrue});
    }

    return terms;
}

/** Sets the guards of the lazily executed nodes of `dag`, whose nodes come from `action_nodes`. The root is evaluated on all
  * the rows it is given; a lazy argument of a short-circuit function on the rows the function needs it on; a lazy argument
  * of another lazy function on the rows of that function. Other nodes are evaluated on all rows.
  * Returns false if compiled code cannot match the interpreter: a node evaluated under two different guards (a common
  * descendant of arguments evaluated on different rows), a guarded node that is an input of the compiled function (the
  * interpreter would evaluate it on all rows), or a condition that is computed after the node it guards.
  */
static bool setGuards(
    CompileDAG & dag,
    const std::vector<const ActionsDAG::Node *> & action_nodes,
    const std::unordered_set<const ActionsDAG::Node *> & lazy_executed_nodes)
{
    const size_t size = dag.getNodesCount();
    std::vector<std::optional<std::vector<CompileDAG::GuardTerm>>> guards(size);
    guards[size - 1].emplace();

    /// Parents come after their children in the DAG.
    for (size_t i = size; i > 0; --i)
    {
        const size_t index = i - 1;
        const auto & node = dag[index];
        if (node.type != CompileDAG::CompileType::FUNCTION)
            continue;

        const bool is_short_circuit = isShortCircuitFunctionWithGuards(*node.function);
        const auto & action_node = *action_nodes[index];
        for (size_t argument = 0; argument < node.arguments.size(); ++argument)
        {
            const size_t child = node.arguments[argument];
            if (dag[child].type == CompileDAG::CompileType::CONSTANT)
                continue;

            std::vector<CompileDAG::GuardTerm> guard;
            if (lazy_executed_nodes.contains(removeAliasIfNecessary(action_node.children[argument])))
            {
                guard = *guards[index];
                if (is_short_circuit)
                    for (const auto & term : shortCircuitGuardTerms(node, argument))
                        guard.push_back(term);
            }

            for (const auto & term : guard)
                if (term.condition >= child)
                    return false;
            if (!guard.empty() && dag[child].type == CompileDAG::CompileType::INPUT)
                return false;

            if (!guards[child])
                guards[child] = std::move(guard);
            else if (*guards[child] != guard)
                return false;
        }
    }

    for (size_t i = 0; i < size; ++i)
        if (dag[i].type == CompileDAG::CompileType::FUNCTION && guards[i])
            dag[i].guard = std::move(*guards[i]);
    return true;
}

static CompileDAG getCompilableDAG(
    const ActionsDAG::Node * root,
    ActionsDAG::NodeRawConstPtrs & children,
    const std::unordered_set<const ActionsDAG::Node *> & lazy_executed_nodes,
    LazySubtreeCompilableCache & lazy_subtree_cache,
    bool & guards_match_interpreter)
{
    /// Extract CompileDAG from root actions dag node.

    CompileDAG dag;

    std::unordered_map<const ActionsDAG::Node *, size_t> visited_node_to_compile_dag_position;
    /// The `ActionsDAG` node of each node of `dag`.
    std::vector<const ActionsDAG::Node *> action_nodes;

    struct Frame
    {
        const ActionsDAG::Node * node;
        size_t next_child_to_visit = 0;
        size_t skip_compile = false;
    };

    std::stack<Frame> stack;
    stack.emplace(Frame{.node = root});

    while (!stack.empty())
    {
        auto & frame = stack.top();
        const auto * node = frame.node;

        bool is_compilable_constant = isCompilableConstant(*node);
        bool is_compilable_function = isCompilableFunction(*node, lazy_executed_nodes, lazy_subtree_cache);

        if (!is_compilable_function || is_compilable_constant || frame.skip_compile)
        {
            CompileDAG::Node compile_node;
            compile_node.function = node->function_base;
            compile_node.result_type = node->result_type;

            if (is_compilable_constant)
            {
                compile_node.type = CompileDAG::CompileType::CONSTANT;
                compile_node.column = node->column;
            }
            else if (frame.skip_compile)
            {
                compile_node.type = CompileDAG::CompileType::CONSTANT;
                compile_node.column = node->column;
                compile_node.skip_compile = true;
            }
            else
            {
                compile_node.type = CompileDAG::CompileType::INPUT;
                children.emplace_back(node);
            }

            visited_node_to_compile_dag_position[node] = dag.getNodesCount();
            action_nodes.push_back(node);
            dag.addNode(std::move(compile_node));
            stack.pop();
            continue;
        }

        const auto & function = *node->function_base;
        auto skip_arguments = function.getArgumentsThatDontParticipateInCompilation(function.getArgumentTypes());
        while (frame.next_child_to_visit < node->children.size())
        {
            const auto & child = node->children[frame.next_child_to_visit];
            if (visited_node_to_compile_dag_position.contains(child))
            {
                ++frame.next_child_to_visit;
                continue;
            }

            bool skip_compile = std::find(skip_arguments.begin(), skip_arguments.end(), frame.next_child_to_visit) != skip_arguments.end();
            if (skip_compile && (!child->column || child->column->onlyNull()))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Only constant nodes with non-null value could skip compilation");

            stack.emplace(Frame{.node = child, .skip_compile = skip_compile});
            break;
        }

        bool all_children_visited = frame.next_child_to_visit == node->children.size();

        if (!all_children_visited)
            continue;

        /// Here we process only functions that are not compiled constants

        CompileDAG::Node compile_node;
        compile_node.function = node->function_base;
        compile_node.result_type = node->result_type;
        compile_node.type = CompileDAG::CompileType::FUNCTION;
        compile_node.name = node->result_name;

        for (const auto * child : node->children)
            compile_node.arguments.push_back(visited_node_to_compile_dag_position[child]);

        visited_node_to_compile_dag_position[node] = dag.getNodesCount();
        action_nodes.push_back(node);

        dag.addNode(std::move(compile_node));
        stack.pop();
    }

    guards_match_interpreter = setGuards(dag, action_nodes, lazy_executed_nodes);
    return dag;
}

void ActionsDAG::compileFunctions(size_t min_count_to_compile_expression, const std::unordered_set<const ActionsDAG::Node *> & lazy_executed_nodes)
{
    struct Data
    {
        bool is_compilable_in_isolation = false;
        bool all_parents_compilable = true;
        size_t compilable_children_size = 0;
        size_t children_size = 0;
    };

    std::unordered_map<const Node *, Data> node_to_data;
    LazySubtreeCompilableCache lazy_subtree_cache;

    /// Check which nodes can be compiled in isolation

    for (const auto & node : nodes)
    {
        bool node_is_compilable_in_isolation = isCompilableFunction(node, lazy_executed_nodes, lazy_subtree_cache) && !isCompilableConstant(node);
        node_to_data[&node].is_compilable_in_isolation = node_is_compilable_in_isolation;
    }

    struct Frame
    {
        const Node * node;
        size_t next_child_to_visit = 0;
    };

    std::stack<Frame> stack;
    std::unordered_set<const Node *> visited_nodes;

    /** Algorithm is to iterate over each node in ActionsDAG, and update node compilable_children_size.
      * After this procedure data for each node is initialized.
      */

    for (auto & node : nodes)
    {
        if (visited_nodes.contains(&node))
            continue;

        stack.emplace(Frame{.node = &node});

        while (!stack.empty())
        {
            auto & current_frame = stack.top();
            auto & current_node = current_frame.node;

            while (current_frame.next_child_to_visit < current_node->children.size())
            {
                const auto & child = current_node->children[current_frame.next_child_to_visit];

                if (visited_nodes.contains(child))
                {
                    ++current_frame.next_child_to_visit;
                    continue;
                }

                stack.emplace(Frame{.node=child});
                break;
            }

            bool all_children_visited = current_frame.next_child_to_visit == current_node->children.size();

            if (!all_children_visited)
                continue;

            auto & current_node_data = node_to_data[current_node];

            if (current_node_data.is_compilable_in_isolation)
            {
                for (const auto * child : current_node->children)
                {
                    auto & child_data = node_to_data[child];

                    if (child_data.is_compilable_in_isolation)
                    {
                        current_node_data.compilable_children_size += child_data.compilable_children_size;
                        current_node_data.compilable_children_size += 1;
                    }

                    current_node_data.children_size += node_to_data[child].children_size;
                }

                current_node_data.children_size += current_node->children.size();
            }

            visited_nodes.insert(current_node);
            stack.pop();
        }
    }

    for (const auto & node : nodes)
    {
        auto & node_data = node_to_data[&node];
        bool node_is_valid_for_compilation = node_data.is_compilable_in_isolation && node_data.compilable_children_size > 0;

        for (const auto & child : node.children)
            node_to_data[child].all_parents_compilable &= node_is_valid_for_compilation;
    }

    for (const auto & output_node : outputs)
    {
        /// Force output nodes to compile
        node_to_data[output_node].all_parents_compilable = false;
    }

    std::vector<Node *> nodes_to_compile;

    for (auto & node : nodes)
    {
        auto & node_data = node_to_data[&node];

        bool node_is_valid_for_compilation = node_data.is_compilable_in_isolation && node_data.compilable_children_size > 0;

        /// If all parents are compilable then this node should not be standalone compiled
        bool should_compile = node_is_valid_for_compilation && !node_data.all_parents_compilable;

        if (!should_compile)
            continue;

        nodes_to_compile.emplace_back(&node);
    }

    /** Sort nodes before compilation using their children size to avoid compiling subexpression before compile parent expression.
      * This is needed to avoid compiling expression more than once with different names because of compilation order.
      */
    ::sort(nodes_to_compile.begin(), nodes_to_compile.end(), [&](const Node * lhs, const Node * rhs)
    {
        return node_to_data[lhs].children_size > node_to_data[rhs].children_size;
    });

    for (auto & node : nodes_to_compile)
    {
        NodeRawConstPtrs new_children;
        bool guards_match_interpreter = false;
        auto dag = getCompilableDAG(node, new_children, lazy_executed_nodes, lazy_subtree_cache, guards_match_interpreter);

        if (dag.getInputNodesCount() == 0 || !guards_match_interpreter)
            continue;

        if (auto fn = compile(dag, min_count_to_compile_expression))
        {
            ColumnsWithTypeAndName arguments;
            arguments.reserve(new_children.size());
            for (const auto * child : new_children)
                arguments.emplace_back(child->column, child->result_type, child->result_name);

            node->type = ActionsDAG::ActionType::FUNCTION;
            node->function_base = fn;
            node->function = fn->prepare(arguments);
            node->children.swap(new_children);
            node->is_function_compiled = true;
            node->column = nullptr;
        }
    }
}

}

#endif
