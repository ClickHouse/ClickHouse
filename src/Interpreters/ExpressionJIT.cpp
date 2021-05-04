#include <Interpreters/ExpressionJIT.h>

#if USE_EMBEDDED_COMPILER

#include <optional>
#include <stack>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Native.h>
#include <Functions/IFunctionAdaptors.h>

#include <Interpreters/JIT/CHJIT.h>
#include <Interpreters/JIT/CompileDAG.h>
#include <Interpreters/JIT/compileFunction.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static CHJIT & getJITInstance()
{
    static CHJIT jit;
    return jit;
}

class LLVMExecutableFunction : public IExecutableFunctionImpl
{
    std::string name;
    void * function = nullptr;
public:
    explicit LLVMExecutableFunction(const std::string & name_)
        : name(name_)
    {
        function = getJITInstance().findCompiledFunction(name);

        if (!function)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find compiled function {}", name);
    }

    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (!canBeNativeType(*result_type))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "LLVMExecutableFunction unexpected result type in: {}", result_type->getName());

        auto result_column = result_type->createColumn();

        if (input_rows_count)
        {
            result_column = result_column->cloneResized(input_rows_count);

            std::vector<ColumnData> columns(arguments.size() + 1);
            for (size_t i = 0; i < arguments.size(); ++i)
            {
                const auto * column = arguments[i].column.get();
                if (!column)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is missing", arguments[i].name);

                columns[i] = getColumnData(column);
            }

            columns[arguments.size()] = getColumnData(result_column.get());
            auto * function_typed = reinterpret_cast<void (*) (size_t, ColumnData *)>(function);
            function_typed(input_rows_count, columns.data());

            #if defined(MEMORY_SANITIZER)
            /// Memory sanitizer don't know about stores from JIT-ed code.
            /// But maybe we can generate this code with MSan instrumentation?

            if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(result_column.get()))
            {
                const auto & nested_column = nullable_column->getNestedColumn();
                const auto & null_map_column = nullable_column->getNullMapColumn();

                auto nested_column_raw_data = nested_column.getRawData();
                __msan_unpoison(nested_column_raw_data.data, nested_column_raw_data.size);

                auto null_map_column_raw_data = null_map_column.getRawData();
                __msan_unpoison(null_map_column_raw_data.data, null_map_column_raw_data.size);
            }
            else
            {
                __msan_unpoison(result_column->getRawData().data, result_column->getRawData().size);
            }

            #endif
        }

        return result_column;
    }

};

class LLVMFunction : public IFunctionBaseImpl
{
public:

    explicit LLVMFunction(const CompileDAG & dag_)
        : name(dag_.dump())
        , dag(dag_)
    {
        for (size_t i = 0; i < dag.getNodesCount(); ++i)
        {
            const auto & node = dag[i];

            if (node.type == CompileDAG::CompileType::FUNCTION)
                nested_functions.emplace_back(node.function);
            else if (node.type == CompileDAG::CompileType::INPUT)
                argument_types.emplace_back(node.result_type);
        }

        module_info = compileFunction(getJITInstance(), *this);
    }

    ~LLVMFunction() override
    {
        getJITInstance().deleteCompiledModule(module_info);
    }

    size_t getCompiledSize() const { return module_info.size; }

    bool isCompilable() const override { return true; }

    llvm::Value * compile(llvm::IRBuilderBase & builder, Values values) const override
    {
        return dag.compile(builder, values);
    }

    String getName() const override { return name; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }

    const DataTypePtr & getResultType() const override { return dag.back().result_type; }

    ExecutableFunctionImplPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<LLVMExecutableFunction>(name);
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
        Monotonicity result(true, true, true);
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

    static void applyFunction(IFunctionBase & function, Field & value)
    {
        const auto & type = function.getArgumentTypes().at(0);
        ColumnsWithTypeAndName args{{type->createColumnConst(1, value), type, "x" }};
        auto col = function.execute(args, function.getResultType(), 1);
        col->get(0, value);
    }

private:
    std::string name;
    CompileDAG dag;
    DataTypes argument_types;
    std::vector<FunctionBasePtr> nested_functions;
    CHJIT::CompiledModuleInfo module_info;
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

    FunctionBasePtr fn;

    if (auto * compilation_cache = CompiledExpressionCacheFactory::instance().tryGetCache())
    {
        auto [compiled_function, was_inserted] = compilation_cache->getOrSet(hash_key, [&dag] ()
        {
            auto llvm_function = std::make_unique<LLVMFunction>(dag);
            size_t compiled_size = llvm_function->getCompiledSize();

            FunctionBasePtr llvm_function_wrapper = std::make_shared<FunctionBaseAdaptor>(std::move(llvm_function));
            CompiledFunction compiled_function
            {
                .function = llvm_function_wrapper,
                .compiled_size = compiled_size
            };

            return std::make_shared<CompiledFunction>(compiled_function);
        });

        fn = compiled_function->function;
    }
    else
    {
        fn = std::make_shared<FunctionBaseAdaptor>(std::make_unique<LLVMFunction>(dag));
    }

    return fn;
}

static bool isCompilable(const IFunctionBase & function)
{
    if (!canBeNativeType(*function.getResultType()))
        return false;

    for (const auto & type : function.getArgumentTypes())
    {
        if (!canBeNativeType(*type))
            return false;
    }

    return function.isCompilable();
}

static bool isCompilableConstant(const ActionsDAG::Node & node)
{
    return node.column && isColumnConst(*node.column) && canBeNativeType(*node.result_type) && node.allow_constant_folding;
}

static bool isCompilableFunction(const ActionsDAG::Node & node)
{
    return node.type == ActionsDAG::ActionType::FUNCTION && isCompilable(*node.function_base);
}

static CompileDAG getCompilableDAG(
    const ActionsDAG::Node * root,
    ActionsDAG::NodeRawConstPtrs & children,
    const std::unordered_set<const ActionsDAG::Node *> & used_in_result)
{
    CompileDAG dag;

    std::unordered_map<const ActionsDAG::Node *, size_t> positions;
    struct Frame
    {
        const ActionsDAG::Node * node;
        size_t next_child_to_visit = 0;
    };

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});

    while (!stack.empty())
    {
        auto & frame = stack.top();
        bool is_const = isCompilableConstant(*frame.node);
        bool can_inline = stack.size() == 1 || !used_in_result.count(frame.node);
        bool is_compilable_function = !is_const && can_inline && isCompilableFunction(*frame.node);

        while (is_compilable_function && frame.next_child_to_visit < frame.node->children.size())
        {
            const auto * child = frame.node->children[frame.next_child_to_visit];

            if (positions.count(child))
                ++frame.next_child_to_visit;
            else
            {
                stack.emplace(Frame{.node = child});
                break;
            }
        }

        if (!is_compilable_function || frame.next_child_to_visit == frame.node->children.size())
        {
            CompileDAG::Node node;
            node.function = frame.node->function_base;
            node.result_type = frame.node->result_type;

            if (is_compilable_function)
            {
                node.type = CompileDAG::CompileType::FUNCTION;
                for (const auto * child : frame.node->children)
                    node.arguments.push_back(positions[child]);
            }
            else if (is_const)
            {
                node.type = CompileDAG::CompileType::CONSTANT;
                node.column = frame.node->column;
            }
            else
            {
                node.type = CompileDAG::CompileType::INPUT;
                children.emplace_back(frame.node);

            }

            positions[frame.node] = dag.getNodesCount();
            dag.addNode(std::move(node));
            stack.pop();
        }
    }

    return dag;
}

void ActionsDAG::compileFunctions(size_t min_count_to_compile_expression)
{
    struct Data
    {
        bool is_compilable = false;
        bool all_parents_compilable = true;
        size_t num_inlineable_nodes = 0;
    };

    std::unordered_map<const Node *, Data> data;
    std::unordered_set<const Node *> used_in_result;

    for (const auto & node : nodes)
        data[&node].is_compilable = isCompilableConstant(node) || isCompilableFunction(node);

    for (const auto & node : nodes)
        if (!data[&node].is_compilable)
            for (const auto * child : node.children)
                data[child].all_parents_compilable = false;

    for (const auto * node : index)
        used_in_result.insert(node);

    struct Frame
    {
        const Node * node;
        size_t next_child_to_visit = 0;
    };

    std::stack<Frame> stack;
    std::unordered_set<const Node *> visited;

    for (auto & node : nodes)
    {
        if (visited.count(&node))
            continue;

        stack.emplace(Frame{.node = &node});
        while (!stack.empty())
        {
            auto & frame = stack.top();

            while (frame.next_child_to_visit < frame.node->children.size())
            {
                const auto * child = frame.node->children[frame.next_child_to_visit];

                if (visited.count(child))
                    ++frame.next_child_to_visit;
                else
                {
                    stack.emplace(Frame{.node = child});
                    break;
                }
            }

            if (frame.next_child_to_visit == frame.node->children.size())
            {
                auto & cur = data[frame.node];
                if (cur.is_compilable)
                {
                    cur.num_inlineable_nodes = 1;

                    if (!isCompilableConstant(*frame.node))
                        for (const auto * child : frame.node->children)
                            if (!used_in_result.count(child))
                                cur.num_inlineable_nodes += data[child].num_inlineable_nodes;

                    /// Check if we should inline current node.
                    bool should_compile = true;

                    /// Inline parents instead of node is possible.
                    if (!used_in_result.count(frame.node) && cur.all_parents_compilable)
                        should_compile = false;

                    /// There is no reason to inline single node.
                    /// The result of compiling function in isolation is pretty much the same as its `execute` method.
                    if (cur.num_inlineable_nodes <= 1)
                        should_compile = false;

                    if (should_compile)
                    {
                        NodeRawConstPtrs new_children;
                        auto dag = getCompilableDAG(frame.node, new_children, used_in_result);

                        if (dag.getInputNodesCount() > 0)
                        {
                            if (auto fn = compile(dag, min_count_to_compile_expression))
                            {
                                ColumnsWithTypeAndName arguments;
                                arguments.reserve(new_children.size());
                                for (const auto * child : new_children)
                                    arguments.emplace_back(child->column, child->result_type, child->result_name);

                                auto * frame_node = const_cast<Node *>(frame.node);
                                frame_node->type = ActionsDAG::ActionType::FUNCTION;
                                frame_node->function_base = fn;
                                frame_node->function = fn->prepare(arguments);
                                frame_node->children.swap(new_children);
                                frame_node->is_function_compiled = true;
                                frame_node->column = nullptr;
                            }
                        }
                    }
                }

                visited.insert(frame.node);
                stack.pop();
            }
        }
    }
}

CompiledExpressionCacheFactory & CompiledExpressionCacheFactory::instance()
{
    static CompiledExpressionCacheFactory factory;
    return factory;
}

void CompiledExpressionCacheFactory::init(size_t cache_size)
{
    if (cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CompiledExpressionCache was already initialized");

    cache = std::make_unique<CompiledExpressionCache>(cache_size);
}

CompiledExpressionCache * CompiledExpressionCacheFactory::tryGetCache()
{
    return cache.get();
}

}

#endif
