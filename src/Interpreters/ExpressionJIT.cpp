#include <Interpreters/ExpressionJIT.h>

#if USE_EMBEDDED_COMPILER

#include <optional>
#include <stack>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Native.h>
#include <Functions/IFunctionAdaptors.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>

#include <Interpreters/JIT/CHJIT.h>

namespace ProfileEvents
{
    extern const Event CompileFunction;
    extern const Event CompileExpressionsMicroseconds;
    extern const Event CompileExpressionsBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    struct ColumnData
    {
        const char * data = nullptr;
        const char * null = nullptr;
        size_t stride = 0;
    };

    struct ColumnDataPlaceholder
    {
        llvm::Value * data_init; /// first row
        llvm::Value * null_init;
        llvm::Value * stride;
        llvm::PHINode * data; /// current row
        llvm::PHINode * null;
    };
}

static ColumnData getColumnData(const IColumn * column)
{
    ColumnData result;
    const bool is_const = isColumnConst(*column);
    if (is_const)
        column = &reinterpret_cast<const ColumnConst *>(column)->getDataColumn();
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(column))
    {
        result.null = nullable->getNullMapColumn().getRawData().data;
        column = &nullable->getNestedColumn();
    }
    result.data = column->getRawData().data;
    result.stride = is_const ? 0 : column->sizeOfValueIfFixed();
    return result;
}

static void applyFunction(IFunctionBase & function, Field & value)
{
    const auto & type = function.getArgumentTypes().at(0);
    ColumnsWithTypeAndName args{{type->createColumnConst(1, value), type, "x" }};
    auto col = function.execute(args, function.getResultType(), 1);
    col->get(0, value);
}

static CHJIT jit;

template <typename... Ts>
static bool castToEitherWithNullable(IColumn * column)
{
    return ((typeid_cast<Ts *>(column)
            || (typeid_cast<ColumnNullable *>(column) && typeid_cast<Ts *>(&(typeid_cast<ColumnNullable *>(column)->getNestedColumn())))) || ...);
}

class LLVMExecutableFunction : public IExecutableFunctionImpl
{
    std::string name;
    void * function;

public:
    explicit LLVMExecutableFunction(const std::string & name_)
        : name(name_)
        , function(jit.findCompiledFunction(name))
    {
        if (!function)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find compiled function {}", name_);
    }

    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto col_res = result_type->createColumn();

        if (input_rows_count)
        {
            if (!castToEitherWithNullable<
                ColumnUInt8, ColumnUInt16, ColumnUInt32, ColumnUInt64,
                ColumnInt8, ColumnInt16, ColumnInt32, ColumnInt64,
                ColumnFloat32, ColumnFloat64>(col_res.get()))
                throw Exception("Unexpected column in LLVMExecutableFunction: " + col_res->getName(), ErrorCodes::LOGICAL_ERROR);
            col_res = col_res->cloneResized(input_rows_count);
            std::vector<ColumnData> columns(arguments.size() + 1);
            for (size_t i = 0; i < arguments.size(); ++i)
            {
                const auto * column = arguments[i].column.get();
                if (!column)
                    throw Exception("Column " + arguments[i].name + " is missing", ErrorCodes::LOGICAL_ERROR);
                columns[i] = getColumnData(column);
            }
            columns[arguments.size()] = getColumnData(col_res.get());
            reinterpret_cast<void (*) (size_t, ColumnData *)>(function)(input_rows_count, columns.data());

            /// Memory sanitizer don't know about stores from JIT-ed code.
            /// But maybe we can generate this code with MSan instrumentation?
            __msan_unpoison(col_res->getRawData().data, col_res->getRawData().size);
        }

        return col_res;
    }
};

static void compileFunction(llvm::Module & module, const IFunctionBaseImpl & f)
{
    ProfileEvents::increment(ProfileEvents::CompileFunction);

    const auto & arg_types = f.getArgumentTypes();

    llvm::IRBuilder<> b(module.getContext());
    auto * size_type = b.getIntNTy(sizeof(size_t) * 8);
    auto * data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy(), size_type);
    auto * func_type = llvm::FunctionType::get(b.getVoidTy(), { size_type, data_type->getPointerTo() }, /*isVarArg=*/false);

    auto * func = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, f.getName(), module);
    auto * args = func->args().begin();
    llvm::Value * counter_arg = &*args++;
    llvm::Value * columns_arg = &*args++;

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", func);
    b.SetInsertPoint(entry);
    std::vector<ColumnDataPlaceholder> columns(arg_types.size() + 1);
    for (size_t i = 0; i <= arg_types.size(); ++i)
    {
        const auto & type = i == arg_types.size() ? f.getResultType() : arg_types[i];
        auto * data = b.CreateLoad(b.CreateConstInBoundsGEP1_32(data_type, columns_arg, i));
        columns[i].data_init = b.CreatePointerCast(b.CreateExtractValue(data, {0}), toNativeType(b, removeNullable(type))->getPointerTo());
        columns[i].null_init = type->isNullable() ? b.CreateExtractValue(data, {1}) : nullptr;
        columns[i].stride = b.CreateExtractValue(data, {2});
    }

    /// assume nonzero initial value in `counter_arg`
    auto * loop = llvm::BasicBlock::Create(b.getContext(), "loop", func);
    b.CreateBr(loop);
    b.SetInsertPoint(loop);
    auto * counter_phi = b.CreatePHI(counter_arg->getType(), 2);
    counter_phi->addIncoming(counter_arg, entry);
    for (auto & col : columns)
    {
        col.data = b.CreatePHI(col.data_init->getType(), 2);
        col.data->addIncoming(col.data_init, entry);
        if (col.null_init)
        {
            col.null = b.CreatePHI(col.null_init->getType(), 2);
            col.null->addIncoming(col.null_init, entry);
        }
    }
    ValuePlaceholders arguments(arg_types.size());
    for (size_t i = 0; i < arguments.size(); ++i) // NOLINT
    {
        arguments[i] = [&b, &col = columns[i], &type = arg_types[i]]() -> llvm::Value *
        {
            auto * value = b.CreateLoad(col.data);
            if (!col.null)
                return value;
            auto * is_null = b.CreateICmpNE(b.CreateLoad(col.null), b.getInt8(0));
            auto * nullable = llvm::Constant::getNullValue(toNativeType(b, type));
            return b.CreateInsertValue(b.CreateInsertValue(nullable, value, {0}), is_null, {1});
        };
    }
    auto * result = f.compile(b, std::move(arguments));
    if (columns.back().null)
    {
        b.CreateStore(b.CreateExtractValue(result, {0}), columns.back().data);
        b.CreateStore(b.CreateSelect(b.CreateExtractValue(result, {1}), b.getInt8(1), b.getInt8(0)), columns.back().null);
    }
    else
    {
        b.CreateStore(result, columns.back().data);
    }

    auto * cur_block = b.GetInsertBlock();
    for (auto & col : columns)
    {
        /// stride is either 0 or size of native type; output column is never constant; neither is at least one input
        auto * is_const = &col == &columns.back() || columns.size() <= 2 ? b.getFalse() : b.CreateICmpEQ(col.stride, llvm::ConstantInt::get(size_type, 0));
        col.data->addIncoming(b.CreateSelect(is_const, col.data, b.CreateConstInBoundsGEP1_32(nullptr, col.data, 1)), cur_block);
        if (col.null)
            col.null->addIncoming(b.CreateSelect(is_const, col.null, b.CreateConstInBoundsGEP1_32(nullptr, col.null, 1)), cur_block);
    }
    counter_phi->addIncoming(b.CreateSub(counter_phi, llvm::ConstantInt::get(size_type, 1)), cur_block);

    auto * end = llvm::BasicBlock::Create(b.getContext(), "end", func);
    b.CreateCondBr(b.CreateICmpNE(counter_phi, llvm::ConstantInt::get(size_type, 1)), loop, end);
    b.SetInsertPoint(end);
    b.CreateRetVoid();
}

static llvm::Constant * getNativeValue(llvm::Type * type, const IColumn & column, size_t i)
{
    /// TODO: Change name this is just for constants
    if (!type || column.size() <= i)
        return nullptr;
    if (const auto * constant = typeid_cast<const ColumnConst *>(&column))
        return getNativeValue(type, constant->getDataColumn(), 0);
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        auto * value = getNativeValue(type->getContainedType(0), nullable->getNestedColumn(), i);
        auto * is_null = llvm::ConstantInt::get(type->getContainedType(1), nullable->isNullAt(i));
        return value ? llvm::ConstantStruct::get(static_cast<llvm::StructType *>(type), value, is_null) : nullptr;
    }
    if (type->isFloatTy())
        return llvm::ConstantFP::get(type, assert_cast<const ColumnVector<Float32> &>(column).getElement(i));
    if (type->isDoubleTy())
        return llvm::ConstantFP::get(type, assert_cast<const ColumnVector<Float64> &>(column).getElement(i));
    if (type->isIntegerTy())
        return llvm::ConstantInt::get(type, column.getUInt(i));
    /// TODO: if (type->isVectorTy())
    return nullptr;
}

/// Same as IFunctionBase::compile, but also for constants and input columns.
using CompilableExpression = std::function<llvm::Value * (llvm::IRBuilderBase &, const ValuePlaceholders &)>;

static CompilableExpression subexpression(ColumnPtr c, DataTypePtr type)
{
    return [=](llvm::IRBuilderBase & b, const ValuePlaceholders &) { return getNativeValue(toNativeType(b, type), *c, 0); };
}

static CompilableExpression subexpression(size_t i)
{
    return [=](llvm::IRBuilderBase &, const ValuePlaceholders & inputs) { return inputs[i](); };
}

static CompilableExpression subexpression(const IFunctionBase & f, std::vector<CompilableExpression> args)
{
    return [&, args = std::move(args)](llvm::IRBuilderBase & builder, const ValuePlaceholders & inputs)
    {
        ValuePlaceholders input;
        for (const auto & arg : args)
            input.push_back([&]() { return arg(builder, inputs); });
        auto * result = f.compile(builder, input);
        if (result->getType() != toNativeType(builder, f.getResultType()))
            throw Exception("Function " + f.getName() + " generated an llvm::Value of invalid type", ErrorCodes::LOGICAL_ERROR);
        return result;
    };
}


LLVMFunction::LLVMFunction(const CompileDAG & dag)
    : name(dag.dump())
{
    std::vector<CompilableExpression> expressions;
    expressions.reserve(dag.size());

    auto & context = jit.getContext();
    llvm::IRBuilder<> builder(context);

    for (const auto & node : dag)
    {
        switch (node.type)
        {
            case CompileNode::NodeType::CONSTANT:
            {
                const auto * col = typeid_cast<const ColumnConst *>(node.column.get());

                /// TODO: implement `getNativeValue` for all types & replace the check with `c.column && toNativeType(...)`
                if (!getNativeValue(toNativeType(builder, node.result_type), col->getDataColumn(), 0))
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                                    "Cannot compile constant of type {} = {}",
                                    node.result_type->getName(),
                                    applyVisitor(FieldVisitorToString(), col->getDataColumn()[0]));

                expressions.emplace_back(subexpression(col->getDataColumnPtr(), node.result_type));
                break;
            }
            case CompileNode::NodeType::FUNCTION:
            {
                std::vector<CompilableExpression> args;
                args.reserve(node.arguments.size());

                for (auto arg : node.arguments)
                    args.emplace_back(expressions[arg]);

                originals.push_back(node.function);
                expressions.emplace_back(subexpression(*node.function, std::move(args)));
                break;
            }
            case CompileNode::NodeType::INPUT:
            {
                expressions.emplace_back(subexpression(arg_types.size()));
                arg_types.push_back(node.result_type);
                break;
            }
        }
    }

    expression = std::move(expressions.back());

    auto module_for_compilation = jit.createModuleForCompilation();
    compileFunction(*module_for_compilation, *this);
    jit.compileModule(std::move(module_for_compilation));
}

llvm::Value * LLVMFunction::compile(llvm::IRBuilderBase & builder, ValuePlaceholders values) const
{
    return expression(builder, values);
}

ExecutableFunctionImplPtr LLVMFunction::prepare(const ColumnsWithTypeAndName &) const { return std::make_unique<LLVMExecutableFunction>(name); }

bool LLVMFunction::isDeterministic() const
{
    for (const auto & f : originals)
        if (!f->isDeterministic())
            return false;
    return true;
}

bool LLVMFunction::isDeterministicInScopeOfQuery() const
{
    for (const auto & f : originals)
        if (!f->isDeterministicInScopeOfQuery())
            return false;
    return true;
}

bool LLVMFunction::isSuitableForConstantFolding() const
{
    for (const auto & f : originals)
        if (!f->isSuitableForConstantFolding())
            return false;
    return true;
}

bool LLVMFunction::isInjective(const ColumnsWithTypeAndName & sample_block) const
{
    for (const auto & f : originals)
        if (!f->isInjective(sample_block))
            return false;
    return true;
}

bool LLVMFunction::hasInformationAboutMonotonicity() const
{
    for (const auto & f : originals)
        if (!f->hasInformationAboutMonotonicity())
            return false;
    return true;
}

LLVMFunction::Monotonicity LLVMFunction::getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const
{
    const IDataType * type_ptr = &type;
    Field left_mut = left;
    Field right_mut = right;
    Monotonicity result(true, true, true);
    /// monotonicity is only defined for unary functions, so the chain must describe a sequence of nested calls
    for (size_t i = 0; i < originals.size(); ++i)
    {
        Monotonicity m = originals[i]->getMonotonicityForRange(*type_ptr, left_mut, right_mut);
        if (!m.is_monotonic)
            return m;
        result.is_positive ^= !m.is_positive;
        result.is_always_monotonic &= m.is_always_monotonic;
        if (i + 1 < originals.size())
        {
            if (left_mut != Field())
                applyFunction(*originals[i], left_mut);
            if (right_mut != Field())
                applyFunction(*originals[i], right_mut);
            if (!m.is_positive)
                std::swap(left_mut, right_mut);
            type_ptr = originals[i]->getResultType().get();
        }
    }
    return result;
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

static LLVMFunction::CompileDAG getCompilableDAG(
    const ActionsDAG::Node * root,
    ActionsDAG::NodeRawConstPtrs & children,
    const std::unordered_set<const ActionsDAG::Node *> & used_in_result)
{
    LLVMFunction::CompileDAG dag;

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
            LLVMFunction::CompileNode node;
            node.function = frame.node->function_base;
            node.result_type = frame.node->result_type;
            node.type = is_const ? LLVMFunction::CompileNode::NodeType::CONSTANT
                                 : (is_compilable_function ? LLVMFunction::CompileNode::NodeType::FUNCTION
                                                           : LLVMFunction::CompileNode::NodeType::INPUT);

            if (node.type == LLVMFunction::CompileNode::NodeType::FUNCTION)
                for (const auto * child : frame.node->children)
                    node.arguments.push_back(positions[child]);

            if (node.type == LLVMFunction::CompileNode::NodeType::CONSTANT)
                node.column = frame.node->column;

            if (node.type == LLVMFunction::CompileNode::NodeType::INPUT)
                children.emplace_back(frame.node);

            positions[frame.node] = dag.size();
            dag.push_back(std::move(node));
            stack.pop();
        }
    }

    return dag;
}

std::string LLVMFunction::CompileDAG::dump() const
{
    WriteBufferFromOwnString out;
    bool first = true;
    for (const auto & node : *this)
    {
        if (!first)
            out << " ; ";
        first = false;

        switch (node.type)
        {
            case CompileNode::NodeType::CONSTANT:
            {
                const auto * column = typeid_cast<const ColumnConst *>(node.column.get());
                const auto & data = column->getDataColumn();
                out << node.result_type->getName() << " = " << applyVisitor(FieldVisitorToString(), data[0]);
                break;
            }
            case CompileNode::NodeType::FUNCTION:
            {
                out << node.result_type->getName() << " = ";
                out << node.function->getName() << "(";

                for (size_t i = 0; i < node.arguments.size(); ++i)
                {
                    if (i)
                        out << ", ";

                    out << node.arguments[i];
                }

                out << ")";
                break;
            }
            case CompileNode::NodeType::INPUT:
            {
                out << node.result_type->getName();
                break;
            }
        }
    }

    return out.str();
}

UInt128 LLVMFunction::CompileDAG::hash() const
{
    SipHash hash;
    for (const auto & node : *this)
    {
        hash.update(node.type);
        hash.update(node.result_type->getName());

        switch (node.type)
        {
            case CompileNode::NodeType::CONSTANT:
            {
                typeid_cast<const ColumnConst *>(node.column.get())->getDataColumn().updateHashWithValue(0, hash);
                break;
            }
            case CompileNode::NodeType::FUNCTION:
            {
                hash.update(node.function->getName());
                for (size_t arg : node.arguments)
                    hash.update(arg);

                break;
            }
            case CompileNode::NodeType::INPUT:
            {
                break;
            }
        }
    }

    UInt128 result;
    hash.get128(result.low, result.high);
    return result;
}

static FunctionBasePtr compile(
    const LLVMFunction::CompileDAG & dag,
    size_t min_count_to_compile_expression)
{
    static std::unordered_map<UInt128, UInt32, UInt128Hash> counter;
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
        std::tie(fn, std::ignore) = compilation_cache->getOrSet(hash_key, [&dag] ()
        {
            Stopwatch watch;
            FunctionBasePtr result_fn;
            result_fn = std::make_shared<FunctionBaseAdaptor>(std::make_unique<LLVMFunction>(dag));
            ProfileEvents::increment(ProfileEvents::CompileExpressionsMicroseconds, watch.elapsedMicroseconds());
            return result_fn;
        });
    }
    else
    {
        Stopwatch watch;
        fn = std::make_shared<FunctionBaseAdaptor>(std::make_unique<LLVMFunction>(dag));
        ProfileEvents::increment(ProfileEvents::CompileExpressionsMicroseconds, watch.elapsedMicroseconds());
    }

    return fn;
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
                        for (const auto * child_node : frame.node->children)
                            std::cerr << child_node->result_name << std::endl;

                        NodeRawConstPtrs new_children;
                        auto dag = getCompilableDAG(frame.node, new_children, used_in_result);

                        bool all_constants = true;

                        for (const auto & compiled_node : dag)
                        {
                            if (compiled_node.type == LLVMFunction::CompileNode::NodeType::INPUT)
                            {
                                all_constants = false;
                                break;
                            }
                        }

                        if (!all_constants)
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
