#include <GPU/GPUExpressionCompiler.h>

#if USE_GPU

#if !USE_EMBEDDED_COMPILER
#    error "The GPU engine compiles its expressions with the embedded compiler"
#endif

#include <GPU/GPUTypeMapping.h>

#include <Columns/ColumnConst.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/Hex.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Native.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/JIT/CompileDAG.h>
#include <Interpreters/convertFieldToType.h>

#include <cuda_runtime_api.h>

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/IntrinsicsNVPTX.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>
#include <llvm/ADT/SmallString.h>
#include <llvm/MC/MCSubtargetInfo.h>
#include <llvm/MC/TargetRegistry.h>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/Target/TargetOptions.h>
#include <llvm/TargetParser/Triple.h>

#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

extern "C" void LLVMInitializeNVPTXTargetInfo();
extern "C" void LLVMInitializeNVPTXTarget();
extern "C" void LLVMInitializeNVPTXTargetMC();
extern "C" void LLVMInitializeNVPTXAsmPrinter();

namespace DB::ErrorCodes
{
    extern const int CANNOT_COMPILE_CODE;
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

namespace
{

constexpr const char * nvptx_triple = "nvptx64-nvidia-cuda";

/// What the kernel of an expression writes for a row.
enum class Output
{
    /// A bit of the mask for each row that the predicate holds for.
    Bitmask,
    Float64,
};

/// The PTX of a kernel, which lives as long as the process, so that a `GPUExpression` can point into it.
struct Kernel
{
    std::string name;
    std::string ptx;
};

/// The `sm_XY` of the device that the server runs queries on, which the PTX is emitted for.
const std::string & deviceArchitecture()
{
    static const std::string architecture = []
    {
        int device = 0;
        int major = 0;
        int minor = 0;
        if (cudaGetDevice(&device) != cudaSuccess
            || cudaDeviceGetAttribute(&major, cudaDevAttrComputeCapabilityMajor, device) != cudaSuccess
            || cudaDeviceGetAttribute(&minor, cudaDevAttrComputeCapabilityMinor, device) != cudaSuccess)
        {
            cudaGetLastError();
            throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "Cannot tell the compute capability of the CUDA device");
        }
        return "sm_" + std::to_string(major) + std::to_string(minor);
    }();
    return architecture;
}

std::unique_ptr<llvm::TargetMachine> nvptxTargetMachine()
{
    static std::once_flag initialized;
    std::call_once(initialized, []
    {
        LLVMInitializeNVPTXTargetInfo();
        LLVMInitializeNVPTXTarget();
        LLVMInitializeNVPTXTargetMC();
        LLVMInitializeNVPTXAsmPrinter();
    });

    const llvm::Triple triple(nvptx_triple);
    std::string error;
    const llvm::Target * target = llvm::TargetRegistry::lookupTarget(triple, error);
    if (!target)
        throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "Cannot find the NVPTX target of LLVM: {}", error);

    const std::string & architecture = deviceArchitecture();
    std::unique_ptr<llvm::TargetMachine> machine(target->createTargetMachine(
        triple, architecture, "", llvm::TargetOptions(), std::nullopt, std::nullopt, llvm::CodeGenOptLevel::Aggressive));
    if (!machine)
        throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "Cannot create an NVPTX target machine of LLVM for {}", architecture);
    if (!machine->getMCSubtargetInfo()->isCPUStringValid(architecture))
        throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "The NVPTX target of LLVM does not know the device's {}", architecture);
    return machine;
}

bool isComparison(const String & name)
{
    return name == "equals" || name == "notEquals" || name == "less" || name == "lessOrEquals" || name == "greater"
        || name == "greaterOrEquals";
}

const ActionsDAG::Node * withoutAliases(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    return node;
}

bool isConstantToConvert(const ActionsDAG::Node & node)
{
    return node.type == ActionsDAG::ActionType::COLUMN && node.column && isColumnConst(*node.column)
        && (isString(node.result_type) || isDecimal(node.result_type));
}

/// The intrinsics that NVPTX lowers into instructions of the device. The code of a function that calls another, such as
/// a function of libm, is refused, as is one that calls an intrinsic the device has no instruction for, which LLVM
/// cannot select an instruction for.
bool deviceHasIntrinsic(llvm::StringRef name)
{
    static const std::unordered_set<std::string_view> intrinsics = {
        "abs", "assume", "bitreverse", "bswap", "ceil", "copysign", "ctlz", "ctpop", "cttz", "expect", "fabs", "floor",
        "fma", "fmuladd", "fshl", "fshr", "lifetime.end", "lifetime.start", "maximum", "maxnum", "minimum", "minnum",
        "nearbyint", "rint", "round", "roundeven", "sadd.sat", "sadd.with.overflow", "smax", "smin", "smul.with.overflow",
        "sqrt", "ssub.sat", "ssub.with.overflow", "trunc", "uadd.sat", "uadd.with.overflow", "umax", "umin",
        "umul.with.overflow", "usub.sat", "usub.with.overflow",
    };

    if (!name.consume_front("llvm."))
        return false;
    if (name.starts_with("nvvm."))
        return true;
    /// Without the suffix of the overloaded types, `llvm.smax.i32`.
    for (size_t dot = name.size(); dot != llvm::StringRef::npos; dot = name.substr(0, dot).rfind('.'))
    {
        if (intrinsics.contains(std::string_view(name.substr(0, dot))))
            return true;
    }
    return false;
}

/// The part of `ActionsDAG` that an expression is of, as `CompileDAG` compiles it: every function of it compiles, and
/// its inputs are the columns it reads.
class DeviceDAG
{
public:
    explicit DeviceDAG(String & refusal_) : refusal(refusal_) { }

    bool build(const ActionsDAG::Node * root) { return add(root).has_value(); }

    CompileDAG dag;
    std::vector<NameAndTypePair> columns;

private:
    bool refused(const String & reason)
    {
        refusal = reason;
        return false;
    }

    std::optional<size_t> refuse(const String & reason)
    {
        refusal = reason;
        return {};
    }

    std::optional<size_t> added(CompileDAG::Node node)
    {
        dag.addNode(std::move(node));
        return dag.getNodesCount() - 1;
    }

    std::optional<size_t> add(const ActionsDAG::Node * node)
    {
        node = withoutAliases(node);
        if (auto it = positions.find(node); it != positions.end())
            return it->second;

        std::optional<size_t> position;
        switch (node->type)
        {
            case ActionsDAG::ActionType::INPUT:
                position = addInput(*node);
                break;
            case ActionsDAG::ActionType::COLUMN:
                position = addConstant(node->column, node->result_type);
                break;
            case ActionsDAG::ActionType::FUNCTION:
                position = addFunction(*node);
                break;
            case ActionsDAG::ActionType::ALIAS:
            case ActionsDAG::ActionType::ARRAY_JOIN:
            case ActionsDAG::ActionType::PLACEHOLDER:
                return refuse("an expression that is not of functions, columns and constants");
        }

        if (position)
            positions.emplace(node, *position);
        return position;
    }

    std::optional<size_t> addInput(const ActionsDAG::Node & node)
    {
        const auto element_type = filterElementTypeOf(*node.result_type);
        if (!element_type || *element_type == GPUElementType::String || !canBeNativeType(*node.result_type) || node.result_type->isNullable())
            return refuse("a column of type " + node.result_type->getName() + ", which the device does not read");
        if (columns.size() >= max_expression_columns)
            return refuse("an expression of more columns than the device takes");

        columns.emplace_back(node.result_name, node.result_type);
        return added({.type = CompileDAG::CompileType::INPUT, .result_type = node.result_type, .column = {}, .function = {}, .arguments = {}});
    }

    std::optional<size_t> addConstant(const ColumnConstPtr & column, const DataTypePtr & type)
    {
        if (!column)
            return refuse("a column of an expression that is not a constant");
        if (!canBeNativeType(*type) || type->isNullable())
            return refuse("a constant of type " + type->getName() + ", which does not compile");
        return added({.type = CompileDAG::CompileType::CONSTANT, .result_type = type, .column = column, .function = {}, .arguments = {}});
    }

    /// A string or `Decimal` constant that a comparison converts to the type of the column it is compared with, converted
    /// here, and the comparison of the converted constant: `d < '1994-01-01'` of a `Date` is `d < 8766`.
    bool convertConstants(
        const ActionsDAG::Node & node, FunctionBasePtr & function, ColumnsWithTypeAndName & arguments, std::vector<ColumnConstPtr> & converted_constants)
    {
        const String name = node.function_base->getName();
        if (!isComparison(name) || node.children.size() != 2)
            return true;

        const ActionsDAG::Node * children[2] = {withoutAliases(node.children[0]), withoutAliases(node.children[1])};
        bool converted = false;
        for (size_t i = 0; i < 2; ++i)
        {
            const ActionsDAG::Node & constant = *children[i];
            const ActionsDAG::Node & other = *children[1 - i];
            if (!isConstantToConvert(constant) || isConstantToConvert(other))
                continue;

            DataTypePtr type;
            if (isString(constant.result_type))
            {
                const WhichDataType which(other.result_type);
                if (which.isDateOrDate32() || which.isDateTime() || which.isNativeInt() || which.isNativeUInt() || which.isNativeFloat())
                    type = other.result_type;
            }
            else if (isFloat(other.result_type))
            {
                /// ClickHouse compares a `Decimal` with a float as both converted to `Float64`.
                type = std::make_shared<DataTypeFloat64>();
            }
            if (!type)
                return refused("a comparison of a constant of type " + constant.result_type->getName() + " with one of " + other.result_type->getName());

            const Field value = convertFieldToType((*constant.column)[0], *type);
            if (value.isNull())
                return refused("a constant compared with a column that it does not convert to the column's type");

            converted_constants[i] = type->createColumnConst(1, value);
            arguments[i] = {converted_constants[i], type, constant.result_name};
            converted = true;
        }

        if (!converted)
            return true;

        /// The query's settings of comparisons, as the comparison is built with in the plan.
        const ContextPtr query_context = CurrentThread::tryGetQueryContext();
        if (!query_context)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "An expression for the GPU compiled outside of a query");
        function = FunctionFactory::instance().get(name, query_context)->build(arguments);
        return true;
    }

    std::optional<size_t> addFunction(const ActionsDAG::Node & node)
    {
        if (!node.function_base)
            return refuse("a function node without a function");

        FunctionBasePtr function = node.function_base;
        ColumnsWithTypeAndName arguments;
        for (const auto * child : node.children)
        {
            const auto * argument = withoutAliases(child);
            arguments.push_back({argument->column, argument->result_type, argument->result_name});
        }
        std::vector<ColumnConstPtr> converted_constants(node.children.size());
        if (!convertConstants(node, function, arguments, converted_constants))
            return {};

        const DataTypePtr & result_type = function->getResultType();
        if (result_type->isNullable() || !canBeNativeType(*result_type))
            return refuse("a function of a result of type " + result_type->getName() + ", which does not compile");

        const DataTypes argument_types = function->getArgumentTypes();
        const auto skipped = function->getArgumentsThatDontParticipateInCompilation(argument_types);
        std::vector<size_t> positions_of_arguments;
        for (size_t i = 0; i < node.children.size(); ++i)
        {
            const ActionsDAG::Node * child = withoutAliases(node.children[i]);
            if (std::ranges::find(skipped, i) != skipped.end())
            {
                if (!child->column || child->column->onlyNull())
                    return refuse("an argument of `" + function->getName() + "` that it takes as a constant, which is not one");
                positions_of_arguments.push_back(*added({
                    .type = CompileDAG::CompileType::CONSTANT,
                    .result_type = child->result_type,
                    .column = child->column,
                    .skip_compile = true,
                    .function = {},
                    .arguments = {},
                }));
                continue;
            }

            if (argument_types[i]->isNullable() || !canBeNativeType(*argument_types[i]))
                return refuse("an argument of `" + function->getName() + "` of type " + argument_types[i]->getName() + ", which does not compile");

            /// A converted constant is not of the DAG.
            const auto position = converted_constants[i] ? addConstant(converted_constants[i], arguments[i].type) : add(child);
            if (!position)
                return {};
            positions_of_arguments.push_back(*position);
        }

        if (!function->isCompilable())
            return refuse("a function that does not compile: `" + function->getName() + "` of " + std::to_string(argument_types.size()) + " arguments");

        return added({
            .type = CompileDAG::CompileType::FUNCTION,
            .result_type = node.result_type,
            .column = {},
            .function = function,
            .arguments = std::move(positions_of_arguments),
        });
    }

    String & refusal;
    std::unordered_map<const ActionsDAG::Node *, size_t> positions;
};

/// The kernel of `dag`: `void name(ptr column_0, ..., ptr result, i64 rows)`, a thread for each row.
void emitKernel(llvm::Module & module, const DeviceDAG & device_dag, const DataTypePtr & result_type, Output output, const std::string & name)
{
    llvm::LLVMContext & context = module.getContext();
    llvm::IRBuilder<> b(context);

    std::vector<llvm::Type *> parameters(device_dag.columns.size() + 1, b.getPtrTy());
    parameters.push_back(b.getInt64Ty());
    auto * kernel = llvm::Function::Create(llvm::FunctionType::get(b.getVoidTy(), parameters, false), llvm::Function::ExternalLinkage, name, module);
    kernel->setCallingConv(llvm::CallingConv::PTX_Kernel);

    auto * entry = llvm::BasicBlock::Create(context, "entry", kernel);
    auto * body = llvm::BasicBlock::Create(context, "body", kernel);
    auto * done = llvm::BasicBlock::Create(context, "done", kernel);

    b.SetInsertPoint(entry);
    auto read = [&](llvm::Intrinsic::ID id) { return b.CreateZExt(b.CreateIntrinsic(id, {}), b.getInt64Ty()); };
    llvm::Value * thread = read(llvm::Intrinsic::nvvm_read_ptx_sreg_tid_x);
    llvm::Value * row = b.CreateAdd(b.CreateMul(read(llvm::Intrinsic::nvvm_read_ptx_sreg_ctaid_x), read(llvm::Intrinsic::nvvm_read_ptx_sreg_ntid_x)), thread);
    llvm::Value * rows = kernel->getArg(static_cast<unsigned>(device_dag.columns.size() + 1));
    llvm::Value * result = kernel->getArg(static_cast<unsigned>(device_dag.columns.size()));
    llvm::Value * in_range = b.CreateICmpULT(row, rows);
    b.CreateCondBr(in_range, body, done);

    b.SetInsertPoint(body);
    ValuesWithType values;
    for (size_t i = 0; i < device_dag.columns.size(); ++i)
    {
        const DataTypePtr & type = device_dag.columns[i].type;
        llvm::Type * element = toNativeType(b, type);
        values.emplace_back(b.CreateLoad(element, b.CreateInBoundsGEP(element, kernel->getArg(static_cast<unsigned>(i)), row)), type);
    }
    llvm::Value * value = device_dag.dag.compile(b, values).value;

    if (output == Output::Float64)
    {
        b.CreateStore(nativeCast(b, result_type, value, std::make_shared<DataTypeFloat64>()), b.CreateInBoundsGEP(b.getDoubleTy(), result, row));
        b.CreateBr(done);
        b.SetInsertPoint(done);
        b.CreateRetVoid();
        return;
    }

    llvm::Value * holds = b.CreateICmpNE(value, llvm::Constant::getNullValue(value->getType()));
    llvm::BasicBlock * body_end = b.GetInsertBlock();
    b.CreateBr(done);

    /// Every thread of a warp votes, those past the last row with false, and the first one writes the warp's word.
    b.SetInsertPoint(done);
    llvm::PHINode * passes = b.CreatePHI(b.getInt1Ty(), 2);
    passes->addIncoming(b.getFalse(), entry);
    passes->addIncoming(holds, body_end);
    llvm::Value * word = b.CreateIntrinsic(llvm::Intrinsic::nvvm_vote_ballot_sync, {}, {b.getInt32(0xffffffffU), passes});

    auto * write = llvm::BasicBlock::Create(context, "write", kernel);
    auto * exit = llvm::BasicBlock::Create(context, "exit", kernel);
    llvm::Value * first_of_warp = b.CreateICmpEQ(b.CreateAnd(thread, b.getInt64(31)), b.getInt64(0));
    b.CreateCondBr(b.CreateAnd(in_range, first_of_warp), write, exit);

    b.SetInsertPoint(write);
    b.CreateStore(word, b.CreateInBoundsGEP(b.getInt32Ty(), result, b.CreateLShr(row, b.getInt64(5))));
    b.CreateBr(exit);

    b.SetInsertPoint(exit);
    b.CreateRetVoid();
}

/// The PTX of `module`, optimized as the CPU's JIT optimizes its code, or nothing with `refusal` saying what the device
/// does not run.
std::optional<std::string> emitPTX(llvm::Module & module, llvm::TargetMachine & machine, String & refusal)
{
    std::string errors;
    llvm::raw_string_ostream errors_stream(errors);
    if (llvm::verifyModule(module, &errors_stream))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The kernel of an expression for the GPU is not valid: {}", errors);

    {
        llvm::LoopAnalysisManager lam;
        llvm::FunctionAnalysisManager fam;
        llvm::CGSCCAnalysisManager cgam;
        llvm::ModuleAnalysisManager mam;
        llvm::PassBuilder pass_builder(&machine);
        pass_builder.registerModuleAnalyses(mam);
        pass_builder.registerCGSCCAnalyses(cgam);
        pass_builder.registerFunctionAnalyses(fam);
        pass_builder.registerLoopAnalyses(lam);
        pass_builder.crossRegisterProxies(lam, fam, cgam, mam);
        pass_builder.buildPerModuleDefaultPipeline(llvm::OptimizationLevel::O3).run(module, mam);
    }

    for (const llvm::Function & function : module)
    {
        if (!function.isDeclaration() || function.use_empty())
            continue;
        if (!function.isIntrinsic() || !deviceHasIntrinsic(function.getName()))
        {
            refusal = "an expression whose code calls `" + function.getName().str() + "`, which the device does not run";
            return {};
        }
    }

    llvm::SmallString<0> ptx;
    llvm::raw_svector_ostream ptx_stream(ptx);
    llvm::legacy::PassManager code_generation;
    if (machine.addPassesToEmitFile(code_generation, ptx_stream, nullptr, llvm::CodeGenFileType::AssemblyFile))
        throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "The NVPTX target of LLVM does not emit PTX");
    code_generation.run(module);
    return std::string(ptx.str());
}

std::optional<CompiledGPUExpression> compile(const ActionsDAG & dag, const String & output_name, Output output, String & refusal)
{
    const ActionsDAG::Node * root = dag.tryFindInOutputs(output_name);
    if (!root)
    {
        refusal = "an expression that is not among the outputs of its actions";
        return {};
    }

    DeviceDAG device_dag(refusal);
    if (!device_dag.build(root))
        return {};

    const DataTypePtr result_type = withoutAliases(root)->result_type;
    const WhichDataType which(result_type);
    if (output == Output::Bitmask && !(which.isNativeUInt() || which.isNativeInt()))
    {
        refusal = "a predicate of type " + result_type->getName();
        return {};
    }
    if (output == Output::Float64 && !which.isFloat64())
    {
        refusal = "an expression of type " + result_type->getName() + " instead of a `Float64`";
        return {};
    }

    SipHash hash;
    const UInt128 dag_hash = device_dag.dag.hash();
    hash.update(dag_hash);
    hash.update(output);
    hash.update(deviceArchitecture());
    const std::string name = "clickhouse_gpu_" + getHexUIntLowercase(hash.get128());

    static std::mutex mutex;
    static std::unordered_map<std::string, std::unique_ptr<Kernel>> kernels;

    const Kernel * kernel = nullptr;
    {
        std::lock_guard lock(mutex);
        if (auto it = kernels.find(name); it != kernels.end())
            kernel = it->second.get();
    }

    if (!kernel)
    {
        auto machine = nvptxTargetMachine();
        llvm::LLVMContext context;
        llvm::Module module(name, context);
        module.setTargetTriple(llvm::Triple(nvptx_triple));
        module.setDataLayout(machine->createDataLayout());

        emitKernel(module, device_dag, result_type, output, name);
        auto ptx = emitPTX(module, *machine, refusal);
        if (!ptx)
            return {};

        std::lock_guard lock(mutex);
        auto & stored = kernels[name];
        if (!stored)
            stored = std::make_unique<Kernel>(Kernel{.name = name, .ptx = std::move(*ptx)});
        kernel = stored.get();
    }

    CompiledGPUExpression compiled;
    compiled.expression.ptx = kernel->ptx.c_str();
    compiled.expression.kernel = kernel->name.c_str();
    compiled.expression.num_columns = static_cast<uint32_t>(device_dag.columns.size());
    for (size_t i = 0; i < device_dag.columns.size(); ++i)
        compiled.expression.column_types[i] = *filterElementTypeOf(*device_dag.columns[i].type);
    compiled.columns = std::move(device_dag.columns);
    return compiled;
}

}

std::optional<CompiledGPUExpression> compileGPUFilter(const ActionsDAG & dag, const String & filter_column_name, String & refusal)
{
    return compile(dag, filter_column_name, Output::Bitmask, refusal);
}

std::optional<CompiledGPUExpression> compileGPUExpression(const ActionsDAG & dag, const String & output_name, String & refusal)
{
    return compile(dag, output_name, Output::Float64, refusal);
}

}

#endif
