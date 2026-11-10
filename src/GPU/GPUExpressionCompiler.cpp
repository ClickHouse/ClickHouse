#include <GPU/GPUExpressionCompiler.h>

#if USE_GPU

#if !USE_EMBEDDED_COMPILER
#    error "The GPU engine compiles its expressions with the embedded compiler"
#endif

#include <GPU/GPUDevice.h>
#include <GPU/GPUKernelCache.h>
#include <GPU/GPUTypeMapping.h>

#include <Columns/ColumnConst.h>
#include <Common/Exception.h>
#include <Common/Hex.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Native.h>
#include <Interpreters/JIT/CHJIT.h>
#include <Interpreters/JIT/CompileDAG.h>

#include <cuda_runtime_api.h>

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/InstIterator.h>
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
#include <unordered_set>


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

std::unique_ptr<llvm::TargetMachine> makeNVPTXTargetMachine()
{
    initializeLLVMTargets();

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

llvm::TargetMachine & nvptxTargetMachine()
{
    thread_local const std::unique_ptr<llvm::TargetMachine> machine = makeNVPTXTargetMachine();
    return *machine;
}

const ActionsDAG::Node * withoutAliases(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    return node;
}

bool deviceHasType(const llvm::Type * type)
{
    if (const auto * vector = llvm::dyn_cast<llvm::VectorType>(type))
        return deviceHasType(vector->getElementType());
    if (const auto * structure = llvm::dyn_cast<llvm::StructType>(type))
        return std::ranges::all_of(structure->elements(), [](const llvm::Type * element) { return deviceHasType(element); });
    if (type->isIntegerTy())
        return type->getIntegerBitWidth() <= 64;
    if (type->isFloatingPointTy())
        return type->isFloatTy() || type->isDoubleTy();
    return true;
}

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
    for (size_t dot = name.size(); dot != llvm::StringRef::npos; dot = name.substr(0, dot).rfind('.'))
    {
        if (intrinsics.contains(std::string_view(name.substr(0, dot))))
            return true;
    }
    return false;
}

struct DeviceDAG
{
    CompileDAG dag;
    std::vector<NameAndTypePair> columns;
};

std::optional<DeviceDAG> deviceDAGOf(const ActionsDAG::Node & root, String & refusal)
{
    ActionsDAG::NodeRawConstPtrs inputs;
    DeviceDAG result{.dag = getCompilableDAG(&root, inputs, {}), .columns = {}};

    for (const auto * input : inputs)
    {
        const ActionsDAG::Node & node = *withoutAliases(input);
        if (node.type == ActionsDAG::ActionType::FUNCTION)
            refusal = "a function that does not compile: `" + node.function_base->getName() + "`";
        else if (node.type != ActionsDAG::ActionType::INPUT)
            refusal = "a constant of type " + node.result_type->getName() + ", which does not compile";
        else if (const auto type = filterElementTypeOf(*node.result_type); !type || *type == GPUElementType::String)
            refusal = "a column of type " + node.result_type->getName() + ", which the device does not read";
        else
        {
            result.columns.emplace_back(node.result_name, node.result_type);
            continue;
        }
        return {};
    }

    for (size_t i = 0; i < result.dag.getNodesCount(); ++i)
    {
        if (result.dag[i].result_type->isNullable())
        {
            refusal = "an expression of a `Nullable` value";
            return {};
        }
    }
    return result;
}

void emitKernel(llvm::Module & module, const DeviceDAG & device_dag, const DataTypePtr & result_type, GPUExpressionResult output, const std::string & name)
{
    llvm::LLVMContext & context = module.getContext();
    llvm::IRBuilder<> builder(context);

    /// `void name(ptr column_0, ..., ptr result, i64 rows)`, as `evaluateGPUExpression` passes the arguments.
    std::vector<llvm::Type *> parameters(device_dag.columns.size() + 1, builder.getPtrTy());
    parameters.push_back(builder.getInt64Ty());
    auto * kernel = llvm::Function::Create(llvm::FunctionType::get(builder.getVoidTy(), parameters, false), llvm::Function::ExternalLinkage, name, module);
    /// An entry of PTX, which the host launches, rather than a function of the device.
    kernel->setCallingConv(llvm::CallingConv::PTX_Kernel);

    auto * entry = llvm::BasicBlock::Create(context, "entry", kernel);
    auto * body = llvm::BasicBlock::Create(context, "body", kernel);
    auto * done = llvm::BasicBlock::Create(context, "done", kernel);

    builder.SetInsertPoint(entry);
    auto read = [&](llvm::Intrinsic::ID id) { return builder.CreateZExt(builder.CreateIntrinsic(id, {}), builder.getInt64Ty()); };
    llvm::Value * thread = read(llvm::Intrinsic::nvvm_read_ptx_sreg_tid_x);
    // row = block_id * block_size + thread_id
    llvm::Value * row = builder.CreateAdd(builder.CreateMul(read(llvm::Intrinsic::nvvm_read_ptx_sreg_ctaid_x), read(llvm::Intrinsic::nvvm_read_ptx_sreg_ntid_x)), thread);
    llvm::Value * rows = kernel->getArg(static_cast<unsigned>(device_dag.columns.size() + 1));
    llvm::Value * result = kernel->getArg(static_cast<unsigned>(device_dag.columns.size()));
    /// The last block has threads past the last row, as the grid is a whole number of blocks.
    llvm::Value * in_range = builder.CreateICmpULT(row, rows);
    builder.CreateCondBr(in_range, body, done);

    /// The values of the row in each column, which the expression is computed of.
    builder.SetInsertPoint(body);
    ValuesWithType values;
    for (size_t i = 0; i < device_dag.columns.size(); ++i)
    {
        const DataTypePtr & type = device_dag.columns[i].type;
        llvm::Type * element = toNativeType(builder, type);
        values.emplace_back(builder.CreateLoad(element, builder.CreateInBoundsGEP(element, kernel->getArg(static_cast<unsigned>(i)), row)), type);
    }
    /// The same code that the CPU's JIT emits for one row.
    llvm::Value * value = device_dag.dag.compile(builder, values).value;

    if (output == GPUExpressionResult::Value)
    {
        llvm::Type * native = toNativeType(builder, result_type);
        builder.CreateStore(value, builder.CreateInBoundsGEP(native, result, row));
        builder.CreateBr(done);
        builder.SetInsertPoint(done);
        builder.CreateRetVoid();
        return;
    }

    llvm::Value * holds = builder.CreateICmpNE(value, llvm::Constant::getNullValue(value->getType()));
    /// The expression may have ended `body` in blocks of its own, as `if` does.
    llvm::BasicBlock * body_end = builder.GetInsertBlock();
    builder.CreateBr(done);

    builder.SetInsertPoint(done);
    /// A thread past the last row passes nothing, so that it votes as the others of its warp do.
    llvm::PHINode * passes = builder.CreatePHI(builder.getInt1Ty(), 2);
    passes->addIncoming(builder.getFalse(), entry);
    passes->addIncoming(holds, body_end);
    /// Bit `i` of the word is whether the row of the warp's thread `i` passes: a word of the mask for 32 rows.
    llvm::Value * word = builder.CreateIntrinsic(llvm::Intrinsic::nvvm_vote_ballot_sync, {}, {builder.getInt32(0xffffffffU), passes});

    auto * write = llvm::BasicBlock::Create(context, "write", kernel);
    auto * exit = llvm::BasicBlock::Create(context, "exit", kernel);
    /// The first thread of a warp writes its word, at the row of the warp divided by 32.
    llvm::Value * first_of_warp = builder.CreateICmpEQ(builder.CreateAnd(thread, builder.getInt64(31)), builder.getInt64(0));
    builder.CreateCondBr(builder.CreateAnd(in_range, first_of_warp), write, exit);

    builder.SetInsertPoint(write);
    builder.CreateStore(word, builder.CreateInBoundsGEP(builder.getInt32Ty(), result, builder.CreateLShr(row, builder.getInt64(5))));
    builder.CreateBr(exit);

    builder.SetInsertPoint(exit);
    builder.CreateRetVoid();
}

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
        for (const llvm::Instruction & instruction : llvm::instructions(function))
        {
            const bool of_device_types = deviceHasType(instruction.getType())
                && std::ranges::all_of(instruction.operands(), [](const llvm::Use & operand) { return deviceHasType(operand->getType()); });
            if (!of_device_types)
            {
                refusal = "an expression of integers wider than 64 bits or floats other than `Float32` and `Float64`, which the device does not compute";
                return {};
            }
        }

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

std::optional<CompiledGPUExpression> compile(const ActionsDAG & dag, const String & output_name, GPUExpressionResult output, String & refusal)
{
    const ActionsDAG::Node * output_node = dag.tryFindInOutputs(output_name);
    if (!output_node)
    {
        refusal = "an expression that is not among the outputs of its actions";
        return {};
    }

    ActionsDAG expression = ActionsDAG::cloneSubDAG({output_node}, true);
    const ActionsDAG::Node * root = withoutAliases(expression.getOutputs().front());

    auto compiled_dag = deviceDAGOf(*root, refusal);
    if (!compiled_dag)
        return {};
    DeviceDAG & device_dag = *compiled_dag;

    const DataTypePtr result_type = root->result_type;
    const WhichDataType which(result_type);
    if (output == GPUExpressionResult::Bitmask && !(which.isNativeUInt() || which.isNativeInt()))
    {
        refusal = "a predicate of type " + result_type->getName();
        return {};
    }
    const auto value_type = filterElementTypeOf(*result_type);
    if (output == GPUExpressionResult::Value && (!value_type || *value_type == GPUElementType::String))
    {
        refusal = "an expression of type " + result_type->getName() + ", which the device does not write";
        return {};
    }

    SipHash hash;
    const UInt128 dag_hash = device_dag.dag.hash();
    hash.update(dag_hash);
    hash.update(output);
    hash.update(deviceArchitecture());
    const UInt128 key = hash.get128();
    const std::string name = "clickhouse_gpu_" + getHexUIntLowercase(key);

    GPUKernelCache & kernels = GPUDevice::instance().kernelCache();
    std::shared_ptr<const GPUKernel> kernel = kernels.get(key);
    if (!kernel)
    {
        llvm::TargetMachine & machine = nvptxTargetMachine();
        llvm::LLVMContext context;
        llvm::Module module(name, context);
        module.setTargetTriple(llvm::Triple(nvptx_triple));
        module.setDataLayout(machine.createDataLayout());

        emitKernel(module, device_dag, result_type, output, name);
        auto ptx = emitPTX(module, machine, refusal);
        if (!ptx)
            return {};

        std::vector<GPUElementType> column_types;
        for (const auto & column : device_dag.columns)
            column_types.push_back(*filterElementTypeOf(*column.type));
        kernel = kernels.getOrSet(key, [&] { return GPUKernel::load(name, *ptx, std::move(column_types)); }).first;
    }

    CompiledGPUExpression compiled;
    compiled.expression.kernel = kernel->kernel;
    compiled.expression.result = output;
    if (output == GPUExpressionResult::Value)
        compiled.expression.value_type = *value_type;
    compiled.result_type = result_type;
    compiled.expression.block_size = kernel->block_size;
    compiled.expression.name = kernel->name.c_str();
    compiled.expression.column_types = kernel->column_types.data();
    compiled.expression.num_columns = static_cast<uint32_t>(kernel->column_types.size());
    compiled.columns = std::move(device_dag.columns);
    compiled.kernel = std::move(kernel);
    return compiled;
}

}

std::optional<CompiledGPUExpression> compileGPUFilter(const ActionsDAG & dag, const String & filter_column_name, String & refusal)
{
    return compile(dag, filter_column_name, GPUExpressionResult::Bitmask, refusal);
}

std::optional<CompiledGPUExpression> compileGPUExpression(const ActionsDAG & dag, const String & output_name, String & refusal)
{
    return compile(dag, output_name, GPUExpressionResult::Value, refusal);
}

}

#endif
