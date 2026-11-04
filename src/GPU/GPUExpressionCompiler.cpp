#include <GPU/GPUExpressionCompiler.h>

#if USE_GPU

#if !USE_EMBEDDED_COMPILER
#    error "The GPU engine compiles its expressions with the embedded compiler"
#endif

#include <GPU/GPUTypeMapping.h>

#include <Columns/ColumnConst.h>
#include <Common/CacheBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/HashTable/Hash.h>
#include <Common/logger_useful.h>
#include <Core/Defines.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/Hex.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Native.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Interpreters/castColumn.h>
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

#include <boost/noncopyable.hpp>

#include <memory>
#include <mutex>
#include <unordered_set>

extern "C" void LLVMInitializeNVPTXTargetInfo();
extern "C" void LLVMInitializeNVPTXTarget();
extern "C" void LLVMInitializeNVPTXTargetMC();
extern "C" void LLVMInitializeNVPTXAsmPrinter();

namespace CurrentMetrics
{
    extern const Metric GPUExpressionKernelCacheBytes;
    extern const Metric GPUExpressionKernelCacheCount;
}

namespace DB::ErrorCodes
{
    extern const int CANNOT_COMPILE_CODE;
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

struct GPUKernel : private boost::noncopyable
{
    std::string name;
    cudaLibrary_t library = nullptr;
    cudaKernel_t kernel = nullptr;
    uint32_t block_size = 0;
    /// What the kernel weighs in the cache of the compiler.
    size_t ptx_size = 0;

    ~GPUKernel()
    {
        if (!library)
            return;

        /// Nothing keeps the kernel any longer, but a launch of it that a query queued may still be running.
        cudaError_t status = cudaDeviceSynchronize();
        if (status == cudaSuccess)
            status = cudaLibraryUnload(library);
        if (status != cudaSuccess)
        {
            cudaGetLastError();
            LOG_ERROR(getLogger("GPUExpressionCompiler"), "Cannot unload kernel {}: {}", name, cudaGetErrorString(status));
        }
    }
};

namespace
{

constexpr const char * nvptx_triple = "nvptx64-nvidia-cuda";

/// Of the PTX of the kernels that the cache keeps loaded, besides those that expressions in use keep.
constexpr size_t max_kernel_cache_bytes = 256UL << 20;

struct GPUKernelWeight
{
    size_t operator()(const GPUKernel & kernel) const { return kernel.ptx_size; }
};

using GPUKernelCache = CacheBase<UInt128, GPUKernel, UInt128Hash, GPUKernelWeight>;

/// The kernels compiled so far, a bounded number of them, as `CompiledExpressionCache` bounds the CPU's. One that it
/// evicts is unloaded once the last expression of it is gone.
GPUKernelCache & kernelCache()
{
    /// Never destroyed: the unloading of the kernels at the exit of the process would come after CUDA is gone.
    static auto * cache = new GPUKernelCache(
        CurrentMetrics::GPUExpressionKernelCacheBytes,
        CurrentMetrics::GPUExpressionKernelCacheCount,
        max_kernel_cache_bytes,
        DEFAULT_COMPILED_EXPRESSION_CACHE_MAX_ENTRIES);
    return *cache;
}


void checkLoading(cudaError_t status, const std::string & what)
{
    if (status == cudaSuccess)
        return;
    cudaGetLastError();
    throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "Cannot {}: {}", what, cudaGetErrorString(status));
}

/// The threads of a block of `kernel` that keep the most threads of a multiprocessor busy, a whole number of warps, the
/// fewest of them where several do: as `cudaOccupancyMaxPotentialBlockSize` chooses, which is a template of the CUDA
/// headers for `nvcc`.
uint32_t blockSizeOf(cudaKernel_t kernel, const std::string & name)
{
    const void * function = reinterpret_cast<const void *>(kernel);
    cudaFuncAttributes attributes{};
    checkLoading(cudaFuncGetAttributes(&attributes, function), "tell the attributes of kernel " + name);

    uint32_t best_size = 0;
    int best_threads = 0;
    for (int size = 32; size <= attributes.maxThreadsPerBlock; size += 32)
    {
        int blocks = 0;
        checkLoading(cudaOccupancyMaxActiveBlocksPerMultiprocessor(&blocks, function, size, 0), "tell the occupancy of kernel " + name);
        if (blocks * size > best_threads)
        {
            best_threads = blocks * size;
            best_size = static_cast<uint32_t>(size);
        }
    }
    if (best_size == 0)
        throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "Kernel {} runs in no block of the device", name);
    return best_size;
}

/// `name` of `ptx`, loaded on the device.
std::shared_ptr<GPUKernel> loadKernel(const std::string & name, const std::string & ptx)
{
    auto kernel = std::make_shared<GPUKernel>();
    kernel->name = name;
    kernel->ptx_size = ptx.size();
    checkLoading(
        cudaLibraryLoadData(&kernel->library, ptx.c_str(), nullptr, nullptr, 0, nullptr, nullptr, 0), "load the PTX of kernel " + name);
    checkLoading(cudaLibraryGetKernel(&kernel->kernel, kernel->library, name.c_str()), "find kernel " + name + " in its PTX");
    kernel->block_size = blockSizeOf(kernel->kernel, name);
    return kernel;
}

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

/// The comparisons of `dag` with a string or `Decimal` constant, which do not compile, with the constant converted to the
/// type of the column it is compared with, as the comparison converts it, which compile: `d < '1994-01-01'` of a `Date`
/// is `d < 8766`. The nodes of `dag`, a copy that is not shared, are changed in place.
void convertConstantsOfComparisons(ActionsDAG & dag)
{
    std::optional<ComparisonParams> params;
    ContextPtr query_context;

    for (const auto & const_node : dag.getNodes())
    {
        if (const_node.type != ActionsDAG::ActionType::FUNCTION || const_node.children.size() != 2
            || !isComparison(const_node.function_base->getName()))
            continue;

        auto & node = const_cast<ActionsDAG::Node &>(const_node);
        bool converted = false;
        for (size_t i = 0; i < 2; ++i)
        {
            const ActionsDAG::Node & constant = *withoutAliases(node.children[i]);
            const ActionsDAG::Node & other = *withoutAliases(node.children[1 - i]);
            if (!isConstantToConvert(constant) || isConstantToConvert(other))
                continue;

            if (!params)
            {
                /// With the query's settings of comparisons, as the comparison of the plan is built.
                query_context = CurrentThread::tryGetQueryContext();
                if (!query_context)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "An expression for the GPU compiled outside of a query");
                params.emplace(query_context);
            }

            /// Converted as `FunctionComparison` converts it: a string to the other's type, read as `cast_string_to_date_time_mode`
            /// says, and a `Decimal` compared with a float, with the float, to a `Float64`.
            const WhichDataType which(other.result_type);
            DataTypePtr type;
            Field value;
            if (isString(constant.result_type)
                && (which.isDateOrDate32() || which.isDateTime() || which.isNativeInt() || which.isNativeUInt() || which.isNativeFloat()))
            {
                type = other.result_type;
                value = convertFieldToType((*constant.column)[0], *type, constant.result_type.get(), params->format_settings);
            }
            else if (isDecimal(constant.result_type) && which.isNativeFloat())
            {
                type = std::make_shared<DataTypeFloat64>();
                value = (*castColumn({constant.column, constant.result_type, constant.result_name}, type))[0];
            }

            /// What does not convert is compared otherwise, and the comparison does not compile.
            if (value.isNull())
                continue;

            node.children[i] = &dag.addColumn(type->createColumnConst(1, value), type, constant.result_name);
            converted = true;
        }

        if (!converted)
            continue;

        ColumnsWithTypeAndName arguments;
        for (const auto * child : node.children)
            arguments.push_back({child->column, child->result_type, child->result_name});

        node.function_base = FunctionFactory::instance().get(node.function_base->getName(), query_context)->build(arguments);
        node.function = node.function_base->prepare(arguments);
    }
}

/// The part of an expression that compiles, as the CPU's JIT takes it, and the columns it reads, which are its inputs.
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
        else if (result.columns.size() >= max_expression_columns)
            refusal = "an expression of more columns than the device takes";
        else
        {
            result.columns.emplace_back(node.result_name, node.result_type);
            continue;
        }
        return {};
    }

    /// The code of a `Nullable` value is a pair of the value and a flag, which the device does not read.
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

/// The kernel of `dag`: `void name(ptr column_0, ..., ptr result, i64 rows)`, a thread for each row.
void emitKernel(llvm::Module & module, const DeviceDAG & device_dag, const DataTypePtr & result_type, GPUExpressionResult output, const std::string & name)
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

    if (output == GPUExpressionResult::Float64)
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

std::optional<CompiledGPUExpression> compile(const ActionsDAG & dag, const String & output_name, GPUExpressionResult output, String & refusal)
{
    const ActionsDAG::Node * output_node = dag.tryFindInOutputs(output_name);
    if (!output_node)
    {
        refusal = "an expression that is not among the outputs of its actions";
        return {};
    }

    /// Without aliases, which `getCompilableDAG` takes as inputs, so that `(x + y AS s) > 0` compiles as `x + y > 0` does.
    ActionsDAG expression = ActionsDAG::cloneSubDAG({output_node}, true);
    convertConstantsOfComparisons(expression);
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
    if (output == GPUExpressionResult::Float64 && !which.isFloat64())
    {
        refusal = "an expression of type " + result_type->getName() + " instead of a `Float64`";
        return {};
    }

    SipHash hash;
    const UInt128 dag_hash = device_dag.dag.hash();
    hash.update(dag_hash);
    hash.update(output);
    hash.update(deviceArchitecture());
    const UInt128 key = hash.get128();
    const std::string name = "clickhouse_gpu_" + getHexUIntLowercase(key);

    std::shared_ptr<const GPUKernel> kernel = kernelCache().get(key);
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

        /// Loaded once, by the first query that compiles it, which another one compiling it waits for.
        kernel = kernelCache().getOrSet(key, [&] { return loadKernel(name, *ptx); }).first;
    }

    CompiledGPUExpression compiled;
    compiled.expression.kernel = kernel->kernel;
    compiled.expression.result = output;
    compiled.expression.block_size = kernel->block_size;
    compiled.expression.name = kernel->name.c_str();
    compiled.expression.num_columns = static_cast<uint32_t>(device_dag.columns.size());
    for (size_t i = 0; i < device_dag.columns.size(); ++i)
        compiled.expression.column_types[i] = *filterElementTypeOf(*device_dag.columns[i].type);
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
    return compile(dag, output_name, GPUExpressionResult::Float64, refusal);
}

}

#endif
