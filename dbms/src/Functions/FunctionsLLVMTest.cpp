#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Mangler.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Verifier.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/ExecutionEngine/Orc/CompileUtils.h>
#include <llvm/ExecutionEngine/Orc/IRCompileLayer.h>
#include <llvm/ExecutionEngine/Orc/NullResolver.h>
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Target/TargetMachine.h>

#include <iostream>
#include <fstream>


namespace
{

struct LLVMTargetInitializer {
    LLVMTargetInitializer() {
        llvm::InitializeNativeTarget();
        llvm::InitializeNativeTargetAsmPrinter();
    }
};

LLVMTargetInitializer llvmInit;

}


namespace DB
{

namespace ErrorCodes {
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionSomething : public IFunction
{
    llvm::LLVMContext context;
    std::unique_ptr<llvm::TargetMachine> machine{llvm::EngineBuilder().selectTarget()};
    llvm::orc::RTDyldObjectLinkingLayer objectLayer{[]() { return std::make_shared<llvm::SectionMemoryManager>(); }};
    llvm::orc::IRCompileLayer<decltype(objectLayer), llvm::orc::SimpleCompiler> compileLayer{objectLayer, llvm::orc::SimpleCompiler(*machine)};
    double (*jitted)(double, double);

public:
    static constexpr auto name = "something";

    FunctionSomething() {
        llvm::DataLayout layout = machine->createDataLayout();
        auto module = std::make_shared<llvm::Module>("something", context);
        module->setDataLayout(layout);
        module->setTargetTriple(machine->getTargetTriple().getTriple());

        {
            auto doubleType = llvm::Type::getDoubleTy(context);
            auto funcType = llvm::FunctionType::get(doubleType, {doubleType, doubleType}, /*isVarArg=*/false);
            auto func = llvm::Function::Create(funcType, llvm::Function::ExternalLinkage, name, module.get());
            llvm::Argument * args[] = {nullptr, nullptr};
            size_t i = 0;
            for (auto& arg : func->args())
            {
                args[i++] = &arg;
            }
            llvm::IRBuilder<> builder(context);
            builder.SetInsertPoint(llvm::BasicBlock::Create(context, name, func));
            builder.CreateRet(builder.CreateFAdd(args[0], args[1], "add"));
        }

        std::string mangledName;
        llvm::raw_string_ostream mangledNameStream(mangledName);
        llvm::Mangler::getNameWithPrefix(mangledNameStream, name, layout);
        llvm::cantFail(compileLayer.addModule(module, std::make_shared<llvm::orc::NullResolver>()));
        jitted = reinterpret_cast<decltype(jitted)>(compileLayer.findSymbol(mangledNameStream.str(), false).getAddress().get());
    }

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionSomething>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        auto a = checkAndGetColumn<ColumnVector<Float64>>(block.getByPosition(arguments[0]).column.get());
        if (!a)
            throw Exception("Argument #1 (" + block.getByPosition(arguments[0]).column->getName() + ") of function " + getName() + " has invalid type",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        auto b = checkAndGetColumn<ColumnVector<Float64>>(block.getByPosition(arguments[1]).column.get());
        if (!b)
            throw Exception("Argument #2 (" + block.getByPosition(arguments[1]).column->getName() + ") of function " + getName() + " has invalid type",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto col_res = ColumnVector<Float64>::create();
        auto & vec_a = a->getData();
        auto & vec_b = b->getData();
        auto & vec_res = col_res->getData();
        vec_res.resize(a->size());
        for (size_t i = 0; i < vec_res.size(); ++i)
            vec_res[i] = jitted(vec_a[i], vec_b[i]);
        block.getByPosition(result).column = std::move(col_res);
    }
};


void registerFunctionsLLVMTest(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSomething>();
}

}
