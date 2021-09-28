#include <iostream>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_EMBEDDED_COMPILER

#include <llvm/IR/IRBuilder.h>

#include <Interpreters/JIT/CHJIT.h>

void test_function()
{
    std::cerr << "Test function" << std::endl;
}

int main(int argc, char **argv)
{
    (void)(argc);
    (void)(argv);

    auto jit = DB::CHJIT();

    jit.registerExternalSymbol("test_function", reinterpret_cast<void *>(&test_function));

    auto compiled_module = jit.compileModule([](llvm::Module & module)
    {
        auto & context = module.getContext();
        llvm::IRBuilder<> b (context);

        auto * func_declaration_type = llvm::FunctionType::get(b.getVoidTy(), { }, /*isVarArg=*/false);
        auto * func_declaration = llvm::Function::Create(func_declaration_type, llvm::Function::ExternalLinkage, "test_function", module);

        auto * value_type = b.getInt64Ty();
        auto * pointer_type = value_type->getPointerTo();

        auto * func_type = llvm::FunctionType::get(b.getVoidTy(), { pointer_type }, /*isVarArg=*/false);
        auto * function = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, "test_name", module);
        auto * entry = llvm::BasicBlock::Create(context, "entry", function);

        auto * argument = function->args().begin();
        b.SetInsertPoint(entry);

        b.CreateCall(func_declaration);

        auto * load_argument = b.CreateLoad(value_type, argument);
        auto * value = b.CreateAdd(load_argument, load_argument);
        b.CreateRet(value);
    });

    for (const auto & [compiled_function_name, _] : compiled_module.function_name_to_symbol)
    {
        std::cerr << compiled_function_name << std::endl;
    }

    int64_t value = 5;
    auto * symbol = compiled_module.function_name_to_symbol["test_name"];
    auto * test_name_function = reinterpret_cast<int64_t (*)(int64_t *)>(symbol);
    auto result = test_name_function(&value);
    std::cerr << "Result " << result << std::endl;

    return 0;
}

#else

int main(int argc, char **argv)
{
    (void)(argc);
    (void)(argv);
    return 0;
}

#endif
