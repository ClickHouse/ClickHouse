#include <iostream>

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

    auto compiled_module_info = jit.compileModule([](llvm::Module & module)
    {
        auto & context = module.getContext();
        llvm::IRBuilder<> b (context);

        auto * func_declaration_type = llvm::FunctionType::get(b.getVoidTy(), { }, /*isVarArg=*/false);
        auto * func_declaration = llvm::Function::Create(func_declaration_type, llvm::Function::ExternalLinkage, "test_function", module);

        auto * func_type = llvm::FunctionType::get(b.getVoidTy(), { b.getInt64Ty() }, /*isVarArg=*/false);
        auto * function = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, "test_name", module);
        auto * entry = llvm::BasicBlock::Create(context, "entry", function);

        auto * argument = function->args().begin();
        b.SetInsertPoint(entry);

        b.CreateCall(func_declaration);

        auto * value = b.CreateAdd(argument, argument);
        b.CreateRet(value);
    });

    std::cerr << "Compile module info " << compiled_module_info.module_identifier << " size " << compiled_module_info.size << std::endl;
    for (const auto & compiled_function_name : compiled_module_info.compiled_functions)
    {
        std::cerr << compiled_function_name << std::endl;
    }

    auto * test_name_function = reinterpret_cast<int64_t (*)(int64_t)>(jit.findCompiledFunction("test_name"));
    auto result = test_name_function(5);
    std::cerr << "Result " << result << std::endl;

    return 0;
}
