#pragma once

#include <Interpreters/WebAssembly/WasmTypes.h>

#include <span>

namespace DB::WebAssembly
{

class WasmCompartment;

class WasmHostFunction
{
public:
    using HostFunctionRawPtr = std::optional<WasmVal>(*)(void *, std::string_view, WasmCompartment *, std::span<WasmVal>);

    WasmHostFunction(WasmFunctionDeclaration func_decl_, void * ctx_, HostFunctionRawPtr invoker_)
        : func_decl(std::move(func_decl_)), ctx(ctx_), invoker(invoker_)
    {
    }

    std::optional<WasmVal> operator()(WasmCompartment * compartment, std::span<WasmVal> args) const
    {
        return invoker(ctx, func_decl.getName(), compartment, args);
    }

    const WasmFunctionDeclaration & getFunctionDeclaration() const { return func_decl; }

private:
    WasmFunctionDeclaration func_decl;

    void * ctx;
    HostFunctionRawPtr invoker;
};

WasmHostFunction getHostFunction(std::string_view function_name);

}
