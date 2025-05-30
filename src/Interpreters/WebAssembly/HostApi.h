#pragma once

#include <Interpreters/WebAssembly/WasmTypes.h>

#include <span>

namespace DB::WebAssembly
{

class WasmCompartment;

class WasmHostFunction : public IWasmFunctionDeclaration
{
public:
    virtual std::optional<WasmVal> operator()(WasmCompartment * compartment, std::span<WasmVal> args) const = 0;
};

std::unique_ptr<WasmHostFunction> getHostFunction(std::string_view function_name);

}
