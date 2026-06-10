#include <Functions/UserDefined/UserDefinedWebAssemblyTypeHelpers.h>

namespace DB::WebAssembly
{

std::optional<WasmValKind> wasmKindForDataType(const IDataType * type)
{
    std::optional<WasmValKind> kind;
    tryExecuteForNumericTypes([type, &kind]<typename T>()
    {
        if (typeid_cast<const DataTypeNumber<T> *>(type))
        {
            kind = wasmKindFor<T>();
            return true;
        }
        return false;
    });
    return kind;
}

bool canCoerce(WasmValKind from, WasmValKind to)
{
    if (from == to) return true;
    if (from == WasmValKind::I32 && to == WasmValKind::I64) return true;
    if (from == WasmValKind::F32 && to == WasmValKind::F64) return true;
    const bool from_int = from == WasmValKind::I32 || from == WasmValKind::I64;
    if (from_int && (to == WasmValKind::F32 || to == WasmValKind::F64)) return true;
    return false;
}

}
