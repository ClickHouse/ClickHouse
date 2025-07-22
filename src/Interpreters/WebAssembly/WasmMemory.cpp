
#include <Interpreters/WebAssembly/HostApi.h>
#include <Interpreters/WebAssembly/WasmEngine.h>
#include <Interpreters/WebAssembly/WasmMemory.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
}

namespace DB::WebAssembly
{

WasmMemoryGuard::WasmMemoryGuard(const WasmMemoryManager * wmm_, WasmPtr ptr_) : ptr(ptr_), wmm(wmm_)
{
}

WasmMemoryGuard::WasmMemoryGuard(WasmMemoryGuard && other) noexcept
{
    *this = std::move(other);
}

WasmMemoryGuard & WasmMemoryGuard::operator=(WasmMemoryGuard && other) noexcept
{
    if (this == &other)
        return *this;

    reset(other.ptr);
    wmm = other.wmm;

    other.ptr = 0;

    return *this;
}

void WasmMemoryGuard::reset(WasmPtr ptr_) noexcept
{
    if (ptr != 0)
    {
        try
        {
            wmm->destroyBuffer(ptr);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, fmt::format("Cannot deallocate memory in wasm, ptr: {}", ptr));
        }
    }
    ptr = ptr_;
}


WasmMemoryGuard::~WasmMemoryGuard()
{
    reset();
}


}
