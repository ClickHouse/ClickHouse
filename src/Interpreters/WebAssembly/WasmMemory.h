#pragma once

#include <span>

#include <Interpreters/WebAssembly/WasmTypes.h>


namespace DB::WebAssembly
{

/// Interface for handling memory within a specific WebAssembly compartment.
/// Implementations should offer methods to allocate and deallocate memory buffers in WebAssembly.
/// They may specify the names of guest functions to invoke for memory operations
/// and assume a particular layout for allocated objects.
class WasmMemoryManager
{
public:
    virtual WasmPtr createBuffer(WasmSizeT size) const = 0;
    virtual void destroyBuffer(WasmPtr handle) const = 0;
    virtual std::span<uint8_t> getMemoryView(WasmPtr handle) const = 0;

    virtual ~WasmMemoryManager() = default;
};

/// Guard for managing memory buffers allocated in WebAssembly.
/// Automatically deallocates the buffer when going out of scope.
class WasmMemoryGuard
{
public:
    WasmMemoryGuard(const WasmMemoryManager * wmm_, WasmPtr ptr_);

    WasmMemoryGuard(std::nullptr_t) { } /// NOLINT
    WasmMemoryGuard(const WasmMemoryGuard &) = delete;
    WasmMemoryGuard & operator=(const WasmMemoryGuard &) = delete;

    WasmMemoryGuard(WasmMemoryGuard && other) noexcept;
    WasmMemoryGuard & operator=(WasmMemoryGuard &&) noexcept;
    ~WasmMemoryGuard();

    void reset(WasmPtr ptr_ = 0) noexcept;

    operator bool() const { return ptr != 0; } /// NOLINT

    std::span<uint8_t> getMemoryView() const { return wmm->getMemoryView(ptr); }
    WasmPtr getHandle() const { return ptr; }

protected:
    WasmPtr ptr = 0;

    const WasmMemoryManager * wmm = nullptr;
};

WasmMemoryGuard allocateInWasmMemory(const WasmMemoryManager * wmm, size_t size);

}
