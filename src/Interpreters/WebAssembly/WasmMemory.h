#pragma once

#include <span>

#include <Interpreters/WebAssembly/WasmTypes.h>
#include <Common/Exception.h>


namespace DB::ErrorCodes
{
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int WASM_ERROR;
}

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
    virtual void destroyBuffer(WasmPtr ptr) const = 0;
    virtual std::span<uint8_t> getMemoryView(WasmPtr ptr, WasmSizeT size) const = 0;

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

    WasmPtr getPtr() const { return ptr; }

protected:
    WasmPtr ptr = 0;

    const WasmMemoryManager * wmm = nullptr;
};


template <typename T>
class WasmTypedMemoryHolder : public WasmMemoryGuard
{
public:
    using WasmMemoryGuard::WasmMemoryGuard;

    WasmTypedMemoryHolder(const WasmMemoryManager * wmm_, WasmPtr ptr_) : WasmMemoryGuard(wmm_, ptr_) { }

    explicit WasmTypedMemoryHolder(WasmMemoryGuard && holder) : WasmMemoryGuard(std::move(holder)) { }
    T * ref() const { return reinterpret_cast<T *>(wmm->getMemoryView(ptr, sizeof(T)).data()); }
};

template <typename T>
WasmTypedMemoryHolder<T> allocateInWasmMemory(const WasmMemoryManager * wmm, size_t size)
{
    if (size > std::numeric_limits<WasmSizeT>::max())
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Data is too large for wasm, size: {}", size);

    auto buf = wmm->createBuffer(static_cast<WasmSizeT>(size));
    if (buf == 0)
        throw Exception(ErrorCodes::WASM_ERROR, "Cannot allocate buffer of size {}", size);
    return WasmTypedMemoryHolder<T>(wmm, buf);
}

}
