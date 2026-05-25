#pragma once

#include <IO/WriteBuffer.h>
#include <Common/StopToken.h>
#include <Common/Exception.h>
#include <Interpreters/WebAssembly/WasmMemory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int WASM_ERROR;
}

/** A WriteBuffer that writes directly into pre-allocated WebAssembly memory.

  * On overflow (when next() is called and the buffer is full), the buffer is
  * grown via WasmMemoryManager::reallocBuffer(). The buffer is grown in-place
  * or moved by the WASM-side realloc; pending data is preserved.
  *
  * finalizeImpl() and sync() are no-ops because the buffer lives in WASM
  * memory and is managed by WasmMemoryGuard.
  */
class WriteBufferForWasmMemory : public WriteBuffer
{
public:
    WriteBufferForWasmMemory(
        const WebAssembly::WasmMemoryManager * wmm_,
        StopToken stop_token_,
        size_t initial_size)
        : WriteBuffer(nullptr, 0)
        , wmm(wmm_)
        , stop_token(stop_token_)
        , current_size(initial_size)
    {
        if (initial_size == 0)
            current_size = 65536;

        allocateInitialBuffer();
    }

    ~WriteBufferForWasmMemory() override = default;

    /// Return the final buffer handle owned by buf_guard.
    WebAssembly::WasmPtr getHandle() const { return buf_guard.getHandle(); }

private:
    void allocateInitialBuffer()
    {
        auto ptr = wmm->createBuffer(static_cast<WebAssembly::WasmSizeT>(current_size));
        buf_guard.reset(ptr);
        auto mem = buf_guard.getMemoryView();
        set(reinterpret_cast<char *>(mem.data()), mem.size(), 0);
    }

    void nextImpl() override
    {
        size_t old_offset = offset();
        size_t new_size = std::max(current_size * 2, size_t(65536));

        auto new_handle = wmm->reallocBuffer(
            buf_guard.getHandle(), static_cast<WebAssembly::WasmSizeT>(new_size));
        if (new_handle == WebAssembly::WasmPtr(0))
            throw Exception(ErrorCodes::WASM_ERROR,
                "Cannot reallocate wasm buffer to size {}", new_size);

        buf_guard.reset(new_handle);
        auto mem = buf_guard.getMemoryView();
        current_size = mem.size();

        BufferBase::set(reinterpret_cast<char *>(mem.data()), mem.size(), old_offset);
    }

    void finalizeImpl() override { }

    void sync() override { }

    const WebAssembly::WasmMemoryManager * wmm = nullptr;
    StopToken stop_token;
    WebAssembly::WasmMemoryGuard buf_guard = nullptr;
    size_t current_size = 0;
};

}
