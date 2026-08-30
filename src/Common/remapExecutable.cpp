#include <Common/remapExecutable.h>

#if defined(OS_LINUX) && defined(__amd64__) && defined(__SSE2__) && !defined(SANITIZER) && defined(NDEBUG)

#include <cstring>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/syscall.h>

#include <emmintrin.h>

#include <Common/getMappedArea.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <fmt/format.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}


namespace
{

/** Profile instrumentation (WITH_COVERAGE) must not touch the remap machinery.
  * remapToHugeStep2 and the scratch copy of our_syscall execute at a shifted
  * address while the original text is unmapped: an instrumented prologue's
  * RIP-relative counter access (and the __llvm_profile_counter_bias load under
  * runtime counter relocation) then dereferences a shifted, unrelated address -
  * a deterministic segfault with relocation, silent memory corruption without.
  * Step1/Step3 run at original addresses, but are excluded too so the whole
  * mechanism stays uninstrumented.
  */
#if defined(__clang__)
#    define NO_PROFILE_INSTRUMENTATION __attribute__((no_profile_instrument_function))
#else
#    define NO_PROFILE_INSTRUMENTATION
#endif

/// NOLINTNEXTLINE(cert-dcl50-cpp)
NO_PROFILE_INSTRUMENTATION __attribute__((__noinline__)) int64_t our_syscall(...)
{
    __asm__ __volatile__ (R"(
        movq %%rdi,%%rax;
        movq %%rsi,%%rdi;
        movq %%rdx,%%rsi;
        movq %%rcx,%%rdx;
        movq %%r8,%%r10;
        movq %%r9,%%r8;
        movq 8(%%rsp),%%r9;
        syscall;
        ret
    )" : : : "memory");
    return 0;
}


NO_PROFILE_INSTRUMENTATION __attribute__((__noinline__)) void remapToHugeStep3(void * scratch, size_t size, size_t offset)
{
    /// The function should not use the stack, otherwise various optimizations, including "omit-frame-pointer" may break the code.

    /// Unmap the scratch area.
    our_syscall(SYS_munmap, scratch, size);

    /** The return address of this function is pointing to scratch area (because it was called from there).
      * But the scratch area no longer exists. We should correct the return address by subtracting the offset.
      */
    __asm__ __volatile__("subq %0, 8(%%rsp)" : : "r"(offset) : "memory");
}


NO_PROFILE_INSTRUMENTATION __attribute__((__noinline__)) void remapToHugeStep2(void * begin, size_t size, void * scratch, void * syscall_in_scratch, void * step3_in_place)
{
    /** Unmap old memory region with the code of our program.
      * Our instruction pointer is located inside scratch area and this function can execute after old code is unmapped.
      * But it cannot call any other functions because they are not available at usual addresses
      * - that's why we have to use "our_syscall" function and a substitution for memcpy.
      * (Relative addressing may continue to work but we should not assume that).
      *
      * The callee addresses (our_syscall in the scratch copy, and step3 in its original place) are captured by
      * step1 while it still runs from the original mapping and passed in here. We must not recompute them here:
      * in position-independent (PIE) builds a reference to our_syscall/step3 taken while running from scratch
      * resolves into the scratch copy, so adding the offset again would double-count and jump into garbage.
      */

    int64_t offset = reinterpret_cast<intptr_t>(scratch) - reinterpret_cast<intptr_t>(begin);
    int64_t (*syscall_func)(...) = reinterpret_cast<int64_t (*)(...)>(syscall_in_scratch);

    int64_t munmap_res = syscall_func(SYS_munmap, begin, size);
    if (munmap_res != 0)
        return;

    /// Map new anonymous memory region in place of old region with code.

    int64_t mmap_res = syscall_func(SYS_mmap, begin, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0);
    if (-1 == mmap_res)
        syscall_func(SYS_exit, 1);

    /// As the memory region is anonymous, we can do madvise with MADV_HUGEPAGE.

    syscall_func(SYS_madvise, begin, size, MADV_HUGEPAGE);

    /// Copy the code from scratch area to the old memory location.

    {
        __m128i * __restrict dst = reinterpret_cast<__m128i *>(begin);
        const __m128i * __restrict src = reinterpret_cast<const __m128i *>(scratch);
        const __m128i * __restrict src_end = reinterpret_cast<const __m128i *>(reinterpret_cast<const char *>(scratch) + size);
        while (src < src_end)
        {
            _mm_storeu_si128(dst, _mm_loadu_si128(src));

            ++dst;
            ++src;
        }
    }

    /// Make the memory area with the code executable and non-writable.

    syscall_func(SYS_mprotect, begin, size, PROT_READ | PROT_EXEC);

    /** Step 3 function should unmap the scratch area.
      * The currently executed code is located in the scratch area and cannot be removed here.
      * We have to call another function and use its address from the original location (not in scratch area).
      * To do it, we obtain its pointer and call by pointer.
      */

    void(* volatile step3)(void*, size_t, size_t) = reinterpret_cast<void(*)(void*, size_t, size_t)>(step3_in_place);
    step3(scratch, size, offset);
}


NO_PROFILE_INSTRUMENTATION __attribute__((__noinline__)) void remapToHugeStep1(void * begin, size_t size)
{
    /// Allocate scratch area and copy the code there.

    void * scratch = mmap(nullptr, size, PROT_READ | PROT_WRITE | PROT_EXEC, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);  // NOLINT(clang-analyzer-security.MmapWriteExec)
    if (MAP_FAILED == scratch)
        throw ErrnoException(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Cannot mmap {} bytes", size);

    memcpy(scratch, begin, size);

    /// Offset to the scratch area from previous location.

    int64_t offset = reinterpret_cast<intptr_t>(scratch) - reinterpret_cast<intptr_t>(begin);

    /// Capture callee addresses here, while we still run from the original mapping, so they stay valid once
    /// step2 executes from the scratch copy. our_syscall is relocated into scratch (+offset); step3 must be
    /// called at its original place (the code is restored before step3 runs). In PIE builds these references
    /// cannot be recomputed inside step2, hence we pass them as plain pointers.
    void * syscall_in_scratch = reinterpret_cast<void *>(reinterpret_cast<intptr_t>(our_syscall) + offset);
    void * step3_in_place = reinterpret_cast<void *>(remapToHugeStep3);

    /// Jump to the next function inside the scratch area.

    reinterpret_cast<void(*)(void*, size_t, void*, void*, void*)>(reinterpret_cast<intptr_t>(remapToHugeStep2) + offset)(
        begin, size, scratch, syscall_in_scratch, step3_in_place);
}

}


size_t remapExecutable()
{
    auto [begin, size] = getMappedArea(reinterpret_cast<void *>(remapExecutable));
    remapToHugeStep1(begin, size);
    return size;
}

}

#else

namespace DB
{

size_t remapExecutable() { return 0; }

}

#endif
