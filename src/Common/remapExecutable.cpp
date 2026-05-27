#include <Common/remapExecutable.h>

#if defined(OS_LINUX) && defined(__amd64__) && defined(__SSE2__) && !defined(SANITIZER) && defined(NDEBUG)

#include <cstring>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/syscall.h>

#include <emmintrin.h>

#include <Common/getMappedArea.h>
#include <Common/Exception.h>
#include <fmt/format.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}


namespace
{

/// NOLINTNEXTLINE(cert-dcl50-cpp)
__attribute__((__noinline__)) int64_t our_syscall(...)
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


__attribute__((__noinline__)) void remapToHugeStep3(void * scratch, size_t size, size_t offset)
{
    /// The function should not use the stack, otherwise various optimizations, including "omit-frame-pointer" may break the code.

    /// Unmap the scratch area.
    our_syscall(SYS_munmap, scratch, size);

    /** The return address of this function is pointing to scratch area (because it was called from there).
      * But the scratch area no longer exists. We should correct the return address by subtracting the offset.
      */
    __asm__ __volatile__("subq %0, 8(%%rsp)" : : "r"(offset) : "memory");
}


__attribute__((__noinline__)) void remapToHugeStep2(void * begin, size_t size, void * scratch)
{
    /** Unmap old memory region with the code of our program.
      * Our instruction pointer is located inside scratch area and this function can execute after old code is unmapped.
      * But it cannot call any other functions because they are not available at usual addresses
      * - that's why we have to use "our_syscall" function and a substitution for memcpy.
      * (Relative addressing may continue to work but we should not assume that).
      */

    int64_t offset = reinterpret_cast<intptr_t>(scratch) - reinterpret_cast<intptr_t>(begin);
    int64_t (*syscall_func)(...) = reinterpret_cast<int64_t (*)(...)>(reinterpret_cast<intptr_t>(our_syscall) + offset);

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

    void(* volatile step3)(void*, size_t, size_t) = remapToHugeStep3;
    step3(scratch, size, offset);
}


__attribute__((__noinline__)) void remapToHugeStep1(void * begin, size_t size)
{
    /// Allocate scratch area and copy the code there.

    void * scratch = mmap(nullptr, size, PROT_READ | PROT_WRITE | PROT_EXEC, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (MAP_FAILED == scratch)
        throw ErrnoException(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Cannot mmap {} bytes", size);

    memcpy(scratch, begin, size);

    /// Offset to the scratch area from previous location.

    int64_t offset = reinterpret_cast<intptr_t>(scratch) - reinterpret_cast<intptr_t>(begin);

    /// Jump to the next function inside the scratch area.

    reinterpret_cast<void(*)(void*, size_t, void*)>(reinterpret_cast<intptr_t>(remapToHugeStep2) + offset)(begin, size, scratch);
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
