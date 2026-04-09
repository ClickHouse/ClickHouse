/// Test documenting an MSan limitation with struct padding in return values.
///
/// MSan tracks shadow per-field in LLVM IR, not per-byte of the padded struct.
/// When a struct with padding is returned by value, the padding bytes have no
/// shadow entry and appear "uninitialized" in the caller — even if the callee
/// value-initialized the struct.
///
/// On the main thread stack this is harmless (OS zero-inits pages), but on
/// heap-allocated fiber stacks the dirty padding shadow can propagate via stack
/// slot reuse. This is why FiberStack::allocate calls __msan_unpoison.

#include <base/defines.h>
#include <base/MemorySanitizer.h>

#include <Common/FiberStack.h>
#include <Common/Fiber.h>

#include <dragonbox/dragonbox.h>

#include <gtest/gtest.h>


/// Demonstrates the MSan limitation: struct padding is uninitialized in return values.
TEST(DragonboxMSan, ReturnValuePaddingShadowIsLost)
{
#if defined(MEMORY_SANITIZER)
    using FP = jkj::dragonbox::unsigned_fp_t<double>;
    /// { uint64_t significand; int exponent; /* 4 bytes padding */ }
    static_assert(sizeof(FP) == 16);

    /// Local value-init: padding shadow IS clean.
    {
        FP local{};
        local.significand = 1;
        local.exponent = -1;
        EXPECT_EQ(__msan_test_shadow(&local, sizeof(local)), static_cast<intptr_t>(-1))
            << "Local value-init should have clean padding";
    }

    /// Returned from a function: padding shadow is LOST — this is the MSan limitation.
    {
        auto returned = jkj::dragonbox::to_decimal(0.1);
        intptr_t first_uninit = __msan_test_shadow(&returned, sizeof(returned));
        /// Padding starts at offset 12 (after uint64_t + int).
        EXPECT_GE(first_uninit, static_cast<intptr_t>(12))
            << "MSan should lose padding shadow through return values";
    }
#else
    GTEST_SKIP() << "This test requires MSan";
#endif
}
