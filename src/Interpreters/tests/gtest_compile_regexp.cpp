#include <Interpreters/JIT/CompileRegexp.h>

#include "config.h"

#if USE_EMBEDDED_COMPILER

#include <Common/RegexpJIT/RegexpProgram.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>

#include <gtest/gtest.h>

/// A process without a compiled-expression cache has nowhere to keep a compiled matcher, so it would
/// recompile the same pattern on every call. Such a process must stay on RE2.
TEST(CompileRegexp, NoMatcherWithoutCompiledExpressionCache)
{
    /// 221 bytes, ASCII, one group: inside every bail-out of `tryCompileToProgram` and close to
    /// `MAX_PROGRAM_SIZE`, so among the most expensive modules the JIT will build.
    const std::string pattern
        = "Enum16('cV4' = -31542, 'cV2' = -28243, 'cV12' = -24026, 'cV13' = -21772, 'cV11' = -6779, "
          "'cV3' = -6045, 'cV0' = 4182, 'cV1' = 7308, 'cV7' = 9629, 'cV10' = 11923, 'cV6' = 17482, "
          "'cV5' = 17619, 'cV8' = 27353, 'cV9' = 29936)";

    /// Without this the test would also pass if the parser stopped accepting the pattern.
    DB::RegexpJIT::ParseFlags flags;
    flags.case_insensitive = true;
    flags.dot_all = true;
    ASSERT_TRUE(DB::RegexpJIT::tryCompileToProgram(pattern, flags).has_value());

    /// Fail loudly if this binary ever starts initialising the cache: the test would then be vacuous.
    ASSERT_EQ(DB::CompiledExpressionCacheFactory::instance().tryGetCache(), nullptr);

    /// More calls than `min_count_to_compile`, so the compile step is reached.
    for (size_t i = 0; i < 8; ++i)
        EXPECT_FALSE(static_cast<bool>(DB::getRegexpJITMatcher(
            pattern, /* case_insensitive */ true, /* dot_all */ true, /* min_count_to_compile */ 3)))
            << "call " << i;
}

#endif
