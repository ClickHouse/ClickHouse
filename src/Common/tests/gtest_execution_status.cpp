#include <Common/Exception.h>

#include <gtest/gtest.h>

using namespace DB;

/// tryDeserializeText must be atomic: on a malformed payload it returns false and leaves the target
/// unchanged. deserializeText reads `code` before it can fail on the rest of the payload, so a
/// non-atomic implementation would leave a partially-overwritten status (e.g. "0garbage" -> code=0).
/// Callers that keep a sentinel and read it back on failure (getExecutionStatus / the Replicated DDL
/// status cross-check) rely on this.

TEST(ExecutionStatus, TryDeserializeTextKeepsTargetOnMalformedInput)
{
    for (const auto & bad : {std::string("0garbage"), std::string("0"), std::string("garbage"), std::string("")})
    {
        ExecutionStatus status(-1, "Cannot obtain error message");
        EXPECT_FALSE(status.tryDeserializeText(bad)) << "payload: " << bad;
        EXPECT_EQ(status.code, -1) << "payload: " << bad;
        EXPECT_EQ(status.message, "Cannot obtain error message") << "payload: " << bad;
    }
}

TEST(ExecutionStatus, TryDeserializeTextRoundTrip)
{
    ExecutionStatus original(42, "boom");

    ExecutionStatus parsed(-1, "Cannot obtain error message");
    EXPECT_TRUE(parsed.tryDeserializeText(original.serializeText()));
    EXPECT_EQ(parsed.code, 42);
    EXPECT_EQ(parsed.message, "boom");
}
