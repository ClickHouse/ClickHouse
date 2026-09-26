/// `assert_cast` guards its type check with DEBUG_OR_SANITIZER_BUILD, so <Common/assert_cast.h>
/// has to bring the definition of that macro with it. This translation unit reaches the header
/// before anything else, gtest_assert_cast_reference.cpp reaches it after a ClickHouse header,
/// and both must get the same `assert_cast`.
#include <Common/assert_cast.h>

#include <gtest/gtest.h>

bool assertCastRejectsCastToAncestorAfterCommonHeaders();

namespace
{

/// Local to this translation unit, so the `assert_cast` instantiation below is local too and the
/// linker cannot merge it with the one of the reference translation unit.
struct Ancestor
{
};
struct Descendant : Ancestor
{
};

bool assertCastRejectsCastToAncestor()
{
    const Descendant descendant;
    try
    {
        [[maybe_unused]] const Ancestor & ancestor = assert_cast<const Ancestor &>(descendant);
        return false;
    }
    catch (const std::exception &)
    {
        return true;
    }
}

}

TEST(AssertCast, TypeCheckDoesNotDependOnIncludeOrder)
{
    EXPECT_EQ(assertCastRejectsCastToAncestor(), assertCastRejectsCastToAncestorAfterCommonHeaders())
        << "assert_cast must check types the same way regardless of what was included before its header";
}
