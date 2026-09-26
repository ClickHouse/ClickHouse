/// The reference translation unit for gtest_assert_cast.cpp: here <Common/assert_cast.h> is
/// reached after a ClickHouse header, the way nearly every translation unit reaches it.
#include <Common/Exception.h>
#include <Common/assert_cast.h>

bool assertCastRejectsCastToAncestorAfterCommonHeaders();

namespace
{

/// Local to this translation unit, see the comment on the same types in gtest_assert_cast.cpp.
struct Ancestor
{
};
struct Descendant : Ancestor
{
};

}

bool assertCastRejectsCastToAncestorAfterCommonHeaders()
{
    const Descendant descendant;
    try
    {
        [[maybe_unused]] const Ancestor & ancestor = assert_cast<const Ancestor &>(descendant);
        return false;
    }
    catch (const DB::Exception &)
    {
        return true;
    }
}
