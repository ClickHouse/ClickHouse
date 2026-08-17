/// `skip_degree` never exceeds one degree past the thinning budget. A deserialized buffer can
/// declare a count that thinning cannot reduce, so both writers of the field enforce that bound:
/// `read()` rejects a larger value off the wire, and `shrinkIfNeed()` refuses to raise the degree
/// past it instead of looping.
///
/// These arms use only the public API, so `skip_degree` is recovered from the first byte of the
/// serialized form.

#include <AggregateFunctions/UniquesHashSet.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace
{

using Set = UniquesHashSet<TrivialHash>;

/// A state in `write()`'s wire format but without `write()`'s validation, so a non-canonical
/// payload can be produced.
std::string craft(UInt8 skip_degree, UInt32 declared_size, const std::vector<UInt32> & words)
{
    std::string out;
    {
        DB::WriteBufferFromString wb(out);
        DB::writeBinaryLittleEndian(skip_degree, wb);
        DB::writeVarUInt(declared_size, wb);
        for (UInt32 w : words)
            DB::writeBinaryLittleEndian(w, wb);
        wb.finalize();
    }
    return out;
}

Set readCrafted(const std::string & buf)
{
    Set s;
    DB::ReadBufferFromString rb(buf);
    s.read(rb);
    return s;
}

UInt8 skipDegreeOf(const Set & s)
{
    std::string out;
    DB::WriteBufferFromString wb(out);
    s.write(wb);
    wb.finalize();
    return static_cast<UInt8>(out.at(0));
}

/// Hash 0 sets `has_zero`, which `merge()` counts without a buffer entry behind it.
Set withZeroOnly()
{
    Set s;
    s.insert(0);
    return s;
}

} /// namespace

/// 65536 copies of one hash with 31 trailing zero bits: `reinsertImpl()` does not dedupe, so the
/// declared count is honest, and no degree can thin any of them out. Merging in a `has_zero`
/// contribution pushes the count past UNIQUES_HASH_MAX_SIZE with nothing droppable behind it.
TEST(UniquesHashSetSkipDegree, MergeRejectsCountThinningCannotReduce)
{
    std::vector<UInt32> words(65536, 0x80000000u);
    Set dst = readCrafted(craft(15, 65536, words));
    Set rhs = withZeroOnly();

    try
    {
        dst.merge(rhs);
        FAIL() << "merge() accepted a count that thinning cannot reduce";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::INCORRECT_DATA);
    }
}

/// The same shape with a droppable payload: thinning can reduce the count, so one degree suffices.
TEST(UniquesHashSetSkipDegree, MergeAcceptsDroppableCount)
{
    std::vector<UInt32> words;
    words.reserve(65536);
    for (UInt32 i = 1; i <= 65536; ++i)
        words.push_back(i);

    Set dst = readCrafted(craft(0, 65536, words));
    Set rhs = withZeroOnly();

    ASSERT_NO_THROW(dst.merge(rhs));
    EXPECT_EQ(skipDegreeOf(dst), 1);
    EXPECT_EQ(dst.size(), 65539u);
}

/// The same crafted state on the other side of the merge: the destination's own buffer is
/// droppable, so the count still converges. The two positions differ, and the arms above are not
/// simply reporting a broken harness.
TEST(UniquesHashSetSkipDegree, MergeAcceptsCraftedStateAsSource)
{
    std::vector<UInt32> words(65536, 0x80000000u);
    Set rhs = readCrafted(craft(15, 65536, words));
    Set dst = withZeroOnly();

    ASSERT_NO_THROW(dst.merge(rhs));
    EXPECT_EQ(dst.size(), 79676u);
}

/// A canonical set built through `insert()` alone, of the highest degree distinct hashes can drive:
/// every value is a distinct multiple of 2 ^ 15, and there are more of them than
/// UNIQUES_HASH_MAX_SIZE. It reaches the bound exactly and must be accepted.
TEST(UniquesHashSetSkipDegree, CanonicalSetReachesTheBound)
{
    Set s;
    ASSERT_NO_THROW({
        for (UInt64 k = 1; k < 131072; ++k)
            s.insert(static_cast<UInt32>(k * 32768));
    });

    EXPECT_EQ(skipDegreeOf(s), 16);
    EXPECT_EQ(s.size(), 48882212795u);
}
