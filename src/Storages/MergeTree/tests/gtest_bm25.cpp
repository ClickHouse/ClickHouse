#include <gtest/gtest.h>

#include <Storages/MergeTree/BM25Kernel.h>

using namespace DB;

/// The BM25 contribution is monotonic in `tf` (more occurrences => higher score) for a fixed
/// document length, and monotonic decreasing in the doc-length byte (longer document => lower score)
/// for a fixed `tf`. This pins the kernel's ordering used for scoring.
TEST(BM25, ContributionMonotonicity)
{
    BM25Params params;
    BM25LengthNormCache lnc(/*avgdl=*/ 10.0, params);
    BM25Weight weight(/*idf=*/ 2.0, params, &lnc);

    /// Monotonic non-decreasing in tf for a fixed doc length.
    UInt8 dl_byte = SmallFloat::toInt4Byte(10);
    Float32 prev = 0;
    for (UInt32 tf = 1; tf <= 50; ++tf)
    {
        Float32 contribution = weight.contribution(tf, dl_byte);
        EXPECT_GE(contribution, prev);
        prev = contribution;
    }

    /// Monotonic non-increasing in the doc length for a fixed tf: a longer document dilutes the term.
    Float32 short_doc = weight.contribution(/*tf=*/ 3, SmallFloat::toInt4Byte(5));
    Float32 long_doc = weight.contribution(/*tf=*/ 3, SmallFloat::toInt4Byte(100));
    EXPECT_GT(short_doc, long_doc);

    /// A zero-weight term contributes nothing.
    BM25Weight zero_weight(/*idf=*/ 0.0, params, &lnc);
    EXPECT_FLOAT_EQ(zero_weight.contribution(/*tf=*/ 7, dl_byte), 0.0f);
}
