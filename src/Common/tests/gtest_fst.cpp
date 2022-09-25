#include <string>
#include <vector>

#include <IO/WriteBufferFromVector.h>
#include <Common/FST.h>
#include <gtest/gtest.h>

TEST(FST, SimpleTest)
{
    std::vector<std::pair<std::string, DB::FST::Output>> indexed_data
    {
        {"mop", 100},
        {"moth", 91},
        {"pop", 72},
        {"star", 83},
        {"stop", 54},
        {"top", 55},
    };

    std::vector<std::pair<std::string, DB::FST::Output>> not_indexed_dta
    {
        {"mo", 100},
        {"moth1", 91},
        {"po", 72},
        {"star2", 83},
        {"sto", 54},
        {"top33", 55},
    };

    std::vector<UInt8> buffer;
    DB::WriteBufferFromVector<std::vector<UInt8>> wbuf(buffer);
    DB::FST::FSTBuilder builder(wbuf);

    for (auto& [term, output] : indexed_data)
    {
        builder.add(term, output);
    }
    builder.build();
    wbuf.finalize();

    DB::FST::FiniteStateTransducer fst(buffer);
    for (auto& [term, output] : indexed_data)
    {
        auto [result, found] = fst.getOutput(term);
        ASSERT_EQ(found, true);
        ASSERT_EQ(result, output);
    }

    for (auto& [term, output] : not_indexed_dta)
    {
        auto [result, found] = fst.getOutput(term);
        ASSERT_EQ(found, false);
    }
}
