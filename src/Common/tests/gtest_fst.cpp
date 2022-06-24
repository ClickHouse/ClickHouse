#include <string>
#include <vector>

#include <IO/WriteBufferFromVector.h>
#include <Common/FST.h>
#include <gtest/gtest.h>

using namespace std;

TEST(FST, SimpleTest)
{
	vector<pair<string, uint64_t>> data
	{
		{"mop",		100},
		{"moth",	91},
		{"pop",		72},
		{"star",	83},
		{"stop",	54},
		{"top",		55},
	};

	vector<pair<string, uint64_t>> bad_data
	{
		{"mo",		100},
		{"moth1",	91},
		{"po",		72},
		{"star2",	83},
		{"sto",	    54},
		{"top33",	55},
	};

    std::vector<UInt8> buffer;
    DB::WriteBufferFromVector<std::vector<UInt8>> wbuf(buffer);
    DB::FSTBuilder builder(wbuf);

	for (auto& term_output : data)
	{
		builder.add(term_output.first, term_output.second);
	}
    builder.build();
    wbuf.finalize();

    DB::FST fst(std::move(buffer));
	for (auto& item : data)
	{
		auto output = fst.getOutput(item.first);
		ASSERT_EQ(output.first, true);
		ASSERT_EQ(output.second, item.second);
	}

	for (auto& item : bad_data)
	{
		auto output = fst.getOutput(item.first);
		ASSERT_EQ(output.first, false);
	}
}
