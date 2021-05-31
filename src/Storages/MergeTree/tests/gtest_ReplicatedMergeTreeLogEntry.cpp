#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>

#include <IO/ReadBufferFromString.h>

#include <Core/iostream_debug_helpers.h>

#include <type_traits>
#include <regex>

#include <gtest/gtest.h>

namespace DB
{
std::ostream & operator<<(std::ostream & ostr, const MergeTreeDataPartType & type)
{
    return ostr << type.toString();
}

std::ostream & operator<<(std::ostream & ostr, const UInt128 & v)
{
    return ostr << v.toHexString();
}

template <typename T, typename Tag>
std::ostream & operator<<(std::ostream & ostr, const StrongTypedef<T, Tag> & v)
{
    return ostr << v.toUnderType();
}

std::ostream & operator<<(std::ostream & ostr, const MergeType & v)
{
    return ostr << toString(v);
}

}

namespace std
{

std::ostream & operator<<(std::ostream & ostr, const std::exception_ptr & exception)
{
    try
    {
        if (exception)
        {
            std::rethrow_exception(exception);
        }
        return ostr << "<NULL EXCEPTION>";
    }
    catch (const std::exception& e)
    {
        return ostr << e.what();
    }
}

template <typename T>
inline std::ostream& operator<<(std::ostream & ostr, const std::vector<T> & v)
{
    ostr << "[";
    for (size_t i = 0; i < v.size(); ++i)
    {
        ostr << i;
        if (i != v.size() - 1)
            ostr << ", ";
    }
    return ostr << "] (" << v.size() << ") items";
}

}

namespace
{
using namespace DB;

template <typename T>
void compareAttributes(::testing::AssertionResult & result, const char * name, const T & expected_value, const T & actual_value);

#define CMP_ATTRIBUTE(attribute) compareAttributes(result, #attribute, expected.attribute, actual.attribute)

::testing::AssertionResult compare(
        const ReplicatedMergeTreeLogEntryData::ReplaceRangeEntry & expected,
        const ReplicatedMergeTreeLogEntryData::ReplaceRangeEntry & actual)
{
    auto result = ::testing::AssertionSuccess();

    CMP_ATTRIBUTE(drop_range_part_name);
    CMP_ATTRIBUTE(from_database);
    CMP_ATTRIBUTE(from_table);
    CMP_ATTRIBUTE(src_part_names);
    CMP_ATTRIBUTE(new_part_names);
    CMP_ATTRIBUTE(part_names_checksums);
    CMP_ATTRIBUTE(columns_version);

    return result;
}

template <typename T>
bool compare(const T & expected, const T & actual)
{
    return expected == actual;
}

template <typename T>
::testing::AssertionResult compare(const std::shared_ptr<T> & expected, const std::shared_ptr<T> & actual)
{
    if (!!expected != !!actual)
        return ::testing::AssertionFailure()
                << "expected : " << static_cast<const void*>(expected.get())
                << "\nactual   : " << static_cast<const void*>(actual.get());

    if (expected && actual)
        return compare(*expected, *actual);

    return ::testing::AssertionSuccess();
}

template <typename T>
void compareAttributes(::testing::AssertionResult & result, const char * name, const T & expected_value, const T & actual_value)
{
    const auto cmp_result = compare(expected_value, actual_value);
    if (cmp_result == false)
    {
        if (result)
            result = ::testing::AssertionFailure();

        result << "\nMismatching attribute: \"" << name << "\"";
        if constexpr (std::is_same_v<std::decay_t<decltype(cmp_result)>, ::testing::AssertionResult>)
            result << "\n" << cmp_result.message();
        else
            result << "\n\texpected: " << expected_value
                   << "\n\tactual  : " << actual_value;
    }
};

::testing::AssertionResult compare(const ReplicatedMergeTreeLogEntryData & expected, const ReplicatedMergeTreeLogEntryData & actual)
{
    ::testing::AssertionResult result = ::testing::AssertionSuccess();

    CMP_ATTRIBUTE(znode_name);
    CMP_ATTRIBUTE(type);
    CMP_ATTRIBUTE(source_replica);
    CMP_ATTRIBUTE(new_part_name);
    CMP_ATTRIBUTE(new_part_type);
    CMP_ATTRIBUTE(block_id);
    CMP_ATTRIBUTE(actual_new_part_name);
    CMP_ATTRIBUTE(new_part_uuid);
    CMP_ATTRIBUTE(source_parts);
    CMP_ATTRIBUTE(deduplicate);
    CMP_ATTRIBUTE(deduplicate_by_columns);
    CMP_ATTRIBUTE(merge_type);
    CMP_ATTRIBUTE(column_name);
    CMP_ATTRIBUTE(index_name);
    CMP_ATTRIBUTE(detach);
    CMP_ATTRIBUTE(replace_range_entry);
    CMP_ATTRIBUTE(alter_version);
    CMP_ATTRIBUTE(have_mutation);
    CMP_ATTRIBUTE(columns_str);
    CMP_ATTRIBUTE(metadata_str);
    CMP_ATTRIBUTE(currently_executing);
    CMP_ATTRIBUTE(removed_by_other_entry);
    CMP_ATTRIBUTE(num_tries);
    CMP_ATTRIBUTE(exception);
    CMP_ATTRIBUTE(last_attempt_time);
    CMP_ATTRIBUTE(num_postponed);
    CMP_ATTRIBUTE(postpone_reason);
    CMP_ATTRIBUTE(last_postpone_time);
    CMP_ATTRIBUTE(create_time);
    CMP_ATTRIBUTE(quorum);

    return result;
}
}


class ReplicatedMergeTreeLogEntryDataTest : public ::testing::TestWithParam<std::tuple<ReplicatedMergeTreeLogEntryData, const char* /* serialized RE*/>>
{};

TEST_P(ReplicatedMergeTreeLogEntryDataTest, transcode)
{
    const auto & [expected, match_regex] = GetParam();
    const auto str = expected.toString();

    if (match_regex)
    {
        try
        {
            // egrep since "." matches newline and we can also use "\n" explicitly
            std::regex re(match_regex, std::regex::egrep);
            EXPECT_TRUE(std::regex_match(str, re))
                    << "Failed to match serialized ReplicatedMergeTreeLogEntryData: {\n"
                    << str << "} \nwith regex: \"" << match_regex << "\"\n";
        }
        catch (const std::regex_error &e)
        {
            FAIL() << e.what()
                   << " on regex: " << match_regex
                   << " (" << strlen(match_regex) << " bytes)" << std::endl;
        }
        catch (...)
        {
            throw;
        }
    }

    ReplicatedMergeTreeLogEntryData actual;
    {
        DB::ReadBufferFromString buffer(str);
        EXPECT_NO_THROW(actual.readText(buffer)) << "While reading:\n" << str;
    }

    ASSERT_TRUE(compare(expected, actual)) << "Via text:\n" << str;
}

// Enabling this warning would ruin test brievity without adding anything else in return,
// since most of the fields have default constructors or be will be zero-initialized as by standard,
// so values are predicatable and stable across runs.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"

INSTANTIATE_TEST_SUITE_P(Merge, ReplicatedMergeTreeLogEntryDataTest,
        ::testing::ValuesIn(std::initializer_list<std::tuple<ReplicatedMergeTreeLogEntryData, const char*>>{
    {
        {
            // Basic: minimal set of attributes.
            .type = ReplicatedMergeTreeLogEntryData::MERGE_PARTS,
            .new_part_type = MergeTreeDataPartType::WIDE,
            .create_time = 123, // 0 means 'now' which could cause flaky tests.
        },
        R"re(^format version: 4.+merge.+into.+deduplicate: 0.+$)re"
    },
    {
        {
            .type = ReplicatedMergeTreeLogEntryData::MERGE_PARTS,
            .new_part_type = MergeTreeDataPartType::WIDE,

            // Format version 4
            .deduplicate = true,

            .create_time = 123,
        },
        R"re(^format version: 4.+merge.+into.+deduplicate: 1.+$)re"
    },
    {
        {
            .type = ReplicatedMergeTreeLogEntryData::MERGE_PARTS,
            .new_part_type = MergeTreeDataPartType::WIDE,

            // Format version 5
            .new_part_uuid = UUID(UInt128(123456789, 10111213141516)),

            .create_time = 123,
        },
        R"re(^format version: 5.+merge.+into.+deduplicate: 0.+into_uuid: 00000000-075b-cd15-0000-093233447e0c.+$)re"
    },
    {
        {
            .type = ReplicatedMergeTreeLogEntryData::MERGE_PARTS,
            .new_part_type = MergeTreeDataPartType::WIDE,

            // Format version 6
            .deduplicate = true,
            .deduplicate_by_columns = {"foo", "bar", "qux"},

            .create_time = 123,
        },
        R"re(^format version: 6.+merge.+into.+deduplicate: 1.+deduplicate_by_columns: 'foo','bar','qux'.*$)re"
    },
    {
        {
            .type = ReplicatedMergeTreeLogEntryData::MERGE_PARTS,
            .new_part_type = MergeTreeDataPartType::WIDE,

            // Mixing features
            .new_part_uuid = UUID(UInt128(123456789, 10111213141516)),
            .deduplicate = true,
            .deduplicate_by_columns = {"foo", "bar", "qux"},

            .create_time = 123,
        },
        R"re(^format version: 6.+merge.+into.+deduplicate: 1.+into_uuid: 00000000-075b-cd15-0000-093233447e0c.+deduplicate_by_columns: 'foo','bar','qux'.*$)re"
    },
    {
        // Validate that exotic column names are serialized/deserialized properly
        {
            .type = ReplicatedMergeTreeLogEntryData::MERGE_PARTS,
            .new_part_type = MergeTreeDataPartType::WIDE,

            // Mixing features
            .new_part_uuid = UUID(UInt128(123456789, 10111213141516)),
            .deduplicate = true,
            .deduplicate_by_columns = {"name with space", "\"column\"", "'column'", "колонка", "\u30ab\u30e9\u30e0", "\x01\x03 column \x10\x11\x12"},

            .create_time = 123,
        },
        R"re(^format version: 6.+merge.+deduplicate_by_columns: 'name with space','"column"','\\'column\\'','колонка')re"
                ",'\u30ab\u30e9\u30e0','\x01\x03 column \x10\x11\x12'.*$"
    },
}));

#pragma GCC diagnostic pop

// This is just an example of how to set all fields. Can't be used as is since depending on type,
// only some fields are serialized/deserialized, and even if everything works perfectly,
// some fields in deserialized object would be unset (hence differ from expected).
// INSTANTIATE_TEST_SUITE_P(Full, ReplicatedMergeTreeLogEntryDataTest,
//         ::testing::ValuesIn(std::initializer_list<ReplicatedMergeTreeLogEntryData>{
//     {
//         .znode_name = "znode name",
//         .type = ReplicatedMergeTreeLogEntryData::MERGE_PARTS,
//         .source_replica = "source replica",
//         .new_part_name = "new part name",
//         .new_part_type = MergeTreeDataPartType::WIDE,
//         .block_id = "block id",
//         .actual_new_part_name = "new part name",
//         .new_part_uuid = UUID(UInt128(123456789, 10111213141516)),
//         .source_parts = {"part1", "part2"},
//         .deduplicate = true,
//         .deduplicate_by_columns = {"col1", "col2"},
//         .merge_type = MergeType::REGULAR,
//         .column_name = "column name",
//         .index_name = "index name",
//         .detach = false,
//         .replace_range_entry = std::make_shared<ReplicatedMergeTreeLogEntryData::ReplaceRangeEntry>(
//             ReplicatedMergeTreeLogEntryData::ReplaceRangeEntry
//             {
//                 .drop_range_part_name = "drop range part name",
//                 .from_database = "from database",
//                 .src_part_names = {"src part name1", "src part name2"},
//                 .new_part_names = {"new part name1", "new part name2"},
//                 .columns_version = 123456,
//             }),
//         .alter_version = 56789,
//         .have_mutation = false,
//         .columns_str = "columns str",
//         .metadata_str = "metadata str",
//         // Those attributes are not serialized to string, hence it makes no sense to set.
//         // .currently_executing
//         // .removed_by_other_entry
//         // .num_tries
//         // .exception
//         // .last_attempt_time
//         // .num_postponed
//         // .postpone_reason
//         // .last_postpone_time,
//         .create_time = static_cast<time_t>(123456789),
//         .quorum = 321,
//     },
// }));
