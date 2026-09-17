#include <Storages/System/StorageSystemUDTs.h>

#include <gtest/gtest.h>

#include <array>
#include <string_view>
#include <utility>

namespace DB
{
namespace
{

TEST(StorageSystemUDTs, CompleteDefinitionOnlySchemaIsStable)
{
    using ExpectedColumn = std::pair<std::string_view, std::string_view>;
    static constexpr std::array<ExpectedColumn, 23> expected{
        ExpectedColumn{"database", "String"},
        ExpectedColumn{"name", "String"},
        ExpectedColumn{"uuid", "UUID"},
        ExpectedColumn{"revision", "UInt64"},
        ExpectedColumn{"parameters", "Array(Tuple(name String, kind String))"},
        ExpectedColumn{"decreases_parameter", "Nullable(String)"},
        ExpectedColumn{"semantic_abi_identity", "String"},
        ExpectedColumn{"checker_certificate_hash", "String"},
        ExpectedColumn{"underlying_type", "String"},
        ExpectedColumn{"definition_hash", "String"},
        ExpectedColumn{"storage_fingerprint", "Nullable(String)"},
        ExpectedColumn{"create_query", "String"},
        ExpectedColumn{"has_input", "UInt8"},
        ExpectedColumn{"has_output", "UInt8"},
        ExpectedColumn{"default_expression", "Nullable(String)"},
        ExpectedColumn{"constraints", "Array(Tuple(name String, kind Enum8('INPUT' = 1, 'VALUE' = 2), expression String))"},
        ExpectedColumn{
            "dependencies",
            "Array(Tuple(database_uuid UUID, type_uuid UUID, revision UInt64, definition_hash String, application String, name String))"},
        ExpectedColumn{"owner", "String"},
        ExpectedColumn{"comment", "String"},
        ExpectedColumn{"creation_time", "DateTime64(6)"},
        ExpectedColumn{"storage_backend", "LowCardinality(String)"},
        ExpectedColumn{
            "status", "Enum8('ACTIVE' = 1, 'CONFLICTED' = 2, 'INVALID' = 3, 'INCOMPLETE' = 4, 'QUARANTINED' = 5, 'OVER_QUOTA' = 6)"},
        ExpectedColumn{"last_error", "String"},
    };

    const auto columns = StorageSystemUDTs::getColumnsDescription().getAllPhysical();
    ASSERT_EQ(columns.size(), expected.size());
    size_t index = 0;
    for (const auto & column : columns)
    {
        EXPECT_EQ(column.name, expected[index].first) << "column index " << index;
        EXPECT_EQ(column.type->getName(), expected[index].second) << "column " << expected[index].first;
        ++index;
    }
}

}
}
