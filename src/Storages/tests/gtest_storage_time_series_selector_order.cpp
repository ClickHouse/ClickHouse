#include <gtest/gtest.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageTimeSeriesSelector.h>

namespace DB
{

namespace
{

StorageMetadataPtr metadataWithSortingKey(
    ASTPtr first,
    ASTPtr second,
    std::vector<bool> reverse_flags = {})
{
    auto metadata = std::make_shared<StorageInMemoryMetadata>();
    auto first_column_name = first->getColumnName();
    auto second_column_name = second->getColumnName();
    metadata->sorting_key.expression_list_ast = make_intrusive<ASTExpressionList>();
    metadata->sorting_key.expression_list_ast->children.push_back(std::move(first));
    metadata->sorting_key.expression_list_ast->children.push_back(std::move(second));
    metadata->sorting_key.column_names = {std::move(first_column_name), std::move(second_column_name)};
    metadata->sorting_key.reverse_flags = std::move(reverse_flags);
    return metadata;
}

}

TEST(StorageTimeSeriesSelector, OrderedSamplesReadAdmission)
{
    auto exact_metadata = metadataWithSortingKey(
        make_intrusive<ASTIdentifier>("id"),
        make_intrusive<ASTIdentifier>("bucket"));
    EXPECT_TRUE(StorageTimeSeriesSelector::hasSamplesIdBucketOrder(exact_metadata));

    auto descending_metadata = metadataWithSortingKey(
        make_intrusive<ASTIdentifier>("id"),
        make_intrusive<ASTIdentifier>("bucket"),
        {true, false});
    EXPECT_FALSE(StorageTimeSeriesSelector::hasSamplesIdBucketOrder(descending_metadata));

    auto function_metadata = metadataWithSortingKey(
        make_intrusive<ASTIdentifier>("id"),
        makeASTFunction("toUInt64", make_intrusive<ASTIdentifier>("bucket")));
    EXPECT_FALSE(StorageTimeSeriesSelector::hasSamplesIdBucketOrder(function_metadata));

    auto qualified_metadata = metadataWithSortingKey(
        make_intrusive<ASTIdentifier>(std::vector<String>{"samples", "id"}),
        make_intrusive<ASTIdentifier>("bucket"));
    EXPECT_FALSE(StorageTimeSeriesSelector::hasSamplesIdBucketOrder(qualified_metadata));

    auto malformed_directions_metadata = metadataWithSortingKey(
        make_intrusive<ASTIdentifier>("id"),
        make_intrusive<ASTIdentifier>("bucket"),
        {false});
    EXPECT_FALSE(StorageTimeSeriesSelector::hasSamplesIdBucketOrder(malformed_directions_metadata));

    auto short_key_metadata = std::make_shared<StorageInMemoryMetadata>();
    short_key_metadata->sorting_key.expression_list_ast = make_intrusive<ASTExpressionList>();
    short_key_metadata->sorting_key.expression_list_ast->children.push_back(make_intrusive<ASTIdentifier>("id"));
    short_key_metadata->sorting_key.column_names = {"id"};
    EXPECT_FALSE(StorageTimeSeriesSelector::hasSamplesIdBucketOrder(short_key_metadata));

    EXPECT_FALSE(StorageTimeSeriesSelector::hasSamplesIdBucketOrder(nullptr));
    EXPECT_FALSE(StorageTimeSeriesSelector::canReadSamplesInOrder(nullptr, exact_metadata));
}

}
