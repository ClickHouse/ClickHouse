#include <Analyzer/UDT/SelectedOutputTypeBindings.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

SelectedOutputTypeBindings physicalBindings(size_t count)
{
    SelectedOutputTypeBindings result;
    result.reserve(count);
    for (size_t index = 0; index < count; ++index)
    {
        SelectedOutputTypeBinding binding;
        binding.output_name = "column_" + std::to_string(index);
        binding.physical_type
            = index % 2 ? DataTypePtr(std::make_shared<DataTypeString>()) : DataTypePtr(std::make_shared<DataTypeUInt64>());
        result.push_back(std::move(binding));
        EXPECT_TRUE(result.back().isPhysicalOnly());
        EXPECT_TRUE(result.back().isValid());
    }
    return result;
}

}

TEST(UDTSelectedOutputCollector, CompleteBindingRequirementIsImmutablePerDDLAnalysis)
{
    const SelectedOutputTypeBindingCollector ordinary;
    const SelectedOutputTypeBindingCollector physicalized_analysis(/*complete_bindings_required_=*/true);
    EXPECT_FALSE(ordinary.requiresCompleteBindings());
    EXPECT_TRUE(physicalized_analysis.requiresCompleteBindings());
}

TEST(UDTSelectedOutputCollector, EmptyCollectorCannotPublishCompleteOrConsumeWithoutAnOwner)
{
    SelectedOutputTypeBindingCollector collector;
    int stranger = 0;
    EXPECT_FALSE(collector.publish(physicalBindings(1)));
    EXPECT_FALSE(collector.publishNoLogicalSourceFastPath());
    EXPECT_FALSE(collector.markPublisherComplete(nullptr));
    EXPECT_FALSE(collector.markPublisherComplete(&stranger));
    collector.abandonPublisher(nullptr);
    collector.abandonPublisher(&stranger);
    EXPECT_FALSE(collector.take().has_value());

    int owner = 0;
    EXPECT_TRUE(collector.tryClaimPublisher(&owner));
    EXPECT_TRUE(collector.publishNoLogicalSourceFastPath());
    EXPECT_FALSE(collector.take().has_value());
    EXPECT_TRUE(collector.markPublisherComplete(&owner));
    EXPECT_TRUE(collector.take().has_value());
    EXPECT_FALSE(collector.take().has_value());
}

TEST(UDTSelectedOutputCollector, PublicationIsInvisibleUntilFinalAnalyzerBarrier)
{
    SelectedOutputTypeBindingCollector collector;
    int owner = 0;
    int stranger = 0;
    EXPECT_FALSE(collector.tryClaimPublisher(nullptr));
    EXPECT_TRUE(collector.tryClaimPublisher(&owner));
    EXPECT_TRUE(collector.tryClaimPublisher(&owner));
    EXPECT_FALSE(collector.tryClaimPublisher(&stranger));

    EXPECT_TRUE(collector.publish(physicalBindings(4)));
    EXPECT_FALSE(collector.publishNoLogicalSourceFastPath());
    EXPECT_FALSE(collector.take().has_value());
    EXPECT_FALSE(collector.markPublisherComplete(&stranger));
    EXPECT_FALSE(collector.take().has_value());
    EXPECT_TRUE(collector.markPublisherComplete(&owner));
    EXPECT_FALSE(collector.markPublisherComplete(&owner));

    auto result = collector.take();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->kind, SelectedOutputTypeBindingCollectionKind::CompleteBindings);
    ASSERT_EQ(result->bindings.size(), 4);
    for (size_t index = 0; index < result->bindings.size(); ++index)
        EXPECT_EQ(result->bindings[index].output_name, "column_" + std::to_string(index));
    EXPECT_FALSE(collector.take().has_value());
    EXPECT_FALSE(collector.tryClaimPublisher(&owner));
}

TEST(UDTSelectedOutputCollector, ExceptionAbandonmentDiscardsEarlyPublicationAndPoisonsGeneration)
{
    SelectedOutputTypeBindingCollector collector(/*complete_bindings_required_=*/true);
    int owner = 0;
    int stranger = 0;
    ASSERT_TRUE(collector.tryClaimPublisher(&owner));
    ASSERT_TRUE(collector.publish(physicalBindings(2)));

    collector.abandonPublisher(&stranger);
    EXPECT_FALSE(collector.take().has_value());
    EXPECT_TRUE(collector.markPublisherComplete(&owner));
    auto unaffected = collector.take();
    ASSERT_TRUE(unaffected);
    EXPECT_EQ(unaffected->kind, SelectedOutputTypeBindingCollectionKind::CompleteBindings);

    SelectedOutputTypeBindingCollector failed_collector;
    ASSERT_TRUE(failed_collector.tryClaimPublisher(&owner));
    ASSERT_TRUE(failed_collector.publishNoLogicalSourceFastPath());
    failed_collector.abandonPublisher(&owner);
    EXPECT_FALSE(failed_collector.markPublisherComplete(&owner));
    EXPECT_FALSE(failed_collector.take().has_value());
    EXPECT_FALSE(failed_collector.tryClaimPublisher(&owner));
    EXPECT_FALSE(failed_collector.publish(physicalBindings(1)));
}

TEST(UDTSelectedOutputCollector, ExactlyOneConcurrentPublisherAndConsumerWin)
{
    constexpr size_t thread_count = 16;
    SelectedOutputTypeBindingCollector collector;
    std::vector<int> owners(thread_count);
    std::vector<UInt8> claimed(thread_count);
    std::vector<std::thread> publishers;
    publishers.reserve(thread_count);
    for (size_t index = 0; index < thread_count; ++index)
    {
        publishers.emplace_back([&, index] { claimed[index] = collector.tryClaimPublisher(&owners[index]); });
    }
    for (auto & thread : publishers)
        thread.join();

    size_t winner = thread_count;
    for (size_t index = 0; index < thread_count; ++index)
    {
        if (claimed[index])
        {
            ASSERT_EQ(winner, thread_count);
            winner = index;
        }
    }
    ASSERT_LT(winner, thread_count);
    ASSERT_TRUE(collector.publish(physicalBindings(32)));
    ASSERT_TRUE(collector.markPublisherComplete(&owners[winner]));

    std::vector<UInt8> consumed(thread_count);
    std::vector<std::thread> consumers;
    consumers.reserve(thread_count);
    for (size_t index = 0; index < thread_count; ++index)
    {
        consumers.emplace_back(
            [&, index]
            {
                auto value = collector.take();
                if (value)
                {
                    EXPECT_EQ(value->kind, SelectedOutputTypeBindingCollectionKind::CompleteBindings);
                    EXPECT_EQ(value->bindings.size(), 32);
                    consumed[index] = 1;
                }
            });
    }
    for (auto & thread : consumers)
        thread.join();

    EXPECT_EQ(std::count(consumed.begin(), consumed.end(), UInt8{1}), 1);
}

TEST(UDTSelectedOutputCollector, FastNegativePublicationHasNoAllocatedPerOutputVector)
{
    SelectedOutputTypeBindingCollector collector;
    int owner = 0;
    ASSERT_TRUE(collector.tryClaimPublisher(&owner));
    ASSERT_TRUE(collector.publishNoLogicalSourceFastPath());
    ASSERT_TRUE(collector.markPublisherComplete(&owner));

    auto result = collector.take();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->kind, SelectedOutputTypeBindingCollectionKind::NoLogicalSourceFastPath);
    EXPECT_TRUE(result->bindings.empty());
}

}
