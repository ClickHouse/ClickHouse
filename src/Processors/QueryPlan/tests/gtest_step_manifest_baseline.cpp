#include <gtest/gtest.h>

#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/StepManifest.h>

using namespace DB;

/// The declarations of every step manifest, in the canonical form of `describeManifest`. A change
/// here is a change of what a step puts on the wire in the framed format. On master a new line is
/// fine when it belongs to a new name or to a new appended format at the current plan version; an
/// existing line must not change, because readers inside the support window depend on it. On a
/// release branch nothing here may change at all. Update the text below only after that review.
static const char * expected_manifests = R"MANIFESTS(name Distinct introduced_in 1 full_digest always logical_digest predicate
format 1 introduced_in 12
  field columns Logical vector<String>
  field limit_hint Logical UInt64
  field distinct_sort_desc Logical SortDescription
  field skip_stream_merging Physical bool
  setting max_rows_in_distinct Logical UInt64
  setting max_bytes_in_distinct Logical UInt64
  setting distinct_overflow_mode Logical enum8
  initializers 00000000
name Limit introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  field limit Logical UInt64
  field offset Logical UInt64
  field always_read_till_end Logical bool
  field with_ties Logical bool
  field description Logical SortDescription
  field is_shard_limit Logical bool
  initializers 000000000000
name Offset introduced_in 1 full_digest always logical_digest always
format 1 introduced_in 12
  field offset Logical UInt64
  initializers 00
name PreDistinct introduced_in 1 full_digest always logical_digest predicate
format 1 introduced_in 12
  field columns Logical vector<String>
  field limit_hint Logical UInt64
  field distinct_sort_desc Logical SortDescription
  field skip_stream_merging Physical bool
  setting max_rows_in_distinct Logical UInt64
  setting max_bytes_in_distinct Logical UInt64
  setting distinct_overflow_mode Logical enum8
  initializers 00000000
)MANIFESTS";

TEST(StepManifestBaseline, DeclarationsAreUnchanged)
{
    if (!QueryPlanStepRegistry::instance().hasStep("Expression"))
        QueryPlanStepRegistry::registerPlanSteps();

    String actual = StepManifestCatalog::instance().dump();
    EXPECT_EQ(actual, expected_manifests)
        << "The step manifests changed. If the change is a new name or a new appended format at the current plan version, "
           "replace the expected text with this:\n" << actual;
}
