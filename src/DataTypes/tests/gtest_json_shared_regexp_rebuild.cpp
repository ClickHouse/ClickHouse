#include <gtest/gtest.h>

#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeObject.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

/// Re-versioning a state in a typed path rebuilds the `JSON` type through `cloneWithChildren`,
/// and the rebuilt type must keep its `SHARED REGEXP` rules.
GTEST_TEST(DataTypeObjectSharedRegexp, RebuildWithNewChildrenKeepsRules)
{
    tryRegisterAggregateFunctions();

    DataTypePtr json = DataTypeFactory::instance().get(
        "JSON(x AggregateFunction(sumMap, Array(UInt64), Array(UInt64)), SHARED REGEXP '^tag_')");

    DataTypePtr assigned = json;
    setVersionToAggregateFunctions(assigned, /*if_empty=*/true, /*revision=*/std::nullopt);

    /// The typed path got a new version, so the type was rebuilt.
    ASSERT_NE(assigned.get(), json.get());
    const auto & assigned_object = typeid_cast<const DataTypeObject &>(*assigned);
    const auto & state = typeid_cast<const DataTypeAggregateFunction &>(*assigned_object.getTypedPaths().at("x"));
    ASSERT_EQ(state.getVersion(), 0u);

    ASSERT_EQ(assigned_object.getSharedDataPathRegexps(), std::vector<String>{"^tag_"});
    ASSERT_NE(assigned->getName().find("SHARED REGEXP '^tag_'"), String::npos);
}
