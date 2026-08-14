#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>

#include <Common/assert_cast.h>
#include <Common/FailPoint.h>

#include <gtest/gtest.h>

#include <base/scope_guard.h>

namespace DB
{
namespace FailPoints
{
extern const char json_shared_regexp_force_combined_compile_failure[];
}

namespace
{

using MatchMode = JSONPathRegexpMatchMode;

DataTypePtr makeJSONType(std::vector<JSONPathRegexpRule> rules, String prefix = {})
{
    return std::make_shared<DataTypeObject>(
        DataTypeObject::SchemaFormat::JSON,
        std::unordered_map<String, DataTypePtr>{},
        std::unordered_set<String>{},
        std::vector<String>{},
        DataTypeObject::DEFAULT_MAX_DYNAMIC_PATHS,
        DataTypeDynamic::DEFAULT_MAX_DYNAMIC_TYPES,
        std::move(rules),
        std::move(prefix));
}

DataTypePtr makeJSONTypeWithTypedPath(std::vector<JSONPathRegexpRule> rules, DataTypePtr nested_type)
{
    return std::make_shared<DataTypeObject>(
        DataTypeObject::SchemaFormat::JSON,
        std::unordered_map<String, DataTypePtr>{{"typed", std::move(nested_type)}},
        std::unordered_set<String>{},
        std::vector<String>{},
        DataTypeObject::DEFAULT_MAX_DYNAMIC_PATHS,
        DataTypeDynamic::DEFAULT_MAX_DYNAMIC_TYPES,
        std::move(rules));
}

std::vector<JSONPathRegexpRule> makeRules(std::string_view prefix, size_t first, size_t count)
{
    std::vector<JSONPathRegexpRule> rules;
    rules.reserve(count);
    for (size_t i = first; i != first + count; ++i)
        rules.push_back({String{prefix} + std::to_string(i), MatchMode::Full});
    return rules;
}

const DataTypeObject & asJSON(const DataTypePtr & type)
{
    return assert_cast<const DataTypeObject &>(*type);
}

void expectProvenanceTop(const DataTypePtr & type)
{
    const auto & json = asJSON(type);
    ASSERT_EQ(json.getSharedDataPathRules().size(), 1);
    EXPECT_EQ(json.getSharedDataPathRules().front(), (JSONPathRegexpRule{"(?s:.*)", MatchMode::Full}));
    EXPECT_TRUE(json.getSharedDataPathPrefix().empty());

    const auto & matcher = json.getSharedDataPathMatcher();
    ASSERT_TRUE(matcher);
    EXPECT_TRUE(matcher->matches("ordinary.path"));
    EXPECT_TRUE(matcher->matches("path\nwith-newline"));
}

TEST(DataTypeObjectSharedRegexp, ProvenanceJoinIsCanonicalAndAlgebraicWithinLimits)
{
    const auto first = makeJSONType({{"first", MatchMode::Partial}, {"common", MatchMode::Full}}, "root.");
    const auto second = makeJSONType({{"second", MatchMode::Full}, {"common", MatchMode::Full}}, "root.");
    const auto third = makeJSONType({{"third", MatchMode::Partial}}, "root.");

    const auto first_second = mergeJSONSharedDataPathRules(first, second);
    EXPECT_EQ(asJSON(first_second).getSharedDataPathRules().size(), 3);
    EXPECT_EQ(asJSON(first_second).getSharedDataPathPrefix(), "root.");
    EXPECT_TRUE(first_second->equals(*mergeJSONSharedDataPathRules(second, first)));
    EXPECT_EQ(mergeJSONSharedDataPathRules(first, first).get(), first.get());
    EXPECT_EQ(mergeJSONSharedDataPathRules(first_second, first).get(), first_second.get());

    const auto left_associative = mergeJSONSharedDataPathRules(first_second, third);
    const auto right_associative = mergeJSONSharedDataPathRules(first, mergeJSONSharedDataPathRules(second, third));
    EXPECT_TRUE(left_associative->equals(*right_associative));
}

TEST(DataTypeObjectSharedRegexp, IdempotentJoinPreservesPointerIdentityThroughTypedPathContainers)
{
    const auto first = makeJSONTypeWithTypedPath(
        {{"root_first", MatchMode::Partial}},
        std::make_shared<DataTypeArray>(makeJSONType({{"nested_first", MatchMode::Partial}})));
    const auto second = makeJSONTypeWithTypedPath(
        {{"root_second", MatchMode::Partial}},
        std::make_shared<DataTypeArray>(makeJSONType({{"nested_second", MatchMode::Partial}})));

    const auto joined = mergeJSONSharedDataPathRules(first, second);
    EXPECT_EQ(mergeJSONSharedDataPathRules(joined, first).get(), joined.get());
}

TEST(DataTypeObjectSharedRegexp, ProvenanceJoinSaturatesAtRuleLimitAfterDeduplication)
{
    const auto first = makeJSONType(makeRules("^path_", 0, JSONPathRegexpMatcher::MAX_RULES / 2));
    const auto exact_limit = makeJSONType(makeRules(
        "^path_", JSONPathRegexpMatcher::MAX_RULES / 2, JSONPathRegexpMatcher::MAX_RULES / 2));

    const auto exact = mergeJSONSharedDataPathRules(first, exact_limit);
    EXPECT_EQ(asJSON(exact).getSharedDataPathRules().size(), JSONPathRegexpMatcher::MAX_RULES);

    auto overlapping_rules = makeRules("^path_", 0, JSONPathRegexpMatcher::MAX_RULES / 2);
    overlapping_rules.push_back({"^one_more$", MatchMode::Full});
    expectProvenanceTop(mergeJSONSharedDataPathRules(exact, makeJSONType(std::move(overlapping_rules))));
}

TEST(DataTypeObjectSharedRegexp, ProvenanceJoinSaturatesAtTotalByteLimit)
{
    constexpr size_t rules_per_side = 17;
    constexpr size_t repeated_characters = 32 * 1024;

    auto make_large_rules = [](std::string_view suffix)
    {
        std::vector<JSONPathRegexpRule> rules;
        rules.reserve(rules_per_side);
        for (size_t i = 0; i != rules_per_side; ++i)
        {
            /// Repeated characters in a character class keep the compiled regexp small while
            /// exercising the persisted-pattern byte bound.
            rules.push_back({
                "[" + String(repeated_characters, 'a') + "]" + String{suffix} + std::to_string(i),
                MatchMode::Full});
        }
        return rules;
    };

    const auto first = makeJSONType(make_large_rules("left"));
    const auto second = makeJSONType(make_large_rules("right"));
    expectProvenanceTop(mergeJSONSharedDataPathRules(first, second));
}

TEST(DataTypeObjectSharedRegexp, ProvenanceJoinSaturatesWhenCombinedMatcherCannotCompile)
{
    const auto first = makeJSONType({{"first", MatchMode::Full}});
    const auto second = makeJSONType({{"second", MatchMode::Full}});

    /// `RE2`'s exact compiled-set size depends on its version and target architecture. Exercise the
    /// internal provenance fallback deterministically instead of relying on patterns that happen to
    /// exceed its private memory accounting on one build.
    FailPointInjection::enableFailPoint(FailPoints::json_shared_regexp_force_combined_compile_failure);
    SCOPE_EXIT({ FailPointInjection::disableFailPoint(FailPoints::json_shared_regexp_force_combined_compile_failure); });
    expectProvenanceTop(mergeJSONSharedDataPathRules(first, second));
}

TEST(DataTypeObjectSharedRegexp, ProvenanceJoinSaturatesForDifferentRootPrefixes)
{
    const auto first = makeJSONType({{"^left[.]path$", MatchMode::Full}}, "left.");
    const auto second = makeJSONType({{"^right[.]path$", MatchMode::Full}}, "right.");

    const auto saturated = mergeJSONSharedDataPathRules(first, second);
    expectProvenanceTop(saturated);
    EXPECT_TRUE(saturated->equals(*mergeJSONSharedDataPathRules(second, first)));
    EXPECT_EQ(mergeJSONSharedDataPathRules(saturated, first).get(), saturated.get());
    EXPECT_TRUE(saturated->equals(*mergeJSONSharedDataPathRules(makeJSONType({}), saturated)));
}

TEST(DataTypeObjectSharedRegexp, HasSaturatedPolicyDetectsTopThroughNestedContainers)
{
    const auto plain = makeJSONType({{"^left", MatchMode::Partial}});
    EXPECT_FALSE(hasSaturatedJSONSharedDataPathPolicy(*plain));
    EXPECT_FALSE(hasSaturatedJSONSharedDataPathPolicy(*makeJSONType({})));

    const auto saturated = makeJSONType({{"(?s:.*)", MatchMode::Full}});
    EXPECT_TRUE(hasSaturatedJSONSharedDataPathPolicy(*saturated));
    /// The same pattern with partial match semantics is a user rule, not the saturated fallback.
    EXPECT_FALSE(hasSaturatedJSONSharedDataPathPolicy(*makeJSONType({{"(?s:.*)", MatchMode::Partial}})));

    EXPECT_TRUE(hasSaturatedJSONSharedDataPathPolicy(*std::make_shared<DataTypeArray>(saturated)));
    EXPECT_TRUE(hasSaturatedJSONSharedDataPathPolicy(*makeJSONTypeWithTypedPath({}, std::make_shared<DataTypeArray>(saturated))));
    EXPECT_FALSE(hasSaturatedJSONSharedDataPathPolicy(*makeJSONTypeWithTypedPath({}, std::make_shared<DataTypeArray>(plain))));

    /// A join that saturates must be detected the same way the merge and mutation warnings detect it.
    EXPECT_TRUE(hasSaturatedJSONSharedDataPathPolicy(*mergeJSONSharedDataPathRules(
        makeJSONType({{"^a", MatchMode::Full}}, "left."), makeJSONType({{"^b", MatchMode::Full}}, "right."))));
}

TEST(DataTypeObjectSharedRegexp, MergeLooksThroughNullableMismatchOnEitherSide)
{
    /// A projection expression can add or remove Nullable relative to its source column
    /// (assumeNotNull(j) strips it, toNullable(j)/if(cond, j, NULL) add it); the JSON structure
    /// underneath is otherwise unrelated to that wrapper, so the policy must still be found and
    /// merged regardless of which side carries the extra Nullable.
    const auto rule_bearing = makeJSONType({{"^tag_", MatchMode::Partial}});

    const auto merged_from_nullable_source = mergeJSONSharedDataPathRules(
        makeJSONType({}), std::make_shared<DataTypeNullable>(rule_bearing));
    EXPECT_EQ(asJSON(merged_from_nullable_source).getSharedDataPathRules().size(), 1);

    const auto merged_from_bare_source = mergeJSONSharedDataPathRules(
        std::make_shared<DataTypeNullable>(makeJSONType({})), rule_bearing);
    const auto * result_nullable = typeid_cast<const DataTypeNullable *>(merged_from_bare_source.get());
    ASSERT_TRUE(result_nullable);
    EXPECT_EQ(asJSON(result_nullable->getNestedType()).getSharedDataPathRules().size(), 1);
}

TEST(DataTypeObjectSharedRegexp, MergeLooksThroughArrayMismatchOnTargetSide)
{
    /// A projection expression like `array(j)` wraps its argument in a single-element array; the
    /// element itself is the same JSON value j always was, so the policy should still transfer
    /// even though the source column was never itself an Array.
    const auto rule_bearing = makeJSONType({{"^tag_", MatchMode::Partial}});

    const auto merged = mergeJSONSharedDataPathRules(
        std::make_shared<DataTypeArray>(makeJSONType({})), rule_bearing);
    const auto * result_array = typeid_cast<const DataTypeArray *>(merged.get());
    ASSERT_TRUE(result_array);
    EXPECT_EQ(asJSON(result_array->getNestedType()).getSharedDataPathRules().size(), 1);
}

TEST(DataTypeObjectSharedRegexp, MergeLooksThroughSingleElementTupleMismatchOnTargetSide)
{
    /// `tuple(j)` has the same one-to-one relationship to its source as `array(j)` does.
    const auto rule_bearing = makeJSONType({{"^tag_", MatchMode::Partial}});

    const auto merged = mergeJSONSharedDataPathRules(
        std::make_shared<DataTypeTuple>(DataTypes{makeJSONType({})}), rule_bearing);
    const auto * result_tuple = typeid_cast<const DataTypeTuple *>(merged.get());
    ASSERT_TRUE(result_tuple);
    ASSERT_EQ(result_tuple->getElements().size(), 1);
    EXPECT_EQ(asJSON(result_tuple->getElements().front()).getSharedDataPathRules().size(), 1);

    /// A multi-element tuple has no single source column this policy unambiguously belongs to, so
    /// the merge must leave it untouched rather than guess.
    const auto multi_element = std::make_shared<DataTypeTuple>(DataTypes{makeJSONType({}), makeJSONType({})});
    EXPECT_EQ(mergeJSONSharedDataPathRules(multi_element, rule_bearing).get(), multi_element.get());
}

TEST(DataTypeObjectSharedRegexp, MergeLooksThroughMapValueMismatchOnTargetSide)
{
    /// `map('k', j)`: the JSON value only ever lives in the value type.
    const auto rule_bearing = makeJSONType({{"^tag_", MatchMode::Partial}});

    const auto merged = mergeJSONSharedDataPathRules(
        std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), makeJSONType({})), rule_bearing);
    const auto * result_map = typeid_cast<const DataTypeMap *>(merged.get());
    ASSERT_TRUE(result_map);
    EXPECT_EQ(asJSON(result_map->getValueType()).getSharedDataPathRules().size(), 1);
}

TEST(DataTypeObjectSharedRegexp, MergeLooksThroughArrayMismatchOnSourceSide)
{
    /// The reverse shape, e.g. `arr[1]` over `arr Array(JSON(...))`: the expression's own output
    /// type is bare, but the resolved source column still carries the Array.
    const auto rule_bearing = makeJSONType({{"^tag_", MatchMode::Partial}});

    const auto merged = mergeJSONSharedDataPathRules(
        makeJSONType({}), std::make_shared<DataTypeArray>(rule_bearing));
    EXPECT_EQ(asJSON(merged).getSharedDataPathRules().size(), 1);
}

TEST(DataTypeObjectSharedRegexp, MergeLooksThroughSingleElementTupleMismatchOnSourceSide)
{
    /// Source-side mirror: `tupleElement(t, 1)` over a single-element `t`.
    const auto rule_bearing = makeJSONType({{"^tag_", MatchMode::Partial}});

    const auto merged = mergeJSONSharedDataPathRules(
        makeJSONType({}), std::make_shared<DataTypeTuple>(DataTypes{rule_bearing}));
    EXPECT_EQ(asJSON(merged).getSharedDataPathRules().size(), 1);

    /// A multi-element tuple source is equally ambiguous in reverse; leave the target untouched.
    const auto bare_target = makeJSONType({});
    const auto multi_element_source = std::make_shared<DataTypeTuple>(DataTypes{rule_bearing, makeJSONType({})});
    EXPECT_EQ(mergeJSONSharedDataPathRules(bare_target, multi_element_source).get(), bare_target.get());
}

TEST(DataTypeObjectSharedRegexp, MergeLooksThroughMapValueMismatchOnSourceSide)
{
    /// Source-side mirror: the JSON value can only have come from the map's value type.
    const auto rule_bearing = makeJSONType({{"^tag_", MatchMode::Partial}});

    const auto merged = mergeJSONSharedDataPathRules(
        makeJSONType({}), std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), rule_bearing));
    EXPECT_EQ(asJSON(merged).getSharedDataPathRules().size(), 1);
}

TEST(DataTypeObjectSharedRegexp, GetTypeOfNestedObjectsPropagatesRulesAndExtendsPrefix)
{
    /// A JSON(SHARED REGEXP '^arr[.]forced$') column dynamically inferring `arr`'s elements as
    /// their own nested JSON objects (see ObjectJSONNode::getDynamicNodeForPath in
    /// JSONExtractTree.cpp) needs each element's own type to carry the same rules, with the
    /// prefix extended by "arr.", so a path like "forced" *within* an element is matched
    /// root-relative as "arr.forced" -- not evaluated bare, and not silently losing the policy.
    const auto root = makeJSONType({{"^arr[.]forced$", MatchMode::Full}});
    const auto & root_object = asJSON(root);

    const auto no_path = root_object.getTypeOfNestedObjects();
    EXPECT_TRUE(asJSON(no_path).getSharedDataPathRules().empty());

    /// Unlike the no-arg overload, passing even an empty prefix still propagates the root's rules
    /// (it only controls how much prefix is *appended*, not whether rules are carried at all) --
    /// so this is deliberately not equal to `no_path`.
    const auto empty_path = root_object.getTypeOfNestedObjects("");
    const auto & empty_path_object = asJSON(empty_path);
    EXPECT_FALSE(empty_path_object.getSharedDataPathRules().empty());
    EXPECT_EQ(empty_path_object.getSharedDataPathPrefix(), "");

    const auto for_arr = root_object.getTypeOfNestedObjects("arr.");
    const auto & for_arr_object = asJSON(for_arr);
    ASSERT_EQ(for_arr_object.getSharedDataPathRules().size(), 1);
    EXPECT_EQ(for_arr_object.getSharedDataPathRules().front(), (JSONPathRegexpRule{"^arr[.]forced$", MatchMode::Full}));
    EXPECT_EQ(for_arr_object.getSharedDataPathPrefix(), "arr.");

    /// The matcher itself always expects a full, root-relative path -- callers reconstruct that by
    /// prepending the prefix (see ColumnObject::shouldForceSharedData), the element's own bare path
    /// is never matched directly. So "forced" alone must NOT match here, only "arr." + "forced".
    const auto & matcher = for_arr_object.getSharedDataPathMatcher();
    ASSERT_TRUE(matcher);
    EXPECT_FALSE(matcher->matches("forced"));
    EXPECT_TRUE(matcher->matches(for_arr_object.getSharedDataPathPrefix() + "forced"));

    /// A nested call two levels deep composes prefixes, matching buildSubObjectTypeAndSerialization's
    /// own `shared_data_path_prefix + prefix` convention for the explicit ^-subcolumn accessor.
    const auto for_arr_inner = for_arr_object.getTypeOfNestedObjects("inner.");
    EXPECT_EQ(asJSON(for_arr_inner).getSharedDataPathPrefix(), "arr.inner.");
}

TEST(DataTypeObjectSharedRegexp, GetTypeOfNestedObjectsProjectsTypedAndSkipPathsUnderPrefix)
{
    /// JSON(`arr.forced` UInt64, SKIP arr.skip, SHARED REGEXP '^arr[.]forced$'): `forced` and `skip`
    /// are declared relative to the root, but once `arr`'s elements are inferred as their own nested
    /// JSON object, those two policies must still apply -- projected onto the element's own root as
    /// plain "forced"/"skip" -- so typed paths and literal SKIP keep taking precedence over
    /// SHARED REGEXP for inferred nested objects the same way they already do for declared ones.
    const auto root = std::make_shared<DataTypeObject>(
        DataTypeObject::SchemaFormat::JSON,
        std::unordered_map<String, DataTypePtr>{{"arr.forced", std::make_shared<DataTypeString>()}, {"other.field", std::make_shared<DataTypeString>()}},
        std::unordered_set<String>{"arr.skip", "other.skip"},
        std::vector<String>{},
        DataTypeObject::DEFAULT_MAX_DYNAMIC_PATHS,
        DataTypeDynamic::DEFAULT_MAX_DYNAMIC_TYPES,
        std::vector<JSONPathRegexpRule>{{"^arr[.]forced$", MatchMode::Full}});
    const auto & root_object = asJSON(root);

    const auto for_arr = root_object.getTypeOfNestedObjects("arr.");
    const auto & for_arr_object = asJSON(for_arr);

    const auto & nested_typed_paths = for_arr_object.getTypedPaths();
    ASSERT_EQ(nested_typed_paths.size(), 1);
    ASSERT_TRUE(nested_typed_paths.contains("forced"));
    EXPECT_TRUE(nested_typed_paths.at("forced")->equals(DataTypeString{}));

    const auto & nested_paths_to_skip = for_arr_object.getPathsToSkip();
    ASSERT_EQ(nested_paths_to_skip.size(), 1);
    EXPECT_TRUE(nested_paths_to_skip.contains("skip"));

    /// A prefix with no typed paths or SKIP entries of its own still carries the root's rules
    /// (`shared_data_path_rules` isn't empty, so this doesn't take the bare-fallback shortcut), but
    /// must not pick up "other.field"/"other.skip" -- those belong to a sibling, not this prefix.
    const auto for_unrelated = root_object.getTypeOfNestedObjects("unrelated.");
    EXPECT_TRUE(asJSON(for_unrelated).getTypedPaths().empty());
    EXPECT_TRUE(asJSON(for_unrelated).getPathsToSkip().empty());
    EXPECT_FALSE(asJSON(for_unrelated).getSharedDataPathRules().empty());

    /// With no rules and nothing under the prefix at all, the cheap bare-fallback shortcut applies.
    const auto no_rules = makeJSONTypeWithTypedPath({}, std::make_shared<DataTypeString>());
    const auto for_prefix_without_rules = asJSON(no_rules).getTypeOfNestedObjects("unrelated.");
    EXPECT_TRUE(asJSON(for_prefix_without_rules).getTypedPaths().empty());
    EXPECT_TRUE(asJSON(for_prefix_without_rules).getSharedDataPathRules().empty());
}

}
}
