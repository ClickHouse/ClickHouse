#pragma once
#include <Core/Joins.h>
#include <Interpreters/joinDispatch.h>
namespace DB
{
template <JoinKind KIND, JoinStrictness STRICTNESS, typename Map>
struct JoinFeatures
{
    static constexpr bool is_any_join = STRICTNESS == JoinStrictness::Any;
    static constexpr bool is_all_join = STRICTNESS == JoinStrictness::All;
    static constexpr bool is_asof_join = STRICTNESS == JoinStrictness::Asof;
    static constexpr bool is_semi_join = STRICTNESS == JoinStrictness::Semi;
    static constexpr bool is_anti_join = STRICTNESS == JoinStrictness::Anti;
    static constexpr bool is_any_or_semi_join = is_any_join || STRICTNESS == JoinStrictness::RightAny || (is_semi_join && KIND == JoinKind::Left);

    static constexpr bool left = KIND == JoinKind::Left;
    static constexpr bool right = KIND == JoinKind::Right;
    static constexpr bool inner = KIND == JoinKind::Inner;
    static constexpr bool full = KIND == JoinKind::Full;

    /** Whether we may need duplicate rows from the left table.
      * For example, when we have row (key1, attr1) in left table
      * and rows (key1, attr2), (key1, attr3) in right table,
      * then we need to duplicate row (key1, attr1) for each of joined rows from right table, so result will be
      * (key1, attr1, key1, attr2)
      * (key1, attr1, key1, attr3)
      */
    static constexpr bool need_replication = is_all_join || (is_any_join && right) || (is_semi_join && right);

    /// Whether we need to filter rows from the left table that do not have matches in the right table.
    static constexpr bool need_filter = !need_replication && (inner || right || (is_semi_join && left) || (is_anti_join && left));

    /// Whether we need to add default values for columns from the left table.
    static constexpr bool add_missing = (left || full) && !is_semi_join;

    /// Whether we need to store flags for rows from the right table table
    /// that indicates if they have matches in the left table.
    static constexpr bool need_flags = MapGetter<KIND, STRICTNESS, std::is_same_v<std::decay_t<Map>, HashJoin::MapsAll>>::flagged;

    static constexpr bool is_maps_all = std::is_same_v<std::decay_t<Map>, HashJoin::MapsAll>;
};

}
