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

    static constexpr bool need_replication = is_all_join || (is_any_join && right) || (is_semi_join && right);
    static constexpr bool need_filter = !need_replication && (inner || right || (is_semi_join && left) || (is_anti_join && left));
    static constexpr bool add_missing = (left || full) && !is_semi_join;

    static constexpr bool need_flags = MapGetter<KIND, STRICTNESS, std::is_same_v<std::decay_t<Map>, HashJoin::MapsAll>>::flagged;
    static constexpr bool is_maps_all = std::is_same_v<std::decay_t<Map>, HashJoin::MapsAll>;
};

}
