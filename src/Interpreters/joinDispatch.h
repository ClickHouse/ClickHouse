#pragma once

#include <array>
#include <base/constexpr_helpers.h>

#include <Interpreters/HashJoin.h>


/** Used in implementation of Join to process different data structures.
  */

namespace DB
{

template <JoinKind kind, JoinStrictness join_strictness>
struct MapGetter;

template <> struct MapGetter<JoinKind::Left, JoinStrictness::RightAny>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<JoinKind::Inner, JoinStrictness::RightAny> { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<JoinKind::Right, JoinStrictness::RightAny> { using Map = HashJoin::MapsOne; static constexpr bool flagged = true; };
template <> struct MapGetter<JoinKind::Full, JoinStrictness::RightAny>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = true; };

template <> struct MapGetter<JoinKind::Left, JoinStrictness::Any>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<JoinKind::Inner, JoinStrictness::Any> { using Map = HashJoin::MapsOne; static constexpr bool flagged = true; };
template <> struct MapGetter<JoinKind::Right, JoinStrictness::Any> { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <> struct MapGetter<JoinKind::Full, JoinStrictness::Any>  { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };

template <> struct MapGetter<JoinKind::Left, JoinStrictness::All>  { using Map = HashJoin::MapsAll; static constexpr bool flagged = false; };
template <> struct MapGetter<JoinKind::Inner, JoinStrictness::All> { using Map = HashJoin::MapsAll; static constexpr bool flagged = false; };
template <> struct MapGetter<JoinKind::Right, JoinStrictness::All> { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <> struct MapGetter<JoinKind::Full, JoinStrictness::All>  { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };

/// Only SEMI LEFT and SEMI RIGHT are valid. INNER and FULL are here for templates instantiation.
template <> struct MapGetter<JoinKind::Left, JoinStrictness::Semi>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<JoinKind::Inner, JoinStrictness::Semi> { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<JoinKind::Right, JoinStrictness::Semi> { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <> struct MapGetter<JoinKind::Full, JoinStrictness::Semi>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };

/// Only SEMI LEFT and SEMI RIGHT are valid. INNER and FULL are here for templates instantiation.
template <> struct MapGetter<JoinKind::Left, JoinStrictness::Anti>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<JoinKind::Inner, JoinStrictness::Anti> { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetter<JoinKind::Right, JoinStrictness::Anti> { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <> struct MapGetter<JoinKind::Full, JoinStrictness::Anti>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };

template <JoinKind kind>
struct MapGetter<kind, JoinStrictness::Asof> { using Map = HashJoin::MapsAsof; static constexpr bool flagged = false; };

static constexpr std::array<JoinStrictness, 6> STRICTNESSES = {
    JoinStrictness::RightAny,
    JoinStrictness::Any,
    JoinStrictness::All,
    JoinStrictness::Asof,
    JoinStrictness::Semi,
    JoinStrictness::Anti,
};

static constexpr std::array<JoinKind, 4> KINDS = {
    JoinKind::Left,
    JoinKind::Inner,
    JoinKind::Full,
    JoinKind::Right
};

/// Init specified join map
inline bool joinDispatchInit(JoinKind kind, JoinStrictness strictness, HashJoin::MapsVariant & maps)
{
    return static_for<0, KINDS.size() * STRICTNESSES.size()>([&](auto ij)
    {
        constexpr auto i = ij / STRICTNESSES.size();
        constexpr auto j = ij % STRICTNESSES.size();
        if (kind == KINDS[i] && strictness == STRICTNESSES[j])
        {
            maps = typename MapGetter<KINDS[i], STRICTNESSES[j]>::Map();
            return true;
        }
        return false;
    });
}

/// Call function on specified join map
template <typename MapsVariant, typename Func>
inline bool joinDispatch(JoinKind kind, JoinStrictness strictness, MapsVariant & maps, Func && func)
{
    return static_for<0, KINDS.size() * STRICTNESSES.size()>([&](auto ij)
    {
        // NOTE: Avoid using nested static loop as GCC and CLANG have bugs in different ways
        // See https://stackoverflow.com/questions/44386415/gcc-and-clang-disagree-about-c17-constexpr-lambda-captures
        constexpr auto i = ij / STRICTNESSES.size();
        constexpr auto j = ij % STRICTNESSES.size();
        if (kind == KINDS[i] && strictness == STRICTNESSES[j])
        {
            func(
                std::integral_constant<JoinKind, KINDS[i]>(),
                std::integral_constant<JoinStrictness, STRICTNESSES[j]>(),
                std::get<typename MapGetter<KINDS[i], STRICTNESSES[j]>::Map>(maps));
            return true;
        }
        return false;
    });
}

/// Call function on specified join map
template <typename MapsVariant, typename Func>
inline bool joinDispatch(JoinKind kind, JoinStrictness strictness, std::vector<const MapsVariant *> & mapsv, Func && func)
{
    return static_for<0, KINDS.size() * STRICTNESSES.size()>([&](auto ij)
    {
        // NOTE: Avoid using nested static loop as GCC and CLANG have bugs in different ways
        // See https://stackoverflow.com/questions/44386415/gcc-and-clang-disagree-about-c17-constexpr-lambda-captures
        constexpr auto i = ij / STRICTNESSES.size();
        constexpr auto j = ij % STRICTNESSES.size();
        if (kind == KINDS[i] && strictness == STRICTNESSES[j])
        {
            using MapType = typename MapGetter<KINDS[i], STRICTNESSES[j]>::Map;
            std::vector<const MapType *> v;
            for (const auto & el : mapsv)
            {
                v.push_back(&std::get<MapType>(*el));
            }

            func(
                std::integral_constant<JoinKind, KINDS[i]>(),
                std::integral_constant<JoinStrictness, STRICTNESSES[j]>(),
                v
                /*std::get<typename MapGetter<KINDS[i], STRICTNESSES[j]>::Map>(maps)*/);
            return true;
        }
        return false;
    });
}


}
