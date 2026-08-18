#pragma once

#include <array>
#include <base/constexpr_helpers.h>

#include <Interpreters/HashJoin/HashJoin.h>


/** Used in implementation of Join to process different data structures.
  */

namespace DB
{

/// The map a join shape runs on for `JoinMapsKind::Default` and `JoinMapsKind::All`; `flagged` indicates
/// whether we need to store flags for each row whether it has been used in the join. See JoinUsedFlags.h.
template <JoinKind kind, JoinStrictness join_strictness, bool prefer_use_maps_all>
struct MapGetterImpl;

template <bool prefer_use_maps_all> struct MapGetterImpl<JoinKind::Right, JoinStrictness::RightAny, prefer_use_maps_all> { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <bool prefer_use_maps_all> struct MapGetterImpl<JoinKind::Full, JoinStrictness::RightAny, prefer_use_maps_all>  { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };

template <> struct MapGetterImpl<JoinKind::Inner, JoinStrictness::RightAny, false> { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetterImpl<JoinKind::Inner, JoinStrictness::RightAny, true> { using Map = HashJoin::MapsAll; static constexpr bool flagged = false; };
template <> struct MapGetterImpl<JoinKind::Left, JoinStrictness::RightAny, false> { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetterImpl<JoinKind::Left, JoinStrictness::RightAny, true> { using Map = HashJoin::MapsAll; static constexpr bool flagged = false; };

template <> struct MapGetterImpl<JoinKind::Left, JoinStrictness::Any, false>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetterImpl<JoinKind::Left, JoinStrictness::Any, true>  { using Map = HashJoin::MapsAll; static constexpr bool flagged = false; };
template <> struct MapGetterImpl<JoinKind::Inner, JoinStrictness::Any, true> { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <> struct MapGetterImpl<JoinKind::Inner, JoinStrictness::Any, false> { using Map = HashJoin::MapsOne; static constexpr bool flagged = true; };
template <bool prefer_use_maps_all> struct MapGetterImpl<JoinKind::Right, JoinStrictness::Any, prefer_use_maps_all> { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <bool prefer_use_maps_all> struct MapGetterImpl<JoinKind::Full, JoinStrictness::Any, prefer_use_maps_all>  { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };

template <bool prefer_use_maps_all> struct MapGetterImpl<JoinKind::Left, JoinStrictness::All, prefer_use_maps_all>  { using Map = HashJoin::MapsAll; static constexpr bool flagged = false; };
template <bool prefer_use_maps_all> struct MapGetterImpl<JoinKind::Inner, JoinStrictness::All, prefer_use_maps_all> { using Map = HashJoin::MapsAll; static constexpr bool flagged = false; };
template <bool prefer_use_maps_all> struct MapGetterImpl<JoinKind::Right, JoinStrictness::All, prefer_use_maps_all> { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <bool prefer_use_maps_all> struct MapGetterImpl<JoinKind::Full, JoinStrictness::All, prefer_use_maps_all>  { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };

/// Only SEMI LEFT and SEMI RIGHT are valid. INNER and FULL are here for templates instantiation.
template <> struct MapGetterImpl<JoinKind::Left, JoinStrictness::Semi, false>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetterImpl<JoinKind::Left, JoinStrictness::Semi, true>  { using Map = HashJoin::MapsAll; static constexpr bool flagged = false; };
template <bool prefer_use_maps_all> struct MapGetterImpl<JoinKind::Inner, JoinStrictness::Semi, prefer_use_maps_all> { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <bool prefer_use_maps_all> struct MapGetterImpl<JoinKind::Right, JoinStrictness::Semi, prefer_use_maps_all> { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <bool prefer_use_maps_all> struct MapGetterImpl<JoinKind::Full, JoinStrictness::Semi, prefer_use_maps_all>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };

/// Only ANTI LEFT and ANTI RIGHT are valid. INNER and FULL are here for templates instantiation.
template <> struct MapGetterImpl<JoinKind::Left, JoinStrictness::Anti, false>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <> struct MapGetterImpl<JoinKind::Left, JoinStrictness::Anti, true>  { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <bool prefer_use_maps_all> struct MapGetterImpl<JoinKind::Inner, JoinStrictness::Anti, prefer_use_maps_all> { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };
template <bool prefer_use_maps_all> struct MapGetterImpl<JoinKind::Right, JoinStrictness::Anti, prefer_use_maps_all> { using Map = HashJoin::MapsAll; static constexpr bool flagged = true; };
template <bool prefer_use_maps_all> struct MapGetterImpl<JoinKind::Full, JoinStrictness::Anti, prefer_use_maps_all>  { using Map = HashJoin::MapsOne; static constexpr bool flagged = false; };

template <JoinKind kind, bool prefer_use_maps_all>
struct MapGetterImpl<kind, JoinStrictness::Asof, prefer_use_maps_all> { using Map = HashJoin::MapsAsof; static constexpr bool flagged = false; };

/// `Default` and `All` select between the two mapped flavours; `Set` is only defined for the shapes that
/// can run on a set and falls back to the default map elsewhere, so that dispatch below stays total.
template <JoinKind kind, JoinStrictness join_strictness, JoinMapsKind maps_kind>
struct MapGetter : MapGetterImpl<kind, join_strictness, maps_kind == JoinMapsKind::All>
{
};

/// LEFT ANTI emits a row only when the key is missing, and LEFT SEMI emits the left row alone when
/// nothing of the right side is selected, so neither ever reads a right row.
template <> struct MapGetter<JoinKind::Left, JoinStrictness::Anti, JoinMapsKind::Set> { using Map = HashJoin::MapsSet; static constexpr bool flagged = false; };
template <> struct MapGetter<JoinKind::Left, JoinStrictness::Semi, JoinMapsKind::Set> { using Map = HashJoin::MapsSet; static constexpr bool flagged = false; };

/// Constrain the pairs of routines that differ only in whether a right row can be read from the map.
template <typename Maps>
concept SetJoinMaps = std::is_same_v<std::decay_t<Maps>, HashJoin::MapsSet>;

template <typename Maps>
concept MappedJoinMaps = !SetJoinMaps<Maps>;

/// The maps flavour a given maps type belongs to, for the templates that are handed the type rather than
/// the flavour (see `JoinFeatures`).
template <typename Map>
constexpr JoinMapsKind mapsKindOf()
{
    if constexpr (std::is_same_v<std::decay_t<Map>, HashJoin::MapsAll>)
        return JoinMapsKind::All;
    else if constexpr (std::is_same_v<std::decay_t<Map>, HashJoin::MapsSet>)
        return JoinMapsKind::Set;
    else
        return JoinMapsKind::Default;
}

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

/// Turn the runtime maps flavour into a compile-time one for `func`.
template <typename Func>
inline void dispatchOnMapsKind(JoinMapsKind maps_kind, Func && func)
{
    switch (maps_kind)
    {
        case JoinMapsKind::Default:
            func.template operator()<JoinMapsKind::Default>();
            return;
        case JoinMapsKind::All:
            func.template operator()<JoinMapsKind::All>();
            return;
        case JoinMapsKind::Set:
            func.template operator()<JoinMapsKind::Set>();
            return;
    }
}

/// Init specified join map
inline bool joinDispatchInit(JoinKind kind, JoinStrictness strictness, HashJoin::MapsVariant & maps, JoinMapsKind maps_kind = JoinMapsKind::Default)
{
    return static_for<0, KINDS.size() * STRICTNESSES.size()>([&](auto ij)
    {
        constexpr auto i = ij / STRICTNESSES.size();
        constexpr auto j = ij % STRICTNESSES.size();
        if (kind == KINDS[i] && strictness == STRICTNESSES[j])
        {
            dispatchOnMapsKind(maps_kind, [&]<JoinMapsKind mk>()
            {
                maps = typename MapGetter<KINDS[i], STRICTNESSES[j], mk>::Map();
            });
            return true;
        }
        return false;
    });
}

/// Call function on specified join map
template <typename MapsVariant, typename Func>
inline bool joinDispatch(JoinKind kind, JoinStrictness strictness, MapsVariant & maps, JoinMapsKind maps_kind, Func && func)
{
    return static_for<0, KINDS.size() * STRICTNESSES.size()>([&](auto ij)
    {
        // NOTE: Avoid using nested static loop as GCC and CLANG have bugs in different ways
        // See https://stackoverflow.com/questions/44386415/gcc-and-clang-disagree-about-c17-constexpr-lambda-captures
        constexpr auto i = ij / STRICTNESSES.size();
        constexpr auto j = ij % STRICTNESSES.size();
        if (kind == KINDS[i] && strictness == STRICTNESSES[j])
        {
            dispatchOnMapsKind(maps_kind, [&]<JoinMapsKind mk>()
            {
                func(
                    std::integral_constant<JoinKind, KINDS[i]>(),
                    std::integral_constant<JoinStrictness, STRICTNESSES[j]>(),
                    std::get<typename MapGetter<KINDS[i], STRICTNESSES[j], mk>::Map>(maps));
            });
            return true;
        }
        return false;
    });
}

/// Call function on specified join map
template <typename MapsVariant, typename Func>
inline bool joinDispatch(JoinKind kind, JoinStrictness strictness, std::vector<const MapsVariant *> & mapsv, JoinMapsKind maps_kind, Func && func)
{
    return static_for<0, KINDS.size() * STRICTNESSES.size()>([&](auto ij)
    {
        // NOTE: Avoid using nested static loop as GCC and CLANG have bugs in different ways
        // See https://stackoverflow.com/questions/44386415/gcc-and-clang-disagree-about-c17-constexpr-lambda-captures
        constexpr auto i = ij / STRICTNESSES.size();
        constexpr auto j = ij % STRICTNESSES.size();
        if (kind == KINDS[i] && strictness == STRICTNESSES[j])
        {
            dispatchOnMapsKind(maps_kind, [&]<JoinMapsKind mk>()
            {
                using MapType = typename MapGetter<KINDS[i], STRICTNESSES[j], mk>::Map;
                std::vector<const MapType *> v;
                v.reserve(mapsv.size());
                for (const auto & el : mapsv)
                    v.push_back(&std::get<MapType>(*el));

                func(std::integral_constant<JoinKind, KINDS[i]>(), std::integral_constant<JoinStrictness, STRICTNESSES[j]>(), v);
            });
            return true;
        }
        return false;
    });
}


}
