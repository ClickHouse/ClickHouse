#pragma once

#include <array>
#include <common/constexpr_helpers.h>

#include <Interpreters/HashJoin.h>


/** Used in implementation of Join to process different data structures.
  */

namespace DB
{

template <ASTTableJoin::Kind kind, typename ASTTableJoin::Strictness>
struct MapGetter;

template <> struct MapGetter<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::RightAny> { using Map = HashJoin::MapsOne; };
template <> struct MapGetter<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::RightAny> { using Map = HashJoin::MapsOne; };
template <> struct MapGetter<ASTTableJoin::Kind::Right, ASTTableJoin::Strictness::RightAny> { using Map = HashJoin::MapsOneFlagged; };
template <> struct MapGetter<ASTTableJoin::Kind::Full, ASTTableJoin::Strictness::RightAny> { using Map = HashJoin::MapsOneFlagged; };

template <> struct MapGetter<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Any> { using Map = HashJoin::MapsOne; };
template <> struct MapGetter<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::Any> { using Map = HashJoin::MapsOneFlagged; };
template <> struct MapGetter<ASTTableJoin::Kind::Right, ASTTableJoin::Strictness::Any> { using Map = HashJoin::MapsAllFlagged; };
template <> struct MapGetter<ASTTableJoin::Kind::Full, ASTTableJoin::Strictness::Any> { using Map = HashJoin::MapsAllFlagged; };

template <> struct MapGetter<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::All> { using Map = HashJoin::MapsAll; };
template <> struct MapGetter<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::All> { using Map = HashJoin::MapsAll; };
template <> struct MapGetter<ASTTableJoin::Kind::Right, ASTTableJoin::Strictness::All> { using Map = HashJoin::MapsAllFlagged; };
template <> struct MapGetter<ASTTableJoin::Kind::Full, ASTTableJoin::Strictness::All> { using Map = HashJoin::MapsAllFlagged; };

/// Only SEMI LEFT and SEMI RIGHT are valid. INNER and FULL are here for templates instantiation.
template <> struct MapGetter<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Semi> { using Map = HashJoin::MapsOne; };
template <> struct MapGetter<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::Semi> { using Map = HashJoin::MapsOne; };
template <> struct MapGetter<ASTTableJoin::Kind::Right, ASTTableJoin::Strictness::Semi> { using Map = HashJoin::MapsAllFlagged; };
template <> struct MapGetter<ASTTableJoin::Kind::Full, ASTTableJoin::Strictness::Semi> { using Map = HashJoin::MapsOne; };

/// Only SEMI LEFT and SEMI RIGHT are valid. INNER and FULL are here for templates instantiation.
template <> struct MapGetter<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Anti> { using Map = HashJoin::MapsOne; };
template <> struct MapGetter<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::Anti> { using Map = HashJoin::MapsOne; };
template <> struct MapGetter<ASTTableJoin::Kind::Right, ASTTableJoin::Strictness::Anti> { using Map = HashJoin::MapsAllFlagged; };
template <> struct MapGetter<ASTTableJoin::Kind::Full, ASTTableJoin::Strictness::Anti> { using Map = HashJoin::MapsOne; };

template <ASTTableJoin::Kind kind>
struct MapGetter<kind, ASTTableJoin::Strictness::Asof>
{
    using Map = HashJoin::MapsAsof;
};


static constexpr std::array<ASTTableJoin::Strictness, 6> STRICTNESSES = {
    ASTTableJoin::Strictness::RightAny,
    ASTTableJoin::Strictness::Any,
    ASTTableJoin::Strictness::All,
    ASTTableJoin::Strictness::Asof,
    ASTTableJoin::Strictness::Semi,
    ASTTableJoin::Strictness::Anti,
};

static constexpr std::array<ASTTableJoin::Kind, 4> KINDS = {
    ASTTableJoin::Kind::Left,
    ASTTableJoin::Kind::Inner,
    ASTTableJoin::Kind::Full,
    ASTTableJoin::Kind::Right
};

/// Init specified join map
inline bool joinDispatchInit(ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness, HashJoin::MapsVariant & maps)
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
inline bool joinDispatch(ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness, MapsVariant & maps, Func && func)
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
                std::integral_constant<ASTTableJoin::Kind, KINDS[i]>(),
                std::integral_constant<ASTTableJoin::Strictness, STRICTNESSES[j]>(),
                std::get<typename MapGetter<KINDS[i], STRICTNESSES[j]>::Map>(maps));
            return true;
        }
        return false;
    });
}

}
