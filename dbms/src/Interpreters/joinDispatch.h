#pragma once

#include <array>
#include <common/constexpr_helpers.h>

#include <Interpreters/Join.h>


/** Used in implementation of Join to process different data structures.
  */

namespace DB
{

template <bool fill_right, typename ASTTableJoin::Strictness>
struct MapGetterImpl;

template <>
struct MapGetterImpl<false, ASTTableJoin::Strictness::Any>
{
    using Map = Join::MapsAny;
};

template <>
struct MapGetterImpl<true, ASTTableJoin::Strictness::Any>
{
    using Map = Join::MapsAnyFull;
};

template <>
struct MapGetterImpl<false, ASTTableJoin::Strictness::All>
{
    using Map = Join::MapsAll;
};

template <>
struct MapGetterImpl<true, ASTTableJoin::Strictness::All>
{
    using Map = Join::MapsAllFull;
};

template <bool fill_right>
struct MapGetterImpl<fill_right, ASTTableJoin::Strictness::Asof>
{
    using Map = Join::MapsAsof;
};

template <ASTTableJoin::Kind KIND>
struct KindTrait
{
    // Affects the Adder trait so that when the right part is empty, adding a default value on the left
    static constexpr bool fill_left = static_in_v<KIND, ASTTableJoin::Kind::Left, ASTTableJoin::Kind::Full>;

    // Affects the Map trait so that a `used` flag is attached to map slots in order to
    // generate default values on the right when the left part is empty
    static constexpr bool fill_right = static_in_v<KIND, ASTTableJoin::Kind::Right, ASTTableJoin::Kind::Full>;
};

template <ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness>
using Map = typename MapGetterImpl<KindTrait<kind>::fill_right, strictness>::Map;

static constexpr std::array<ASTTableJoin::Strictness, 3> STRICTNESSES = {
    ASTTableJoin::Strictness::Any,
    ASTTableJoin::Strictness::All,
    ASTTableJoin::Strictness::Asof
};

static constexpr std::array<ASTTableJoin::Kind, 4> KINDS = {
    ASTTableJoin::Kind::Left,
    ASTTableJoin::Kind::Inner,
    ASTTableJoin::Kind::Full,
    ASTTableJoin::Kind::Right
};

/// Init specified join map
inline bool joinDispatchInit(ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness, Join::MapsVariant & maps)
{
    return static_for<0, KINDS.size() * STRICTNESSES.size()>([&](auto ij)
    {
        constexpr auto i = ij / STRICTNESSES.size();
        constexpr auto j = ij % STRICTNESSES.size();
        if (kind == KINDS[i] && strictness == STRICTNESSES[j])
        {
            maps = Map<KINDS[i], STRICTNESSES[j]>();
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
                std::get<Map<KINDS[i], STRICTNESSES[j]>>(maps));
            return true;
        }
        return false;
    });
}

}
