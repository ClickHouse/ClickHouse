#pragma once

#include <Functions/MatchImpl.h>
#include <Functions/FunctionsStringSearch.h>

namespace DB
{

struct NameSimilarTo
{
    static constexpr auto name = "similarTo";
};

using SimilarToImpl = MatchImpl<NameSimilarTo, MatchTraits::Syntax::SimilarTo, MatchTraits::Case::Sensitive, MatchTraits::Result::DontNegate>;
using FunctionSimilarTo = FunctionsStringSearch<SimilarToImpl>;

}
