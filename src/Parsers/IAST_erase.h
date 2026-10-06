#pragma once

#include <Parsers/IAST_fwd.h>

#include <algorithm>

namespace std
{

inline typename DB::ASTs::size_type erase(DB::ASTs & asts, const DB::ASTPtr & element) /// NOLINT(cert-dcl58-cpp)
{
    auto old_size = asts.size();
    asts.erase(std::remove(asts.begin(), asts.end(), element), asts.end());
    return old_size - asts.size();
}

template <class Predicate>
inline typename DB::ASTs::size_type erase_if(DB::ASTs & asts, Predicate pred) /// NOLINT(cert-dcl58-cpp)
{
    auto old_size = asts.size();
    asts.erase(std::remove_if(asts.begin(), asts.end(), pred), asts.end());
    return old_size - asts.size();
}

}
