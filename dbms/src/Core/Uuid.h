#pragma once

#include <Common/UInt128.h>

namespace DB
{

using Uuid = DB::UInt128;

template <> struct IsNumber<Uuid>     { static constexpr bool value = true; };
template <> struct TypeName<Uuid>     { static std::string get() { return "Uuid"; } };

}
