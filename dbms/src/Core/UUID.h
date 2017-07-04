#pragma once

#include <Common/UInt128.h>

namespace DB
{

using UUID = DB::UInt128;

template <> struct IsNumber<UUID>     { static constexpr bool value = true; };
template <> struct TypeName<UUID>     { static std::string get() { return "UUID"; } };

}
