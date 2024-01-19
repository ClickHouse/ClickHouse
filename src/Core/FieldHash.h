#pragma once

#include <Core/Field.h>
#include <Common/HashTable/Hash.h>

#include <bit>
#include <functional>

namespace
{
static_assert(sizeof(size_t) == 4 || sizeof(size_t) == 8);
using size_t_equivalent = std::conditional_t<sizeof(size_t) == 4, UInt32, UInt64>;

inline size_t hash64(UInt64 x)
{
    return static_cast<size_t>(intHash64(x));
}

inline size_t hash32(UInt32 x)
{
    /// It's ok to just treat it as 64bits
    return static_cast<size_t>(hash64(x));
}

inline size_t hash_size_t(size_t x)
{
    if constexpr (sizeof(size_t) == 4)
        return hash32(std::bit_cast<size_t_equivalent>(x));
    else
        return hash64(std::bit_cast<size_t_equivalent>(x));
}

inline size_t combine_hashes(size_t h1, size_t h2)
{
    return hash_size_t(h1) ^ h2;
}
}


namespace std
{
/// Extends std::hash to support DB::Field so it can be used in std::unordered_map and similar structures
template <>
struct hash<DB::Field>
{
    size_t operator()(const DB::Field & field) const noexcept
    {
        using Which = DB::Field::Types::Which;
        size_t type_hash = ::hash32(field.getType());
        switch (field.getType())
        {
            case Which::Null:
                return 0;
            case Which::Bool:
                [[fallthrough]];
            case Which::UInt64:
                return ::combine_hashes(type_hash, ::hash64(field.get<UInt64>()));
            case Which::IPv6:
                [[fallthrough]];
            case Which::UUID:
                [[fallthrough]];
            case Which::UInt128: {
                UInt128 n = field.get<UInt128>();
                size_t h1 = ::hash64(n.items[0]);
                size_t h2 = ::hash64(n.items[1]);
                return ::combine_hashes(type_hash, ::combine_hashes(h1, h2));
            }
            case Which::UInt256: {
                UInt256 n = field.get<UInt256>();
                size_t h1 = ::hash64(n.items[0]);
                size_t h2 = ::hash64(n.items[1]);
                size_t h3 = ::hash64(n.items[2]);
                size_t h4 = ::hash64(n.items[3]);
                return ::combine_hashes(::combine_hashes(type_hash, ::combine_hashes(h1, h2)), ::combine_hashes(h3, h4));
            }
            case Which::Int64: {
                UInt64 n = std::bit_cast<UInt64>(&field.get<Int64>());
                return ::combine_hashes(type_hash, n);
            }
            case Which::Int128: {
                UInt128 n = std::bit_cast<UInt128>(field.get<Int128>());
                size_t h1 = ::hash64(n.items[0]);
                size_t h2 = ::hash64(n.items[1]);
                return ::combine_hashes(type_hash, ::combine_hashes(h1, h2));
            }
            case Which::Int256: {
                UInt256 n = std::bit_cast<UInt256>(field.get<Int256>());
                size_t h1 = ::hash64(n.items[0]);
                size_t h2 = ::hash64(n.items[1]);
                size_t h3 = ::hash64(n.items[2]);
                size_t h4 = ::hash64(n.items[3]);
                return ::combine_hashes(::combine_hashes(type_hash, ::combine_hashes(h1, h2)), ::combine_hashes(h3, h4));
            }
            case Which::IPv4: {
                UInt32 n = field.get<DB::IPv4>();
                return ::combine_hashes(type_hash, ::hash32(n));
            }
            case Which::Float64:
                return ::combine_hashes(type_hash, std::hash<Float64>{}(field.get<Float64>()));
            case Which::String:
                return ::combine_hashes(type_hash, std::hash<String>{}(field.get<String>()));
            case Which::Array: {
                auto const & array = field.get<DB::Array>();
                size_t res = type_hash;
                for (const auto & e : array)
                    res = ::combine_hashes(res, std::hash<DB::Field>{}(e));
                return res;
            }
            case Which::Tuple: {
                auto const & tuple = field.get<DB::Tuple>();
                size_t res = type_hash;
                for (const auto & e : tuple)
                    res = ::combine_hashes(res, std::hash<DB::Field>{}(e));
                return res;
            }
            case Which::Map: {
                auto const & map = field.get<DB::Map>();
                size_t res = type_hash;
                for (const auto & e : map)
                    res = ::combine_hashes(res, std::hash<DB::Field>{}(e));
                return res;
            }
            case Which::Object: {
                auto const & object = field.get<DB::Object>();
                size_t res = type_hash;
                for (const auto & e : object)
                    res = ::combine_hashes(res, ::combine_hashes(std::hash<String>{}(e.first), std::hash<DB::Field>{}(e.second)));
                return res;
            }
            case Which::Decimal32:
                return ::combine_hashes(type_hash, std::hash<DB::Decimal32>{}(field.get<DB::Decimal32>()));
            case Which::Decimal64:
                return ::combine_hashes(type_hash, std::hash<DB::Decimal64>{}(field.get<DB::Decimal64>()));
            case Which::Decimal128:
                return ::combine_hashes(type_hash, std::hash<DB::Decimal128>{}(field.get<DB::Decimal128>()));
            case Which::Decimal256:
                return ::combine_hashes(type_hash, std::hash<DB::Decimal256>{}(field.get<DB::Decimal256>()));
            case Which::AggregateFunctionState: {
                auto const & agg = field.get<DB::AggregateFunctionStateData>();
                return ::combine_hashes(type_hash, ::combine_hashes(std::hash<String>{}(agg.name), std::hash<String>{}(agg.data)));
            }
            case Which::CustomType:
                return ::combine_hashes(type_hash, std::hash<String>{}(field.get<DB::CustomType>().toString()));
        }
    }
};
}
