#pragma once

#include <Core/DecimalField.h>
#include <Core/Field.h>
#include <Core/Types.h>

namespace DB
{
template <typename F, typename FieldRef>
inline auto Field::dispatch(F && f, FieldRef && field)
{
    switch (field.which)
    {
        case Types::Null:    return f(field.template get<Null>());
        case Types::UInt64:  return f(field.template get<UInt64>());
        case Types::UInt128: return f(field.template get<UInt128>());
        case Types::UInt256: return f(field.template get<UInt256>());
        case Types::Int64:   return f(field.template get<Int64>());
        case Types::Int128:  return f(field.template get<Int128>());
        case Types::Int256:  return f(field.template get<Int256>());
        case Types::UUID:    return f(field.template get<UUID>());
        case Types::IPv4:    return f(field.template get<IPv4>());
        case Types::IPv6:    return f(field.template get<IPv6>());
        case Types::Float64: return f(field.template get<Float64>());
        case Types::String:  return f(field.template get<String>());
        case Types::Array:   return f(field.template get<Array>());
        case Types::Tuple:   return f(field.template get<Tuple>());
        case Types::Map:     return f(field.template get<Map>());
        case Types::Bool:
        {
            bool value = bool(field.template get<UInt64>());
            return f(value);
        }
        case Types::Object:     return f(field.template get<Object>());
        case Types::Decimal32:  return f(field.template get<DecimalField<Decimal32>>());
        case Types::Decimal64:  return f(field.template get<DecimalField<Decimal64>>());
        case Types::Decimal128: return f(field.template get<DecimalField<Decimal128>>());
        case Types::Decimal256: return f(field.template get<DecimalField<Decimal256>>());
        case Types::AggregateFunctionState: return f(field.template get<AggregateFunctionStateData>());
        case Types::CustomType: return f(field.template get<CustomType>());
    }

    UNREACHABLE();
}

}
