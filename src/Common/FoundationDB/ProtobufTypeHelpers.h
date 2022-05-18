#pragma once

/// Help functions for converting between proto-generated types and basic types.
/// Such as Proto::UUID <=> DB::UUID, google::protobuf::RepeatedPtrField <=> std::vector.

#include <utility>
#include <vector>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <base/Decimal.h>
#include <base/UUID.h>
#include <base/extended_types.h>
#include <google/protobuf/repeated_field.h>
#include <Common/Exception.h>
#include "Core/Types_fwd.h"
#include "IPv4.pb.h"
#include "base/IPv4andIPv6.h"

#include <UUID.pb.h>
#include <Field.pb.h>

#include <Core/Field.h>
#include <Core/Types.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB::FoundationDB
{
inline void toNativeUUID(const Proto::UUID & proto_uuid, UUID & uuid)
{
    uuid.toUnderType().items[0] = proto_uuid.high_int64();
    uuid.toUnderType().items[1] = proto_uuid.low_int64();
}

inline UUID toNativeUUID(const Proto::UUID & proto_uuid)
{
    UUID uuid;
    toNativeUUID(proto_uuid, uuid);
    return uuid;
}

inline void toProtoUUID(const UUID & uuid, Proto::UUID & proto_uuid)
{
    proto_uuid.set_high_int64(uuid.toUnderType().items[0]);
    proto_uuid.set_low_int64(uuid.toUnderType().items[1]);
}

inline IPv4 toNativeIPv4(const Proto::IPv4 & proto_ipv4)
{
    IPv4 ipv4;
    ipv4.toUnderType() = proto_ipv4.value();
    return ipv4;
}

inline IPv6 toNativeIPv6(const Proto::IPv6 & proto_ipv6)
{
    IPv6 ipv6;
    ipv6.toUnderType().items[0] = proto_ipv6.high_int64();
    ipv6.toUnderType().items[1] = proto_ipv6.low_int64();
    return ipv6;
}

inline void toProtoIPv4(const IPv4 & ipv4, Proto::IPv4 & proto_ipv4)
{
    proto_ipv4.set_value(ipv4.toUnderType());
}

inline void toProtoIPv6(const IPv6 & ipv6, Proto::IPv6 & proto_ipv6)
{
    proto_ipv6.set_high_int64(ipv6.toUnderType().items[0]);
    proto_ipv6.set_low_int64(ipv6.toUnderType().items[1]);
}

template <typename Signed>
inline Proto::UInt128 toProtoWideInteger(const wide::integer<128, Signed> & integer)
{
    Proto::UInt128 proto;
    proto.set_high(integer.items[0]);
    proto.set_low(integer.items[1]);
    return proto;
}

template <typename Signed>
inline Proto::UInt256 toProtoWideInteger(const wide::integer<256, Signed> & integer)
{
    Proto::UInt256 proto;
    proto.set_item1(integer.items[0]);
    proto.set_item2(integer.items[1]);
    proto.set_item3(integer.items[2]);
    proto.set_item4(integer.items[3]);
    return proto;
}

inline Proto::UUID toProtoUUID(const UUID & uuid)
{
    Proto::UUID proto_uuid;
    toProtoUUID(uuid, proto_uuid);
    return proto_uuid;
}

/// Convert Field => Proto::Field
class FieldVisitorProto
{
public:
    void operator()(const Null &, Proto::Field & field) const { field.clear_data(); }
    void operator()(const bool & x, Proto::Field & field) const { field.set_bool_(x); }
    void operator()(const UInt64 & x, Proto::Field & field) const { field.set_uint64(x); }
    void operator()(const UInt128 & x, Proto::Field & field) const { *field.mutable_uint128() = toProtoWideInteger(x); }
    void operator()(const UInt256 & x, Proto::Field & field) const { *field.mutable_uint256() = toProtoWideInteger(x); }
    void operator()(const Int64 & x, Proto::Field & field) const { field.set_int64(x); }
    void operator()(const Int128 & x, Proto::Field & field) const { *field.mutable_int128() = toProtoWideInteger(x); }
    void operator()(const Int256 & x, Proto::Field & field) const { *field.mutable_int256() = toProtoWideInteger(x); }
    void operator()(const UUID & x, Proto::Field & field) const { toProtoUUID(x, *field.mutable_uuid()); }
    void operator()(const Float64 & x, Proto::Field & field) const { field.set_float64(x); }
    void operator()(const String & x, Proto::Field & field) const { field.set_string(x); }
    void operator()(const Array & x, Proto::Field & field) const { assignProtoFieldVector(*field.mutable_array(), x); }
    void operator()(const Tuple & x, Proto::Field & field) const { assignProtoFieldVector(*field.mutable_tuple(), x); }
    void operator()(const Map & x, Proto::Field & field) const { assignProtoFieldVector(*field.mutable_map(), x); }
    void operator()(const Object & x, Proto::Field & field) const { assignProtoFieldMap(*field.mutable_object(), x); }
    void operator()(const IPv4 & x, Proto::Field & field) const { toProtoIPv4(x, *field.mutable_ipv4()); }
    void operator()(const IPv6 & x, Proto::Field & field) const { toProtoIPv6(x, *field.mutable_ipv6()); }
    void operator()(const CustomType & x, Proto::Field & field) const { field.set_custom(x.toString()); }

    void operator()(const DecimalField<Decimal32> & x, Proto::Field & field) const
    {
        field.mutable_decimal32()->set_value(x.getValue());
        field.mutable_decimal32()->set_scale(x.getScale());
    }

    void operator()(const DecimalField<Decimal64> & x, Proto::Field & field) const
    {
        field.mutable_decimal64()->set_value(x.getValue());
        field.mutable_decimal64()->set_scale(x.getScale());
    }

    void operator()(const DecimalField<Decimal128> & x, Proto::Field & field) const
    {
        *field.mutable_decimal128()->mutable_value() = toProtoWideInteger(x.getValue().value);
        field.mutable_decimal128()->set_scale(x.getScale());
    }

    void operator()(const DecimalField<Decimal256> & x, Proto::Field & field) const
    {
        *field.mutable_decimal256()->mutable_value() = toProtoWideInteger(x.getValue().value);
        field.mutable_decimal256()->set_scale(x.getScale());
    }

    void operator()(const AggregateFunctionStateData & x, Proto::Field & field) const
    {
        field.mutable_aggregatefunctionstate()->set_name(x.name);
        field.mutable_aggregatefunctionstate()->set_data(x.data);
    }

private:
    static void assignProtoFieldVector(Proto::FieldVector & proto, const FieldVector & vec)
    {
        for (const auto & item : vec)
        {
            Field::dispatch([&proto](const auto & value) { FieldVisitorProto()(value, *proto.add_items()); }, item);
        }
    }

    static void assignProtoFieldMap(Proto::FieldMap & proto, const FieldMap & map)
    {
        auto & data = *proto.mutable_data();
        for (const auto & [key, value] : map)
        {
            Proto::Field field;
            Field::dispatch([&field](const auto & item) { FieldVisitorProto()(item, field); }, value);
            data[key] = std::move(field);
        }
    }
};

/// Convert Field to Proto::Field.
/// If a FieldRef is passed in, toProto() will extract the actual value instead of the reference to the block.
inline Proto::Field toProto(const Field & field)
{
    Proto::Field proto;
    Field::dispatch([&proto](const auto & value) { FieldVisitorProto()(value, proto); }, field);
    return proto;
}

/// Convert Proto::Field => Field
inline Field makeField(const Proto::Field & proto);

template <class Integer>
inline Integer makeInteger(const Proto::UInt128 & proto)
{
    return Integer{proto.high(), proto.low()};
}

template <class Integer>
inline Integer makeInteger(const Proto::UInt256 & proto)
{
    return Integer{proto.item1(), proto.item2(), proto.item3(), proto.item4()};
}

template <class Array>
inline Array makeArray(const Proto::FieldVector & proto)
{
    Array array;
    for (const auto & item : proto.items())
    {
        array.emplace_back(makeField(item));
    }
    return array;
}

inline Object makeObject(const Proto::FieldMap & proto)
{
    Object obj;
    for (const auto & [key, value] : proto.data())
    {
        obj[key] = makeField(value);
    }
    return obj;
}

inline Field makeField(const Proto::Field & proto)
{
    using DataCase = Proto::Field::DataCase;
    Field field;
    switch (proto.data_case())
    {
        case DataCase::DATA_NOT_SET:
            return Field();
        case DataCase::kUint64:
            return proto.uint64();
        case DataCase::kUint128:
            return makeInteger<UInt128>(proto.uint128());
        case DataCase::kUint256:
            return makeInteger<UInt256>(proto.uint256());
        case DataCase::kUuid:
            return toNativeUUID(proto.uuid());
        case DataCase::kInt64:
            return proto.int64();
        case DataCase::kInt128:
            return makeInteger<Int128>(proto.int128());
        case DataCase::kInt256:
            return makeInteger<Int256>(proto.int256());
        case DataCase::kFloat64:
            return proto.float64();
        case DataCase::kString:
            return proto.string();
        case DataCase::kArray:
            return makeArray<Array>(proto.array());
        case DataCase::kTuple:
            return makeArray<Tuple>(proto.tuple());
        case DataCase::kDecimal32:
            return DecimalField<Decimal32>(proto.decimal32().value(), proto.decimal32().scale());
        case DataCase::kDecimal64:
            return DecimalField<Decimal64>(proto.decimal64().value(), proto.decimal64().scale());
        case DataCase::kDecimal128:
            return DecimalField<Decimal128>(makeInteger<Int128>(proto.decimal128().value()), proto.decimal128().scale());
        case DataCase::kDecimal256:
            return DecimalField<Decimal256>(makeInteger<Int256>(proto.decimal256().value()), proto.decimal256().scale());
        case DataCase::kMap:
            return makeArray<Map>(proto.map());
        case DataCase::kObject:
            return makeObject(proto.object());
        case DataCase::kAggregatefunctionstate: {
            AggregateFunctionStateData value;
            value.name = proto.aggregatefunctionstate().name();
            value.data = proto.aggregatefunctionstate().data();
            return value;
        }
        case DataCase::kBool:
            return proto.bool_();
        case DataCase::kIpv4:
            return toNativeIPv4(proto.ipv4());
        case DataCase::kIpv6:
            return toNativeIPv6(proto.ipv6());
        case DataCase::kCustom:
            return proto.custom();
    }
    return Field();
}
}
