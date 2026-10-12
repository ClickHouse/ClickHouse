#pragma once

#include "config.h"

#if USE_DATASKETCHES

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/transformEndianness.h>
#include <Core/Field.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <hll.hpp>

#include <algorithm>
#include <bit>
#include <cmath>
#include <exception>
#include <memory>
#include <new>
#include <type_traits>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}


/// Keeps insertion and union states separate to preserve the estimator used before merging.
class HllSketchData
{
private:
    /// Folding is deferred until the state is read, so it can happen in const methods.
    mutable std::unique_ptr<datasketches::hll_sketch> sk_update;
    mutable std::unique_ptr<datasketches::hll_union> sk_union;

    datasketches::hll_sketch * getSkUpdate(uint8_t lg_config_k, datasketches::target_hll_type tgt_type)
    {
        if (!sk_update)
            sk_update = std::make_unique<datasketches::hll_sketch>(lg_config_k, tgt_type);
        return sk_update.get();
    }

    datasketches::hll_union * getSkUnion(uint8_t lg_config_k)
    {
        if (!sk_union)
            sk_union = std::make_unique<datasketches::hll_union>(lg_config_k);
        return sk_union.get();
    }

    /// Inserts keep going into `sk_update` even when `sk_union` exists, to avoid creating and merging a sketch per row.
    void foldUpdateIntoUnionIfNeeded() const
    {
        if (sk_union && sk_update)
        {
            sk_union->update(*sk_update);
            sk_update = nullptr;
        }
    }

public:
    template <typename T>
    void insert(T value, uint8_t lg_config_k, datasketches::target_hll_type tgt_type)
    {
        getSkUpdate(lg_config_k, tgt_type)->update(value);
    }

    void insertData(const char * data, size_t size, uint8_t lg_config_k, datasketches::target_hll_type tgt_type)
    {
        getSkUpdate(lg_config_k, tgt_type)->update(static_cast<const void *>(data), size);
    }

    /// With `use_composite_estimate` the estimate is computed from the registers of the sketch only, so it does not depend on the order
    /// of the inserts and of the merges. The estimate of the library is the HIP estimate while the sketch is in order, which is more
    /// accurate for a small sketch, but it depends on that order.
    UInt64 size(bool use_composite_estimate) const
    {
        foldUpdateIntoUnionIfNeeded();

        /// Rounding preserves exact cardinalities despite floating-point error.
        if (sk_union)
            return static_cast<UInt64>(std::llround(use_composite_estimate ? sk_union->get_composite_estimate() : sk_union->get_estimate()));
        if (sk_update)
            return static_cast<UInt64>(std::llround(use_composite_estimate ? sk_update->get_composite_estimate() : sk_update->get_estimate()));
        return 0;
    }

    void merge(const HllSketchData & rhs, uint8_t lg_config_k)
    {
        const bool rhs_has_update = rhs.sk_update && !rhs.sk_update->is_empty();
        if (!rhs_has_update && !rhs.sk_union)
            return;

        datasketches::hll_union * u = getSkUnion(lg_config_k);

        /// `rhs` may hold both sketches; take both without modifying it, in the order of `foldUpdateIntoUnionIfNeeded`: the union, then the
        /// updates. The registers of the result do not depend on the order, but the HIP estimate and the bytes of the sketch do.
        /// The union accepts a sketch of any type, and `HLL_8` is its own type, so there is no re-encoding.
        if (rhs.sk_union)
            u->update(rhs.sk_union->get_result(datasketches::HLL_8));

        if (rhs_has_update)
            u->update(*rhs.sk_update);

        foldUpdateIntoUnionIfNeeded();
    }

    /// You can only call this for an empty object. The sketch is kept as it was written, with its own resolution and
    /// storage type: reading a state must not lose what writing it back would then persist. It is folded into a union,
    /// at the declared resolution, only when it is merged.
    void read(ReadBuffer & in)
    {
        datasketches::hll_sketch::vector_bytes bytes;
        readVectorBinary(bytes, in);
        if (bytes.empty())
            return;

        try
        {
            auto sk = datasketches::hll_sketch::deserialize(bytes.data(), bytes.size());
            sk_update = std::make_unique<datasketches::hll_sketch>(std::move(sk));
        }
        catch (const DB::Exception &)
        {
            throw;
        }
        catch (const std::bad_alloc &)
        {
            throw;
        }
        catch (const std::exception & e)
        {
            /// Translate malformed input to avoid a logical exception in `SerializationAggregateFunction`.
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Cannot deserialize HLL sketch state: {}", e.what());
        }
    }

    void write(WriteBuffer & out, uint8_t lg_config_k, datasketches::target_hll_type tgt_type) const
    {
        foldUpdateIntoUnionIfNeeded();

        datasketches::hll_sketch::vector_bytes bytes;
        if (sk_update)
            bytes = sk_update->serialize_compact();
        else if (sk_union)
            bytes = sk_union->get_result(tgt_type).serialize_compact();
        else
            bytes = datasketches::hll_sketch(lg_config_k, tgt_type).serialize_compact();
        writeVectorBinary(bytes, out);
    }
};


template <typename T>
class AggregateFunctionUniqApacheHLL final : public IAggregateFunctionDataHelper<HllSketchData, AggregateFunctionUniqApacheHLL<T>>
{
    using Base = IAggregateFunctionDataHelper<HllSketchData, AggregateFunctionUniqApacheHLL<T>>;

    uint8_t lg_config_k;
    datasketches::target_hll_type target_type;
    bool use_composite_estimate;

public:
    AggregateFunctionUniqApacheHLL(
        uint8_t lg_config_k_,
        datasketches::target_hll_type target_type_,
        bool use_composite_estimate_,
        const DataTypes & argument_types_,
        const Array & params_)
        : Base(argument_types_, params_, std::make_shared<DataTypeUInt64>())
        , lg_config_k(lg_config_k_)
        , target_type(target_type_)
        , use_composite_estimate(use_composite_estimate_)
    {
    }

    String getName() const override { return "uniqApacheHLL"; }

    bool allocatesMemoryInArena() const override { return false; }

    /// A `Nullable` argument only has its `NULL` rows skipped: the state is the same bare sketch, with the same type.
    bool stateIsIndependentOfNullability() const override { return true; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & data = this->data(place);

        if constexpr (std::is_same_v<T, String>)
        {
            const auto value = columns[0]->getDataAt(row_num);

            /// Other DataSketches implementations ignore empty strings, and the sketch of a mixed
            /// input must be the same as theirs.
            if (value.empty())
                return;

            data.insertData(value.data(), value.size(), lg_config_k, target_type);
        }
        else
        {
            const auto & value = assert_cast<const ColumnVectorOrDecimal<T> &>(*columns[0]).getData()[row_num];

            if constexpr (std::is_same_v<T, UUID>)
            {
                /// The two halves of a `UUID` are numbers in host order; the canonical bytes are their big-endian
                /// representation on every platform.
                UInt64 halves[2] = {UUIDHelpers::getHighBytes(value), UUIDHelpers::getLowBytes(value)};
                transformEndianness<std::endian::big>(halves[0]);
                transformEndianness<std::endian::big>(halves[1]);
                data.insertData(reinterpret_cast<const char *>(halves), sizeof(halves), lg_config_k, target_type);
            }
            else if constexpr (std::is_same_v<T, IPv6>)
                /// Already held in network order, which is the canonical form.
                data.insertData(reinterpret_cast<const char *>(&value), sizeof(value), lg_config_k, target_type);
            else if constexpr (is_decimal<T>)
                /// Hash `DateTime64` as epoch ticks; external producers must use the same scale.
                data.insert(static_cast<Int64>(value.value), lg_config_k, target_type);
            else if constexpr (std::is_same_v<T, IPv4>)
                data.insert(static_cast<UInt64>(value.toUnderType()), lg_config_k, target_type);
            else if constexpr (std::is_same_v<T, BFloat16> || std::is_floating_point_v<T>)
                data.insert(static_cast<Float64>(value), lg_config_k, target_type);
            else if constexpr (std::is_signed_v<T>)
                data.insert(static_cast<Int64>(value), lg_config_k, target_type);
            else
                data.insert(static_cast<UInt64>(value), lg_config_k, target_type);
        }
    }

    /// Serialized sketches carry their configuration, so parameters need not match, and nullability of the argument does not matter.
    bool haveSameStateRepresentationImpl(const IAggregateFunction & rhs) const override
    {
        const auto & lhs_types = this->getArgumentTypes();
        const auto & rhs_types = rhs.getArgumentTypes();

        return getName() == rhs.getName()
            && std::equal(
                lhs_types.begin(),
                lhs_types.end(),
                rhs_types.begin(),
                rhs_types.end(),
                [](const auto & lhs, const auto & rhs_type) { return removeNullable(lhs)->equals(*removeNullable(rhs_type)); });
    }

    /// The parameters do not change what a state means, so every spelling of them is the same type wherever a common type is
    /// needed (`UNION ALL`, arrays, `if`) and wherever a state is accepted by the name of its type. The default parameters
    /// stand for all of them.
    DataTypePtr getNormalizedStateType() const override
    {
        DataTypes normalized_argument_types;
        normalized_argument_types.reserve(this->argument_types.size());
        for (const auto & type : this->argument_types)
            normalized_argument_types.emplace_back(type->getNormalizedType());

        AggregateFunctionProperties properties;
        auto function = AggregateFunctionFactory::instance().get("uniqApacheHLL", NullsAction::EMPTY, normalized_argument_types, {}, properties);
        return std::make_shared<DataTypeAggregateFunction>(function, normalized_argument_types, Array{});
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs), lg_config_k);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf, lg_config_k, target_type);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).size(use_composite_estimate));
    }
};

}

#endif
