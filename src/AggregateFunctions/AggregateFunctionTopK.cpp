#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadHelpersArena.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnArray.h>

#include <Common/SpaceSaving.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace
{

inline constexpr UInt64 TOP_K_MAX_SIZE = 0xFFFFFF;

template <typename T>
struct AggregateFunctionTopKData
{
    using Set = SpaceSaving<T, HashCRC32<T>>;

    Set value;
};


template <typename T, bool is_weighted>
class AggregateFunctionTopK
    : public IAggregateFunctionDataHelper<AggregateFunctionTopKData<T>, AggregateFunctionTopK<T, is_weighted>>
{
protected:
    using State = AggregateFunctionTopKData<T>;
    UInt64 threshold;
    UInt64 reserved;

public:
    AggregateFunctionTopK(UInt64 threshold_, UInt64 load_factor, const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionTopKData<T>, AggregateFunctionTopK<T, is_weighted>>(argument_types_, params, createResultType(argument_types_))
        , threshold(threshold_), reserved(load_factor * threshold)
    {}

    AggregateFunctionTopK(UInt64 threshold_, UInt64 load_factor, const DataTypes & argument_types_, const Array & params, const DataTypePtr & result_type_)
        : IAggregateFunctionDataHelper<AggregateFunctionTopKData<T>, AggregateFunctionTopK<T, is_weighted>>(argument_types_, params, result_type_)
        , threshold(threshold_), reserved(load_factor * threshold)
    {}

    String getName() const override { return is_weighted ? "topKWeighted" : "topK"; }

    static DataTypePtr createResultType(const DataTypes & argument_types_)
    {
        return std::make_shared<DataTypeArray>(argument_types_[0]);
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & set = this->data(place).value;
        if (set.capacity() != reserved)
            set.resize(reserved);

        if constexpr (is_weighted)
            set.insert(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num], columns[1]->getUInt(row_num));
        else
            set.insert(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & set = this->data(place).value;
        if (set.capacity() != reserved)
            set.resize(reserved);
        set.merge(this->data(rhs).value);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).value.write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version  */, Arena *) const override
    {
        auto & set = this->data(place).value;
        set.resize(reserved);
        set.read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const typename State::Set & set = this->data(place).value;
        auto result_vec = set.topK(threshold);
        size_t size = result_vec.size();

        offsets_to.push_back(offsets_to.back() + size);

        typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + size);

        size_t i = 0;
        for (auto it = result_vec.begin(); it != result_vec.end(); ++it, ++i)
            data_to[old_size + i] = it->key;
    }
};


/// Generic implementation, it uses serialized representation as object descriptor.
struct AggregateFunctionTopKGenericData
{
    using Set = SpaceSaving<StringRef, StringRefHash>;

    Set value;
};

/** Template parameter with true value should be used for columns that store their elements in memory continuously.
 *  For such columns topK() can be implemented more efficiently (especially for small numeric arrays).
 */
template <bool is_plain_column, bool is_weighted>
class AggregateFunctionTopKGeneric
    : public IAggregateFunctionDataHelper<AggregateFunctionTopKGenericData, AggregateFunctionTopKGeneric<is_plain_column, is_weighted>>
{
private:
    using State = AggregateFunctionTopKGenericData;

    UInt64 threshold;
    UInt64 reserved;

    static void deserializeAndInsert(StringRef str, IColumn & data_to);

public:
    AggregateFunctionTopKGeneric(
        UInt64 threshold_, UInt64 load_factor, const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionTopKGenericData, AggregateFunctionTopKGeneric<is_plain_column, is_weighted>>(argument_types_, params, createResultType(argument_types_))
        , threshold(threshold_), reserved(load_factor * threshold) {}

    String getName() const override { return is_weighted ? "topKWeighted" : "topK"; }

    static DataTypePtr createResultType(const DataTypes & argument_types_)
    {
        return std::make_shared<DataTypeArray>(argument_types_[0]);
    }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).value.write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        set.clear();

        // Specialized here because there's no deserialiser for StringRef
        size_t size = 0;
        readVarUInt(size, buf);
        if (unlikely(size > TOP_K_MAX_SIZE))
            throw Exception(
                ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "Too large size ({}) for aggregate function '{}' state (maximum is {})",
                size,
                getName(),
                TOP_K_MAX_SIZE);
        set.resize(size);
        for (size_t i = 0; i < size; ++i)
        {
            auto ref = readStringBinaryInto(*arena, buf);
            UInt64 count;
            UInt64 error;
            readVarUInt(count, buf);
            readVarUInt(error, buf);
            set.insert(ref, count, error);
            arena->rollback(ref.size);
        }

        set.readAlphaMap(buf);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        if (set.capacity() != reserved)
            set.resize(reserved);

        if constexpr (is_plain_column)
        {
            if constexpr (is_weighted)
                set.insert(columns[0]->getDataAt(row_num), columns[1]->getUInt(row_num));
            else
                set.insert(columns[0]->getDataAt(row_num));
        }
        else
        {
            const char * begin = nullptr;
            StringRef str_serialized = columns[0]->serializeValueIntoArena(row_num, *arena, begin);
            if constexpr (is_weighted)
                set.insert(str_serialized, columns[1]->getUInt(row_num));
            else
                set.insert(str_serialized);
            arena->rollback(str_serialized.size);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & set = this->data(place).value;
        if (set.capacity() != reserved)
            set.resize(reserved);
        set.merge(this->data(rhs).value);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & data_to = arr_to.getData();

        auto result_vec = this->data(place).value.topK(threshold);
        offsets_to.push_back(offsets_to.back() + result_vec.size());

        for (auto & elem : result_vec)
        {
            if constexpr (is_plain_column)
                data_to.insertData(elem.key.data, elem.key.size);
            else
                data_to.deserializeAndInsertFromArena(elem.key.data);
        }
    }
};


/// Substitute return type for Date and DateTime
template <bool is_weighted>
class AggregateFunctionTopKDate : public AggregateFunctionTopK<DataTypeDate::FieldType, is_weighted>
{
public:
    using AggregateFunctionTopK<DataTypeDate::FieldType, is_weighted>::AggregateFunctionTopK;

    AggregateFunctionTopKDate(UInt64 threshold_, UInt64 load_factor, const DataTypes & argument_types_, const Array & params)
        : AggregateFunctionTopK<DataTypeDate::FieldType, is_weighted>(
            threshold_,
            load_factor,
            argument_types_,
            params,
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate>()))
    {}
};

template <bool is_weighted>
class AggregateFunctionTopKDateTime : public AggregateFunctionTopK<DataTypeDateTime::FieldType, is_weighted>
{
public:
    using AggregateFunctionTopK<DataTypeDateTime::FieldType, is_weighted>::AggregateFunctionTopK;

    AggregateFunctionTopKDateTime(UInt64 threshold_, UInt64 load_factor, const DataTypes & argument_types_, const Array & params)
        : AggregateFunctionTopK<DataTypeDateTime::FieldType, is_weighted>(
            threshold_,
            load_factor,
            argument_types_,
            params,
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()))
    {}
};

template <bool is_weighted>
class AggregateFunctionTopKIPv4 : public AggregateFunctionTopK<DataTypeIPv4::FieldType, is_weighted>
{
public:
    using AggregateFunctionTopK<DataTypeIPv4::FieldType, is_weighted>::AggregateFunctionTopK;

    AggregateFunctionTopKIPv4(UInt64 threshold_, UInt64 load_factor, const DataTypes & argument_types_, const Array & params)
        : AggregateFunctionTopK<DataTypeIPv4::FieldType, is_weighted>(
            threshold_,
            load_factor,
            argument_types_,
            params,
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeIPv4>()))
    {}
};


template <bool is_weighted>
IAggregateFunction * createWithExtraTypes(const DataTypes & argument_types, UInt64 threshold, UInt64 load_factor, const Array & params)
{
    if (argument_types.empty())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Got empty arguments list");

    WhichDataType which(argument_types[0]);
    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionTopKDate<is_weighted>(threshold, load_factor, argument_types, params);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionTopKDateTime<is_weighted>(threshold, load_factor, argument_types, params);
    if (which.idx == TypeIndex::IPv4)
        return new AggregateFunctionTopKIPv4<is_weighted>(threshold, load_factor, argument_types, params);

    /// Check that we can use plain version of AggregateFunctionTopKGeneric
    if (argument_types[0]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        return new AggregateFunctionTopKGeneric<true, is_weighted>(threshold, load_factor, argument_types, params);
    else
        return new AggregateFunctionTopKGeneric<false, is_weighted>(threshold, load_factor, argument_types, params);
}


template <bool is_weighted>
AggregateFunctionPtr createAggregateFunctionTopK(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (!is_weighted)
    {
        assertUnary(name, argument_types);
    }
    else
    {
        assertBinary(name, argument_types);
        if (!isInteger(argument_types[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The second argument for aggregate function 'topKWeighted' must have integer type");
    }

    UInt64 threshold = 10;  /// default values
    UInt64 load_factor = 3;

    if (!params.empty())
    {
        if (params.size() > 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Aggregate function '{}' requires two parameters or less", name);

        if (params.size() == 2)
        {
            load_factor = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[1]);

            if (load_factor < 1)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                "Too small parameter 'load_factor' for aggregate function '{}' (got {}, minimum is 1)", name, load_factor);
        }

        threshold = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

        if (threshold > DB::TOP_K_MAX_SIZE || load_factor > DB::TOP_K_MAX_SIZE || threshold * load_factor > DB::TOP_K_MAX_SIZE)
            throw Exception(
                ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "Too large parameter(s) for aggregate function '{}' (maximum is {})",
                name,
                toString(DB::TOP_K_MAX_SIZE));

        if (threshold == 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Parameter 0 is illegal for aggregate function '{}'", name);
    }

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionTopK, is_weighted>(
        *argument_types[0], threshold, load_factor, argument_types, params));

    if (!res)
        res = AggregateFunctionPtr(createWithExtraTypes<is_weighted>(argument_types, threshold, load_factor, params));

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument for aggregate function '{}'", argument_types[0]->getName(), name);
    return res;
}

}

void registerAggregateFunctionTopK(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };

    factory.registerFunction("topK", { createAggregateFunctionTopK<false>, properties });
    factory.registerFunction("topKWeighted", { createAggregateFunctionTopK<true>, properties });
}

}
