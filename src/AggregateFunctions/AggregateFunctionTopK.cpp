#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypesNumber.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadHelpersArena.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnArray.h>

#include <Common/SpaceSaving.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
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
    bool include_counts;
    bool is_approx_top_k;

public:
    AggregateFunctionTopK(UInt64 threshold_, UInt64 reserved_, bool include_counts_, bool is_approx_top_k_, const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionTopKData<T>, AggregateFunctionTopK<T, is_weighted>>(argument_types_, params, createResultType(argument_types_, include_counts_))
        , threshold(threshold_), reserved(reserved_), include_counts(include_counts_), is_approx_top_k(is_approx_top_k_)
    {}

        AggregateFunctionTopK(UInt64 threshold_, UInt64 reserved_, bool include_counts_, bool is_approx_top_k_, const DataTypes & argument_types_, const Array & params, const DataTypePtr & result_type_)
        : IAggregateFunctionDataHelper<AggregateFunctionTopKData<T>, AggregateFunctionTopK<T, is_weighted>>(argument_types_, params, result_type_)
        , threshold(threshold_), reserved(reserved_), include_counts(include_counts_), is_approx_top_k(is_approx_top_k_)
    {}

    String getName() const override
    {
        if (is_approx_top_k)
            return  is_weighted ? "approx_top_sum" : "approx_top_k";
        return is_weighted ? "topKWeighted" : "topK";
    }

    static DataTypePtr createResultType(const DataTypes & argument_types_, bool include_counts_)
    {
        if (include_counts_)
        {
            DataTypes types
            {
                argument_types_[0],
                std::make_shared<DataTypeUInt64>(),
                std::make_shared<DataTypeUInt64>(),
            };

            Strings names
            {
                "item",
                "count",
                "error",
            };

            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                std::move(types),
                std::move(names)
            ));
        }
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

        IColumn & data_to = arr_to.getData();

        if (include_counts)
        {
            auto & column_tuple = assert_cast<ColumnTuple &>(data_to);

            auto & column_key = assert_cast<ColumnVector<T> &>(column_tuple.getColumn(0)).getData();
            auto & column_count = assert_cast<ColumnVector<UInt64> &>(column_tuple.getColumn(1)).getData();
            auto & column_error = assert_cast<ColumnVector<UInt64> &>(column_tuple.getColumn(2)).getData();
            size_t old_size = column_key.size();
            column_key.resize(old_size + size);
            column_count.resize(old_size + size);
            column_error.resize(old_size + size);

            size_t i = 0;
            for (auto it = result_vec.begin(); it != result_vec.end(); ++it, ++i)
            {
                column_key[old_size + i] = it->key;
                column_count[old_size + i] = it->count;
                column_error[old_size + i] = it->error;
            }

        } else
        {

            auto & column_key = assert_cast<ColumnVector<T> &>(data_to).getData();
            size_t old_size = column_key.size();
            column_key.resize(old_size + size);
            size_t i = 0;
            for (auto it = result_vec.begin(); it != result_vec.end(); ++it, ++i)
            {
                column_key[old_size + i] = it->key;
            }
        }
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
    bool include_counts;
    bool is_approx_top_k;

public:
    AggregateFunctionTopKGeneric(
        UInt64 threshold_, UInt64 reserved_, bool include_counts_, bool is_approx_top_k_, const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionTopKGenericData, AggregateFunctionTopKGeneric<is_plain_column, is_weighted>>(argument_types_, params, createResultType(argument_types_, include_counts_))
        , threshold(threshold_), reserved(reserved_), include_counts(include_counts_), is_approx_top_k(is_approx_top_k_) {}

    String getName() const override
    {
        if (is_approx_top_k)
            return  is_weighted ? "approx_top_sum" : "approx_top_k";
        return is_weighted ? "topKWeighted" : "topK";
    }

    static DataTypePtr createResultType(const DataTypes & argument_types_, bool include_counts_)
    {
        if (include_counts_)
        {
            DataTypes types
            {
                argument_types_[0],
                std::make_shared<DataTypeUInt64>(),
                std::make_shared<DataTypeUInt64>(),
            };

            Strings names
            {
                "item",
                "count",
                "error",
            };

            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                std::move(types),
                std::move(names)
            ));

        }
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
        if (!this->data(rhs).value.size())
            return;

        auto & set = this->data(place).value;
        if (set.capacity() != reserved)
            set.resize(reserved);
        set.merge(this->data(rhs).value);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const typename State::Set & set = this->data(place).value;
        auto result_vec = set.topK(threshold);
        size_t size = result_vec.size();
        offsets_to.push_back(offsets_to.back() + size);

        IColumn & data_to = arr_to.getData();

        if (include_counts)
        {
            auto & column_tuple = assert_cast<ColumnTuple &>(data_to);
            IColumn & column_key = column_tuple.getColumn(0);
            IColumn & column_count = column_tuple.getColumn(1);
            IColumn & column_error = column_tuple.getColumn(2);
            for (auto &elem : result_vec)
            {
                column_count.insert(elem.count);
                column_error.insert(elem.error);
                deserializeAndInsert<is_plain_column>(elem.key, column_key);
            }
        } else
        {
            for (auto & elem : result_vec)
            {
                deserializeAndInsert<is_plain_column>(elem.key, data_to);
            }
        }
    }
};


/// Substitute return type for Date and DateTime
template <bool is_weighted>
class AggregateFunctionTopKDate : public AggregateFunctionTopK<DataTypeDate::FieldType, is_weighted>
{
public:
    using AggregateFunctionTopK<DataTypeDate::FieldType, is_weighted>::AggregateFunctionTopK;

    AggregateFunctionTopKDate(UInt64 threshold_, UInt64 reserved_, bool include_counts_, bool is_approx_top_k_, const DataTypes & argument_types_, const Array & params)
        : AggregateFunctionTopK<DataTypeDate::FieldType, is_weighted>(
            threshold_,
            reserved_,
            include_counts_,
            is_approx_top_k_,
            argument_types_,
            params)
    {}
};

template <bool is_weighted>
class AggregateFunctionTopKDateTime : public AggregateFunctionTopK<DataTypeDateTime::FieldType, is_weighted>
{
public:
    using AggregateFunctionTopK<DataTypeDateTime::FieldType, is_weighted>::AggregateFunctionTopK;

    AggregateFunctionTopKDateTime(UInt64 threshold_, UInt64 reserved_, bool include_counts_, bool is_approx_top_k_, const DataTypes & argument_types_, const Array & params)
        : AggregateFunctionTopK<DataTypeDateTime::FieldType, is_weighted>(
            threshold_,
            reserved_,
            include_counts_,
            is_approx_top_k_,
            argument_types_,
            params)
    {}
};

template <bool is_weighted>
class AggregateFunctionTopKIPv4 : public AggregateFunctionTopK<DataTypeIPv4::FieldType, is_weighted>
{
public:
    using AggregateFunctionTopK<DataTypeIPv4::FieldType, is_weighted>::AggregateFunctionTopK;

    AggregateFunctionTopKIPv4(UInt64 threshold_, UInt64 reserved_, bool include_counts_, bool is_approx_top_k_, const DataTypes & argument_types_, const Array & params)
        : AggregateFunctionTopK<DataTypeIPv4::FieldType, is_weighted>(
            threshold_,
            reserved_,
            include_counts_,
            is_approx_top_k_,
            argument_types_,
            params)
    {}
};


template <bool is_weighted>
IAggregateFunction * createWithExtraTypes(const DataTypes & argument_types, UInt64 threshold, UInt64 reserved, bool include_counts, bool is_approx_top_k, const Array & params)
{
    if (argument_types.empty())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Got empty arguments list");

    WhichDataType which(argument_types[0]);
    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionTopKDate<is_weighted>(threshold, reserved, include_counts, is_approx_top_k, argument_types, params);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionTopKDateTime<is_weighted>(threshold, reserved, include_counts, is_approx_top_k, argument_types, params);
    if (which.idx == TypeIndex::IPv4)
        return new AggregateFunctionTopKIPv4<is_weighted>(threshold, reserved, include_counts, is_approx_top_k, argument_types, params);

    /// Check that we can use plain version of AggregateFunctionTopKGeneric
    if (argument_types[0]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        return new AggregateFunctionTopKGeneric<true, is_weighted>(threshold, reserved, include_counts, is_approx_top_k, argument_types, params);
    return new AggregateFunctionTopKGeneric<false, is_weighted>(
        threshold, reserved, include_counts, is_approx_top_k, argument_types, params);
}


template <bool is_weighted, bool is_approx_top_k>
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
    bool include_counts = is_approx_top_k;
    UInt64 reserved = threshold * load_factor;

    if (!params.empty())
    {
        if (params.size() > 3)
            throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                            "Aggregate function '{}' requires three parameters or less", name);

        threshold = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

        if (params.size() >= 2)
        {
            if (is_approx_top_k)
            {
                reserved = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[1]);

                if (reserved < 1)
                    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                    "Too small parameter 'reserved' for aggregate function '{}' (got {}, minimum is 1)", name, reserved);
            } else
            {
                load_factor = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[1]);

                if (load_factor < 1)
                    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                    "Too small parameter 'load_factor' for aggregate function '{}' (got {}, minimum is 1)", name, load_factor);
            }
        }

        if (params.size() == 3)
        {
            String option = params.at(2).safeGet<String>();

            if (option == "counts")
                include_counts = true;
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} doesn't support a parameter: {}", name, option);

        }

        if (!is_approx_top_k)
        {
            reserved = threshold * load_factor;
        }

        if (reserved > DB::TOP_K_MAX_SIZE || load_factor > DB::TOP_K_MAX_SIZE || threshold > DB::TOP_K_MAX_SIZE)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                            "Too large parameter(s) for aggregate function '{}' (maximum is {})", name, toString(TOP_K_MAX_SIZE));

        if (threshold == 0 || reserved == 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Parameter 0 is illegal for aggregate function '{}'", name);
    }

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionTopK, is_weighted>(
        *argument_types[0], threshold, reserved, include_counts, is_approx_top_k, argument_types, params));

    if (!res)
        res = AggregateFunctionPtr(createWithExtraTypes<is_weighted>(argument_types, threshold, reserved, include_counts, is_approx_top_k, params));

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument for aggregate function '{}'", argument_types[0]->getName(), name);
    return res;
}

}

void registerAggregateFunctionTopK(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };

    factory.registerFunction("topK", { createAggregateFunctionTopK<false, false>, properties });
    factory.registerFunction("topKWeighted", { createAggregateFunctionTopK<true, false>, properties });
    factory.registerFunction("approx_top_k", { createAggregateFunctionTopK<false, true>, properties }, AggregateFunctionFactory::Case::Insensitive);
    factory.registerFunction("approx_top_sum", { createAggregateFunctionTopK<true, true>, properties }, AggregateFunctionFactory::Case::Insensitive);
    factory.registerAlias("approx_top_count", "approx_top_k", AggregateFunctionFactory::Case::Insensitive);
}

}
