#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeAggregateFunction.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/assert_cast.h>

// TODO include this last because of a broken roaring header. See the comment inside.
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// Counts bitmap operation on numbers.
template <typename T, typename Data>
class AggregateFunctionBitmap final : public IAggregateFunctionDataHelper<Data, AggregateFunctionBitmap<T, Data>>
{
public:
    explicit AggregateFunctionBitmap(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionBitmap<T, Data>>({type}, {}, createResultType())
    {
    }

    String getName() const override { return Data::name(); }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeNumber<T>>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).roaring_bitmap_with_small_set.add(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).roaring_bitmap_with_small_set.merge(this->data(rhs).roaring_bitmap_with_small_set);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).roaring_bitmap_with_small_set.write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).roaring_bitmap_with_small_set.read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnVector<T> &>(to).getData().push_back(
            static_cast<T>(this->data(place).roaring_bitmap_with_small_set.size()));
    }
};


/// This aggregate function takes the states of AggregateFunctionBitmap as its argument.
template <typename T, typename Data, typename Policy>
class AggregateFunctionBitmapL2 final : public IAggregateFunctionDataHelper<Data, AggregateFunctionBitmapL2<T, Data, Policy>>
{
private:
    static constexpr size_t STATE_VERSION_1_MIN_REVISION = 54455;
public:
    explicit AggregateFunctionBitmapL2(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionBitmapL2<T, Data, Policy>>({type}, {}, createResultType())
    {
    }

    String getName() const override { return Policy::name; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeNumber<T>>(); }

    bool allocatesMemoryInArena() const override { return false; }

    DataTypePtr getStateType() const override
    {
        return this->argument_types.at(0);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        Data & data_lhs = this->data(place);
        const Data & data_rhs = this->data(assert_cast<const ColumnAggregateFunction &>(*columns[0]).getData()[row_num]);
        if (!data_lhs.init)
        {
            data_lhs.init = true;
            data_lhs.roaring_bitmap_with_small_set.merge(data_rhs.roaring_bitmap_with_small_set);
        }
        else
        {
            Policy::apply(data_lhs, data_rhs);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        Data & data_lhs = this->data(place);
        const Data & data_rhs = this->data(rhs);

        if (!data_rhs.init)
            return;

        if (!data_lhs.init)
        {
            data_lhs.init = true;
            data_lhs.roaring_bitmap_with_small_set.merge(data_rhs.roaring_bitmap_with_small_set);
        }
        else
        {
            Policy::apply(data_lhs, data_rhs);
        }
    }

    bool isVersioned() const override { return true; }

    size_t getDefaultVersion() const override { return 1; }

    size_t getVersionFromRevision(size_t revision) const override
    {
        if (revision >= STATE_VERSION_1_MIN_REVISION)
            return 1;
        return 0;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        if (!version)
            version = getDefaultVersion();

        if (*version >= 1)
            DB::writeBoolText(this->data(place).init, buf);

        this->data(place).roaring_bitmap_with_small_set.write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena *) const override
    {
        if (!version)
            version = getDefaultVersion();

        if (*version >= 1)
            DB::readBoolText(this->data(place).init, buf);
        this->data(place).roaring_bitmap_with_small_set.read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnVector<T> &>(to).getData().push_back(
            static_cast<T>(this->data(place).roaring_bitmap_with_small_set.size()));
    }
};


template <typename Data>
class BitmapAndPolicy
{
public:
    static constexpr auto name = "groupBitmapAnd";
    static void apply(Data & lhs, const Data & rhs) { lhs.roaring_bitmap_with_small_set.rb_and(rhs.roaring_bitmap_with_small_set); }
};

template <typename Data>
class BitmapOrPolicy
{
public:
    static constexpr auto name = "groupBitmapOr";
    static void apply(Data & lhs, const Data & rhs) { lhs.roaring_bitmap_with_small_set.rb_or(rhs.roaring_bitmap_with_small_set); }
};

template <typename Data>
class BitmapXorPolicy
{
public:
    static constexpr auto name = "groupBitmapXor";
    static void apply(Data & lhs, const Data & rhs) { lhs.roaring_bitmap_with_small_set.rb_xor(rhs.roaring_bitmap_with_small_set); }
};

template <typename T, typename Data>
using AggregateFunctionBitmapL2And = AggregateFunctionBitmapL2<T, Data, BitmapAndPolicy<Data>>;

template <typename T, typename Data>
using AggregateFunctionBitmapL2Or = AggregateFunctionBitmapL2<T, Data, BitmapOrPolicy<Data>>;

template <typename T, typename Data>
using AggregateFunctionBitmapL2Xor = AggregateFunctionBitmapL2<T, Data, BitmapXorPolicy<Data>>;


template <template <typename, typename> class AggregateFunctionTemplate, template <typename> typename Data, typename... TArgs>
IAggregateFunction * createWithIntegerType(const IDataType & argument_type, TArgs &&... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::UInt8) return new AggregateFunctionTemplate<UInt8, Data<UInt8>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt16) return new AggregateFunctionTemplate<UInt16, Data<UInt16>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt32) return new AggregateFunctionTemplate<UInt32, Data<UInt32>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt64) return new AggregateFunctionTemplate<UInt64, Data<UInt64>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Int8) return new AggregateFunctionTemplate<Int8, Data<Int8>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Int16) return new AggregateFunctionTemplate<Int16, Data<Int16>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Int32) return new AggregateFunctionTemplate<Int32, Data<Int32>>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::Int64) return new AggregateFunctionTemplate<Int64, Data<Int64>>(std::forward<TArgs>(args)...);
    return nullptr;
}

template <template <typename> typename Data>
AggregateFunctionPtr createAggregateFunctionBitmap(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    if (!argument_types[0]->canBeUsedInBitOperations())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "The type {} of argument for aggregate function {} "
                        "is illegal, because it cannot be used in Bitmap operations",
                        argument_types[0]->getName(), name);

    AggregateFunctionPtr res(createWithIntegerType<AggregateFunctionBitmap, Data>(*argument_types[0], argument_types[0]));

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
            argument_types[0]->getName(), name);

    return res;
}

// Additional aggregate functions to manipulate bitmaps.
template <template <typename, typename> typename AggregateFunctionTemplate>
AggregateFunctionPtr createAggregateFunctionBitmapL2(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    DataTypePtr argument_type_ptr = argument_types[0];
    WhichDataType which(*argument_type_ptr);
    if (which.idx != TypeIndex::AggregateFunction)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
            argument_types[0]->getName(), name);

    /// groupBitmap needs to know about the data type that was used to create bitmaps.
    /// We need to look inside the type of its argument to obtain it.
    const DataTypeAggregateFunction & datatype_aggfunc = dynamic_cast<const DataTypeAggregateFunction &>(*argument_type_ptr);
    AggregateFunctionPtr aggfunc = datatype_aggfunc.getFunction();

    if (aggfunc->getName() != AggregateFunctionGroupBitmapData<UInt8>::name())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
            argument_types[0]->getName(), name);

    DataTypePtr nested_argument_type_ptr = aggfunc->getArgumentTypes()[0];

    AggregateFunctionPtr res(createWithIntegerType<AggregateFunctionTemplate, AggregateFunctionGroupBitmapData>(
        *nested_argument_type_ptr, argument_type_ptr));

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
            argument_types[0]->getName(), name);

    return res;
}

}

void registerAggregateFunctionsBitmap(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupBitmap", createAggregateFunctionBitmap<AggregateFunctionGroupBitmapData>);
    factory.registerFunction("groupBitmapAnd", createAggregateFunctionBitmapL2<AggregateFunctionBitmapL2And>);
    factory.registerFunction("groupBitmapOr", createAggregateFunctionBitmapL2<AggregateFunctionBitmapL2Or>);
    factory.registerFunction("groupBitmapXor", createAggregateFunctionBitmapL2<AggregateFunctionBitmapL2Xor>);
}

}
