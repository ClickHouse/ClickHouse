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
    FunctionDocumentation::Description description = R"(
Creates a bitmap (bit array) from a column of unsigned integers, then returns the count of unique values (cardinality) in that bitmap.
By appending the `-State` combinator suffix, instead of returning the count, it returns the actual [bitmap object](/sql-reference/functions/bitmap-functions).
    )";
    FunctionDocumentation::Syntax syntax = R"(
groupBitmap(expr)
groupBitmapState(expr)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Expression that results in a `UInt*` type.", {"UInt*"}}
    };
    FunctionDocumentation::Parameters parameters = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the count of type UInt64 type, or a bitmap object when using `-State`.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
CREATE TABLE t (UserID UInt32) ENGINE = Memory;
INSERT INTO t VALUES (1), (1), (2), (3);

SELECT groupBitmap(UserID) AS num FROM t;
        )",
        R"(
┌─num─┐
│   3 │
└─────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction("groupBitmap", {createAggregateFunctionBitmap<AggregateFunctionGroupBitmapData>, documentation});

    FunctionDocumentation::Description description_and = R"(
Calculates the AND of a bitmap column and returns it's cardinality.
If suffix combinator [`-State`](/sql-reference/aggregate-functions/combinators#-state) is added, then it returns a bitmap object.
    )";
    FunctionDocumentation::Syntax syntax_and = R"(
groupBitmapAnd(expr)
groupBitmapAndState(expr)
    )";
    FunctionDocumentation::Arguments arguments_and = {
        {"expr", "Expression that results in an `AggregateFunction(groupBitmap, UInt*)` type.", {"AggregateFunction(groupBitmap, UInt*)"}}
    };
    FunctionDocumentation::Parameters parameters_and = {};
    FunctionDocumentation::ReturnedValue returned_value_and = {"Returns a count of type `UInt64`, or a bitmap object when using `-State`.", {"UInt64"}};
    FunctionDocumentation::Examples examples_and = {
    {
        "Usage example",
        R"(
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2 VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] AS Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] AS Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] AS Array(UInt32))));

SELECT groupBitmapAnd(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
        )",
        R"(
┌─groupBitmapAnd(z)─┐
│               3   │
└───────────────────┘
        )"
    },
    {
        "Using -State combinator",
        R"(
SELECT arraySort(bitmapToArray(groupBitmapAndState(z))) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
        )",
        R"(
┌─arraySort(bitmapToArray(groupBitmapAndState(z)))─┐
│ [6,8,10]                                         │
└──────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_and = {20, 1};
    FunctionDocumentation::Category category_and = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_and = {description_and, syntax_and, arguments_and, parameters_and, returned_value_and, examples_and, introduced_in_and, category_and};

    factory.registerFunction("groupBitmapAnd", {createAggregateFunctionBitmapL2<AggregateFunctionBitmapL2And>, documentation_and});

    FunctionDocumentation::Description description_or = R"(
Calculates the OR of a bitmap column and returns it's cardinality.
If suffix combinator [`-State`](/sql-reference/aggregate-functions/combinators#-state) is added, then it returns a bitmap object.
This is equivalent to `groupBitmapMerge` ([`groupBitmap`](/sql-reference/aggregate-functions/reference/groupbitmap) with the [`-Merge`](/sql-reference/aggregate-functions/combinators#-merge) combinator suffix).
    )";
    FunctionDocumentation::Syntax syntax_or = R"(
groupBitmapOr(expr)
groupBitmapOrState(expr)
    )";
    FunctionDocumentation::Arguments arguments_or = {
        {"expr", "Expression that results in an `AggregateFunction(groupBitmap, UInt*)` type.", {"AggregateFunction(groupBitmap, UInt*)"}}
    };
    FunctionDocumentation::Parameters parameters_or = {};
    FunctionDocumentation::ReturnedValue returned_value_or = {"Returns a count of type `UInt64`, or a bitmap object when using `-State`.", {"UInt64"}};
    FunctionDocumentation::Examples examples_or = {
    {
        "Usage example",
        R"(
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2 VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] AS Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] AS Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] AS Array(UInt32))));

SELECT groupBitmapOr(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
        )",
        R"(
┌─groupBitmapOr(z)─┐
│             15   │
└──────────────────┘
        )"
    },
    {
        "Using -State combinator",
        R"(
SELECT arraySort(bitmapToArray(groupBitmapOrState(z))) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
        )",
        R"(
┌─arraySort(bitmapToArray(groupBitmapOrState(z)))─┐
│ [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]           │
└─────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_or = {20, 1};
    FunctionDocumentation::Category category_or = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_or = {description_or, syntax_or, arguments_or, parameters_or, returned_value_or, examples_or, introduced_in_or, category_or};

    factory.registerFunction("groupBitmapOr", {createAggregateFunctionBitmapL2<AggregateFunctionBitmapL2Or>, documentation_or});

    FunctionDocumentation::Description description_xor = R"(
Calculates the XOR of a bitmap column and returns it's cardinality.
If suffix combinator [`-State`](/sql-reference/aggregate-functions/combinators#-state) is added, then it returns a bitmap object.
    )";
    FunctionDocumentation::Syntax syntax_xor = R"(
groupBitmapXor(expr)
groupBitmapXorState(expr)
    )";
    FunctionDocumentation::Arguments arguments_xor = {
        {"expr", "Expression that results in an `AggregateFunction(groupBitmap, UInt*)` type.", {"AggregateFunction(groupBitmap, UInt*)"}}
    };
    FunctionDocumentation::Parameters parameters_xor = {};
    FunctionDocumentation::ReturnedValue returned_value_xor = {"Returns a count of type `UInt64`, or a bitmap object when using `-State`.", {"UInt64"}};
    FunctionDocumentation::Examples examples_xor = {
    {
        "Usage example",
        R"(
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2 VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] AS Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] AS Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] AS Array(UInt32))));

SELECT groupBitmapXor(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
        )",
        R"(
┌─groupBitmapXor(z)─┐
│              10   │
└───────────────────┘
        )"
    },
    {
        "Using -State combinator",
        R"(
SELECT arraySort(bitmapToArray(groupBitmapXorState(z))) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
        )",
        R"(
┌─arraySort(bitmapToArray(groupBitmapXorState(z)))─┐
│ [1, 3, 5, 6, 8, 10, 11, 13, 14, 15]              │
└──────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_xor = {20, 1};
    FunctionDocumentation::Category category_xor = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_xor = {description_xor, syntax_xor, arguments_xor, parameters_xor, returned_value_xor, examples_xor, introduced_in_xor, category_xor};

    factory.registerFunction("groupBitmapXor", {createAggregateFunctionBitmapL2<AggregateFunctionBitmapL2Xor>, documentation_xor});
}

}
