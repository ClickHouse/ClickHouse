#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Columns/ColumnArray.h>
#include <Common/PODArray_fwd.h>
#include <DataTypes/DataTypeArray.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Moments.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

enum class StatisticsMatrixFunctionKind : uint8_t
{
    covarPopMatrix,
    covarSampMatrix,
    corrMatrix
};

template <StatisticsMatrixFunctionKind _kind>
struct AggregateFunctionVarianceMatrixData
{
    using DataType = std::conditional_t<_kind == StatisticsMatrixFunctionKind::corrMatrix, CorrMoments<Float64>, CovarMoments<Float64>>;

    AggregateFunctionVarianceMatrixData() = default;

    explicit AggregateFunctionVarianceMatrixData(const size_t _num_args)
        : num_args(_num_args)
    {
        data_matrix.resize_fill(num_args * (num_args + 1) / 2, DataType());
    }

    void add(const IColumn ** column, const size_t row_num)
    {
        for (size_t i = 0; i < num_args; ++i)
            for (size_t j = 0; j <= i; ++j)
                 data_matrix[i * (i + 1) / 2 + j].add(column[i]->getFloat64(row_num), column[j]->getFloat64(row_num));
    }

    void merge(const AggregateFunctionVarianceMatrixData & other)
    {
        for (size_t i = 0; i < num_args; ++i)
            for (size_t j = 0; j <= i; ++j)
                data_matrix[i * (i + 1) / 2 + j].merge(other.data_matrix[i * (i + 1) / 2 + j]);
    }

    void serialize(WriteBuffer & buf) const
    {
        for (size_t i = 0; i < num_args; ++i)
            for (size_t j = 0; j <= i; ++j)
                data_matrix[i * (i + 1) / 2 + j].write(buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        for (size_t i = 0; i < num_args; ++i)
            for (size_t j = 0; j <= i; ++j)
                data_matrix[i * (i + 1) / 2 + j].read(buf);
    }

    void insertResultInto(IColumn & to) const
    {
        auto & data_to = assert_cast<ColumnFloat64 &>(assert_cast<ColumnArray &>(assert_cast<ColumnArray &>(to).getData()).getData()).getData();
        auto & root_offsets_to = assert_cast<ColumnArray &>(to).getOffsets();
        auto & nested_offsets_to = assert_cast<ColumnArray &>(assert_cast<ColumnArray &>(to).getData()).getOffsets();
        for (size_t i = 0; i < num_args; ++i)
        {
            for (size_t j = 0; j < num_args; ++j)
            {
                auto & data = i < j ? data_matrix[j * (j + 1) / 2 + i] : data_matrix[i * (i + 1) / 2 + j];
                if constexpr (kind == StatisticsMatrixFunctionKind::covarPopMatrix)
                    data_to.push_back(data.getPopulation());
                if constexpr (kind == StatisticsMatrixFunctionKind::covarSampMatrix)
                    data_to.push_back(data.getSample());
                if constexpr (kind == StatisticsMatrixFunctionKind::corrMatrix)
                    data_to.push_back(data.get());
            }
            nested_offsets_to.push_back(nested_offsets_to.back() + num_args);
        }
        root_offsets_to.push_back(root_offsets_to.back() + num_args);
    }

    static constexpr StatisticsMatrixFunctionKind kind = _kind;
    PaddedPODArray<DataType> data_matrix;
    size_t num_args;
};

template <typename Data>
class AggregateFunctionVarianceMatrix final
    : public IAggregateFunctionDataHelper<Data, AggregateFunctionVarianceMatrix<Data>>
{
public:
    explicit AggregateFunctionVarianceMatrix(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionVarianceMatrix<Data>>(argument_types_, {}, createResultType())
    {}

    AggregateFunctionVarianceMatrix(const IDataType &, const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionVarianceMatrix<Data>>(argument_types_, {}, createResultType())
    {}

    String getName() const override
    {
        switch (Data::kind)
        {
            case StatisticsMatrixFunctionKind::covarPopMatrix: return "covarPopMatrix";
            case StatisticsMatrixFunctionKind::covarSampMatrix: return "covarSampMatrix";
            case StatisticsMatrixFunctionKind::corrMatrix: return "corrMatrix";
        }
    }

    void create(AggregateDataPtr __restrict place) const override /// NOLINT
    {
        new (place) Data(this->argument_types.size());
    }

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>()));
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).add(columns, row_num);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to);
    }
};

using AggregateFunctionCovarPopMatrix = AggregateFunctionVarianceMatrix<AggregateFunctionVarianceMatrixData<StatisticsMatrixFunctionKind::covarPopMatrix>>;
using AggregateFunctionCovarSampMatrix = AggregateFunctionVarianceMatrix<AggregateFunctionVarianceMatrixData<StatisticsMatrixFunctionKind::covarSampMatrix>>;
using AggregateFunctionCorrMatrix = AggregateFunctionVarianceMatrix<AggregateFunctionVarianceMatrixData<StatisticsMatrixFunctionKind::corrMatrix>>;


template <typename FunctionTemplate>
AggregateFunctionPtr createAggregateFunctionVarianceMatrix(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    for (const auto & argument_type : argument_types)
        if (!isNativeNumber(argument_type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function {} only supports numerical types", name);

    return std::make_shared<FunctionTemplate>(argument_types);
}

}

void registerAggregateFunctionsVarianceMatrix(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description covarSampMatrix_description = R"(
Returns the sample covariance matrix over N variables.
    )";
    FunctionDocumentation::Syntax covarSampMatrix_syntax = "covarSampMatrix(x1[, x2, ...])";
    FunctionDocumentation::Arguments covarSampMatrix_arguments = {
        {"x1[, x2, ...]", "One or more parameters over which to compute the sample covariance matrix.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::Parameters covarSampMatrix_parameters = {};
    FunctionDocumentation::ReturnedValue covarSampMatrix_returned_value = {"Returns the sample covariance matrix.", {"Array(Array(Float64))"}};
    FunctionDocumentation::Examples covarSampMatrix_examples = {
    {
        "Basic sample covariance matrix calculation",
        R"(
DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    a UInt32,
    b Float64,
    c Float64,
    d Float64
)
ENGINE = Memory;
INSERT INTO test(a, b, c, d) VALUES (1, 5.6, -4.4, 2.6), (2, -9.6, 3, 3.3), (3, -1.3, -4, 1.2), (4, 5.3, 9.7, 2.3), (5, 4.4, 0.037, 1.222), (6, -8.6, -7.8, 2.1233), (7, 5.1, 9.3, 8.1222), (8, 7.9, -3.6, 9.837), (9, -8.2, 0.62, 8.43555), (10, -3, 7.3, 6.762);

SELECT arrayMap(x -> round(x, 3), arrayJoin(covarSampMatrix(a, b, c, d))) AS covarSampMatrix
FROM test
        )",
        R"(
┌─covarSampMatrix─────────────┐
│ [9.167,-1.956,4.534,7.498]  │
│ [-1.956,45.634,7.206,2.369] │
│ [4.534,7.206,38.011,5.283]  │
│ [7.498,2.369,5.283,11.034]  │
└─────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category covarSampMatrix_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn covarSampMatrix_introduced_in = {23, 2};
    FunctionDocumentation covarSampMatrix_documentation = {covarSampMatrix_description, covarSampMatrix_syntax, covarSampMatrix_arguments, covarSampMatrix_parameters, covarSampMatrix_returned_value, covarSampMatrix_examples, covarSampMatrix_introduced_in, covarSampMatrix_category};
    factory.registerFunction("covarSampMatrix", {createAggregateFunctionVarianceMatrix<AggregateFunctionCovarSampMatrix>, covarSampMatrix_documentation});

    FunctionDocumentation::Description covarPopMatrix_description = R"(
Returns the population covariance matrix over N variables.
    )";
    FunctionDocumentation::Syntax covarPopMatrix_syntax = "covarPopMatrix(x1[, x2, ...])";
    FunctionDocumentation::Arguments covarPopMatrix_arguments = {
        {"x1[, x2, ...]", "A variable number of parameters.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::Parameters covarPopMatrix_parameters = {};
    FunctionDocumentation::ReturnedValue covarPopMatrix_returned_value = {"Returns the population covariance matrix.", {"Array(Array(Float64))"}};
    FunctionDocumentation::Examples covarPopMatrix_examples = {
    {
        "Basic population covariance matrix calculation",
        R"(
DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    a UInt32,
    b Float64,
    c Float64,
    d Float64
)
ENGINE = Memory;
INSERT INTO test(a, b, c, d) VALUES (1, 5.6, -4.4, 2.6), (2, -9.6, 3, 3.3), (3, -1.3, -4, 1.2), (4, 5.3, 9.7, 2.3), (5, 4.4, 0.037, 1.222), (6, -8.6, -7.8, 2.1233), (7, 5.1, 9.3, 8.1222), (8, 7.9, -3.6, 9.837), (9, -8.2, 0.62, 8.43555), (10, -3, 7.3, 6.762);

SELECT arrayMap(x -> round(x, 3), arrayJoin(covarPopMatrix(a, b, c, d))) AS covarPopMatrix
FROM test
        )",
        R"(
┌─covarPopMatrix────────────┐
│ [8.25,-1.76,4.08,6.748]   │
│ [-1.76,41.07,6.486,2.132] │
│ [4.08,6.486,34.21,4.755]  │
│ [6.748,2.132,4.755,9.93]  │
└───────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category covarPopMatrix_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn covarPopMatrix_introduced_in = {23, 2};
    FunctionDocumentation covarPopMatrix_documentation = {covarPopMatrix_description, covarPopMatrix_syntax, covarPopMatrix_arguments, covarPopMatrix_parameters, covarPopMatrix_returned_value, covarPopMatrix_examples, covarPopMatrix_introduced_in, covarPopMatrix_category};

    factory.registerFunction("covarPopMatrix", {createAggregateFunctionVarianceMatrix<AggregateFunctionCovarPopMatrix>, covarPopMatrix_documentation});

    FunctionDocumentation::Description corrMatrix_description = R"(
Computes the correlation matrix over N variables.
    )";
    FunctionDocumentation::Syntax corrMatrix_syntax = "corrMatrix(x1[, x2, ...])";
    FunctionDocumentation::Arguments corrMatrix_arguments = {
        {"x1[, x2, ...]", "One or more parameters for which to compute the correlation matrix over.", {"(U)Int8/16/32/64", "Float*"}}
    };
    FunctionDocumentation::Parameters corrMatrix_parameters = {};
    FunctionDocumentation::ReturnedValue corrMatrix_returned_value = {"Returns the correlation matrix.", {"Array(Array(Float64))"}};
    FunctionDocumentation::Examples corrMatrix_examples = {
    {
        "Basic correlation matrix calculation",
        R"(
DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    a UInt32,
    b Float64,
    c Float64,
    d Float64
)
ENGINE = Memory;
INSERT INTO test(a, b, c, d) VALUES (1, 5.6, -4.4, 2.6), (2, -9.6, 3, 3.3), (3, -1.3, -4, 1.2), (4, 5.3, 9.7, 2.3), (5, 4.4, 0.037, 1.222), (6, -8.6, -7.8, 2.1233), (7, 5.1, 9.3, 8.1222), (8, 7.9, -3.6, 9.837), (9, -8.2, 0.62, 8.43555), (10, -3, 7.3, 6.762);

SELECT arrayMap(x -> round(x, 3), arrayJoin(corrMatrix(a, b, c, d))) AS corrMatrix
FROM test
        )",
        R"(
┌─corrMatrix─────────────┐
│ [1,-0.096,0.243,0.746] │
│ [-0.096,1,0.173,0.106] │
│ [0.243,0.173,1,0.258]  │
│ [0.746,0.106,0.258,1]  │
└────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category corrMatrix_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn corrMatrix_introduced_in = {23, 2};
    FunctionDocumentation corrMatrix_documentation = {corrMatrix_description, corrMatrix_syntax, corrMatrix_arguments, corrMatrix_parameters, corrMatrix_returned_value, corrMatrix_examples, corrMatrix_introduced_in, corrMatrix_category};

    factory.registerFunction("corrMatrix", {createAggregateFunctionVarianceMatrix<AggregateFunctionCorrMatrix>, corrMatrix_documentation});
}

}
