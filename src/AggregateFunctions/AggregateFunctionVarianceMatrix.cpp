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
    factory.registerFunction("covarSampMatrix", createAggregateFunctionVarianceMatrix<AggregateFunctionCovarSampMatrix>);
    factory.registerFunction("covarPopMatrix", createAggregateFunctionVarianceMatrix<AggregateFunctionCovarPopMatrix>);
    factory.registerFunction("corrMatrix", createAggregateFunctionVarianceMatrix<AggregateFunctionCorrMatrix>);
}

}
