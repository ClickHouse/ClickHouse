#pragma once

#include <AggregateFunctions/IAggregateFunction.h>

#include <DataTypes/IDataType.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_DEFAULT_VALUE;
    extern const int NOT_IMPLEMENTED;
}


/** Type - the state of the aggregate function.
  * Type parameters is an aggregate function, the types of its arguments, and its parameters (for parametric aggregate functions).
  */
class DataTypeAggregateFunction final : public IDataType
{
private:
    AggregateFunctionPtr function;
    DataTypes argument_types;
    Array parameters;

public:
    DataTypeAggregateFunction(const AggregateFunctionPtr & function_, const DataTypes & argument_types_, const Array & parameters_)
        : function(function_), argument_types(argument_types_), parameters(parameters_)
    {
    }

    std::string getFunctionName() const { return function->getName(); }
    AggregateFunctionPtr getFunction() const { return function; }

    std::string getName() const override;

    DataTypePtr getReturnType() const { return function->getReturnType(); };
    DataTypes getArgumentsDataTypes() const { return argument_types; }

    DataTypePtr clone() const override { return std::make_shared<DataTypeAggregateFunction>(function, argument_types, parameters); }

    /// NOTE These two functions for serializing single values are incompatible with the functions below.
    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr) const override;

    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const override;

    ColumnPtr createColumn() const override;
    ColumnPtr createConstColumn(size_t size, const Field & field) const override;

    Field getDefault() const override;
};


}

