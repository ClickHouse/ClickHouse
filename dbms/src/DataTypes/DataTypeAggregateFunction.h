#pragma once

#include <AggregateFunctions/IAggregateFunction.h>

#include <DataTypes/IDataType.h>


namespace DB
{

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
    static constexpr bool is_parametric = true;

    DataTypeAggregateFunction(const AggregateFunctionPtr & function_, const DataTypes & argument_types_, const Array & parameters_)
        : function(function_), argument_types(argument_types_), parameters(parameters_)
    {
    }

    std::string getFunctionName() const { return function->getName(); }
    AggregateFunctionPtr getFunction() const { return function; }

    std::string getName() const override;

    const char * getFamilyName() const override { return "AggregateFunction"; }

    bool canBeInsideNullable() const override { return false; }

    DataTypePtr getReturnType() const { return function->getReturnType(); }
    DataTypes getArgumentsDataTypes() const { return argument_types; }

    /// NOTE These two functions for serializing single values are incompatible with the functions below.
    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr) const override;

    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool shouldAlignRightInPrettyFormats() const override { return false; }
};


}

