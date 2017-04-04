#pragma once

#include <Core/Block.h>
#include <Core/Names.h>
#include <DataStreams/IRowOutputStream.h>


namespace DB
{

class WriteBuffer;
class Context;


/** Stream to output data in format "each value in separate row".
  * Usable to show few rows with many columns.
  */
class VerticalRowOutputStream : public IRowOutputStream
{
public:
    VerticalRowOutputStream(WriteBuffer & ostr_, const Block & sample_, const Context & context);

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeRowStartDelimiter() override;
    void writeRowBetweenDelimiter() override;

    void flush() override;

protected:
    virtual void writeValue(const IColumn & column, const IDataType & type, size_t row_num) const;

    WriteBuffer & ostr;
    const Block sample;
    size_t field_number = 0;
    size_t row_number = 0;

    using NamesAndPaddings = std::vector<String>;
    NamesAndPaddings names_and_paddings;
};


/** Same but values are printed without escaping.
  */
class VerticalRawRowOutputStream final : public VerticalRowOutputStream
{
public:
    VerticalRawRowOutputStream(WriteBuffer & ostr_, const Block & sample_, const Context & context)
        : VerticalRowOutputStream(ostr_, sample_, context) {}

protected:
    void writeValue(const IColumn & column, const IDataType & type, size_t row_num) const override;
};

}

