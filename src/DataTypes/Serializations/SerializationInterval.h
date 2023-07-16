#pragma once

#include <DataTypes/Serializations/SerializationCustomSimpleText.h>
#include <Common/IntervalKind.h>


namespace DB
{

struct FormatSettings;
class ReadBuffer;
class WriteBuffer;
class IColumn;


/** Provides two formatting options: numeric (simply as a number) or Kusto (weird for Kusto query language).
  */
class SerializationInterval : public SerializationCustomSimpleText
{
public:
    explicit SerializationInterval(IntervalKind kind_);

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &, bool whole) const override;

private:
    IntervalKind interval_kind;
};

}
