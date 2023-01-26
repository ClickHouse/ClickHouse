#pragma once

#include "SerializationCustomSimpleText.h"

#include <Common/IntervalKind.h>

namespace DB
{
class SerializationInterval : public SerializationCustomSimpleText
{
public:
    explicit SerializationInterval(IntervalKind kind_);

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &, bool whole) const override;

private:
    IntervalKind kind;
};
}
