#pragma once

#include <Core/Types_fwd.h>
#include <Columns/IColumn.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>

#include <concepts>

namespace DB
{

/** Serialization for non-numeric non-string data types serialized as JSON strings
 * For these data types, we support an option, input_format_json_empty_as_default, which, when set to 1,
 * allows for JSON deserialization to treat an encountered empty string as a default value for the specified type.
 * Derived classes must implement the following methods:
 * deserializeTextNoEmptyCheckJSON() and tryDeserializeTextNoEmptyCheckJSON()
 * instead of deserializeTextJSON() and tryDeserializeTextJSON() respectively.
 */
template <typename T>
requires std::derived_from<T, ISerialization>
class SerializationAsStringNonTrivialJSON : public T
{
public:
    using T::T;

    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & format_settings) const override
    {
        if (format_settings.json.empty_as_default && tryMatchEmptyString(istr))
            column.insertDefault();
        else
           deserializeTextNoEmptyCheckJSON(column, istr, format_settings);
    }

    bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & format_settings) const override
    {
        if (format_settings.json.empty_as_default && tryMatchEmptyString(istr))
        {
            column.insertDefault();
            return true;
        }
        else
           return tryDeserializeTextNoEmptyCheckJSON(column, istr, format_settings);
    }

    virtual void deserializeTextNoEmptyCheckJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override = 0;

    virtual bool tryDeserializeTextNoEmptyCheckJSON(IColumn & /*column*/, ReadBuffer & /*istr*/, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method tryDeserializeTextNoEmptyCheckJSON is not supported");
    }
};

}
