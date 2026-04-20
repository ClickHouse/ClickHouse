#pragma once
#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

/// Helper class to define same ISerialization text (de)serialization for all the variants (escaped, quoted, JSON, CSV).
/// You need to define serializeText() and deserializeText() in derived class.
class SimpleTextSerialization : public ISerialization
{
protected:
    SimpleTextSerialization() = default;

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        serializeText(column, row_num, ostr, settings);
    }

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        serializeText(column, row_num, ostr, settings);
    }

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        serializeText(column, row_num, ostr, settings);
    }

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        serializeText(column, row_num, ostr, settings);
    }

    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeText(column, istr, settings, true);
    }

    bool tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        return tryDeserializeText(column, istr, settings, true);
    }

    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeText(column, istr, settings, false);
    }

    bool tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        return tryDeserializeText(column, istr, settings, false);
    }

    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeText(column, istr, settings, false);
    }

    bool tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        return tryDeserializeText(column, istr, settings, false);
    }

    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeText(column, istr, settings, false);
    }

    bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        return tryDeserializeText(column, istr, settings, false);
    }

    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeText(column, istr, settings, false);
    }

    bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        return tryDeserializeText(column, istr, settings, false);
    }

    /// whole = true means that buffer contains only one value, so we should read until EOF.
    /// It's needed to check if there is garbage after parsed field.
    virtual void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &, bool whole) const = 0;

    virtual bool tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
    {
        try
        {
            deserializeText(column, istr, settings, whole);
            return true;
        }
        catch (...)
        {
            return false;
        }
    }
};

}
