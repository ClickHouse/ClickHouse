#pragma once

#include <DataTypes/DataTypeCustom.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;
struct FormatSettings;
class IColumn;

/** Simple IDataTypeCustomTextSerialization that uses serializeText/deserializeText
 * for all serialization and deserialization. */
class DataTypeCustomSimpleTextSerialization : public IDataTypeCustomTextSerialization
{
public:
    virtual ~DataTypeCustomSimpleTextSerialization() override;

    // Methods that subclasses must override in order to get full serialization/deserialization support.
    virtual void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override = 0;
    virtual void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;

    /** Text serialization with escaping but without quoting.
      */
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const  override;

    /** Text serialization as a literal that may be inserted into a query.
      */
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const  override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const  override;

    /** Text serialization for the CSV format.
      */
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    /** delimiter - the delimiter we expect when reading a string value that is not double-quoted
      * (the delimiter is not consumed).
      */
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    /** Text serialization intended for using in JSON format.
      * force_quoting_64bit_integers parameter forces to brace UInt64 and Int64 types into quotes.
      */
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    /** Text serialization for putting into the XML format.
      */
    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
};

} // namespace DB
