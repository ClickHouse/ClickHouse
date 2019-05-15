#pragma once

#include <memory>
#include <cstddef>
#include <Core/Types.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;
struct FormatSettings;
class IColumn;

/** Allow to customize an existing data type and set a different name and/or text serialization/deserialization methods.
 * See use in IPv4 and IPv6 data types, and also in SimpleAggregateFunction.
  */
class IDataTypeCustomName
{
public:
    virtual ~IDataTypeCustomName() {}

    virtual String getName() const = 0;
};

class IDataTypeCustomTextSerialization
{
public:
    virtual ~IDataTypeCustomTextSerialization() {}

    /** Text serialization for displaying on a terminal or saving into a text file, and the like.
      * Without escaping or quoting.
      */
    virtual void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;

    /** Text serialization with escaping but without quoting.
      */
    virtual void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;

    /** Text serialization as a literal that may be inserted into a query.
      */
    virtual void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;

    /** Text serialization for the CSV format.
      */
    virtual void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;

    /** Text serialization intended for using in JSON format.
      */
    virtual void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const = 0;
    virtual void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;

    /** Text serialization for putting into the XML format.
      */
    virtual void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const = 0;
};

using DataTypeCustomNamePtr = std::unique_ptr<const IDataTypeCustomName>;
using DataTypeCustomTextSerializationPtr = std::unique_ptr<const IDataTypeCustomTextSerialization>;

/** Describe a data type customization
 */
struct DataTypeCustomDesc
{
    DataTypeCustomNamePtr name;
    DataTypeCustomTextSerializationPtr text_serialization;

    DataTypeCustomDesc(DataTypeCustomNamePtr name_, DataTypeCustomTextSerializationPtr text_serialization_)
            : name(std::move(name_)), text_serialization(std::move(text_serialization_)) {}
};

using DataTypeCustomDescPtr = std::unique_ptr<DataTypeCustomDesc>;

/** A simple implementation of IDataTypeCustomName
 */
class DataTypeCustomFixedName : public IDataTypeCustomName
{
private:
    String name;
public:
    DataTypeCustomFixedName(String name_) : name(name_) {}
    String getName() const override { return name; }
};

} // namespace DB
