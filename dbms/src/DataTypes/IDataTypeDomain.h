#pragma once

#include <cstddef>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;
struct FormatSettings;
class IColumn;

/** Allow to customize an existing data type and set a different name. Derived class IDataTypeDomainCustomSerialization allows
 * further customization of serialization/deserialization methods. See use in IPv4 and IPv6 data type domains.
 *
 * IDataTypeDomain can be chained for further delegation (only for getName for the moment).
  */
class IDataTypeDomain
{
private:
    mutable DataTypeDomainPtr delegate;

public:
    virtual ~IDataTypeDomain() {}

    String getName() const
    {
        if (delegate)
            return delegate->getName();
        else
            return doGetName();
    }

    void appendDomain(DataTypeDomainPtr delegate_) const
    {
        if (delegate == nullptr)
            delegate = std::move(delegate_);
        else
            delegate->appendDomain(std::move(delegate_));
    }

    const IDataTypeDomain * getDomain() const { return delegate.get(); }

protected:
    virtual String doGetName() const = 0;
};

class IDataTypeDomainCustomSerialization : public IDataTypeDomain
{
public:
    virtual ~IDataTypeDomainCustomSerialization() {}

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

} // namespace DB
