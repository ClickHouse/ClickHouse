#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/formatIPv6.h>
#include <DataTypes/DataTypeDomainWithSimpleSerialization.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataTypeDomain.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsCoding.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int UNSUPPORTED_METHOD;
    extern const int CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING;
}

namespace
{

class DataTypeDomainIPv4 : public DataTypeDomainWithSimpleSerialization
{
public:
    const char * getName() const override
    {
        return "IPv4";
    }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override
    {
        const auto col = checkAndGetColumn<ColumnUInt32>(&column);
        if (!col)
        {
            throw Exception(String(getName()) + " domain can only serialize columns of type UInt32." + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
        }

        char buffer[IPV4_MAX_TEXT_LENGTH + 1] = {'\0'};
        char * ptr = buffer;
        formatIPv4(reinterpret_cast<const unsigned char *>(&col->getData()[row_num]), ptr);

        ostr.write(buffer, strlen(buffer));
    }

    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override
    {
        ColumnUInt32 * col = typeid_cast<ColumnUInt32 *>(&column);
        if (!col)
        {
            throw Exception(String(getName()) + " domain can only deserialize columns of type UInt32." + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
        }

        char buffer[IPV4_MAX_TEXT_LENGTH + 1] = {'\0'};
        istr.read(buffer, sizeof(buffer) - 1);
        UInt32 ipv4_value = 0;
        if (!parseIPv4(buffer, reinterpret_cast<unsigned char *>(&ipv4_value)))
        {
            throw Exception("Invalid IPv4 value.", ErrorCodes::CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING);
        }

        col->insert(ipv4_value);
    }
};

class DataTypeDomainIPv6 : public DataTypeDomainWithSimpleSerialization
{
public:
    const char * getName() const override
    {
        return "IPv6";
    }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override
    {
        const auto col = checkAndGetColumn<ColumnFixedString>(&column);
        if (!col)
        {
            throw Exception(String(getName()) + " domain can only serialize columns of type FixedString(16)." + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
        }

        char buffer[IPV6_MAX_TEXT_LENGTH + 1] = {'\0'};
        char * ptr = buffer;
        formatIPv6(reinterpret_cast<const unsigned char *>(col->getDataAt(row_num).data), ptr);

        ostr.write(buffer, strlen(buffer));
    }

    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override
    {
        ColumnFixedString * col = typeid_cast<ColumnFixedString *>(&column);
        if (!col)
        {
            throw Exception(String(getName()) + " domain can only deserialize columns of type FixedString(16)." + column.getName(), ErrorCodes::ILLEGAL_COLUMN);
        }

        char buffer[IPV6_MAX_TEXT_LENGTH + 1] = {'\0'};
        istr.read(buffer, sizeof(buffer) - 1);

        std::string ipv6_value(IPV6_BINARY_LENGTH, '\0');
        if (!parseIPv6(buffer, reinterpret_cast<unsigned char *>(ipv6_value.data())))
        {
            throw Exception(String("Invalid ") + getName() + " value.", ErrorCodes::CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING);
        }

        col->insertString(ipv6_value);
    }
};

} // namespace

void registerDataTypeDomainIPv4AndIPv6(DataTypeFactory & factory)
{
    factory.registerDataTypeDomain("UInt32", std::make_unique<DataTypeDomainIPv4>());
    factory.registerDataTypeDomain("FixedString(16)", std::make_unique<DataTypeDomainIPv6>());
}

} // namespace DB
