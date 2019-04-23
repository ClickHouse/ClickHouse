#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeDomainWithSimpleSerialization.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING;
}

namespace
{

class DataTypeDomainBool final : public DataTypeDomainWithSimpleSerialization
{
private:
    static constexpr char str_true[5] = "true";
    static constexpr char str_false[6] = "false";
public:
    const char * getName() const override
    {
        return "Bool";
    }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override
    {
        const ColumnUInt8 * col = checkAndGetColumn<ColumnUInt8>(&column);

        if (!col)
            throw Exception(
                String(getName()) + " domain can only serialize columns of type UInt8."
                    + column.getName(),
                ErrorCodes::ILLEGAL_COLUMN
            );

        if (col->getData()[row_num])
            ostr.write(str_true, sizeof(str_true) - 1);
        else
            ostr.write(str_false, sizeof(str_false) - 1);
    }

    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override
    {
        ColumnUInt8 * col = typeid_cast<ColumnUInt8 *>(&column);

        if (!col)
            throw Exception(
                String(getName()) + " domain can only deserialize columns of type UInt8."
                    + column.getName(),
                ErrorCodes::ILLEGAL_COLUMN
            );

        if (!istr.eof())
        {
            bool value = false;

            if (*istr.position() == 't' || *istr.position() == 'f')
                readBoolTextWord(value, istr);
            else if (*istr.position() == '1' || *istr.position() == '0')
                readBoolText(value, istr);
            else
                throw Exception(
                    "Invalid boolean value, should be true, false, 1, or 0.",
                    ErrorCodes::CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING
                );

            col->insert(value);
        }
        else
            throw Exception(
                "Expected boolean value but get EOF.",
                ErrorCodes::CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING
            );
    }

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        // the same as text serialization
        serializeText(column, row_num, ostr, settings);
    }

    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        // the same as text deserialization
        deserializeText(column, istr, settings);
    }
};

}

void registerDataTypeDomainBool(DataTypeFactory & factory)
{
    factory.registerDataTypeDomain("UInt8", std::make_unique<DataTypeDomainBool>());
}

}
