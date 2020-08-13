#include <Core/MySQL/PacketsProtocolText.h>
#include <Core/MySQLProtocol.h>

namespace DB
{

namespace MySQLProtocol
{

namespace ProtocolText
{

ResultSetRow::ResultSetRow(const DataTypes & data_types, const Columns & columns_, int row_num_)
    : columns(columns_), row_num(row_num_)
{
    for (size_t i = 0; i < columns.size(); i++)
    {
        if (columns[i]->isNullAt(row_num))
        {
            payload_size += 1;
            serialized.emplace_back("\xfb");
        }
        else
        {
            WriteBufferFromOwnString ostr;
            data_types[i]->serializeAsText(*columns[i], row_num, ostr, FormatSettings());
            payload_size += getLengthEncodedStringSize(ostr.str());
            serialized.push_back(std::move(ostr.str()));
        }
    }
}

size_t ResultSetRow::getPayloadSize() const
{
    return payload_size;
}

void ResultSetRow::writePayloadImpl(WriteBuffer & buffer) const
{
    for (size_t i = 0; i < columns.size(); i++)
    {
        if (columns[i]->isNullAt(row_num))
            buffer.write(serialized[i].data(), 1);
        else
            writeLengthEncodedString(serialized[i], buffer);
    }
}

}

}

}
