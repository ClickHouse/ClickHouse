#include <Parsers/ASTPartition.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

namespace DB
{

String ASTPartition::getID(char delim) const
{
    if (value)
        return "Partition";
    else
        return "Partition_ID" + (delim + id);
}

ASTPtr ASTPartition::clone() const
{
    auto res = std::make_shared<ASTPartition>(*this);
    res->children.clear();

    if (value)
    {
        res->value = value->clone();
        res->children.push_back(res->value);
    }

    return res;
}

void ASTPartition::formatImpl(const FormattingBuffer & out) const
{
    if (value)
    {
        value->formatImpl(out);
    }
    else
    {
        if (all)
            out.ostr << "ALL";
        else
        {
            out.writeKeyword("ID ");
            WriteBufferFromOwnString id_buf;
            writeQuoted(id, id_buf);
            out.ostr << id_buf.str();
        }
    }
}

}
