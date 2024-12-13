#include <Parsers/ASTPartition.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void ASTPartition::setPartitionID(const ASTPtr & ast)
{
    if (children.empty())
    {
        children.push_back(ast);
        id = children[0].get();
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot have multiple children for partition AST");
}
void ASTPartition::setPartitionValue(const ASTPtr & ast)
{
    if (children.empty())
    {
        children.push_back(ast);
        value = children[0].get();
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot have multiple children for partition AST");
}


String ASTPartition::getID(char delim) const
{
    if (value)
        return "Partition";

    std::string id_string = id ? id->getID() : "";
    return "Partition_ID" + (delim + id_string);
}

ASTPtr ASTPartition::clone() const
{
    auto res = std::make_shared<ASTPartition>(*this);
    res->children.clear();

    if (value)
    {
        res->children.push_back(children[0]->clone());
        res->value = res->children[0].get();
    }

    if (id)
    {
        res->children.push_back(children[0]->clone());
        res->id = res->children[0].get();
    }

    return res;
}

void ASTPartition::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (value)
    {
        value->formatImpl(ostr, settings, state, frame);
    }
    else if (all)
    {
        ostr << "ALL";
    }
    else
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "ID " << (settings.hilite ? hilite_none : "");
        id->formatImpl(ostr, settings, state, frame);
    }
}
}
