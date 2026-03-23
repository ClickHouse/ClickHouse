#include <Parsers/ASTPartition.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

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
    auto res = make_intrusive<ASTPartition>(*this);
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

void ASTPartition::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "Partition");
    w.writeChild("value", value);
    w.writeChild("id", id);
    w.writeBool("all", all);
    if (fields_count.has_value())
        w.writeUInt("fields_count", *fields_count);
}

void ASTPartition::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    all = r.getBool("all");

    if (r.has("fields_count"))
        fields_count = r.getUInt("fields_count");

    auto val_child = r.readChild("value");
    if (val_child)
        setPartitionValue(val_child);

    if (!val_child)
    {
        auto id_child = r.readChild("id");
        if (id_child)
            setPartitionID(id_child);
    }
}

void ASTPartition::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (value)
    {
        value->format(ostr, settings, state, frame);
    }
    else if (all)
    {
        ostr << "ALL";
    }
    else
    {
        ostr << "ID ";
        id->format(ostr, settings, state, frame);
    }
}
}
