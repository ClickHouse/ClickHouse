#include <Parsers/ASTError.h>

#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTToJSON.h>
#include <Parsers/PartialASTCapture.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ASTPtr ASTError::clone() const
{
    return make_intrusive<ASTError>(*this);
}

void ASTError::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "Error");
    w.writeUInt("begin", begin_offset);
    w.writeUInt("end", end_offset);

    w.writeKey("expected");
    out << '[';
    for (size_t i = 0; i < expected.size(); ++i)
    {
        if (i > 0)
            out << ',';
        writeJSONString(expected[i], out, w.getFormatSettings());
    }
    out << ']';
}

void ASTError::setOffsets(const char * query_begin)
{
    begin_offset = static_cast<UInt64>(begin_pos - query_begin);
    end_offset = static_cast<UInt64>(end_pos - query_begin);
}

ASTPtr ASTPartialNode::clone() const
{
    auto res = make_intrusive<ASTPartialNode>(*this);
    res->children.clear();
    res->node = node->clone();
    res->failed_value = failed_value->clone();
    res->children = {res->node, res->failed_value};
    return res;
}

void ASTPartialNode::writeJSON(WriteBuffer & out) const
{
    if (!isPartialASTSlotKey(*node, failed_key))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Key '{}' is not a capture slot of {}", failed_key, node->getID());

    const String json = serializeASTToJSON(*node);
    if (json.empty() || json.back() != '}')
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The JSON of {} is not an object", node->getID());

    out.write(json.data(), json.size() - 1);
    out << ",\"" << failed_key << "\":";
    failed_value->writeJSON(out);
    out << '}';
}

}
