#pragma once

#include <Interpreters/StorageID.h>
#include <Parsers/IAST.h>

class SipHash;

namespace DB
{

class ReadBuffer;

/// COPY query
class ASTCopyQuery : public IAST
{
public:
    enum class QueryType : uint8_t
    {
        COPY_FROM = 0,
        COPY_TO = 1,
    } type;

    ASTPtr data;
    ASTPtr file;

    String getID(char) const override { return "CopyQuery"; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTCopyQuery>(*this);
        res->children.clear();

        if (data)
        {
            res->data = data->clone();
            res->children.push_back(res->data);
        }
        if (file)
        {
            res->file = file->clone();
            res->children.push_back(res->file);
        }

        return res;
    }

    QueryKind getQueryKind() const override { return QueryKind::Copy; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
