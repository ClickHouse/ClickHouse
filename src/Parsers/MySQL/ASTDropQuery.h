#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/MySQL/ASTDeclareIndex.h>
#include <Parsers/MySQL/ASTDeclareColumn.h>
#include <Parsers/MySQL/ASTDeclareTableOptions.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace MySQLParser
{

class ASTDropQuery : public IAST
{
public:
    enum Kind
    {
        Table,
        View,
        Database,
        Index,
        /// TRIGGER,FUNCTION,EVENT and so on, No need for support
        Other,
    };
    Kind kind;
    struct QualifiedName
    {
        String schema;
        String shortName;
    };

    using QualifiedNames = std::vector<QualifiedName>;
    QualifiedNames names;
    bool if_exists{false};
    //drop or truncate
    bool is_truncate{false};

    ASTPtr clone() const override;
    String getID(char /*delim*/) const override {return "ASTDropQuery" ;}

protected:
    void formatImpl(WriteBuffer & /*ostr*/, const FormatSettings & /*settings*/, FormatState & /*state*/, FormatStateStacked /*frame*/) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method formatImpl is not supported by MySQLParser::ASTDropQuery.");
    }
};

class ParserDropQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

}
