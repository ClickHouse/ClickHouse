#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ASTCopyQuery.h>


namespace DB
{


/* Useful for expressions like COPY table_name FROM/TO output_file.
 * This Parser is relevant only for Postgres wire protocol.
 * For more information see https://www.postgresql.org/docs/current/sql-copy.html
 *
 * This class parse table name and copy query type (from or to) and put it in ASTCopyQuery.
 */
class ParserCopyQuery : public IParserBase
{
private:
    bool parseOptions(Pos & pos, std::shared_ptr<ASTCopyQuery> node, Expected & expected);

protected:
    const char * getName() const override { return "COPY query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    ParserCopyQuery() = default;
};

}
