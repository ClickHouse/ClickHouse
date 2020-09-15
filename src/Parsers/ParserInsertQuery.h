#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{


/** Cases:
  *
  * Normal case:
  * INSERT INTO [db.]table (c1, c2, c3) VALUES (v11, v12, v13), (v21, v22, v23), ...
  * INSERT INTO [db.]table VALUES (v11, v12, v13), (v21, v22, v23), ...
  *
  * Insert of data in an arbitrary format.
  * The data itself comes after LF(line feed), if it exists, or after all the whitespace characters, otherwise.
  * INSERT INTO [db.]table (c1, c2, c3) FORMAT format \n ...
  * INSERT INTO [db.]table FORMAT format \n ...
  *
  * Insert the result of the SELECT or WATCH query.
  * INSERT INTO [db.]table (c1, c2, c3) SELECT | WATCH  ...
  * INSERT INTO [db.]table SELECT | WATCH ...
  */
class ParserInsertQuery : public IParserBase
{
private:
    const char * end;

    const char * getName() const override { return "INSERT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    ParserInsertQuery(const char * end_) : end(end_) {}
};

/** Insert accepts an identifier and an asterisk with variants.
  */
class ParserInsertElement : public IParserBase
{
protected:
    const char * getName() const override { return "insert element"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
