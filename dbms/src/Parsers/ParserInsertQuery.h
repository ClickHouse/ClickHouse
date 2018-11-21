#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{


/** Cases:
  *
  * #1 Normal case:
  * INSERT INTO [db.]table (c1, c2, c3) VALUES (v11, v12, v13), (v21, v22, v23), ...
  * INSERT INTO [db.]table VALUES (v11, v12, v13), (v21, v22, v23), ...
  *
  * #2 Insert of data in an arbitrary format.
  * The data itself comes after LF(line feed), if it exists, or after all the whitespace characters, otherwise.
  * INSERT INTO [db.]table (c1, c2, c3) FORMAT format \n ...
  * INSERT INTO [db.]table FORMAT format \n ...
  *
  * #3 Insert the result of the SELECT query.
  * INSERT INTO [db.]table (c1, c2, c3) SELECT ...
  * INSERT INTO [db.]table SELECT ...

  * This syntax is controversial, not open for now.
  * #4 Insert of data in an arbitrary form from file(a bit variant of #2)
  * INSERT INTO [db.]table (c1, c2, c3) FORMAT format INFILE 'url'
  */
class ParserInsertQuery : public IParserBase
{
private:
    const char * end;

    const char * getName() const override { return "INSERT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    ParserInsertQuery(const char * end) : end(end) {}
};

}
