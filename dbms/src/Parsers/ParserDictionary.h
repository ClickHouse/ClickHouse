#pragma once

#include <Parsers/IParser.h>
#include <Parsers/IParserBase.h>

namespace DB
{
/*
 * LIFETIME(MIN 10, MAX 100)
 */
class ParserDictionaryLifetime : public IParserBase
{
protected:
    const char * getName() const override { return "lifetime definition"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/*
 * RANGE(MIN startDate, MAX endDate)
 */
class ParserDictionaryRange : public IParserBase
{
protected:
    const char * getName() const override { return "range definition"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


// LAYOUT(TYPE()) or LAYOUT(TYPE(PARAM value))
class ParserDictionaryLayout : public IParserBase
{
protected:
    const char * getName() const override { return "layout definition"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserDictionary : public IParserBase
{
protected:
    const char * getName() const override { return "dictionary definition"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/**
  * CREATE DICTIONARY db.name
  */
class ParserCreateDictionaryQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE DICTIONARY"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
