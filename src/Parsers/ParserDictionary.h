#pragma once

#include <Parsers/IParserBase.h>

#include <Parsers/ParserSetQuery.h>

namespace DB
{

/// Parser for dictionary lifetime part. It should contain "lifetime" keyword,
/// opening bracket, literal value or two pairs and closing bracket:
/// lifetime(300), lifetime(min 100 max 200). Produces ASTDictionaryLifetime.
class ParserDictionaryLifetime : public IParserBase
{
protected:
    const char * getName() const override { return "lifetime definition"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// Parser for dictionary range part. It should contain "range" keyword opening
/// bracket, two pairs and closing bracket: range(min attr1 max attr2). Produces
/// ASTDictionaryRange.
class ParserDictionaryRange : public IParserBase
{
protected:
    const char * getName() const override { return "range definition"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/// Parser for dictionary layout part. It should contain "layout" keyword,
/// opening bracket, possible pair with param value and closing bracket:
/// layout(type()) or layout(type(param value)). Produces ASTDictionaryLayout.
class ParserDictionaryLayout : public IParserBase
{
protected:
    const char * getName() const override { return "layout definition"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserDictionarySettings: public IParserBase
{
protected:
    const char * getName() const override { return "settings definition"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/// Combines together all parsers from above and also parses primary key and
/// dictionary source, which consists of custom key-value pairs:
///
/// PRIMARY KEY key_column1, key_column2
/// SOURCE(MYSQL(HOST 'localhost' PORT 9000 USER 'default' REPLICA(HOST '127.0.0.1' PRIORITY 1) PASSWORD ''))
/// LAYOUT(CACHE(size_in_cells 50))
/// LIFETIME(MIN 1 MAX 10)
/// RANGE(MIN second_column MAX third_column)
///
/// Produces ASTDictionary.
class ParserDictionary : public IParserBase
{
protected:
    const char * getName() const override { return "dictionary definition"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
