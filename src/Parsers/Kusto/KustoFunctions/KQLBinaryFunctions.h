#pragma once

#include <Parsers/Kusto/IKQLParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
namespace DB
{
class BinaryAnd : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "binary_and()"; }
    bool convertImpl(String & out, IKQLParser::KQLPos & pos) override;
};

class BinaryNot : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "binary_not()"; }
    bool convertImpl(String & out, IKQLParser::KQLPos & pos) override;
};

class BinaryOr : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "binary_or()"; }
    bool convertImpl(String & out, IKQLParser::KQLPos & pos) override;
};

class BinaryShiftLeft : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "binary_shift_left()"; }
    bool convertImpl(String & out, IKQLParser::KQLPos & pos) override;
};

class BinaryShiftRight : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "binary_shift_right()"; }
    bool convertImpl(String & out, IKQLParser::KQLPos & pos) override;
};

class BinaryXor : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "binary_xor()"; }
    bool convertImpl(String & out, IKQLParser::KQLPos & pos) override;
};

class BitsetCountOnes : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "bitset_count_ones()"; }
    bool convertImpl(String & out, IKQLParser::KQLPos & pos) override;
};

}
