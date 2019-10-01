#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

// LIFETIME(MIN 10, MAX 100)
class ASTDictionaryLifetime : public IAST
{
public:
    uint64_t min_sec;
    uint64_t max_sec;

    String getID(char) const override { return "Dictionary lifetime"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

// LAYOUT(TYPE()) or LAYOUT(TYPE(PARAM value))
class ASTDictionaryLayout : public IAST
{
    using KeyValue = std::pair<std::string, ASTLiteral *>;
public:
    String layout_type;
    std::optional<KeyValue> parameter;

    String getID(char) const override { return "Dictionary layout"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


class ASTDictionaryRange : public IAST
{
public:
    String min_attr_name;
    String max_attr_name;

    String getID(char) const override { return "Dictionary range"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


class ASTDictionary : public IAST
{
public:
    IAST * primary_key;
    ASTFunctionWithKeyValueArguments * source;
    ASTDictionaryLifetime * lifetime;
    ASTDictionaryLayout * layout;
    ASTDictionaryRange * range;

    String getID(char) const override { return "Dictionary definition"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
