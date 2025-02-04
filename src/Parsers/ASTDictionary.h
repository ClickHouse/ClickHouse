#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTExpressionList.h>

#include <Parsers/ASTSetQuery.h>

#include <Parsers/ParserSetQuery.h>

namespace DB
{

class ASTLiteral;

/// AST for external dictionary lifetime:
/// lifetime(min 10 max 100)
class ASTDictionaryLifetime : public IAST
{
public:
    UInt64 min_sec = 0;
    UInt64 max_sec = 0;

    String getID(char) const override { return "Dictionary lifetime"; }

    ASTPtr clone() const override;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

/// AST for external dictionary layout. Has name and contain single parameter
/// layout(type()) or layout(type(param value))
class ASTDictionaryLayout : public IAST
{
    using KeyValue = std::pair<std::string, ASTLiteral *>;
public:
    /// flat, cache, hashed, etc.
    String layout_type;
    /// parameters (size_in_cells, ...)
    /// ASTExpressionList -> ASTPair -> (ASTLiteral key, ASTLiteral value).
    ASTExpressionList * parameters;
    /// has brackets after layout type
    bool has_brackets = true;

    String getID(char) const override { return "Dictionary layout"; }

    ASTPtr clone() const override;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    void forEachPointerToChild(std::function<void(void**)> f) override
    {
        f(reinterpret_cast<void **>(&parameters));
    }
};


/// AST for external range-hashed dictionary
/// Range bounded with two attributes from minimum to maximum
/// RANGE(min attr1 max attr2)
class ASTDictionaryRange : public IAST
{
public:
    String min_attr_name;
    String max_attr_name;

    String getID(char) const override { return "Dictionary range"; }

    ASTPtr clone() const override;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

class ASTDictionarySettings : public IAST
{
public:
    SettingsChanges changes;

    String getID(char) const override { return "Dictionary settings"; }

    ASTPtr clone() const override;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


/// AST contains all parts of external dictionary definition except attributes
class ASTDictionary : public IAST
{
public:
    /// Dictionary keys -- one or more
    ASTExpressionList * primary_key = nullptr;
    /// Dictionary external source, doesn't have own AST, because
    /// source parameters absolutely different for different sources
    ASTFunctionWithKeyValueArguments * source = nullptr;

    /// Lifetime of dictionary (required part)
    ASTDictionaryLifetime * lifetime = nullptr;
    /// Layout of dictionary (required part)
    ASTDictionaryLayout * layout = nullptr;
    /// Range for dictionary (only for range-hashed dictionaries)
    ASTDictionaryRange * range = nullptr;
    /// Settings for dictionary (optionally)
    ASTDictionarySettings * dict_settings = nullptr;

    String getID(char) const override { return "Dictionary definition"; }

    ASTPtr clone() const override;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
