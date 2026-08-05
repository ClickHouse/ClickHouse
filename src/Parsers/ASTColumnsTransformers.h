#pragma once

#include <Parsers/IAST.h>

namespace Poco::JSON { class Object; }

namespace DB
{

/// A list of column transformers
class ASTColumnsTransformerList : public IAST
{
public:
    String getID(char) const override { return "ColumnsTransformerList"; }
    ASTPtr clone() const override
    {
        auto clone = make_intrusive<ASTColumnsTransformerList>(*this);
        clone->cloneChildren();
        return clone;
    }
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

/// A `COLUMNS(...)` transformer is pure syntax: applying it needs the expanded column list, which
/// only exists during analysis. See `Interpreters/applyColumnsTransformer.h`.
class IASTColumnsTransformer : public IAST
{
};

class ASTColumnsApplyTransformer : public IASTColumnsTransformer
{
public:
    String getID(char) const override { return "ColumnsApplyTransformer"; }
    ASTPtr clone() const override
    {
        auto res = make_intrusive<ASTColumnsApplyTransformer>(*this);
        if (parameters)
            res->parameters = parameters->clone();
        if (lambda)
            res->lambda = lambda->clone();
        return res;
    }
    void appendColumnName(WriteBuffer & ostr) const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    // Case 1  APPLY (quantile(0.9))
    String func_name;
    ASTPtr parameters;

    // Case 2 APPLY (x -> quantile(0.9)(x))
    ASTPtr lambda;
    String lambda_arg;

    String column_name_prefix;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

class ASTColumnsExceptTransformer : public IASTColumnsTransformer
{
public:
    bool is_strict = false;
    String getID(char) const override { return "ColumnsExceptTransformer"; }
    ASTPtr clone() const override
    {
        auto clone = make_intrusive<ASTColumnsExceptTransformer>(*this);
        clone->cloneChildren();
        return clone;
    }
    void setPattern(String pattern_);
    const std::optional<String> & getPattern() const { return pattern; }
    void appendColumnName(WriteBuffer & ostr) const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    std::optional<String> pattern;
};

class ASTColumnsReplaceTransformer : public IASTColumnsTransformer
{
public:
    class Replacement : public IAST
    {
    public:
        String getID(char) const override { return "ColumnsReplaceTransformer::Replacement"; }
        ASTPtr clone() const override
        {
            auto replacement = make_intrusive<Replacement>(*this);
            replacement->cloneChildren();
            return replacement;
        }

        void appendColumnName(WriteBuffer & ostr) const override;
        void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
        void writeJSON(WriteBuffer & out) const override;
        void readJSON(const Poco::JSON::Object & json) override;

        String name;

    protected:
        void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    };

    bool is_strict = false;
    String getID(char) const override { return "ColumnsReplaceTransformer"; }
    ASTPtr clone() const override
    {
        auto clone = make_intrusive<ASTColumnsReplaceTransformer>(*this);
        clone->cloneChildren();
        return clone;
    }
    void appendColumnName(WriteBuffer & ostr) const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
