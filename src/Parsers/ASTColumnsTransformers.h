#pragma once

#include <Parsers/IAST.h>


namespace re2
{
    class RE2;
}

namespace DB
{

/// A list of column transformers
class ASTColumnsTransformerList : public IAST
{
public:
    String getID(char) const override { return "ColumnsTransformerList"; }
    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTColumnsTransformerList>(*this);
        clone->cloneChildren();
        return clone;
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

class IASTColumnsTransformer : public IAST
{
public:
    virtual void transform(ASTs & nodes) const = 0;
    static void transform(const ASTPtr & transformer, ASTs & nodes);
};

class ASTColumnsApplyTransformer : public IASTColumnsTransformer
{
public:
    String getID(char) const override { return "ColumnsApplyTransformer"; }
    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTColumnsApplyTransformer>(*this);
        if (parameters)
            res->parameters = parameters->clone();
        if (lambda)
            res->lambda = lambda->clone();
        return res;
    }
    void transform(ASTs & nodes) const override;
    void appendColumnName(WriteBuffer & ostr) const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    // Case 1  APPLY (quantile(0.9))
    String func_name;
    ASTPtr parameters;

    // Case 2 APPLY (x -> quantile(0.9)(x))
    ASTPtr lambda;
    String lambda_arg;

    String column_name_prefix;

protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

class ASTColumnsExceptTransformer : public IASTColumnsTransformer
{
public:
    bool is_strict = false;
    String getID(char) const override { return "ColumnsExceptTransformer"; }
    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTColumnsExceptTransformer>(*this);
        clone->cloneChildren();
        return clone;
    }
    void transform(ASTs & nodes) const override;
    void setPattern(String pattern_);
    std::shared_ptr<re2::RE2> getMatcher() const;
    void appendColumnName(WriteBuffer & ostr) const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
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
            auto replacement = std::make_shared<Replacement>(*this);
            replacement->cloneChildren();
            return replacement;
        }

        void appendColumnName(WriteBuffer & ostr) const override;
        void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

        String name;

    protected:
        void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    };

    bool is_strict = false;
    String getID(char) const override { return "ColumnsReplaceTransformer"; }
    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTColumnsReplaceTransformer>(*this);
        clone->cloneChildren();
        return clone;
    }
    void transform(ASTs & nodes) const override;
    void appendColumnName(WriteBuffer & ostr) const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    static void replaceChildren(ASTPtr & node, const ASTPtr & replacement, const String & name);
};

}
