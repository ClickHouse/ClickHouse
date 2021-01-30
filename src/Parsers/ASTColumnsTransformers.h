#pragma once

#include <Parsers/IAST.h>

namespace re2
{
    class RE2;
}

namespace DB
{
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
        return res;
    }
    void transform(ASTs & nodes) const override;
    String func_name;
    String column_name_prefix;
    ASTPtr parameters;

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
    void setPattern(String pattern);
    bool isColumnMatching(const String & column_name) const;

protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    std::shared_ptr<re2::RE2> column_matcher;
    String original_pattern;
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
            replacement->children.clear();
            replacement->expr = expr->clone();
            replacement->children.push_back(replacement->expr);
            return replacement;
        }

        String name;
        ASTPtr expr;

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

protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    static void replaceChildren(ASTPtr & node, const ASTPtr & replacement, const String & name);
};

}
