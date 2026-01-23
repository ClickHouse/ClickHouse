#pragma once

#include <Parsers/IAST.h>

namespace DB
{

/** Name, type, default-specifier, default-expression, comment-expression.
 *  The type is optional if default-expression is specified.
 */
class ASTColumnDeclaration : public IAST
{
public:
    String name;
    String default_specifier;

    std::optional<bool> null_modifier;
    bool ephemeral_default = false;
    bool primary_key_specifier = false;

private:
    static constexpr UInt8 kNotSet = 0xFF;

    UInt8 type_idx = kNotSet;
    UInt8 default_expression_idx = kNotSet;
    UInt8 comment_idx = kNotSet;
    UInt8 codec_idx = kNotSet;
    UInt8 statistics_desc_idx = kNotSet;
    UInt8 ttl_idx = kNotSet;
    UInt8 collation_idx = kNotSet;
    UInt8 settings_idx = kNotSet;

    ASTPtr getChildOrNull(UInt8 idx) const { return idx == kNotSet ? nullptr : children[idx]; }

    void setChild(UInt8 & idx, ASTPtr && node)
    {
        if (!node)
            return;

        if (idx != kNotSet)
            children[idx] = std::move(node);
        else
        {
            idx = static_cast<UInt8>(children.size());
            children.push_back(std::move(node));
        }
    }

public:
    bool hasChildren() const { return !children.empty(); }
    ASTPtr getType() const { return getChildOrNull(type_idx); }
    ASTPtr getDefaultExpression() const { return getChildOrNull(default_expression_idx); }
    ASTPtr getComment() const { return getChildOrNull(comment_idx); }
    ASTPtr getCodec() const { return getChildOrNull(codec_idx); }
    ASTPtr getStatisticsDesc() const { return getChildOrNull(statistics_desc_idx); }
    ASTPtr getTTL() const { return getChildOrNull(ttl_idx); }
    ASTPtr getCollation() const { return getChildOrNull(collation_idx); }
    ASTPtr getSettings() const { return getChildOrNull(settings_idx); }

    void setType(ASTPtr && node) { setChild(type_idx, std::move(node)); }
    void setDefaultExpression(ASTPtr && node) { setChild(default_expression_idx, std::move(node)); }
    void setComment(ASTPtr && node) { setChild(comment_idx, std::move(node)); }
    void setCodec(ASTPtr && node) { setChild(codec_idx, std::move(node)); }
    void setStatisticsDesc(ASTPtr && node) { setChild(statistics_desc_idx, std::move(node)); }
    void setTTL(ASTPtr && node) { setChild(ttl_idx, std::move(node)); }
    void setCollation(ASTPtr && node) { setChild(collation_idx, std::move(node)); }
    void setSettings(ASTPtr && node) { setChild(settings_idx, std::move(node)); }

    String getID(char delim) const override { return "ColumnDeclaration" + (delim + name); }

    ASTPtr clone() const override;


protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & format_settings, FormatState & state, FormatStateStacked frame) const override;
    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override;

private:
    using IAST::children; /// Don't let other manipulate children directly
};

}
