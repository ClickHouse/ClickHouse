#pragma once

#include <Parsers/IAST.h>
#include <Storages/ColumnDefault.h>

namespace DB
{

/// Default specifier for column declarations - compatible with ColumnDefaultKind plus Empty and AutoIncrement
enum class ColumnDefaultSpecifier : UInt8
{
    Empty = 0,
    Default,
    Materialized,
    Alias,
    Ephemeral,
    AutoIncrement
};

const char * toString(ColumnDefaultSpecifier kind);
ColumnDefaultSpecifier columnDefaultSpecifierFromString(std::string_view str);
ColumnDefaultSpecifier toColumnDefaultSpecifier(ColumnDefaultKind kind);
ColumnDefaultKind toColumnDefaultKind(ColumnDefaultSpecifier specifier);

/** Name, type, default-specifier, default-expression, comment-expression.
 *  The type is optional if default-expression is specified.
 */
class ASTColumnDeclaration : public IAST
{
public:
    String name;
    ColumnDefaultSpecifier default_specifier = ColumnDefaultSpecifier::Empty;

    std::optional<bool> null_modifier;
    bool ephemeral_default : 1 = false;
    bool primary_key_specifier : 1 = false;

private:
    /// Pack 8 indices (4 bits each) into a single UInt32. 0xF means "not set".
    static constexpr UInt8 kNotSet = 0xF;
    static constexpr UInt32 kAllNotSet = 0xFFFFFFFF;

    /// Bit positions for each index (4 bits each)
    enum IndexSlot : UInt8
    {
        TYPE = 0,
        DEFAULT_EXPR = 4,
        COMMENT = 8,
        CODEC = 12,
        STATS = 16,
        TTL = 20,
        COLLATION = 24,
        SETTINGS = 28
    };

    UInt32 packed_indices = kAllNotSet;

    UInt8 getIndex(IndexSlot slot) const { return (packed_indices >> slot) & 0xF; }
    void setIndex(IndexSlot slot, UInt8 val) { packed_indices = (packed_indices & ~(0xFU << slot)) | (static_cast<UInt32>(val) << slot); }

    ASTPtr getChildOrNull(IndexSlot slot) const
    {
        UInt8 idx = getIndex(slot);
        return idx == kNotSet ? nullptr : children[idx];
    }

    void setChild(IndexSlot slot, ASTPtr && node)
    {
        if (!node)
            return;

        UInt8 idx = getIndex(slot);
        if (idx != kNotSet)
            children[idx] = std::move(node);
        else
        {
            setIndex(slot, static_cast<UInt8>(children.size()));
            children.push_back(std::move(node));
        }
    }

public:
    bool hasChildren() const { return !children.empty(); }
    ASTPtr getType() const { return getChildOrNull(TYPE); }
    ASTPtr getDefaultExpression() const { return getChildOrNull(DEFAULT_EXPR); }
    ASTPtr getComment() const { return getChildOrNull(COMMENT); }
    ASTPtr getCodec() const { return getChildOrNull(CODEC); }
    ASTPtr getStatisticsDesc() const { return getChildOrNull(STATS); }
    ASTPtr getTTL() const { return getChildOrNull(TTL); }
    ASTPtr getCollation() const { return getChildOrNull(COLLATION); }
    ASTPtr getSettings() const { return getChildOrNull(SETTINGS); }

    void setType(ASTPtr && node) { setChild(TYPE, std::move(node)); }
    void setDefaultExpression(ASTPtr && node) { setChild(DEFAULT_EXPR, std::move(node)); }
    void setComment(ASTPtr && node) { setChild(COMMENT, std::move(node)); }
    void setCodec(ASTPtr && node) { setChild(CODEC, std::move(node)); }
    void setStatisticsDesc(ASTPtr && node) { setChild(STATS, std::move(node)); }
    void setTTL(ASTPtr && node) { setChild(TTL, std::move(node)); }
    void setCollation(ASTPtr && node) { setChild(COLLATION, std::move(node)); }
    void setSettings(ASTPtr && node) { setChild(SETTINGS, std::move(node)); }

    String getID(char delim) const override { return "ColumnDeclaration" + (delim + name); }

    ASTPtr clone() const override;


protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & format_settings, FormatState & state, FormatStateStacked frame) const override;
    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override;

private:
    using IAST::children; /// Don't let other manipulate children directly
};

}
