#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTWithAlias.h>
#include <IO/Operators.h>


namespace DB
{

const char * toString(ColumnDefaultSpecifier kind)
{
    switch (kind)
    {
        case ColumnDefaultSpecifier::Empty: return "";
        case ColumnDefaultSpecifier::Default: return "DEFAULT";
        case ColumnDefaultSpecifier::Materialized: return "MATERIALIZED";
        case ColumnDefaultSpecifier::Alias: return "ALIAS";
        case ColumnDefaultSpecifier::Ephemeral: return "EPHEMERAL";
        case ColumnDefaultSpecifier::AutoIncrement: return "AUTO_INCREMENT";
    }
}

ColumnDefaultSpecifier columnDefaultSpecifierFromString(std::string_view str)
{
    if (str.empty()) return ColumnDefaultSpecifier::Empty;
    if (str == "DEFAULT") return ColumnDefaultSpecifier::Default;
    if (str == "MATERIALIZED") return ColumnDefaultSpecifier::Materialized;
    if (str == "ALIAS") return ColumnDefaultSpecifier::Alias;
    if (str == "EPHEMERAL") return ColumnDefaultSpecifier::Ephemeral;
    if (str == "AUTO_INCREMENT") return ColumnDefaultSpecifier::AutoIncrement;
    return ColumnDefaultSpecifier::Empty;
}

ColumnDefaultSpecifier toColumnDefaultSpecifier(ColumnDefaultKind kind)
{
    switch (kind)
    {
        case ColumnDefaultKind::Default: return ColumnDefaultSpecifier::Default;
        case ColumnDefaultKind::Materialized: return ColumnDefaultSpecifier::Materialized;
        case ColumnDefaultKind::Alias: return ColumnDefaultSpecifier::Alias;
        case ColumnDefaultKind::Ephemeral: return ColumnDefaultSpecifier::Ephemeral;
    }
}

ColumnDefaultKind toColumnDefaultKind(ColumnDefaultSpecifier specifier)
{
    switch (specifier)
    {
        case ColumnDefaultSpecifier::Empty:
        case ColumnDefaultSpecifier::Default:
        case ColumnDefaultSpecifier::AutoIncrement:
            return ColumnDefaultKind::Default;
        case ColumnDefaultSpecifier::Materialized: return ColumnDefaultKind::Materialized;
        case ColumnDefaultSpecifier::Alias: return ColumnDefaultKind::Alias;
        case ColumnDefaultSpecifier::Ephemeral: return ColumnDefaultKind::Ephemeral;
    }
}

ASTPtr ASTColumnDeclaration::clone() const
{
    const auto res = make_intrusive<ASTColumnDeclaration>(*this);
    res->children.clear();
    res->packed_indices = kAllNotSet;

    if (auto node = getType())
        res->setType(node->clone());
    if (auto node = getDefaultExpression())
        res->setDefaultExpression(node->clone());
    if (auto node = getComment())
        res->setComment(node->clone());
    if (auto node = getCodec())
        res->setCodec(node->clone());
    if (auto node = getStatisticsDesc())
        res->setStatisticsDesc(node->clone());
    if (auto node = getTTL())
        res->setTTL(node->clone());
    if (auto node = getCollation())
        res->setCollation(node->clone());
    if (auto node = getSettings())
        res->setSettings(node->clone());

    return res;
}

void ASTColumnDeclaration::formatImpl(WriteBuffer & ostr, const FormatSettings & format_settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    format_settings.writeIdentifier(ostr, name, /*ambiguous=*/true);

    if (auto type = getType())
    {
        ostr << ' ';
        type->format(ostr, format_settings, state, frame);
    }

    if (null_modifier)
    {
        ostr << ' '
                      << (*null_modifier ? "" : "NOT ") << "NULL" ;
    }

    if (auto default_expression = getDefaultExpression())
    {
        ostr << ' ' << toString(default_specifier);
        if (!ephemeral_default)
        {
            ostr << ' ';
            auto nested_frame = frame;
            if (auto * ast_alias = dynamic_cast<ASTWithAlias *>(default_expression.get()); ast_alias && !ast_alias->tryGetAlias().empty())
                nested_frame.need_parens = true;
            default_expression->format(ostr, format_settings, state, nested_frame);
        }
    }

    if (auto comment = getComment())
    {
        ostr << ' '  << "COMMENT"  << ' ';
        comment->format(ostr, format_settings, state, frame);
    }

    if (auto codec = getCodec())
    {
        ostr << ' ';
        codec->format(ostr, format_settings, state, frame);
    }

    if (auto statistics_desc = getStatisticsDesc())
    {
        ostr << ' ';
        statistics_desc->format(ostr, format_settings, state, frame);
    }

    if (auto ttl = getTTL())
    {
        ostr << ' '  << "TTL"  << ' ';
        auto nested_frame = frame;
        if (auto * ast_alias = dynamic_cast<ASTWithAlias *>(ttl.get()); ast_alias && !ast_alias->tryGetAlias().empty())
            nested_frame.need_parens = true;
        ttl->format(ostr, format_settings, state, nested_frame);
    }

    if (auto collation = getCollation())
    {
        ostr << ' '  << "COLLATE"  << ' ';
        collation->format(ostr, format_settings, state, frame);
    }

    if (auto settings = getSettings())
    {
        ostr << ' '  << "SETTINGS"  << ' ' << '(';
        settings->format(ostr, format_settings, state, frame);
        ostr << ')';
    }
}

void ASTColumnDeclaration::forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f)
{
    auto callIfSet = [&](IndexSlot slot)
    {
        UInt8 idx = getIndex(slot);
        if (idx != kNotSet)
            f(nullptr, &children[idx]);
    };
    callIfSet(TYPE);
    callIfSet(DEFAULT_EXPR);
    callIfSet(COMMENT);
    callIfSet(CODEC);
    callIfSet(STATS);
    callIfSet(TTL);
    callIfSet(COLLATION);
    callIfSet(SETTINGS);
}
}
