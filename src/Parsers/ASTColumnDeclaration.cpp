#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTWithAlias.h>
#include <IO/Operators.h>


namespace DB
{

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
        ostr << ' '  << default_specifier ;
        if (!ephemeral_default)
        {
            ostr << ' ';
            default_expression->format(ostr, format_settings, state, frame);
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
