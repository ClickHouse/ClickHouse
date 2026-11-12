#include <Parsers/Access/ASTCreateTokenQuery.h>

#include <Parsers/Access/ASTAuthenticationData.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


namespace DB
{

String ASTCreateTokenQuery::getID(char) const
{
    return "CreateTokenQuery";
}

void ASTCreateTokenQuery::setValidUntil(ASTPtr ast)
{
    if (!ast)
        return;
    setOrReplace(valid_until, std::move(ast));
}

ASTPtr ASTCreateTokenQuery::clone() const
{
    auto res = make_intrusive<ASTCreateTokenQuery>(*this);
    res->children.clear();
    res->valid_until = nullptr;

    /// Keep the child order of a freshly parsed query: the clause subtree first, the output
    /// options after it, so that a clone and a format-and-reparse round trip hash the same.
    if (valid_until)
    {
        res->valid_until = valid_until->clone();
        res->children.push_back(res->valid_until);
    }

    cloneOutputOptions(*res);
    return res;
}

void ASTCreateTokenQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// `valid_until_is_interval` and `grants` are plain members, not children, so the generic
    /// walk over `children` would not see the difference between e.g. `VALID UNTIL` and `VALID FOR`.
    hash_state.update(valid_until_is_interval);
    const auto grants_string = grants.toStringPrecise();
    hash_state.update(grants_string.size());
    hash_state.update(grants_string);
    ASTQueryWithOutput::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTCreateTokenQuery::forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f)
{
    f(nullptr, &valid_until);
}

void ASTCreateTokenQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    ostr << "CREATE TOKEN";

    if (valid_until)
        formatAuthenticationValidUntil(*valid_until, valid_until_is_interval, ostr, settings);

    if (!grants.structurallyEmpty())
        formatAuthenticationGrants(grants, ostr);
}

}
