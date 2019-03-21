#pragma once

#include <unordered_map>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>

namespace DB
{

/// Contain maps of aliases used in phase of * expanding.
struct AsteriskSemanticImpl
{
    using ForwardAliases = std::unordered_map<String, String>;
    using ForwardAliasesPtr = std::shared_ptr<ForwardAliases>;
    using RevertedAliases = std::unordered_map<String, std::vector<String>>;
    using RevertedAliasesPtr = std::shared_ptr<RevertedAliases>;
    using NamesPtr = std::shared_ptr<std::set<String>>;

    ForwardAliasesPtr fwd_aliases;
    RevertedAliasesPtr rev_aliases;
    NamesPtr known_short_names;

    AsteriskSemanticImpl(ForwardAliases && fwd, RevertedAliases && rev, std::set<String> && names)
        : fwd_aliases(std::make_shared<ForwardAliases>())
        , rev_aliases(std::make_shared<RevertedAliases>())
        , known_short_names(std::make_shared<std::set<String>>())
    {
        fwd_aliases->swap(fwd);
        rev_aliases->swap(rev);
        known_short_names->swap(names);
    }

    AsteriskSemanticImpl() = default;
    AsteriskSemanticImpl(const AsteriskSemanticImpl & ) = default;
};


struct AsteriskSemantic
{
    using ForwardAliases = AsteriskSemanticImpl::ForwardAliases;
    using ForwardAliasesPtr = AsteriskSemanticImpl::ForwardAliasesPtr;
    using RevertedAliases = AsteriskSemanticImpl::RevertedAliases;
    using RevertedAliasesPtr = AsteriskSemanticImpl::RevertedAliasesPtr;

    static void set(ASTAsterisk & node, const AsteriskSemanticImpl & sema) { node.semantic = std::make_shared<AsteriskSemanticImpl>(sema); }
    static void set(ASTQualifiedAsterisk & node, const AsteriskSemanticImpl & sema) { node.semantic = std::make_shared<AsteriskSemanticImpl>(sema); }

    static AsteriskSemanticImpl get(const ASTAsterisk & node) { return node.semantic ? *node.semantic : AsteriskSemanticImpl{}; }
    static AsteriskSemanticImpl get(const ASTQualifiedAsterisk & node) { return node.semantic ? *node.semantic : AsteriskSemanticImpl{}; }
};

}
