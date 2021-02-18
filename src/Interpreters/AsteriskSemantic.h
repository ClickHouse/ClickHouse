#pragma once

#include <unordered_map>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>

namespace DB
{

struct AsteriskSemanticImpl
{
    using RevertedAliases = std::unordered_map<String, std::vector<String>>;
    using RevertedAliasesPtr = std::shared_ptr<RevertedAliases>;

    RevertedAliasesPtr aliases; /// map of aliases that should be set in phase of * expanding.
};


struct AsteriskSemantic
{
    using RevertedAliases = AsteriskSemanticImpl::RevertedAliases;
    using RevertedAliasesPtr = AsteriskSemanticImpl::RevertedAliasesPtr;

    static void setAliases(ASTAsterisk & node, const RevertedAliasesPtr & aliases) { node.semantic = makeSemantic(aliases); }
    static void setAliases(ASTQualifiedAsterisk & node, const RevertedAliasesPtr & aliases) { node.semantic = makeSemantic(aliases); }
    static void setAliases(ASTColumnsMatcher & node, const RevertedAliasesPtr & aliases) { node.semantic = makeSemantic(aliases); }

    static RevertedAliasesPtr getAliases(const ASTAsterisk & node) { return node.semantic ? node.semantic->aliases : nullptr; }
    static RevertedAliasesPtr getAliases(const ASTQualifiedAsterisk & node) { return node.semantic ? node.semantic->aliases : nullptr; }
    static RevertedAliasesPtr getAliases(const ASTColumnsMatcher & node) { return node.semantic ? node.semantic->aliases : nullptr; }

private:
    static std::shared_ptr<AsteriskSemanticImpl> makeSemantic(const RevertedAliasesPtr & aliases)
    {
        return std::make_shared<AsteriskSemanticImpl>(AsteriskSemanticImpl{aliases});
    }
};

}
