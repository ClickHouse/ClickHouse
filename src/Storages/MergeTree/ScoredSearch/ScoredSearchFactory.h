#pragma once

#include <Core/Field.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/MergeTree/ScoredSearch/ScoredSearchDescriptor.h>
#include <base/types.h>

#include <boost/noncopyable.hpp>
#include <functional>
#include <string_view>
#include <unordered_map>

namespace DB
{

class ScoredSearchFactory final : private boost::noncopyable
{
public:
    using Family = ScoredSearchDescriptor::Family;
    using Creator = std::function<ScoredSearchDescriptor(const FieldVector &)>;

    static ScoredSearchFactory & instance();

    /// Resolves a scoring function from the scoring-fn argument
    /// AST. Supports `ASTIdentifier` (`bm25`) and `ASTFunction`
    /// (`bm25(1.5, 0.75)`). Throws `UNKNOWN_FUNCTION` on
    /// unregistered names; `BAD_ARGUMENTS` on bad arity / types.
    ScoredSearchDescriptor get(const ASTPtr & ast) const;

    /// Direct lookup by name and already-parsed args. Used by tests
    /// and by the AST overload above.
    ScoredSearchDescriptor get(std::string_view name, const FieldVector & args) const;

    /// Returns the name and family of every registered scoring
    /// function. Used for pretty-printing in error messages and in
    /// `system.scorings` (follow-up).
    std::unordered_map<String, Family> getAllScorings() const;
    void registerScoring(const String & name, Family family, Creator creator);

private:
    ScoredSearchFactory();

    struct Entry
    {
        Family family;
        Creator creator;
    };

    using Scorings = std::unordered_map<String, Entry>;
    Scorings scorings;
};

}
