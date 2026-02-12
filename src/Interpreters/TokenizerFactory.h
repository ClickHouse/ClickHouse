#pragma once

#include <Interpreters/ITokenExtractor.h>
#include <Core/Field.h>
#include <Parsers/IAST_fwd.h>

#include <boost/noncopyable.hpp>
#include <functional>
#include <memory>
#include <set>
#include <unordered_map>

namespace DB
{

class TokenizerFactory final : private boost::noncopyable
{
public:
    using Creator = std::function<std::unique_ptr<ITokenExtractor>(const FieldVector &)>;
    static TokenizerFactory & instance();

    /// Parses a definition of a tokenizer from a string into AST and creates a tokenizer from it.
    std::unique_ptr<ITokenExtractor> get(std::string_view full_name) const;

    /// Creates a tokenizer from an AST node.
    /// Supports ASTIdentifier (name without args),
    /// ASTLiteral with string value (name without args),
    /// and ASTFunction (name with literal args).
    std::unique_ptr<ITokenExtractor> get(const ASTPtr & ast) const;

    /// Creates a tokenizer by name and arguments.
    /// If allowed is non-empty, only tokenizers of the specified types can be created.
    std::unique_ptr<ITokenExtractor> get(
        std::string_view name,
        const FieldVector & args,
        const std::set<ITokenExtractor::Type> & allowed = {}) const;

    void registerTokenizer(const String & name, ITokenExtractor::Type type, Creator creator);

private:
    TokenizerFactory();

    struct Entry
    {
        ITokenExtractor::Type type;
        Creator creator;
    };

    using Tokenizers = std::unordered_map<String, Entry>;
    Tokenizers tokenizers;
};

}
