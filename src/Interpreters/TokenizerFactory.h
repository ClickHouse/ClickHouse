#pragma once

#include <Interpreters/ITokenizer.h>
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
    using Creator = std::function<std::unique_ptr<ITokenizer>(const FieldVector &)>;
    static TokenizerFactory & instance();

    /// Parses a definition of a tokenizer from a string into AST and creates a tokenizer from it.
    std::unique_ptr<ITokenizer> get(std::string_view full_name) const;

    /// Creates a tokenizer from an AST node.
    /// Supports ASTIdentifier (name without args),
    /// ASTLiteral with string value (name without args),
    /// and ASTFunction (name with literal args).
    std::unique_ptr<ITokenizer> get(const ASTPtr & ast) const;

    /// Creates a tokenizer by name and arguments.
    /// If allowed is non-empty, only tokenizers of the specified types can be created.
    std::unique_ptr<ITokenizer> get(
        std::string_view name,
        const FieldVector & args,
        const std::set<ITokenizer::Type> & allowed = {}) const;

    /// Returns the name and type of all registered tokenizers. Used for pretty printing.
    /// There is no concept of aliases (like in the SQL function factory), therefore one tokenizer
    /// type can be registered under multiple names.
    std::unordered_map<String, ITokenizer::Type> getAllTokenizers() const;

    void registerTokenizer(const String & name, ITokenizer::Type type, Creator creator);

private:
    TokenizerFactory();

    struct Entry
    {
        ITokenizer::Type type;
        Creator creator;
    };

    using Tokenizers = std::unordered_map<String, Entry>;
    Tokenizers tokenizers;
};

}
