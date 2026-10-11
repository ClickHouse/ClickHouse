#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <base/types.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

class Field;

/// A function call as the secret arguments finder reads it, implemented over the AST (`FunctionAST`), the
/// query tree (`FunctionTreeNodeImpl`) and `ActionsDAG` nodes (`FunctionActionsDAG`).
class AbstractFunction
{
public:
    class Argument
    {
    public:
        virtual ~Argument() = default;
        virtual std::unique_ptr<AbstractFunction> getFunction() const = 0;
        virtual bool isIdentifier() const = 0;
        virtual bool tryGetString(String * res, bool allow_identifier) const = 0;
        /// The exact literal text of any scalar literal (`1`, `true`, `1.5`), with strings quoted.
        /// Lets a reconstructor keep non-string values like `use_environment_credentials = 1` visible.
        virtual bool tryGetLiteralText(String * res) const = 0;
        /// A `SETTINGS` clause among the arguments (`remote(..., SETTINGS ...)`): not a positional
        /// argument, and it hides its own secret values when formatted.
        virtual bool isSettings() const { return false; }
    };
    class Arguments
    {
    public:
        virtual ~Arguments() = default;
        virtual size_t size() const = 0;
        virtual std::unique_ptr<Argument> at(size_t n) const = 0;
    };

    virtual ~AbstractFunction() = default;
    virtual String name() const = 0;
    bool hasArguments() const { return !!arguments; }

    std::unique_ptr<Arguments> arguments;
};

/// Which arguments of a function are secret.
struct SecretArgumentsResult
{
    /// Result constructed by default means no arguments will be hidden.
    size_t start = static_cast<size_t>(-1);
    size_t count = 0; /// Mostly it's either 0 or 1. There are only a few cases where `count` can be greater than 1 (e.g. see `encrypt`).
                        /// In all known cases secret arguments are consecutive
    bool are_named = false; /// Arguments like `password = 'password'` are considered as named arguments.
    /// Nested maps whose values are hidden with their keys kept, e.g. "headers" in
    /// `url('..', headers('foo' = '[HIDDEN]'))`, each with the keys whose value stays visible.
    std::map<std::string, std::vector<std::string>> nested_maps;
    /// Full replacement of an argument. Only supported when count is 1, otherwise all arguments will be replaced with this string.
    /// It's needed in cases when we don't want to hide the entire parameter, but some part of it, e.g. "connection_string" in
    /// `azureBlobStorage('DefaultEndpointsProtocol=https;AccountKey=secretkey;...', ...)` should be replaced with
    /// `azureBlobStorage('DefaultEndpointsProtocol=https;AccountKey=[HIDDEN];...', ...)`.
    std::string replacement;
    /// Whether to wrap a result using full argument replacement in quotes.
    bool quote_replacement = true;
    /// Per-argument replacements by raw argument index; the text is emitted verbatim (it must carry
    /// its own quoting). Used when only a part of an argument is secret, e.g. a presigned S3 URL
    /// keeps its host and path while the credential query parameters are hidden. Unlike
    /// `replacement`, this composes with the other masking (span, nested maps).
    std::map<size_t, std::string> replaced_arguments;
    /// Individually masked arguments by raw argument index; the value tells whether the argument
    /// is a named `key = value` (the key stays visible and only the value is hidden). Valid S3
    /// syntax can interleave secrets with non-secret arguments (e.g. a named `session_token`
    /// after `format`), which a single contiguous span cannot represent without hiding the
    /// non-secret arguments in between.
    std::map<size_t, bool> masked_arguments;

    bool hasSecrets() const
    {
        return count != 0 || !nested_maps.empty() || !replaced_arguments.empty() || !masked_arguments.empty();
    }
};

/// Knows what is secret in the arguments of every table function, engine, backup locator and dictionary source.
/// The parser only declares it: the knowledge lives with the engines, outside the parser library.
class ISecretArgumentsFinder
{
public:
    virtual ~ISecretArgumentsFinder() = default;
    virtual SecretArgumentsResult find(ASTFunction::Kind kind, const AbstractFunction & function) const = 0;
    /// The SQL text hiding the value of a secret engine setting, or `nullopt` when there is nothing to hide.
    virtual std::optional<String> renderSecretSetting(const String & name, const Field & value) const = 0;
    /// Masks the formatted value of a key of a dictionary `SOURCE(...)` in place. Returns whether it is secret.
    virtual bool maskDictionarySourceValue(const String & key, String & value) const = 0;
};

/// Finds no secrets. For the programs that don't register the engines (the client, the WebAssembly parser):
/// they show the secrets of engine arguments.
class NoSecretArgumentsFinder final : public ISecretArgumentsFinder
{
public:
    static const NoSecretArgumentsFinder & instance();

    SecretArgumentsResult find(ASTFunction::Kind, const AbstractFunction &) const override { return {}; }
    std::optional<String> renderSecretSetting(const String &, const Field &) const override { return {}; }
    bool maskDictionarySourceValue(const String &, String &) const override { return false; }
};

/// Installed once at startup by every program that formats an AST hiding its secrets: `SecretArgumentsRegistry`
/// where the engines are registered (`server`, `local`, `format`), `NoSecretArgumentsFinder` elsewhere.
void setSecretArgumentsFinder(const ISecretArgumentsFinder * finder);
/// Throws `LOGICAL_ERROR` if no finder is installed, so a program that forgets to install one can't show secrets.
const ISecretArgumentsFinder & getSecretArgumentsFinder();

}
