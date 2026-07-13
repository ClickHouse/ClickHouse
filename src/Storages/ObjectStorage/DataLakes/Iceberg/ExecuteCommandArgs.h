#pragma once

#include <functional>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <Core/Field.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

/// Declarative argument parser for `ALTER TABLE ... EXECUTE command(args...)`.
///
/// Supports:
///   - Named arguments:   `command(key = 'value', flag = 1)`
///   - Positional arguments: `command('2026-01-01')` mapped by index to a name
///   - Mixed:             `command('2026-01-01', dry_run = 1)`
///   - Defaults:          filled in after parsing when an argument was not supplied
///   - Type validation:   optional per-argument Field::Types::Which constraint
///
/// Usage:
///     ExecuteCommandArgs schema("remove_orphan_files");
///     schema.addPositional("older_than", Field::Types::String);
///     schema.addNamed("location",  Field::Types::String);
///     schema.addNamed("dry_run",   Field::Types::UInt64);
///     schema.addDefault("dry_run", Field(UInt64(0)));
///
///     auto result = schema.parse(args_ast);
///     // result.getAs<String>("older_than"), result.has("location"), etc.
///
///   Args registered without a type accept any Field type (useful for
///   complex types like Array or values needing custom conversion):
///     schema.addNamed("snapshot_ids");   // no type check
///
class ExecuteCommandArgs
{
public:
    /// Parsed result — a thin wrapper over the resolved name->value map.
    class Result
    {
    public:
        bool has(const String & name) const { return values.contains(name); }

        const Field & get(const String & name) const
        {
            auto it = values.find(name);
            if (it == values.end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument '{}' was not provided and has no default", name);
            return it->second;
        }

        template <typename T>
        T getAs(const String & name) const { return get(name).safeGet<T>(); }

        void set(const String & name, Field value) { values[name] = std::move(value); }

        const std::unordered_map<String, Field> & all() const { return values; }

    private:
        friend class ExecuteCommandArgs;
        std::unordered_map<String, Field> values;
    };

    explicit ExecuteCommandArgs(String command_name_) : command_name(std::move(command_name_)) {}

    /// Register a positional argument with type validation.
    void addPositional(const String & name, Field::Types::Which expected_type)
    {
        positional_names.push_back(name);
        known_names.insert(name);
        expected_types[name] = expected_type;
    }

    /// Register a positional argument that accepts any Field type.
    void addPositional(const String & name)
    {
        positional_names.push_back(name);
        known_names.insert(name);
    }

    /// Register a named-only argument with type validation.
    void addNamed(const String & name, Field::Types::Which expected_type)
    {
        known_names.insert(name);
        expected_types[name] = expected_type;
    }

    /// Register a named-only argument that accepts any Field type.
    void addNamed(const String & name)
    {
        known_names.insert(name);
    }

    /// Register a default value for an argument (positional or named).
    void addDefault(const String & name, Field value)
    {
        defaults[name] = std::move(value);
    }

    /// Register a post-parse callback that runs after all arguments are resolved.
    /// Use for derived defaults (e.g. computing a timestamp from a setting).
    void addPostParse(std::function<void(Result &)> callback)
    {
        post_parse_callbacks.push_back(std::move(callback));
    }

    /// Parse the AST argument list.  `args` may be nullptr (no arguments).
    Result parse(const ASTPtr & args) const
    {
        Result result;
        bool seen_named = false;
        size_t positional_index = 0;

        if (args)
        {
            for (size_t i = 0; i < args->children.size(); ++i)
            {
                auto named = tryExtractNamedArg(args->children[i]);
                if (named.has_value())
                {
                    seen_named = true;
                    const auto & [name, value] = *named;
                    setNamed(result, name, value);
                }
                else
                {
                    if (seen_named)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Positional arguments must precede named arguments in {}()", command_name);

                    const auto * literal = args->children[i]->as<ASTLiteral>();
                    if (!literal)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Argument {} to {}() must be a literal value", i + 1, command_name);

                    setPositional(result, positional_index, literal->value);
                    ++positional_index;
                }
            }
        }

        for (const auto & [name, default_value] : defaults)
        {
            if (!result.has(name))
                result.values[name] = default_value;
        }

        for (const auto & callback : post_parse_callbacks)
            callback(result);

        return result;
    }

private:
    String command_name;
    std::vector<String> positional_names;
    std::unordered_set<String> known_names;
    std::unordered_map<String, Field::Types::Which> expected_types;
    std::unordered_map<String, Field> defaults;
    std::vector<std::function<void(Result &)>> post_parse_callbacks;

    static std::optional<std::pair<String, Field>> tryExtractNamedArg(const ASTPtr & node)
    {
        const auto * func = node->as<ASTFunction>();
        if (!func || func->name != "equals" || !func->arguments || func->arguments->children.size() != 2)
            return std::nullopt;

        const auto * ident = func->arguments->children[0]->as<ASTIdentifier>();
        const auto * lit = func->arguments->children[1]->as<ASTLiteral>();
        if (!ident || !lit)
            return std::nullopt;

        return std::make_pair(ident->name(), lit->value);
    }

    void setNamed(Result & result, const String & name, const Field & value) const
    {
        if (!known_names.contains(name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown parameter '{}' for {}", name, command_name);

        if (result.has(name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Duplicate parameter '{}' for {}()", name, command_name);

        auto it = expected_types.find(name);
        if (it != expected_types.end())
            validateType(name, value, it->second);

        result.values[name] = value;
    }

    void setPositional(Result & result, size_t index, const Field & value) const
    {
        if (index >= positional_names.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{} accepts at most {} positional argument(s)", command_name, positional_names.size());

        const auto & name = positional_names[index];

        if (result.has(name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Duplicate parameter '{}' for {}(): specified both as positional and named argument", name, command_name);

        auto it = expected_types.find(name);
        if (it != expected_types.end())
            validateType(name, value, it->second);

        result.values[name] = value;
    }

    void validateType(const String & name, const Field & value, Field::Types::Which expected) const
    {
        if (value.getType() != expected)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Parameter '{}' for {} has wrong type (got {}, expected {})",
                name, command_name, value.getTypeName(), fieldTypeToString(expected));
    }

    static const char * fieldTypeToString(Field::Types::Which type)
    {
        switch (type)
        {
            case Field::Types::Null:    return "Null";
            case Field::Types::UInt64:  return "UInt64";
            case Field::Types::Int64:   return "Int64";
            case Field::Types::Float64: return "Float64";
            case Field::Types::String:  return "String";
            case Field::Types::Array:   return "Array";
            default:                    return "Unknown";
        }
    }
};

}
