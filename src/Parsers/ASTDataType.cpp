#include <Parsers/ASTDataType.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/TokenIterator.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>
#include <Core/Defines.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

#include <limits>
#include <optional>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

bool equalsCaseInsensitiveString(std::string_view lhs, std::string_view rhs)
{
    if (lhs.size() != rhs.size())
        return false;
    for (size_t i = 0; i < lhs.size(); ++i)
        if (!equalsCaseInsensitive(lhs[i], rhs[i]))
            return false;
    return true;
}

bool containsCaseInsensitive(std::string_view haystack, std::string_view needle)
{
    if (haystack.size() < needle.size())
        return false;
    for (size_t i = 0; i + needle.size() <= haystack.size(); ++i)
        if (equalsCaseInsensitiveString(haystack.substr(i, needle.size()), needle))
            return true;
    return false;
}

bool isBareUUIDTypeName(const String & name)
{
    return equalsCaseInsensitiveString(name, "uuid");
}

bool substituteBareUUIDInPlace(IAST & ast, bool in_table_function_position);
bool foldConstantStringExpression(ASTPtr & argument);

/// Position of the type name argument of a function, or `last_type_name_argument` when the type name is
/// always the last argument (as for the `JSONExtract` family, which takes a variable number of path arguments).
constexpr size_t last_type_name_argument = std::numeric_limits<size_t>::max();

struct TypeNameArgument
{
    std::string_view function_name;
    size_t argument_index;
};

/** Functions that declare their result type through a data type name in a string literal argument.
  *
  * The parser canonicalizes `CAST(expr AS T)` and `expr::T` into `CAST(expr, 'T')`, so inside persisted
  * expressions (column defaults, `ORDER BY` / `PARTITION BY` / TTL expressions, view and `AS SELECT`
  * definitions) a type name written that way survives only as such a literal. The other functions take a type
  * name as a string in the query text to begin with. All of them resolve a bare `UUID` through
  * `DataTypeFactory`, so `uuid_type_version` has to be materialized into them as well - otherwise the setting
  * would be silently ignored there.
  *
  * Deliberately absent are the functions that do not declare a type but look up an existing one by name -
  * `variantElement`, `dynamicElement`, `getTypeSerializationStreams`. There the name has to match a type that
  * is already present in the data, which may well be the historical `UUID`; rewriting it could silently turn
  * such an expression into `NULL`s instead of failing loudly.
  */
constexpr TypeNameArgument type_name_arguments[]
{
    {"CAST", 1},
    {"_CAST", 1},
    {"accurateCast", 1},
    {"accurateCastOrNull", 1},
    {"accurateCastOrDefault", 1},
    {"reinterpret", 1},
    {"defaultValueOfTypeName", 0},
    {"JSONExtract", last_type_name_argument},
    {"JSONExtractKeysAndValues", last_type_name_argument},
    {"JSONExtractCaseInsensitive", last_type_name_argument},
    {"JSONExtractKeysAndValuesCaseInsensitive", last_type_name_argument},
};

bool substituteBareUUIDInTypeNameLiteral(ASTFunction & function)
{
    if (!function.arguments)
        return false;

    auto & arguments = function.arguments->children;
    std::optional<size_t> type_argument_index;

    for (const auto & candidate : type_name_arguments)
    {
        if (!equalsCaseInsensitiveString(function.name, candidate.function_name))
            continue;

        if (candidate.argument_index == last_type_name_argument)
        {
            /// The first argument is the value to extract from, so the type can only be a later argument.
            if (arguments.size() >= 2)
                type_argument_index = arguments.size() - 1;
        }
        else if (candidate.argument_index < arguments.size())
        {
            type_argument_index = candidate.argument_index;
        }

        break;
    }

    if (!type_argument_index)
        return false;

    auto & type_argument = arguments[*type_argument_index];
    if (!foldConstantStringExpression(type_argument))
        return false;

    auto * type_literal = type_argument->as<ASTLiteral>();
    if (!type_literal || type_literal->value.getType() != Field::Types::String)
        return false;

    const auto & type_name = type_literal->value.safeGet<String>();
    if (!containsCaseInsensitive(type_name, "uuid"))
        return false;

    Tokens tokens(type_name.data(), type_name.data() + type_name.size());
    IParser::Pos pos(tokens, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    Expected expected;
    ASTPtr type_ast;

    /// An unparsable type string is left as is: it is not a bare `UUID`, and resolving it will fail later anyway.
    if (!ParserDataType{}.parse(pos, type_ast, expected) || !type_ast || pos->type != TokenType::EndOfStream)
        return false;

    if (!substituteBareUUIDInPlace(*type_ast, false))
        return false;

    type_literal->value = type_ast->formatWithSecretsOneLine();
    return true;
}

/** Rewrites a string literal that holds a whole columns declaration list ("a UInt8, id UUID").
  *
  * Table functions such as `file`, `url`, `s3`, `format`, `input` and `generateRandom` take the schema of the
  * data as such a string. When a definition containing one of them is persisted (a view, a `CREATE TABLE ... AS`
  * a table function), that string is stored verbatim and reparsed on every execution through
  * `parseColumnsListFromString`. Materializing the setting into the literal freezes the persisted schema, the
  * same way it is frozen for a regular column declaration list.
  */
bool substituteBareUUIDInColumnsListLiteral(ASTLiteral & literal)
{
    if (literal.value.getType() != Field::Types::String)
        return false;

    const auto & structure = literal.value.safeGet<String>();
    if (!containsCaseInsensitive(structure, "uuid"))
        return false;

    Tokens tokens(structure.data(), structure.data() + structure.size());
    IParser::Pos pos(tokens, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    Expected expected;
    ASTPtr columns_list;

    /// Requiring the whole literal to parse as a columns declaration list is what makes this safe to try on
    /// every string argument: a path, a format name or a URL does not parse as one, so it is left alone.
    if (!ParserColumnDeclarationList{true, true}.parse(pos, columns_list, expected) || !columns_list
        || pos->type != TokenType::EndOfStream)
        return false;

    if (!substituteBareUUIDInPlace(*columns_list, false))
        return false;

    literal.value = columns_list->formatWithSecretsOneLine();
    return true;
}

/// Functions that take a columns declaration list as a string, in the argument at the given index.
constexpr TypeNameArgument columns_list_arguments[]
{
    {"structureToCapnProtoSchema", 0},
    {"structureToProtobufSchema", 0},
};

/// Return the structure argument of a table function, if this overload has one.
///
/// This must follow the individual table-function signatures. In particular, a string which happens to look like
/// a columns list is still data for `format(format, data)`, and must not be rewritten.
std::vector<ASTPtr *> getTableFunctionStructureArguments(ASTFunction & table_function)
{
    if (!table_function.arguments)
        return {};

    auto & arguments = table_function.arguments->children;
    const auto argument_at = [&arguments](size_t index) -> ASTPtr * { return index < arguments.size() ? &arguments[index] : nullptr; };

    /// Named arguments override a positional structure slot. This also covers the object-storage table-function
    /// family, whose positional signatures differ according to credentials, but all share `structure = ...`.
    for (auto & argument : arguments)
    {
        auto * equals = argument->as<ASTFunction>();
        if (!equals || !equalsCaseInsensitiveString(equals->name, "equals") || !equals->arguments
            || equals->arguments->children.size() != 2)
            continue;

        const auto * name = equals->arguments->children[0]->as<ASTIdentifier>();
        if (name && equalsCaseInsensitiveString(name->name(), "structure"))
            return {&equals->arguments->children[1]};
    }

    const auto all_columns_list_arguments = [&arguments](size_t begin)
    {
        std::vector<ASTPtr *> result;
        if (arguments.size() <= begin)
            return result;
        result.reserve(arguments.size() - begin);
        for (size_t index = begin; index < arguments.size(); ++index)
            result.push_back(&arguments[index]);
        return result;
    };

    /// `azureBlobStorage` has several positional overloads. The structure, when present, is always
    /// the final argument of the corresponding overload; other trailing strings can be credentials,
    /// a format, or a compression method and must stay unchanged.
    const auto azure_structure_argument = [&arguments, &argument_at](size_t first_argument) -> std::vector<ASTPtr *>
    {
        if (arguments.size() < first_argument)
            return {};

        const size_t argument_count = arguments.size() - first_argument;
        switch (argument_count)
        {
            case 4:
            case 6:
            case 7:
            case 8:
            case 9:
            case 10:
                return {argument_at(arguments.size() - 1)};
            default:
                return {};
        }
    };

    if (equalsCaseInsensitiveString(table_function.name, "format"))
        return arguments.size() == 3 ? std::vector{argument_at(1)} : std::vector<ASTPtr *>{};
    if (equalsCaseInsensitiveString(table_function.name, "generateRandom") || equalsCaseInsensitiveString(table_function.name, "input")
        || equalsCaseInsensitiveString(table_function.name, "null"))
        return {argument_at(0)};
    if (equalsCaseInsensitiveString(table_function.name, "values"))
        return arguments.size() > 1 ? std::vector{argument_at(0)} : std::vector<ASTPtr *>{};
    if (equalsCaseInsensitiveString(table_function.name, "file") || equalsCaseInsensitiveString(table_function.name, "url")
        || equalsCaseInsensitiveString(table_function.name, "s3") || equalsCaseInsensitiveString(table_function.name, "gcs")
        || equalsCaseInsensitiveString(table_function.name, "oss") || equalsCaseInsensitiveString(table_function.name, "cosn")
        || equalsCaseInsensitiveString(table_function.name, "hdfs")
        || equalsCaseInsensitiveString(table_function.name, "executable") || equalsCaseInsensitiveString(table_function.name, "paimon")
        || equalsCaseInsensitiveString(table_function.name, "paimonS3") || equalsCaseInsensitiveString(table_function.name, "deltaLake")
        || equalsCaseInsensitiveString(table_function.name, "deltaLakeS3") || equalsCaseInsensitiveString(table_function.name, "hudi")
        || equalsCaseInsensitiveString(table_function.name, "iceberg") || equalsCaseInsensitiveString(table_function.name, "icebergS3")
        || equalsCaseInsensitiveString(table_function.name, "icebergAzure")
        || equalsCaseInsensitiveString(table_function.name, "deltaLakeAzure")
        || equalsCaseInsensitiveString(table_function.name, "paimonAzure")
        || equalsCaseInsensitiveString(table_function.name, "icebergHDFS") || equalsCaseInsensitiveString(table_function.name, "paimonHDFS")
        || equalsCaseInsensitiveString(table_function.name, "icebergLocal")
        || equalsCaseInsensitiveString(table_function.name, "deltaLakeLocal")
        || equalsCaseInsensitiveString(table_function.name, "paimonLocal"))
        return all_columns_list_arguments(2);
    if (equalsCaseInsensitiveString(table_function.name, "azureBlobStorage"))
        return azure_structure_argument(0);
    if (equalsCaseInsensitiveString(table_function.name, "fileCluster") || equalsCaseInsensitiveString(table_function.name, "urlCluster")
        || equalsCaseInsensitiveString(table_function.name, "hdfsCluster") || equalsCaseInsensitiveString(table_function.name, "s3Cluster")
        || equalsCaseInsensitiveString(table_function.name, "paimonCluster")
        || equalsCaseInsensitiveString(table_function.name, "paimonS3Cluster")
        || equalsCaseInsensitiveString(table_function.name, "deltaLakeCluster")
        || equalsCaseInsensitiveString(table_function.name, "deltaLakeS3Cluster")
        || equalsCaseInsensitiveString(table_function.name, "hudiCluster")
        || equalsCaseInsensitiveString(table_function.name, "icebergCluster")
        || equalsCaseInsensitiveString(table_function.name, "icebergS3Cluster")
        || equalsCaseInsensitiveString(table_function.name, "icebergAzureCluster")
        || equalsCaseInsensitiveString(table_function.name, "deltaLakeAzureCluster")
        || equalsCaseInsensitiveString(table_function.name, "paimonAzureCluster")
        || equalsCaseInsensitiveString(table_function.name, "icebergHDFSCluster")
        || equalsCaseInsensitiveString(table_function.name, "paimonHDFSCluster")
        || equalsCaseInsensitiveString(table_function.name, "icebergLocalCluster"))
        return all_columns_list_arguments(3);
    if (equalsCaseInsensitiveString(table_function.name, "azureBlobStorageCluster"))
        return azure_structure_argument(1);
    if (equalsCaseInsensitiveString(table_function.name, "mongodb"))
        return arguments.size() == 3 || arguments.size() == 4 ? std::vector{argument_at(2)}
            : arguments.size() >= 6 && arguments.size() <= 8  ? std::vector{argument_at(5)}
                                                               : std::vector<ASTPtr *>{};
    if (equalsCaseInsensitiveString(table_function.name, "redis"))
        return {argument_at(2)};
    if (equalsCaseInsensitiveString(table_function.name, "ytsaurus"))
        return arguments.size() == 2 ? std::vector{argument_at(1)}
            : arguments.size() == 4  ? std::vector{argument_at(3)}
                                     : std::vector<ASTPtr *>{};
    if (equalsCaseInsensitiveString(table_function.name, "hive"))
        return {argument_at(3)};

    return {};
}

/// Fold every argument of the function into a string, recursively. Fails when any argument is not a
/// constant string expression this folder understands.
bool foldFunctionArgumentsToStrings(ASTFunction & function, std::vector<String> & values)
{
    values.clear();
    values.reserve(function.arguments->children.size());
    for (auto & child : function.arguments->children)
    {
        if (!foldConstantStringExpression(child))
            return false;
        const auto * literal = child->as<ASTLiteral>();
        if (literal->value.getType() != Field::Types::String)
            return false;
        values.push_back(literal->value.safeGet<String>());
    }
    return true;
}

/// Mirrors `replaceOne` / `replaceAll`: non-overlapping occurrences are replaced left to right, and an
/// empty pattern leaves the haystack unchanged (see `ReplaceStringImpl::vectorConstantConstant`).
String replaceSubstring(const String & haystack, const String & needle, const String & replacement, bool replace_all)
{
    if (needle.empty())
        return haystack;

    String result;
    size_t pos = 0;
    while (true)
    {
        const size_t match = haystack.find(needle, pos);
        if (match == String::npos)
            break;
        result.append(haystack, pos, match - pos);
        result += replacement;
        pos = match + needle.size();
        if (!replace_all)
            break;
    }
    result.append(haystack, pos, String::npos);
    return result;
}

/** Materialize the subset of constant expressions accepted as type-name and structure carriers before freezing
  * their type. The carriers are evaluated as constant expressions later, so a persisted `concat('id ', 'UUID')`
  * must become a literal now.
  *
  * The fold is a whitelist of deterministic string functions whose byte-level runtime semantics are mirrored
  * here exactly: this runs during query normalization, where the expression evaluator (and the `Context` it
  * needs) is not available. An expression outside the whitelist is left alone, and the carrier is then not
  * materialized - the same as for any other dynamically built type name, which the setting deliberately does
  * not chase.
  */
bool foldConstantStringExpression(ASTPtr & argument)
{
    if (argument->as<ASTLiteral>())
        return true;

    auto * function = argument->as<ASTFunction>();
    if (!function || !function->arguments || function->parameters)
        return false;

    const auto is = [&](std::string_view name) { return equalsCaseInsensitiveString(function->name, name); };

    std::vector<String> values;

    if (is("concat"))
    {
        if (!foldFunctionArgumentsToStrings(*function, values) || values.empty())
            return false;
        String result;
        for (const auto & value : values)
            result += value;
        argument = make_intrusive<ASTLiteral>(Field(std::move(result)));
        return true;
    }

    if (is("replaceOne") || is("replaceAll") || is("replace"))
    {
        if (!foldFunctionArgumentsToStrings(*function, values) || values.size() != 3)
            return false;
        argument = make_intrusive<ASTLiteral>(Field(replaceSubstring(values[0], values[1], values[2], !is("replaceOne"))));
        return true;
    }

    /// The ASCII-only case flips; `upperUTF8` / `lowerUTF8` are deliberately absent.
    if (is("upper") || is("ucase") || is("lower") || is("lcase"))
    {
        if (!foldFunctionArgumentsToStrings(*function, values) || values.size() != 1)
            return false;
        const bool to_upper = is("upper") || is("ucase");
        for (auto & c : values[0])
        {
            if (to_upper && c >= 'a' && c <= 'z')
                c = static_cast<char>(c - 'a' + 'A');
            else if (!to_upper && c >= 'A' && c <= 'Z')
                c = static_cast<char>(c - 'A' + 'a');
        }
        argument = make_intrusive<ASTLiteral>(Field(std::move(values[0])));
        return true;
    }

    return false;
}

/** Rewrite the schema string of a table function in a persisted definition.
  */
bool substituteBareUUIDInTableFunction(ASTFunction & table_function)
{
    bool substituted = false;
    for (auto * structure_argument : getTableFunctionStructureArguments(table_function))
    {
        if (structure_argument && foldConstantStringExpression(*structure_argument))
            substituted |= substituteBareUUIDInColumnsListLiteral(*(*structure_argument)->as<ASTLiteral>());
    }
    return substituted;
}

bool substituteBareUUIDInColumnsListLiteralArgument(ASTFunction & function)
{
    if (!function.arguments)
        return false;

    auto & arguments = function.arguments->children;
    for (const auto & candidate : columns_list_arguments)
    {
        if (!equalsCaseInsensitiveString(function.name, candidate.function_name))
            continue;
        if (candidate.argument_index >= arguments.size())
            return false;
        if (!foldConstantStringExpression(arguments[candidate.argument_index]))
            return false;
        return substituteBareUUIDInColumnsListLiteral(*arguments[candidate.argument_index]->as<ASTLiteral>());
    }

    return false;
}

/** Rewrite bare `UUID` type names in a persisted definition.
  *
  * `in_table_function_position` tells whether `ast` is (or is nested inside the arguments of) a table function.
  * The schema-string rewrite must only be applied there: several table functions share their name with a regular
  * scalar function, most notably `format`, which is both `format(format_name, structure, data)` and the string
  * formatting function `format(pattern, ...)`. Rewriting by name alone would corrupt the data argument of a
  * persisted `SELECT format('{} {}', 'id UUID', 'x')`.
  */
bool substituteBareUUIDInPlace(IAST & ast, bool in_table_function_position)
{
    bool substituted = false;

    if (auto * data_type = ast.as<ASTDataType>(); data_type && isBareUUIDTypeName(data_type->name))
    {
        data_type->name = "UUID2";
        substituted = true;
    }

    auto * function = ast.as<ASTFunction>();
    if (function)
    {
        substituted |= substituteBareUUIDInTypeNameLiteral(*function);
        substituted |= substituteBareUUIDInColumnsListLiteralArgument(*function);
        if (in_table_function_position)
            substituted |= substituteBareUUIDInTableFunction(*function);
    }

    /// A table function is recognized by its position rather than by its name, so that the schema string of any
    /// table function is frozen, including ones added later.
    const IAST * table_function_child = nullptr;
    if (const auto * table_expression = ast.as<ASTTableExpression>())
        table_function_child = table_expression->table_function.get();
    else if (const auto * create = ast.as<ASTCreateQuery>())
        table_function_child = create->as_table_function;

    /// A table function nested in a wrapper (for example, `loop(url(...))`) does not have a dedicated
    /// table-expression node of its own, so the whole argument subtree of a table function keeps the position.
    const bool propagate_position = in_table_function_position && (function || ast.as<ASTExpressionList>());

    for (const auto & child : ast.children)
        if (child)
            substituted |= substituteBareUUIDInPlace(*child, propagate_position || child.get() == table_function_child);

    return substituted;
}

}

ASTPtr applyUUIDTypeVersion(const ASTPtr & type_ast, UInt64 uuid_type_version)
{
    if (uuid_type_version != 2 || !type_ast)
        return type_ast;

    auto cloned = type_ast->clone();
    if (substituteBareUUIDInPlace(*cloned, false))
        return cloned;
    return type_ast;
}

bool applyUUIDTypeVersionInPlace(IAST & ast, UInt64 uuid_type_version)
{
    if (uuid_type_version != 2)
        return false;

    return substituteBareUUIDInPlace(ast, false);
}

String ASTDataType::getID(char delim) const
{
    return "DataType" + (delim + name);
}

ASTPtr ASTDataType::clone() const
{
    auto res = make_intrusive<ASTDataType>(*this);
    const auto & arguments = getArguments();
    res->children.clear();

    if (arguments)
        res->children.push_back(arguments->clone());

    return res;
}

ASTPtr ASTDataType::getArguments() const
{
    if (!children.empty())
        return children[0];
    return nullptr;
}

void ASTDataType::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "DataType");
    w.writeString("name", name);
    if (auto args = getArguments())
        w.writeChild("arguments", args);
}

void ASTDataType::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    name = r.getString("name");
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'name' for ASTDataType");

    /// `arguments` is the `ASTExpressionList` produced by `ParserDataType`. `formatImpl` only prints
    /// the `(...)` when this child has its own `children`, so a non-list node here would be silently
    /// dropped (e.g. `Nullable(UInt8)` formatting as bare `Nullable`). Reject it at the JSON boundary.
    auto args = r.readChildOfType<ASTExpressionList>("arguments");
    if (args)
        children.push_back(args);
}

void ASTDataType::resetArguments()
{
    children.clear();
}

void ASTDataType::updateTreeHashImpl(SipHash & hash_state, bool) const
{
    hash_state.update(name.size());
    hash_state.update(name);
    /// Children are hashed automatically.
}

void ASTDataType::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << name;

    const auto & arguments = getArguments();
    if (arguments && !arguments->children.empty())
    {
        ostr << '(';

        if (!settings.one_line && settings.print_pretty_type_names && name == "Tuple")
        {
            ++frame.indent;
            std::string indent_str = settings.one_line ? "" : "\n" + std::string(4 * frame.indent, ' ');
            for (size_t i = 0, size = arguments->children.size(); i < size; ++i)
            {
                if (i != 0)
                    ostr << ',';
                ostr << indent_str;
                arguments->children[i]->format(ostr, settings, state, frame);
            }
        }
        else
        {
            frame.expression_list_prepend_whitespace = false;
            arguments->format(ostr, settings, state, frame);
        }

        ostr << ')';
    }
}

}
