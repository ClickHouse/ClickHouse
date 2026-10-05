#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/ReadHelpers.h>

#include <Common/SipHash.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ExpressionElementParsers.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int BAD_ARGUMENTS;
}

/// The quote style of every part, written only when some part is quoted. Double quotes are semantic under
/// `standard` name matching, so the JSON form must keep them to round-trip to the same query.
static void writePartQuotesJSON(JSONObjectWriter & w, const IdentifierName & name_parts)
{
    if (std::ranges::all_of(name_parts, [](const IdentifierPart & part) { return part.quote == IdentifierPartQuote::Unquoted; }))
        return;

    w.writeKey("part_quotes");
    auto & o = w.getOut();
    o << '[';
    for (size_t i = 0; i < name_parts.size(); ++i)
    {
        if (i > 0)
            o << ',';
        writeJSONString(JSONObjectWriter::quoteToJSONString(name_parts[i].quote), o, w.getFormatSettings());
    }
    o << ']';
}

static void readPartQuotesJSON(const JSONObjectReader & r, IdentifierName & name_parts)
{
    auto quotes = r.readStringArray("part_quotes");
    if (quotes.empty())
        return;
    if (quotes.size() != name_parts.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Identifier JSON has {} 'part_quotes' for {} name parts during AST JSON deserialization", quotes.size(), name_parts.size());
    for (size_t i = 0; i < quotes.size(); ++i)
        name_parts[i].quote = JSONObjectReader::quoteFromJSONString(quotes[i], "part_quotes");
}

ASTIdentifier::ASTIdentifier(const String & short_name, ASTPtr && name_param)
    : full_name(short_name), name_parts(std::vector<String>{short_name}), semantic(std::make_shared<IdentifierSemanticImpl>())
{
    if (!name_param)
        chassert(!full_name.empty());
    else
        children.push_back(std::move(name_param));
}

ASTIdentifier::ASTIdentifier(std::vector<String> && name_parts_, bool special, ASTs && name_params)
    : ASTIdentifier(IdentifierName(name_parts_), special, std::move(name_params))
{
}

ASTIdentifier::ASTIdentifier(IdentifierName name_parts_, bool special, ASTs && name_params)
    : name_parts(std::move(name_parts_)), semantic(std::make_shared<IdentifierSemanticImpl>())
{
    chassert(!name_parts.empty());
    semantic->special = special;
    semantic->legacy_compound = true;
    if (!name_params.empty())
    {
        [[maybe_unused]] size_t params = 0;
        for (const auto & part [[maybe_unused]] : name_parts)
        {
            if (part.spelling.empty())
                ++params;
        }
        chassert(params == name_params.size());
        children = std::move(name_params);
    }
    else
    {
        for (const auto & part [[maybe_unused]] : name_parts)
            chassert(!part.spelling.empty());

        if (!special && name_parts.size() >= 2)
            semantic->table = name_parts.end()[-2].spelling;

        resetFullName();
    }
}

bool ASTIdentifier::isParam() const
{
    return !children.empty();
}

ASTPtr ASTIdentifier::getParam() const
{
    chassert(full_name.empty() && children.size() == 1);
    return children.front()->clone();
}

void ASTIdentifier::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "Identifier");
    /// For the parametrised form (e.g. `{x:Identifier}`), `name`/`name_parts` may contain
    /// empty placeholders that correspond to `ASTQueryParameter` children. We always
    /// serialize the children when present so the round-trip preserves them.
    w.writeString("name", full_name);
    if (name_parts.size() > 1)
    {
        w.writeKey("name_parts");
        auto & o = w.getOut();
        o << '[';
        for (size_t i = 0; i < name_parts.size(); ++i)
        {
            if (i > 0) o << ',';
            writeJSONString(name_parts[i].spelling, o, w.getFormatSettings());
        }
        o << ']';
    }
    writePartQuotesJSON(w, name_parts);
    w.writeChildren(children);
    w.writeAlias(*this);
}

void ASTIdentifier::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    children = r.readChildren();
    auto parts = r.readStringArray("name_parts");
    if (!parts.empty())
    {
        size_t empty_parts = 0;
        for (const auto & part : parts)
            if (part.empty())
                ++empty_parts;
        /// Empty entries in `name_parts` are placeholders for `ASTQueryParameter` children
        /// (see the parametrised-identifier ctor). If they don't match, the AST is malformed.
        if (empty_parts != children.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ASTIdentifier JSON has {} empty 'name_parts' placeholder(s) but {} child parameter(s) during AST JSON deserialization",
                empty_parts, children.size());
        /// Every placeholder child must be an `ASTQueryParameter`: visitors such as
        /// `ReplaceQueryParameterVisitor::visitIdentifier` cast children to `ASTQueryParameter`
        /// unconditionally, so a non-parameter child here would later throw a logical error.
        for (const auto & child : children)
            if (!child->as<ASTQueryParameter>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "ASTIdentifier JSON 'name_parts' placeholder child must be an ASTQueryParameter during AST JSON deserialization");
        name_parts = IdentifierName(parts);
        /// Match the parametrised-compound ctor: leave `full_name` empty when there are
        /// query-parameter children, otherwise compute it from `name_parts`.
        if (children.empty())
            resetFullName();
        else
            full_name.clear();
        /// Restore the semantic invariants the compound `ASTIdentifier` ctor establishes:
        /// a compound identifier (>= 2 parts) is a legacy compound, and for the
        /// non-parametrised case the qualifier (`table`) is the second-to-last part.
        /// Visitors rely on this via `supposedToBeCompound`/`restoreTable`/`IdentifierSemantic`.
        if (name_parts.size() >= 2)
        {
            semantic->legacy_compound = true;
            if (children.empty())
                semantic->table = name_parts.end()[-2].spelling;
        }
    }
    else
    {
        String name = r.getString("name");
        if (name.empty())
        {
            /// Empty short name is only valid for a single-parameter identifier (`{x:Identifier}`).
            /// The child must be an `ASTQueryParameter`, like the compound placeholder branch above:
            /// `ReplaceQueryParameterVisitor::visitIdentifier` casts it unconditionally, so a literal
            /// or function child here would later throw a logical error.
            if (children.size() != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "ASTIdentifier JSON with empty 'name' must have exactly one parameter child, got {}",
                    children.size());
            if (!children[0]->as<ASTQueryParameter>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "ASTIdentifier JSON with empty 'name' must have an ASTQueryParameter child during AST JSON deserialization");
            full_name.clear();
            name_parts = IdentifierName(std::vector<String>{""});
        }
        else
        {
            if (!children.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "ASTIdentifier JSON with non-empty 'name' must not have parameter children, got {}",
                    children.size());
            setShortName(name);
        }
    }
    readPartQuotesJSON(r, name_parts);
    r.readAlias(*this);
}

ASTPtr ASTIdentifier::clone() const
{
    auto ret = make_intrusive<ASTIdentifier>(*this);
    ret->semantic = std::make_shared<IdentifierSemanticImpl>(*ret->semantic);
    ret->cloneChildren();
    return ret;
}

bool ASTIdentifier::supposedToBeCompound() const
{
    return semantic->legacy_compound;
}

void ASTIdentifier::setShortName(const String & new_name)
{
    chassert(!new_name.empty());

    full_name = new_name;
    name_parts = IdentifierName(std::vector<String>{new_name});

    bool special = semantic->special;
    auto table = semantic->table;

    *semantic = IdentifierSemanticImpl();
    semantic->special = special;
    semantic->table = table;
}

IdentifierPartQuote identifierPartQuoteFromAST(const IAST * node)
{
    if (const auto * identifier = node ? node->as<ASTIdentifier>() : nullptr)
        if (!identifier->name_parts.empty())
            return identifier->name_parts[0].quote;
    return IdentifierPartQuote::Unquoted;
}

IdentifierPartQuote identifierPartQuoteFromAST(const ASTPtr & node)
{
    return identifierPartQuoteFromAST(node.get());
}

void ASTIdentifier::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// Part boundaries are semantic and survive the format/reparse round-trip, so mix them in.
    /// Quote styles do not (formatting honors identifier_quoting_style), so they stay out of the hash.
    if (name_parts.size() > 1)
    {
        for (const auto & part : name_parts)
            hash_state.update(part.spelling.size());
    }
    ASTWithAlias::updateTreeHashImpl(hash_state, ignore_aliases);
}

const String & ASTIdentifier::name() const
{
    if (children.empty())
    {
        chassert(!name_parts.empty());
        chassert(!full_name.empty());
    }

    return full_name;
}

void ASTIdentifier::formatImplWithoutAlias(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    auto format_element = [&](const String & elem_name)
    {
        if (auto special_delimiter_and_identifier = ParserCompoundIdentifier::splitSpecialDelimiterAndIdentifierIfAny(elem_name))
        {
            ostr << special_delimiter_and_identifier->first;
            settings.writeIdentifier(ostr, special_delimiter_and_identifier->second, /*ambiguous=*/false);
        }
        else
        {
            settings.writeIdentifier(ostr, elem_name, /*ambiguous=*/false);
        }
    };

    if (compound())
    {
        for (size_t i = 0, j = 0, size = name_parts.size(); i < size; ++i)
        {
            if (i != 0)
                ostr << '.';

            /// Some AST rewriting code, like IdentifierSemantic::setColumnLongName,
            /// does not respect children of identifier.
            /// Here we also ignore children if they are empty.
            if (name_parts[i].spelling.empty() && j < children.size())
            {
                children[j]->format(ostr, settings, state, frame);
                ++j;
            }
            else
                format_element(name_parts[i].spelling);
        }
    }
    else
    {
        const auto & name = shortName();
        if (name.empty() && !children.empty())
            children.front()->format(ostr, settings, state, frame);
        else
            format_element(name);
    }
}

void ASTIdentifier::appendColumnNameImpl(WriteBuffer & ostr) const
{
    writeString(name(), ostr);
}

void ASTIdentifier::restoreTable()
{
    if (!compound())
    {
        name_parts.parts.insert(name_parts.parts.begin(), IdentifierPart{semantic->table});
        resetFullName();
    }
}

boost::intrusive_ptr<ASTTableIdentifier> ASTIdentifier::createTable() const
{
    /// A parameterized name is not resolvable: the rebuilt identifier below would drop the
    /// parameter expressions and keep only their empty-string placeholders.
    if (isParam())
        return nullptr;

    if (name_parts.size() == 1 || name_parts.size() == 2)
        return make_intrusive<ASTTableIdentifier>(name_parts);
    return nullptr;
}

void ASTIdentifier::resetFullName()
{
    full_name = name_parts[0].spelling;
    for (size_t i = 1; i < name_parts.size(); ++i)
        full_name += '.' + name_parts[i].spelling;
}

ASTTableIdentifier::ASTTableIdentifier(const String & table_name, ASTs && name_params)
    : ASTIdentifier({table_name}, true, std::move(name_params))
{
}

namespace
{

IdentifierName storageIDToIdentifierName(const StorageID & table_id)
{
    IdentifierName name;
    if (!table_id.database_name.empty())
        name.push_back(IdentifierPart{table_id.database_name, table_id.database_name_quote});
    name.push_back(IdentifierPart{table_id.table_name, table_id.table_name_quote});
    return name;
}

}

ASTTableIdentifier::ASTTableIdentifier(const StorageID & table_id, ASTs && name_params)
    : ASTIdentifier(storageIDToIdentifierName(table_id), true, std::move(name_params))
{
    uuid = table_id.uuid;
}

ASTTableIdentifier::ASTTableIdentifier(IdentifierName name_parts_, ASTs && name_params)
    : ASTIdentifier(std::move(name_parts_), true, std::move(name_params))
{
    chassert(name_parts.size() == 1 || name_parts.size() == 2);
}

ASTTableIdentifier::ASTTableIdentifier(const String & database_name, const String & table_name, ASTs && name_params)
    : ASTIdentifier({database_name, table_name}, true, std::move(name_params))
{
}

void ASTTableIdentifier::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "TableIdentifier");
    /// Mirror `ASTIdentifier`: serialize children when present so parametrised
    /// forms round-trip without loss.
    w.writeString("name", full_name);
    if (name_parts.size() > 1)
    {
        w.writeKey("name_parts");
        auto & o = w.getOut();
        o << '[';
        for (size_t i = 0; i < name_parts.size(); ++i)
        {
            if (i > 0) o << ',';
            writeJSONString(name_parts[i].spelling, o, w.getFormatSettings());
        }
        o << ']';
    }
    if (uuid != UUIDHelpers::Nil)
    {
        /// A nested `ASTTableIdentifier` can carry a `UUID` from the parser (e.g. a `REFRESH DEPENDS ON src UUID '...'`
        /// dependency), but a table reference is formatted through `ASTIdentifier::formatImplWithoutAlias`, which never
        /// emits a `UUID` clause. `readJSON` therefore rejects a `UUID`-bearing reference (see the paired guard there),
        /// so emitting `"uuid"` here would produce JSON that `formatQueryFromJSON` / `clickhouse_json` cannot read back.
        /// Fail closed at serialization instead, honouring the contract that `parseQueryToJSON` rejects unsupported
        /// shapes rather than emitting unreadable JSON.
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "A nested table reference with a UUID cannot be represented as AST JSON: it cannot be formatted back to SQL "
            "faithfully during AST JSON serialization");
    }
    writePartQuotesJSON(w, name_parts);
    w.writeChildren(children);
    w.writeAlias(*this);
}

void ASTTableIdentifier::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    children = r.readChildren();
    auto parts = r.readStringArray("name_parts");
    if (!parts.empty())
    {
        size_t empty_parts = 0;
        for (const auto & part : parts)
            if (part.empty())
                ++empty_parts;
        if (empty_parts != children.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ASTTableIdentifier JSON has {} empty 'name_parts' placeholder(s) but {} child parameter(s) during AST JSON deserialization",
                empty_parts, children.size());
        /// Every placeholder child must be an `ASTQueryParameter` (see `ASTIdentifier::readJSON`).
        for (const auto & child : children)
            if (!child->as<ASTQueryParameter>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "ASTTableIdentifier JSON 'name_parts' placeholder child must be an ASTQueryParameter during AST JSON deserialization");
        name_parts = IdentifierName(parts);
        /// `ASTTableIdentifier` can only represent a one- or two-part table name
        /// (`table` or `database.table`): `ParserCompoundIdentifier` rejects `parts.size() > 2`
        /// for table identifiers. `getTableId`/`getDatabaseName` would otherwise mis-resolve a
        /// longer name (treating `db.tbl.extra` as the single table `db`), formatting a different
        /// target than execution uses. Reject the parser-impossible shape at the JSON boundary.
        if (name_parts.size() > 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ASTTableIdentifier JSON 'name_parts' has {} parts, but a table identifier accepts at most 2 during AST JSON deserialization",
                name_parts.size());
        if (children.empty())
            resetFullName();
        else
            full_name.clear();
        /// Mirror the compound ctor: a table identifier is always `special`, so it is a
        /// legacy compound when it has >= 2 parts, but the `table` qualifier is not stored.
        if (name_parts.size() >= 2)
            semantic->legacy_compound = true;
    }
    else
    {
        String name = r.getString("name");
        if (name.empty())
        {
            /// As in `ASTIdentifier::readJSON`, the single child of an empty-name identifier must be an
            /// `ASTQueryParameter`; query-parameter visitors downcast it unconditionally.
            if (children.size() != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "ASTTableIdentifier JSON with empty 'name' must have exactly one parameter child, got {}",
                    children.size());
            if (!children[0]->as<ASTQueryParameter>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "ASTTableIdentifier JSON with empty 'name' must have an ASTQueryParameter child during AST JSON deserialization");
            full_name.clear();
            name_parts = IdentifierName(std::vector<String>{""});
        }
        else
        {
            if (!children.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "ASTTableIdentifier JSON with non-empty 'name' must not have parameter children, got {}",
                    children.size());
            setShortName(name);
        }
    }
    if (r.has("uuid"))
    {
        /// A nested `ASTTableIdentifier` can carry a `UUID` from the parser (e.g. a `REFRESH DEPENDS ON src UUID '...'`
        /// dependency), and `getTableId` feeds that `UUID` into the executed `StorageID`. But a table reference is
        /// formatted through `ASTIdentifier::formatImplWithoutAlias`, which never emits a `UUID` clause, so
        /// `formatQueryFromJSON` would print a name-only reference while the JSON AST still resolves the table by
        /// `UUID`. Reject the `UUID`-bearing nested reference at the boundary (fail closed) rather than hiding
        /// semantic state behind a lossy round trip.
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ASTTableIdentifier JSON must not carry a 'uuid': a nested table reference with a UUID cannot be "
            "formatted back to SQL faithfully during AST JSON deserialization");
    }
    readPartQuotesJSON(r, name_parts);
    r.readAlias(*this);
}

ASTPtr ASTTableIdentifier::clone() const
{
    auto ret = make_intrusive<ASTTableIdentifier>(*this);
    ret->semantic = std::make_shared<IdentifierSemanticImpl>(*ret->semantic);
    ret->cloneChildren();
    return ret;
}

StorageID ASTTableIdentifier::getTableId() const
{
    if (name_parts.size() == 2)
    {
        StorageID table_id{name_parts[0].spelling, name_parts[1].spelling, uuid};
        table_id.database_name_quote = name_parts[0].quote;
        table_id.table_name_quote = name_parts[1].quote;
        return table_id;
    }
    StorageID table_id{{}, name_parts[0].spelling, uuid};
    table_id.table_name_quote = name_parts[0].quote;
    return table_id;
}

String ASTTableIdentifier::getDatabaseName() const
{
    if (name_parts.size() == 2) return name_parts[0].spelling;
    return {};
}

static ASTPtr makeIdentifierFromPart(const IdentifierPart & part)
{
    auto identifier = make_intrusive<ASTIdentifier>(part.spelling);
    identifier->name_parts[0].quote = part.quote;
    return identifier;
}

ASTPtr ASTTableIdentifier::getTable() const
{
    if (name_parts.size() == 2)
    {
        if (!name_parts[1].spelling.empty())
            return makeIdentifierFromPart(name_parts[1]);

        if (name_parts[0].spelling.empty())
            return make_intrusive<ASTIdentifier>("", children[1]->clone());
        return make_intrusive<ASTIdentifier>("", children[0]->clone());
    }
    if (name_parts.size() == 1)
    {
        if (name_parts[0].spelling.empty())
            return make_intrusive<ASTIdentifier>("", children[0]->clone());
        return makeIdentifierFromPart(name_parts[0]);
    }
    return {};
}

ASTPtr ASTTableIdentifier::getDatabase() const
{
    if (name_parts.size() == 2)
    {
        if (name_parts[0].spelling.empty())
            return make_intrusive<ASTIdentifier>("", children[0]->clone());
        return makeIdentifierFromPart(name_parts[0]);
    }
    return {};
}

void ASTTableIdentifier::resetTable(const String & database_name, const String & table_name)
{
    auto identifier = make_intrusive<ASTTableIdentifier>(StorageID{database_name, table_name});
    full_name.swap(identifier->full_name);
    std::swap(name_parts, identifier->name_parts);
    uuid = identifier->uuid;
}

void ASTTableIdentifier::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(uuid);
    ASTIdentifier::updateTreeHashImpl(hash_state, ignore_aliases);
}

String getIdentifierName(const IAST * ast)
{
    String res;
    if (tryGetIdentifierNameInto(ast, res))
        return res;
    if (ast)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "{} is not an identifier", ast->formatForErrorMessage());
    throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "AST node is nullptr");
}

std::optional<String> tryGetIdentifierName(const IAST * ast)
{
    String res;
    if (tryGetIdentifierNameInto(ast, res))
        return res;
    return {};
}

bool tryGetIdentifierNameInto(const IAST * ast, String & name)
{
    if (ast)
    {
        if (const auto * node = dynamic_cast<const ASTIdentifier *>(ast))
        {
            name = node->name();
            return true;
        }
    }
    return false;
}

void setIdentifierSpecial(ASTPtr & ast)
{
    if (ast)
        if (auto * id = ast->as<ASTIdentifier>())
            id->semantic->special = true;
}

}
