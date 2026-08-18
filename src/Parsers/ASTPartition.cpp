#include <Parsers/ASTPartition.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryParameter.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

void ASTPartition::setPartitionID(const ASTPtr & ast)
{
    if (children.empty())
    {
        children.push_back(ast);
        id = children[0].get();
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot have multiple children for partition AST");
}
void ASTPartition::setPartitionValue(const ASTPtr & ast)
{
    if (children.empty())
    {
        children.push_back(ast);
        value = children[0].get();
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot have multiple children for partition AST");
}


String ASTPartition::getID(char delim) const
{
    if (value)
        return "Partition";

    std::string id_string = id ? id->getID() : "";
    return "Partition_ID" + (delim + id_string);
}

ASTPtr ASTPartition::clone() const
{
    auto res = make_intrusive<ASTPartition>(*this);
    res->children.clear();

    if (value)
    {
        res->children.push_back(children[0]->clone());
        res->value = res->children[0].get();
    }

    if (id)
    {
        res->children.push_back(children[0]->clone());
        res->id = res->children[0].get();
    }

    return res;
}

void ASTPartition::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "Partition");
    w.writeChild("value", value);
    w.writeChild("id", id);
    w.writeBool("all", all);
    if (fields_count.has_value())
        w.writeUInt("fields_count", *fields_count);
}

void ASTPartition::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    all = r.getBool("all");

    if (r.has("fields_count"))
        fields_count = r.getUInt("fields_count");

    auto val_child = r.readChild("value");
    auto id_child = r.readChild("id");

    /// `ParserPartition` produces exactly one shape: `PARTITION ALL` (`all = true`, no value/id),
    /// `PARTITION <expr>` (only `value`), or `PARTITION ID '<id>'` (only `id`). `formatImpl`
    /// unconditionally dereferences `id` when neither `value` nor `all` is set, emits only `ALL`
    /// when `all` is set (dropping any value/id), and emits only `value` when it is set (dropping
    /// any id). Reject all the parser-impossible combinations: `{"type":"Partition"}` (no target),
    /// `all` together with a `value`/`id`, and `value` together with `id`.
    if (!val_child && !id_child && !all)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`Partition` AST requires one of 'value', 'id', or 'all' = true during AST JSON deserialization");
    if (all && (val_child || id_child))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`Partition` AST cannot set 'all' together with 'value' or 'id' during AST JSON deserialization");
    if (val_child && id_child)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`Partition` AST cannot set both 'value' and 'id' during AST JSON deserialization");

    if (id_child)
    {
        /// `PARTITION ID` accepts only a string literal or a query-parameter substitution;
        /// `MergeTreeData::getPartitionIDFromQuery` reads the literal form via
        /// `id->as<ASTLiteral>()->value.safeGet<String>()`, so any other shape would reach an
        /// internal exception path instead of a parse error.
        const auto * id_literal = id_child->as<ASTLiteral>();
        bool id_is_string_literal = id_literal && id_literal->value.getType() == Field::Types::String;
        if (!id_is_string_literal && !id_child->as<ASTQueryParameter>())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "`Partition` AST 'id' must be a string literal or a query parameter during AST JSON deserialization");
        setPartitionID(id_child);
    }

    if (val_child)
    {
        /// `PARTITION <expr>` accepts only a query-parameter substitution, a `tuple(...)` function,
        /// or a literal, and derives `fields_count` from that shape (unset for a substitution).
        /// Recompute it the same way and reject a mismatching stored value, so malformed
        /// `clickhouse_json` cannot smuggle an arbitrary expression or a forged field count into
        /// `MergeTreeData::getPartitionIDFromQuery`.
        std::optional<size_t> expected_fields_count;
        if (val_child->as<ASTQueryParameter>())
        {
            expected_fields_count = std::nullopt;
        }
        else if (const auto * tuple_func = val_child->as<ASTFunction>())
        {
            if (tuple_func->name != "tuple")
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "`Partition` AST 'value' function must be 'tuple' during AST JSON deserialization");
            const auto * arguments_ast = tuple_func->arguments ? tuple_func->arguments->as<ASTExpressionList>() : nullptr;
            expected_fields_count = arguments_ast ? arguments_ast->children.size() : 0;
        }
        else if (const auto * literal_ast = val_child->as<ASTLiteral>())
        {
            if (literal_ast->value.getType() == Field::Types::Tuple)
                expected_fields_count = literal_ast->value.safeGet<Tuple>().size();
            else
                expected_fields_count = 1;
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "`Partition` AST 'value' must be a literal, a 'tuple' function, or a query parameter during AST JSON deserialization");
        }

        if (fields_count != expected_fields_count)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "`Partition` AST 'fields_count' does not match the 'value' shape during AST JSON deserialization");

        setPartitionValue(val_child);
    }
}

void ASTPartition::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (value)
    {
        value->format(ostr, settings, state, frame);
    }
    else if (all)
    {
        ostr << "ALL";
    }
    else
    {
        ostr << "ID ";
        id->format(ostr, settings, state, frame);
    }
}
}
