#include <Parsers/ASTViewTargets.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTFromJSON.h>

#include <Common/quoteString.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <IO/WriteHelpers.h>
#include <base/EnumReflection.h>
#include <Core/UUID.h>

#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    Keyword getKeyword(ViewTarget::Kind kind)
    {
        switch (kind)
        {
            case ViewTarget::To:      return Keyword::TO;      /// TO mydb.mysamples
            case ViewTarget::Inner:   return Keyword::INNER;   /// INNER ENGINE = MergeTree()
            case ViewTarget::Samples: return Keyword::SAMPLES; /// SAMPLES mydb.mysamples
            case ViewTarget::Tags:    return Keyword::TAGS;    /// TAGS mydb.mytags
            case ViewTarget::Metrics: return Keyword::METRICS; /// METRICS mydb.mymetrics
        }
        UNREACHABLE();
    }
}

ViewTarget::~ViewTarget() = default;
ViewTarget::ViewTarget() = default;
ViewTarget::ViewTarget(const ViewTarget & other) = default;
ViewTarget & ViewTarget::operator=(const ViewTarget & other) = default;

ViewTarget::ViewTarget(Kind kind_) : kind(kind_) {}

std::vector<ViewTarget::Kind> ASTViewTargets::getKinds() const
{
    std::vector<ViewTarget::Kind> kinds;
    kinds.reserve(targets.size());
    for (const auto & target : targets)
        kinds.push_back(target.kind);
    return kinds;
}

void ASTViewTargets::setTableID(ViewTarget::Kind kind, const StorageID & table_id_)
{
    for (auto & target : targets)
    {
        if (target.kind == kind)
        {
            target.table_id = table_id_;
            return;
        }
    }
    if (table_id_)
        targets.emplace_back(kind).table_id = table_id_;
}

StorageID ASTViewTargets::getTableID(ViewTarget::Kind kind) const
{
    if (const auto * target = tryGetTarget(kind))
        return target->table_id;
    return StorageID::createEmpty();
}

bool ASTViewTargets::hasTableID(ViewTarget::Kind kind) const
{
    if (const auto * target = tryGetTarget(kind))
        return !target->table_id.empty();
    return false;
}

void ASTViewTargets::setCurrentDatabase(const String & current_database)
{
    for (auto & target : targets)
    {
        auto & table_id = target.table_id;
        if (!table_id.table_name.empty() && table_id.database_name.empty())
            table_id.database_name = current_database;
    }
}

void ASTViewTargets::setInnerUUID(ViewTarget::Kind kind, const UUID & inner_uuid_)
{
    for (auto & target : targets)
    {
        if (target.kind == kind)
        {
            target.inner_uuid = inner_uuid_;
            return;
        }
    }
    if (inner_uuid_ != UUIDHelpers::Nil)
        targets.emplace_back(kind).inner_uuid = inner_uuid_;
}

UUID ASTViewTargets::getInnerUUID(ViewTarget::Kind kind) const
{
    if (const auto * target = tryGetTarget(kind))
        return target->inner_uuid;
    return UUIDHelpers::Nil;
}

bool ASTViewTargets::hasInnerUUID(ViewTarget::Kind kind) const
{
    return getInnerUUID(kind) != UUIDHelpers::Nil;
}

void ASTViewTargets::resetInnerUUIDs()
{
    for (auto & target : targets)
        target.inner_uuid = UUIDHelpers::Nil;
}

bool ASTViewTargets::hasInnerUUIDs() const
{
    for (const auto & target : targets)
    {
        if (target.inner_uuid != UUIDHelpers::Nil)
            return true;
    }
    return false;
}

void ASTViewTargets::setInnerEngine(ViewTarget::Kind kind, ASTPtr new_inner_engine)
{
    for (auto & target : targets)
    {
        if (target.kind == kind)
        {
            if (target.inner_engine == new_inner_engine)
                return;
            if (new_inner_engine)
                setOrReplace(target.inner_engine, std::move(new_inner_engine));
            else
                reset(target.inner_engine);
            return;
        }
    }

    if (new_inner_engine)
    {
        auto & new_target = targets.emplace_back(kind);
        set(new_target.inner_engine, std::move(new_inner_engine));
    }
}

ASTStorage * ASTViewTargets::getInnerEngine(ViewTarget::Kind kind) const
{
    if (const auto * target = tryGetTarget(kind); target && target->inner_engine)
        return target->inner_engine->as<ASTStorage>();
    return nullptr;
}

std::vector<ASTStorage *> ASTViewTargets::getInnerEngines() const
{
    std::vector<ASTStorage *> res;
    res.reserve(targets.size());
    for (const auto & target : targets)
    {
        if (target.inner_engine)
            res.push_back(target.inner_engine->as<ASTStorage>());
    }
    return res;
}

void ASTViewTargets::setInnerColumns(ViewTarget::Kind kind, ASTPtr new_inner_columns)
{
    for (auto & target : targets)
    {
        if (target.kind == kind)
        {
            if (target.inner_columns == new_inner_columns)
                return;
            if (new_inner_columns)
                setOrReplace(target.inner_columns, std::move(new_inner_columns));
            else
                reset(target.inner_columns);
            return;
        }
    }

    if (new_inner_columns)
    {
        auto & new_target = targets.emplace_back(kind);
        set(new_target.inner_columns, std::move(new_inner_columns));
    }
}

ASTColumns * ASTViewTargets::getInnerColumns(ViewTarget::Kind kind) const
{
    if (const auto * target = tryGetTarget(kind); target && target->inner_columns)
        return target->inner_columns->as<ASTColumns>();
    return nullptr;
}

const ViewTarget * ASTViewTargets::tryGetTarget(ViewTarget::Kind kind) const
{
    for (const auto & target : targets)
    {
        if (target.kind == kind)
            return &target;
    }
    return nullptr;
}

ASTPtr ASTViewTargets::clone() const
{
    auto res = make_intrusive<ASTViewTargets>(*this);
    res->children.clear();
    for (auto & target : res->targets)
    {
        if (target.inner_engine)
            res->set(target.inner_engine, target.inner_engine->clone());
        if (target.inner_columns)
            res->set(target.inner_columns, target.inner_columns->clone());
        /// `table_ast` (a parameterized `TO {dst:Identifier}` target produced by JSON deserialization)
        /// is also a registered child, so it must be cloned too; otherwise the clone's `table_ast`
        /// dangles into the original's children.
        if (target.table_ast)
            res->set(target.table_ast, target.table_ast->clone());
    }
    return res;
}

void ASTViewTargets::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    for (const auto & target : targets)
        formatTarget(target, ostr, s, state, frame);
}

void ASTViewTargets::formatTarget(ViewTarget::Kind kind, WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    for (const auto & target : targets)
    {
        if (target.kind == kind)
            formatTarget(target, ostr, s, state, frame);
    }
}

void ASTViewTargets::formatTarget(const ViewTarget & target, WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame)
{
    if (target.table_id)
    {
        ostr << s.nl_or_ws << toStringView(getKeyword(target.kind)) << " "
             << (!target.table_id.database_name.empty() ? backQuoteIfNeed(target.table_id.database_name) + "." : "")
             << backQuoteIfNeed(target.table_id.table_name);
    }

    if (target.inner_uuid != UUIDHelpers::Nil)
    {
        ostr << s.nl_or_ws;
        /// Skip the "kind" prefix for ViewTarget::Inner to avoid producing "INNER INNER UUID".
        if (target.kind != ViewTarget::Inner)
            ostr << toStringView(getKeyword(target.kind)) << " ";
        ostr << "INNER UUID " << quoteString(toString(target.inner_uuid));
    }

    if (target.inner_columns)
    {
        ostr << s.nl_or_ws;
        if (target.kind != ViewTarget::Inner)
            ostr << toStringView(getKeyword(target.kind)) << " ";
        ostr << "INNER COLUMNS" << s.nl_or_ws << "(";
        auto inner_frame = frame;
        inner_frame.expression_list_always_start_on_new_line = true;
        target.inner_columns->format(ostr, s, state, inner_frame);
        if (!s.one_line)
            ostr << "\n";
        ostr << ")";
    }

    if (target.inner_engine)
    {
        /// Skip both the "kind" and "INNER" prefixes for ViewTarget::To to produce just "ENGINE" (and not "TO INNER ENGINE").
        if (target.kind != ViewTarget::To)
        {
            ostr << s.nl_or_ws;
            /// Skip the "kind" prefix for ViewTarget::Inner to avoid producing "INNER INNER ENGINE".
            if (target.kind != ViewTarget::Inner)
                ostr << toStringView(getKeyword(target.kind)) << " ";
            ostr << "INNER";
        }
        target.inner_engine->format(ostr, s, state, frame);
    }
}

void ASTViewTargets::forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f)
{
    for (auto & target : targets)
    {
        f(nullptr, &target.table_ast);
        f(nullptr, &target.inner_engine);
        f(nullptr, &target.inner_columns);
    }
}

void ASTViewTargets::setTableASTWithQueryParams(ViewTarget::Kind kind, const ASTPtr & table_)
{
    for (auto & target : targets)
    {
        if (target.kind == kind)
        {
            target.table_ast = table_;
            return;
        }
    }
    if (table_)
        targets.emplace_back(kind).table_ast = table_;
}

bool ASTViewTargets::hasTableASTWithQueryParams(ViewTarget::Kind kind) const
{
    for (const auto & target : targets)
        if (target.kind == kind)
            return target.table_ast != nullptr;
    return false;
}

ASTPtr ASTViewTargets::getTableASTWithQueryParams(ViewTarget::Kind kind)
{
    for (const auto & target : targets)
        if (target.kind == kind)
            return target.table_ast;
    return nullptr;
}

void ASTViewTargets::resetTableASTWithQueryParams(ViewTarget::Kind kind)
{
    for (auto & target : targets)
        if (target.kind == kind)
            target.table_ast.reset();
}

void ASTViewTargets::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    auto arr = r.getArray("targets");
    if (!arr)
        return;
    /// The SQL parser builds `targets` through setters (`setTableID`, `setInnerEngine`, `setInnerColumns`, ...) that
    /// merge by `ViewTarget::Kind`, so it can never produce two targets of the same kind. JSON input could append
    /// duplicates, which would make formatting (`tryGetTarget` sees only the first match) disagree with execution and
    /// access checks (which iterate every entry in `targets`). Reject duplicates to match the parser-produced shape.
    std::unordered_set<ViewTarget::Kind> seen_kinds;
    for (unsigned int i = 0; i < arr->size(); ++i)
    {
        /// Each view target is a non-AST struct; count it against `max_ast_elements`.
        countJSONDeserializationElement();
        auto target_obj = arr->getObject(i);
        if (!target_obj)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Null element at index {} in 'targets' array during AST JSON deserialization", i);
        ViewTarget target;
        /// Read scalar fields through `JSONObjectReader` so a wrong JSON scalar type is rejected with
        /// `BAD_ARGUMENTS` instead of being coerced (e.g. a number stringified into a table name).
        JSONObjectReader target_reader(*target_obj);
        String kind_str = target_reader.getString("kind");
        auto kind_opt = magic_enum::enum_cast<ViewTarget::Kind>(kind_str);
        if (!kind_opt)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown ViewTarget kind '{}' at index {} in 'targets' array during AST JSON deserialization", kind_str, i);
        target.kind = *kind_opt;
        if (!seen_kinds.insert(target.kind).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate ViewTarget kind '{}' at index {} in 'targets' array during AST JSON deserialization; each kind may appear at most once", kind_str, i);
        if (target_obj->has("table_name"))
        {
            /// Restore the `StorageID` parts separately (see `writeJSON`); the database may be empty
            /// (`TO dst`) and names may contain dots, so do not reconstruct by splitting a full name.
            const String database = target_reader.getString("table_database");
            UUID table_uuid = UUIDHelpers::Nil;
            if (target_obj->has("table_uuid"))
                table_uuid = parseFromString<UUID>(target_reader.getString("table_uuid"));
            target.table_id = StorageID(database, target_reader.getString("table_name"), table_uuid);
        }
        if (target_obj->has("inner_uuid"))
        {
            String uuid_str = target_reader.getString("inner_uuid");
            target.inner_uuid = parseFromString<UUID>(uuid_str);
        }
        if (target_obj->has("inner_engine"))
        {
            auto engine_obj = target_obj->getObject("inner_engine");
            if (!engine_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'inner_engine' is not an object at index {} in 'targets' array during AST JSON deserialization", i);
            target.inner_engine = IAST::createFromJSON(*engine_obj);
            if (!target.inner_engine->as<ASTStorage>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'inner_engine' is not a storage definition at index {} in 'targets' array during AST JSON deserialization", i);
            children.push_back(target.inner_engine);
        }
        if (target_obj->has("inner_columns"))
        {
            auto columns_obj = target_obj->getObject("inner_columns");
            if (!columns_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'inner_columns' is not an object at index {} in 'targets' array during AST JSON deserialization", i);
            target.inner_columns = IAST::createFromJSON(*columns_obj);
            if (!target.inner_columns->as<ASTColumns>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'inner_columns' is not a columns definition at index {} in 'targets' array during AST JSON deserialization", i);
            children.push_back(target.inner_columns);
        }
        if (target_obj->has("table_ast"))
        {
            auto table_ast_obj = target_obj->getObject("table_ast");
            if (!table_ast_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'table_ast' is not an object at index {} in 'targets' array during AST JSON deserialization", i);
            target.table_ast = IAST::createFromJSON(*table_ast_obj);
            if (!target.table_ast->as<ASTTableIdentifier>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'table_ast' is not a table identifier at index {} in 'targets' array during AST JSON deserialization", i);
            children.push_back(target.table_ast);
        }
        targets.push_back(std::move(target));
    }
}

void ASTViewTargets::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ViewTargets");
    if (!targets.empty())
    {
        w.writeKey("targets");
        out << '[';
        for (size_t i = 0; i < targets.size(); ++i)
        {
            if (i > 0)
                out << ',';
            const auto & target = targets[i];
            out << '{';
            out << "\"kind\":";
            writeJSONString(toString(target.kind), out, w.getFormatSettings());
            if (!target.table_id.empty())
            {
                /// Serialize the `StorageID` parts separately instead of `getFullTableName()`: the
                /// latter throws for a database-less target (`TO dst`) and a `db.table` join is
                /// ambiguous for identifiers that themselves contain dots.
                out << ",\"table_database\":";
                writeJSONString(target.table_id.database_name, out, w.getFormatSettings());
                out << ",\"table_name\":";
                writeJSONString(target.table_id.table_name, out, w.getFormatSettings());
                if (target.table_id.uuid != UUIDHelpers::Nil)
                {
                    out << R"(,"table_uuid":")";
                    writeUUIDText(target.table_id.uuid, out);
                    out << '"';
                }
            }
            if (target.inner_uuid != UUIDHelpers::Nil)
            {
                out << R"(,"inner_uuid":")";
                writeUUIDText(target.inner_uuid, out);
                out << '"';
            }
            if (target.inner_engine)
            {
                out << ",\"inner_engine\":";
                target.inner_engine->writeJSON(out);
            }
            if (target.inner_columns)
            {
                out << ",\"inner_columns\":";
                target.inner_columns->writeJSON(out);
            }
            if (target.table_ast)
            {
                out << ",\"table_ast\":";
                target.table_ast->writeJSON(out);
            }
            out << '}';
        }
        out << ']';
    }
}

}
