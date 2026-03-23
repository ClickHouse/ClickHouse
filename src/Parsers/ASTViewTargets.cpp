#include <Parsers/ASTViewTargets.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

#include <Common/quoteString.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/IAST_erase.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

ViewTarget::~ViewTarget() = default;
ViewTarget::ViewTarget() = default;
ViewTarget::ViewTarget(const ViewTarget & other) = default;
ViewTarget & ViewTarget::operator=(const ViewTarget & other) = default;

ViewTarget::ViewTarget(Kind kind_) : kind(kind_) {}

std::string_view toString(ViewTarget::Kind kind)
{
    switch (kind)
    {
        case ViewTarget::To:      return "to";
        case ViewTarget::Inner:   return "inner";
        case ViewTarget::Data:    return "data";
        case ViewTarget::Tags:    return "tags";
        case ViewTarget::Metrics: return "metrics";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} doesn't support kind {}", __FUNCTION__, kind);
}

void parseFromString(ViewTarget::Kind & out, std::string_view str)
{
    for (auto kind : magic_enum::enum_values<ViewTarget::Kind>())
    {
        if (toString(kind) == str)
        {
            out = kind;
            return;
        }
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: Unexpected string {}", __FUNCTION__, str);
}


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

void ASTViewTargets::setInnerEngine(ViewTarget::Kind kind, ASTPtr storage_def)
{
    auto new_inner_engine = boost::static_pointer_cast<ASTStorage>(storage_def);
    if (!new_inner_engine && storage_def)
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Bad cast from type {} to ASTStorage", storage_def->getID());

    for (auto & target : targets)
    {
        if (target.kind == kind)
        {
            if (target.inner_engine == new_inner_engine)
                return;
            if (new_inner_engine)
                children.push_back(new_inner_engine);
            if (target.inner_engine)
                std::erase(children, target.inner_engine);
            target.inner_engine = new_inner_engine;
            return;
        }
    }

    if (new_inner_engine)
    {
        targets.emplace_back(kind).inner_engine = new_inner_engine;
        children.push_back(new_inner_engine);
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
        {
            target.inner_engine = boost::static_pointer_cast<ASTStorage>(target.inner_engine->clone());
            res->children.push_back(target.inner_engine);
        }
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
        auto keyword = getKeywordForTableID(target.kind);
        if (!keyword)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No keyword for table name of kind {}", toString(target.kind));
        ostr <<  " " << toStringView(*keyword)
               << " "
               << (!target.table_id.database_name.empty() ? backQuoteIfNeed(target.table_id.database_name) + "." : "")
               << backQuoteIfNeed(target.table_id.table_name);
    }

    if (target.inner_uuid != UUIDHelpers::Nil)
    {
        auto keyword = getKeywordForInnerUUID(target.kind);
        if (!keyword)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No prefix keyword for inner UUID of kind {}", toString(target.kind));
        ostr << " " << toStringView(*keyword)
               << " " << quoteString(toString(target.inner_uuid));
    }

    if (target.inner_engine)
    {
        auto keyword = getKeywordForInnerStorage(target.kind);
        if (!keyword)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No prefix keyword for table engine of kind {}", toString(target.kind));
        ostr << " " << toStringView(*keyword);
        target.inner_engine->format(ostr, s, state, frame);
    }
}

std::optional<Keyword> ASTViewTargets::getKeywordForTableID(ViewTarget::Kind kind)
{
    switch (kind)
    {
        case ViewTarget::To:      return Keyword::TO;      /// TO mydb.mydata
        case ViewTarget::Inner:   return std::nullopt;
        case ViewTarget::Data:    return Keyword::DATA;    /// DATA mydb.mydata
        case ViewTarget::Tags:    return Keyword::TAGS;    /// TAGS mydb.mytags
        case ViewTarget::Metrics: return Keyword::METRICS; /// METRICS mydb.mymetrics
    }
    UNREACHABLE();
}

std::optional<Keyword> ASTViewTargets::getKeywordForInnerStorage(ViewTarget::Kind kind)
{
    switch (kind)
    {
        case ViewTarget::To:      return std::nullopt;      /// ENGINE = MergeTree()
        case ViewTarget::Inner:   return Keyword::INNER;    /// INNER ENGINE = MergeTree()
        case ViewTarget::Data:    return Keyword::DATA;     /// DATA ENGINE = MergeTree()
        case ViewTarget::Tags:    return Keyword::TAGS;     /// TAGS ENGINE = MergeTree()
        case ViewTarget::Metrics: return Keyword::METRICS;  /// METRICS ENGINE = MergeTree()
    }
    UNREACHABLE();
}

std::optional<Keyword> ASTViewTargets::getKeywordForInnerUUID(ViewTarget::Kind kind)
{
    switch (kind)
    {
        case ViewTarget::To:      return Keyword::TO_INNER_UUID;       /// TO INNER UUID 'XXX'
        case ViewTarget::Inner:   return std::nullopt;
        case ViewTarget::Data:    return Keyword::DATA_INNER_UUID;     /// DATA INNER UUID 'XXX'
        case ViewTarget::Tags:    return Keyword::TAGS_INNER_UUID;     /// TAGS INNER UUID 'XXX'
        case ViewTarget::Metrics: return Keyword::METRICS_INNER_UUID;  /// METRICS INNER UUID 'XXX'
    }
    UNREACHABLE();
}

void ASTViewTargets::forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f)
{
    for (auto & target : targets)
    {
        f(nullptr, &target.table_ast);
        f(nullptr, &target.inner_engine);
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
    for (unsigned int i = 0; i < arr->size(); ++i)
    {
        auto target_obj = arr->getObject(i);
        if (!target_obj)
            continue;
        ViewTarget target;
        String kind_str = target_obj->getValue<String>("kind");
        parseFromString(target.kind, kind_str);
        if (target_obj->has("table_id"))
        {
            String full_name = target_obj->getValue<String>("table_id");
            auto dot_pos = full_name.find('.');
            if (dot_pos != String::npos)
                target.table_id = StorageID(full_name.substr(0, dot_pos), full_name.substr(dot_pos + 1));
            else
                target.table_id = StorageID("", full_name);
        }
        if (target_obj->has("inner_uuid"))
        {
            String uuid_str = target_obj->getValue<String>("inner_uuid");
            target.inner_uuid = parseFromString<UUID>(uuid_str);
        }
        if (target_obj->has("inner_engine"))
        {
            auto engine_obj = target_obj->getObject("inner_engine");
            if (engine_obj)
            {
                target.inner_engine = IAST::createFromJSON(*engine_obj);
                children.push_back(target.inner_engine);
            }
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
                out << ",\"table_id\":";
                writeJSONString(target.table_id.getFullTableName(), out, w.getFormatSettings());
            }
            if (target.inner_uuid != UUIDHelpers::Nil)
            {
                out << ",\"inner_uuid\":\"";
                writeUUIDText(target.inner_uuid, out);
                out << '"';
            }
            if (target.inner_engine)
            {
                out << ",\"inner_engine\":";
                target.inner_engine->writeJSON(out);
            }
            out << '}';
        }
        out << ']';
    }
}

}
