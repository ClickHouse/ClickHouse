#include <Parsers/ASTViewTargets.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/CommonParsers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}


using TargetKind = ViewTarget::Kind;

std::string_view toString(TargetKind kind)
{
    switch (kind)
    {
        case TargetKind::Default: return "default";
        case TargetKind::Inner:   return "inner";
        case TargetKind::Data:    return "data";
        case TargetKind::Tags:    return "tags";
        case TargetKind::Metrics: return "metrics";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} doesn't support kind {}", __FUNCTION__, kind);
}

void parseFromString(TargetKind & out, std::string_view str)
{
    for (auto kind : magic_enum::enum_values<TargetKind>())
    {
        if (toString(kind) == str)
        {
            out = kind;
            return;
        }
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: Unexpected string {}", __FUNCTION__, str);
}


void ASTViewTargets::setTableID(TargetKind kind, const StorageID & table_id_)
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

StorageID ASTViewTargets::getTableID(TargetKind kind) const
{
    if (const auto * target = tryGetTarget(kind))
        return target->table_id;
    return StorageID::createEmpty();
}

bool ASTViewTargets::hasTableID(TargetKind kind) const
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

void ASTViewTargets::setInnerUUID(TargetKind kind, const UUID & inner_uuid_)
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

UUID ASTViewTargets::getInnerUUID(TargetKind kind) const
{
    if (const auto * target = tryGetTarget(kind))
        return target->inner_uuid;
    return UUIDHelpers::Nil;
}

void ASTViewTargets::resetInnerUUIDs()
{
    for (auto & target : targets)
        target.inner_uuid = UUIDHelpers::Nil;
}

void ASTViewTargets::setTableEngine(TargetKind kind, ASTPtr storage_def)
{
    auto new_table_engine = typeid_cast<std::shared_ptr<ASTStorage>>(storage_def);
    if (!new_table_engine && storage_def)
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Bad cast from type {} to ASTStorage", storage_def->getID());

    for (auto & target : targets)
    {
        if (target.kind == kind)
        {
            if (target.table_engine == new_table_engine)
                return;
            if (new_table_engine)
                children.push_back(new_table_engine);
            if (target.table_engine)
                std::erase(children, target.table_engine);
            target.table_engine = new_table_engine;
            return;
        }
    }

    if (new_table_engine)
    {
        targets.emplace_back(kind).table_engine = new_table_engine;
        children.push_back(new_table_engine);
    }
}

std::shared_ptr<ASTStorage> ASTViewTargets::getTableEngine(TargetKind kind) const
{
    if (const auto * target = tryGetTarget(kind))
        return target->table_engine;
    return nullptr;
}

const ViewTarget * ASTViewTargets::tryGetTarget(TargetKind kind) const
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
    auto res = std::make_shared<ASTViewTargets>(*this);
    res->children.clear();
    for (auto & target : res->targets)
    {
        if (target.table_engine)
            res->children.push_back(target.table_engine);
    }
    return res;
}

void ASTViewTargets::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    for (const auto & target : targets)
        formatTarget(target, s, state, frame);
}

void ASTViewTargets::formatTarget(TargetKind kind, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    for (const auto & target : targets)
    {
        if (target.kind == kind)
            formatTarget(target, s, state, frame);
    }
}

void ASTViewTargets::formatTarget(const ViewTarget & target, const FormatSettings & s, FormatState & state, FormatStateStacked frame)
{
    if (target.table_id)
    {
        s.ostr <<  " " << (s.hilite ? hilite_keyword : "") << toStringView(kindToKeywordForTableID(target.kind))
               << (s.hilite ? hilite_none : "") << " "
               << (!target.table_id.database_name.empty() ? backQuoteIfNeed(target.table_id.database_name) + "." : "")
               << backQuoteIfNeed(target.table_id.table_name);
    }

    if (target.inner_uuid != UUIDHelpers::Nil)
    {
        s.ostr << " " << (s.hilite ? hilite_keyword : "") << toStringView(kindToKeywordForInnerUUID(target.kind))
               << (s.hilite ? hilite_none : "") << " " << quoteString(toString(target.inner_uuid));
    }

    if (target.table_engine)
    {
        s.ostr << " " << (s.hilite ? hilite_keyword : "") << toStringView(kindToPrefixForInnerStorage(target.kind)) << (s.hilite ? hilite_none : "");
        target.table_engine->formatImpl(s, state, frame);
    }
}

Keyword ASTViewTargets::kindToKeywordForTableID(TargetKind kind)
{
    switch (kind)
    {
        case TargetKind::Data:
            return Keyword::DATA; /// DATA mydb.mydata

        case TargetKind::Tags:
            return Keyword::TAGS; /// TAGS mydb.mytags

        case TargetKind::Metrics:
            return Keyword::METRICS; /// METRICS mydb.mymetrics

        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} doesn't support kind {}", __FUNCTION__, kind);
    }
}

Keyword ASTViewTargets::kindToPrefixForInnerStorage(TargetKind kind)
{
    switch (kind)
    {
        case TargetKind::Data:
            return Keyword::DATA;     /// DATA ENGINE = MergeTree()

        case TargetKind::Tags:
            return Keyword::TAGS;     /// TAGS ENGINE = MergeTree()

        case TargetKind::Metrics:
            return Keyword::METRICS;  /// METRICS ENGINE = MergeTree()

        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} doesn't support kind {}", __FUNCTION__, kind);
    }
}

Keyword ASTViewTargets::kindToKeywordForInnerUUID(TargetKind kind)
{
    switch (kind)
    {
        case TargetKind::Data:
            return Keyword::DATA_INNER_UUID;     /// DATA INNER UUID 'XXX'

        case TargetKind::Tags:
            return Keyword::TAGS_INNER_UUID;     /// TAGS INNER UUID 'XXX'

        case TargetKind::Metrics:
            return Keyword::METRICS_INNER_UUID;  /// METRICS INNER UUID 'XXX'

        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} doesn't support kind {}", __FUNCTION__, kind);
    }
}

void ASTViewTargets::forEachPointerToChild(std::function<void(void**)> f)
{
    for (auto & target : targets)
    {
        if (target.table_engine)
        {
            ASTStorage * new_table_engine = target.table_engine.get();
            f(reinterpret_cast<void **>(&new_table_engine));
            if (new_table_engine != target.table_engine.get())
            {
                if (new_table_engine)
                    target.table_engine = typeid_cast<std::shared_ptr<ASTStorage>>(new_table_engine->ptr());
                else
                    target.table_engine.reset();
            }
        }
    }
}

}
