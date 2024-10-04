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
    auto new_inner_engine = typeid_cast<std::shared_ptr<ASTStorage>>(storage_def);
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

std::shared_ptr<ASTStorage> ASTViewTargets::getInnerEngine(ViewTarget::Kind kind) const
{
    if (const auto * target = tryGetTarget(kind))
        return target->inner_engine;
    return nullptr;
}

std::vector<std::shared_ptr<ASTStorage>> ASTViewTargets::getInnerEngines() const
{
    std::vector<std::shared_ptr<ASTStorage>> res;
    res.reserve(targets.size());
    for (const auto & target : targets)
    {
        if (target.inner_engine)
            res.push_back(target.inner_engine);
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
    auto res = std::make_shared<ASTViewTargets>(*this);
    res->children.clear();
    for (auto & target : res->targets)
    {
        if (target.inner_engine)
        {
            target.inner_engine = typeid_cast<std::shared_ptr<ASTStorage>>(target.inner_engine->clone());
            res->children.push_back(target.inner_engine);
        }
    }
    return res;
}

void ASTViewTargets::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    for (const auto & target : targets)
        formatTarget(target, s, state, frame);
}

void ASTViewTargets::formatTarget(ViewTarget::Kind kind, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
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
        auto keyword = getKeywordForTableID(target.kind);
        if (!keyword)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No keyword for table name of kind {}", toString(target.kind));
        s.ostr <<  " " << (s.hilite ? hilite_keyword : "") << toStringView(*keyword)
               << (s.hilite ? hilite_none : "") << " "
               << (!target.table_id.database_name.empty() ? backQuoteIfNeed(target.table_id.database_name) + "." : "")
               << backQuoteIfNeed(target.table_id.table_name);
    }

    if (target.inner_uuid != UUIDHelpers::Nil)
    {
        auto keyword = getKeywordForInnerUUID(target.kind);
        if (!keyword)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No prefix keyword for inner UUID of kind {}", toString(target.kind));
        s.ostr << " " << (s.hilite ? hilite_keyword : "") << toStringView(*keyword)
               << (s.hilite ? hilite_none : "") << " " << quoteString(toString(target.inner_uuid));
    }

    if (target.inner_engine)
    {
        auto keyword = getKeywordForInnerStorage(target.kind);
        if (!keyword)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No prefix keyword for table engine of kind {}", toString(target.kind));
        s.ostr << " " << (s.hilite ? hilite_keyword : "") << toStringView(*keyword) << (s.hilite ? hilite_none : "");
        target.inner_engine->formatImpl(s, state, frame);
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

void ASTViewTargets::forEachPointerToChild(std::function<void(void**)> f)
{
    for (auto & target : targets)
    {
        if (target.inner_engine)
        {
            ASTStorage * new_inner_engine = target.inner_engine.get();
            f(reinterpret_cast<void **>(&new_inner_engine));
            if (new_inner_engine != target.inner_engine.get())
            {
                if (new_inner_engine)
                    target.inner_engine = typeid_cast<std::shared_ptr<ASTStorage>>(new_inner_engine->ptr());
                else
                    target.inner_engine.reset();
            }
        }
    }
}

}
