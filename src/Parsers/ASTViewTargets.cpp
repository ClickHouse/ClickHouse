#include <Parsers/ASTViewTargets.h>

#include <Common/quoteString.h>
#include <Common/SipHash.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
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


std::string_view toString(ASTViewTarget::Kind kind)
{
    switch (kind)
    {
        case ASTViewTarget::Kind::To:      return "to";
        case ASTViewTarget::Kind::Inner:   return "inner";
        case ASTViewTarget::Kind::Data:    return "data";
        case ASTViewTarget::Kind::Tags:    return "tags";
        case ASTViewTarget::Kind::Metrics: return "metrics";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} doesn't support kind {}", __FUNCTION__, kind);
}

void parseFromString(ASTViewTarget::Kind & out, std::string_view str)
{
    for (auto kind : magic_enum::enum_values<ASTViewTarget::Kind>())
    {
        if (toString(kind) == str)
        {
            out = kind;
            return;
        }
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: Unexpected string {}", __FUNCTION__, str);
}

StorageID ASTViewTarget::getTableID() const
{
    if (table_identifier)
        return table_identifier->getTableId();
    if (inner_uuid != UUIDHelpers::Nil)
        return StorageID("", "", inner_uuid);
    return StorageID::createEmpty();
}

ASTPtr ASTViewTarget::clone() const
{
    auto res = std::make_shared<ASTViewTarget>(*this);
    res->children.clear();

    if (table_identifier)
        res->set(res->table_identifier, table_identifier->clone());

    if (inner_engine)
        res->set(res->inner_engine, inner_engine->clone());

    return res;
}

void ASTViewTarget::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(static_cast<char>(kind));
    hash_state.update(inner_uuid);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTViewTarget::forEachPointerToChild(std::function<void(void**)> f)
{
    f(reinterpret_cast<void **>(&table_identifier));
    f(reinterpret_cast<void **>(&inner_engine));
}


std::vector<ASTViewTarget::Kind> ASTViewTargets::getKinds() const
{
    std::vector<ASTViewTarget::Kind> kinds;
    kinds.reserve(children.size());

    for (const auto & child : children)
            kinds.push_back(child->as<ASTViewTarget>()->kind);

    return kinds;
}

void ASTViewTargets::setTableID(ASTViewTarget::Kind kind, ASTPtr table_identifier)
{
    for (auto & child : children)
    {
        auto * target = child->as<ASTViewTarget>();
        if (target->kind == kind)
        {
            target->table_identifier = table_identifier->as<ASTTableIdentifier>();
            target->setOrReplace(target->table_identifier, table_identifier);
            return;
        }
    }

    if (table_identifier)
    {
        auto target = std::make_shared<ASTViewTarget>(kind);
        target->set(target->table_identifier, table_identifier);
        children.push_back(target);
    }
}

void ASTViewTargets::setTableID(ASTViewTarget::Kind kind, const StorageID & table_id_)
{
    if (table_id_)
    {
        ASTPtr table_identifier;
        if (!table_id_.database_name.empty())
            table_identifier = std::make_shared<ASTTableIdentifier>(table_id_.database_name, table_id_.table_name);
        else
            table_identifier = std::make_shared<ASTTableIdentifier>(table_id_.table_name);

        setTableID(kind, table_identifier);
    }
}

StorageID ASTViewTargets::getTableID(ASTViewTarget::Kind kind) const
{
    const auto * target = tryGetTarget(kind);
    if (target)
        return target->getTableID();
    return StorageID::createEmpty();
}

bool ASTViewTargets::hasTableID(ASTViewTarget::Kind kind) const
{
    if (const auto * target = tryGetTarget(kind))
        return target->table_identifier;
    return false;
}

void ASTViewTargets::setCurrentDatabase(const String & current_database)
{
    for (const auto & child : children)
    {
        auto * target = child->as<ASTViewTarget>();
        if (target->table_identifier && target->table_identifier->isShort())
        {
            auto storage_id = target->getTableID();
            if (!storage_id.table_name.empty() && storage_id.database_name.empty())
            {
                // Create a new table identifier with the current database
                ASTPtr new_identifier = std::make_shared<ASTTableIdentifier>(
                    current_database, target->table_identifier->shortName());

                target->setOrReplace(target->table_identifier, new_identifier);
            }
        }
    }
}

void ASTViewTargets::setInnerUUID(ASTViewTarget::Kind kind, const UUID & inner_uuid_)
{
    for (auto & child : children)
    {
        auto target = child->as<ASTViewTarget &>();
        if (target.kind == kind)
        {
            target.inner_uuid = inner_uuid_;
            return;
        }
    }
    if (inner_uuid_ != UUIDHelpers::Nil)
    {
        auto target = std::make_shared<ASTViewTarget>(kind);
        target->inner_uuid = inner_uuid_;
        children.push_back(target);
    }
}

UUID ASTViewTargets::getInnerUUID(ASTViewTarget::Kind kind) const
{
    if (const auto * target = tryGetTarget(kind))
        return target->inner_uuid;
    return UUIDHelpers::Nil;
}

bool ASTViewTargets::hasInnerUUID(ASTViewTarget::Kind kind) const
{
    return getInnerUUID(kind) != UUIDHelpers::Nil;
}

void ASTViewTargets::resetInnerUUIDs()
{
    for (auto & child : children)
        child->as<ASTViewTarget &>().inner_uuid = UUIDHelpers::Nil;
}

bool ASTViewTargets::hasInnerUUIDs() const
{
    for (const auto & child : children)
    {
        if (child->as<ASTViewTarget &>().inner_uuid != UUIDHelpers::Nil)
            return true;
    }
    return false;
}

void ASTViewTargets::setInnerEngine(ASTViewTarget::Kind kind, ASTPtr storage_def)
{
    auto new_inner_engine = typeid_cast<std::shared_ptr<ASTStorage>>(storage_def);
    if (!new_inner_engine && storage_def)
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Bad cast from type {} to ASTStorage", storage_def->getID());

    for (auto & child : children)
    {
        auto target = child->as<ASTViewTarget &>();
        if (target.kind == kind)
        {
            target.setOrReplace(target.inner_engine, new_inner_engine);
            return;
        }
    }

    if (new_inner_engine)
    {
        auto new_target = std::make_shared<ASTViewTarget>(kind);
        new_target->set(new_target->inner_engine, new_inner_engine);
        children.push_back(new_target);
    }
}

std::shared_ptr<ASTStorage> ASTViewTargets::getInnerEngine(ASTViewTarget::Kind kind) const
{
    if (const auto * target = tryGetTarget(kind))
        if (target->inner_engine)
            return std::dynamic_pointer_cast<ASTStorage>(target->inner_engine->shared_from_this());
    return nullptr;
}

std::vector<std::shared_ptr<ASTStorage>> ASTViewTargets::getInnerEngines() const
{
    std::vector<std::shared_ptr<ASTStorage>> res;
    res.reserve(children.size());
    for (const auto & child : children)
    {
        auto * target = child->as<ASTViewTarget>();
        if (target->inner_engine)
            res.push_back(std::dynamic_pointer_cast<ASTStorage>(target->inner_engine->shared_from_this()));
    }
    return res;
}

const ASTViewTarget * ASTViewTargets::tryGetTarget(ASTViewTarget::Kind kind) const
{
    for (const auto & child : children)
    {
        if (child->as<ASTViewTarget>()->kind == kind)
            return child->as<ASTViewTarget>();
    }
    return nullptr;
}

ASTPtr ASTViewTargets::clone() const
{
    auto res = std::make_shared<ASTViewTargets>(*this);
    res->children.clear();

    for (const auto & child : children)
    {
        chassert(child->as<ASTViewTarget>());
        res->children.push_back(child->clone());
    }

    return res;
}

void ASTViewTargets::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    for (const auto & target : children)
        formatTarget(target->as<const ASTViewTarget &>(), ostr, s, state, frame);
}

void ASTViewTargets::formatTarget(ASTViewTarget::Kind kind, WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    for (const auto & target : children)
    {
        if (target->as<const ASTViewTarget &>().kind == kind)
            formatTarget(target->as<const ASTViewTarget &>(), ostr, s, state, frame);
    }
}

void ASTViewTargets::formatTarget(const ASTViewTarget & target, WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame)
{
    if (target.kind == ASTViewTarget::To)
    {
        for (const auto & child : target.children)
            child->format(ostr, s, state, frame);
        return;
    }
    if (target.table_identifier)
    {
        auto keyword = getKeywordForTableID(target.kind);
        if (!keyword)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No keyword for table name of kind {}", toString(target.kind));

        auto table_id = target.getTableID();
        ostr <<  " " << (s.hilite ? hilite_keyword : "") << toStringView(*keyword)
               << (s.hilite ? hilite_none : "") << " "
               << (!table_id.database_name.empty() ? backQuoteIfNeed(table_id.database_name) + "." : "")
               << backQuoteIfNeed(table_id.table_name);
    }

    if (target.inner_uuid != UUIDHelpers::Nil)
    {
        auto keyword = getKeywordForInnerUUID(target.kind);
        if (!keyword)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No prefix keyword for inner UUID of kind {}", toString(target.kind));
        ostr << " " << (s.hilite ? hilite_keyword : "") << toStringView(*keyword)
               << (s.hilite ? hilite_none : "") << " " << quoteString(toString(target.inner_uuid));
    }

    if (target.inner_engine)
    {
        auto keyword = getKeywordForInnerStorage(target.kind);
        if (!keyword)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No prefix keyword for table engine of kind {}", toString(target.kind));
        ostr << " " << (s.hilite ? hilite_keyword : "") << toStringView(*keyword) << (s.hilite ? hilite_none : "");
        target.inner_engine->format(ostr, s, state, frame);
    }
}

std::optional<Keyword> ASTViewTargets::getKeywordForTableID(ASTViewTarget::Kind kind)
{
    switch (kind)
    {
        case ASTViewTarget::Kind::To:      return Keyword::TO;      /// TO mydb.mydata
        case ASTViewTarget::Kind::Inner:   return std::nullopt;
        case ASTViewTarget::Kind::Data:    return Keyword::DATA;    /// DATA mydb.mydata
        case ASTViewTarget::Kind::Tags:    return Keyword::TAGS;    /// TAGS mydb.mytags
        case ASTViewTarget::Kind::Metrics: return Keyword::METRICS; /// METRICS mydb.mymetrics
    }
    UNREACHABLE();
}

std::optional<Keyword> ASTViewTargets::getKeywordForInnerStorage(ASTViewTarget::Kind kind)
{
    switch (kind)
    {
        case ASTViewTarget::Kind::To:      return std::nullopt;      /// ENGINE = MergeTree()
        case ASTViewTarget::Kind::Inner:   return Keyword::INNER;    /// INNER ENGINE = MergeTree()
        case ASTViewTarget::Kind::Data:    return Keyword::DATA;     /// DATA ENGINE = MergeTree()
        case ASTViewTarget::Kind::Tags:    return Keyword::TAGS;     /// TAGS ENGINE = MergeTree()
        case ASTViewTarget::Kind::Metrics: return Keyword::METRICS;  /// METRICS ENGINE = MergeTree()
    }
    UNREACHABLE();
}

std::optional<Keyword> ASTViewTargets::getKeywordForInnerUUID(ASTViewTarget::Kind kind)
{
    switch (kind)
    {
        case ASTViewTarget::Kind::To:      return Keyword::TO_INNER_UUID;       /// TO INNER UUID 'XXX'
        case ASTViewTarget::Kind::Inner:   return std::nullopt;
        case ASTViewTarget::Kind::Data:    return Keyword::DATA_INNER_UUID;     /// DATA INNER UUID 'XXX'
        case ASTViewTarget::Kind::Tags:    return Keyword::TAGS_INNER_UUID;     /// TAGS INNER UUID 'XXX'
        case ASTViewTarget::Kind::Metrics: return Keyword::METRICS_INNER_UUID;  /// METRICS INNER UUID 'XXX'
    }
    UNREACHABLE();
}

}
