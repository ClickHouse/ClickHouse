#include <Parsers/ASTViewTargets.h>

#include <Common/quoteString.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/CommonParsers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    Keyword getKeyword(ViewTarget::Kind kind)
    {
        switch (kind)
        {
            case ViewTarget::To:      return Keyword::TO;      /// TO mydb.mydata
            case ViewTarget::Inner:   return Keyword::INNER;   /// INNER ENGINE = MergeTree()
            case ViewTarget::Data:    return Keyword::DATA;    /// DATA mydb.mydata
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
        ostr << s.nl_or_ws << toStringView(getKeyword(target.kind)) << " INNER UUID " << quoteString(toString(target.inner_uuid));
    }

    if (target.inner_engine)
    {
        ostr << s.nl_or_ws << toStringView(getKeyword(target.kind));
        target.inner_engine->format(ostr, s, state, frame);
    }
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
}
