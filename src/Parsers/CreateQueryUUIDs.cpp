#include <Parsers/CreateQueryUUIDs.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>


namespace DB
{

CreateQueryUUIDs::CreateQueryUUIDs(const ASTCreateQuery & query, bool generate_random, bool force_random)
{
    if (!generate_random || !force_random)
    {
        uuid = query.uuid;
        if (query.targets)
        {
            for (const auto & target : query.targets->targets)
                setTargetInnerUUID(target.kind, target.inner_uuid);
        }
    }

    if (generate_random)
    {
        if (uuid == UUIDHelpers::Nil)
            uuid = UUIDHelpers::generateV4();

        /// For an ATTACH query we should never generate UUIDs for its inner target tables
        /// because for an ATTACH query those inner target tables probably already exist and can be accessible by names.
        /// If we generate random UUIDs for already existing tables then those UUIDs will not be correct making those inner target table inaccessible.
        /// Thus it's not safe for example to replace
        /// "ATTACH MATERIALIZED VIEW mv AS SELECT a FROM b" with
        /// "ATTACH MATERIALIZED VIEW mv TO INNER UUID '123e4567-e89b-12d3-a456-426614174000' AS SELECT a FROM b"
        /// This replacement is safe only for CREATE queries when inner target tables don't exist yet.
        if (!query.attach)
        {
            auto generate_target_uuid = [&](ViewTarget::Kind target_kind)
            {
                if ((query.getTargetInnerUUID(target_kind) == UUIDHelpers::Nil) && query.getTargetTableID(target_kind).empty())
                    setTargetInnerUUID(target_kind, UUIDHelpers::generateV4());
            };

            /// If destination table (to_table_id) is not specified for materialized view,
            /// then MV will create inner table. We should generate UUID of inner table here.
            /// An exception is refreshable MV that replaces inner table by renaming, changing UUID on each refresh.
            if (query.is_materialized_view && !(query.refresh_strategy && !query.refresh_strategy->append))
                generate_target_uuid(ViewTarget::To);

            if (query.is_time_series_table)
            {
                generate_target_uuid(ViewTarget::Data);
                generate_target_uuid(ViewTarget::Tags);
                generate_target_uuid(ViewTarget::Metrics);
            }
        }
    }
}

bool CreateQueryUUIDs::empty() const
{
    if (uuid != UUIDHelpers::Nil)
        return false;
    for (const auto & [_, inner_uuid] : targets_inner_uuids)
    {
        if (inner_uuid != UUIDHelpers::Nil)
            return false;
    }
    return true;
}

String CreateQueryUUIDs::toString() const
{
    WriteBufferFromOwnString out;
    out << "{";
    bool need_comma = false;
    auto add_name_and_uuid_to_string = [&](std::string_view name_, const UUID & uuid_)
    {
        if (std::exchange(need_comma, true))
            out << ", ";
        out << "\"" << name_ << "\": \"" << uuid_ << "\"";
    };
    if (uuid != UUIDHelpers::Nil)
        add_name_and_uuid_to_string("uuid", uuid);
    for (const auto & [kind, inner_uuid] : targets_inner_uuids)
    {
        if (inner_uuid != UUIDHelpers::Nil)
            add_name_and_uuid_to_string(::DB::toString(kind), inner_uuid);
    }
    out << "}";
    return out.str();
}

CreateQueryUUIDs CreateQueryUUIDs::fromString(const String & str)
{
    ReadBufferFromString in{str};
    CreateQueryUUIDs res;
    skipWhitespaceIfAny(in);
    in >> "{";
    skipWhitespaceIfAny(in);
    char c;
    while (in.peek(c) && c != '}')
    {
        String name;
        String value;
        readDoubleQuotedString(name, in);
        skipWhitespaceIfAny(in);
        in >> ":";
        skipWhitespaceIfAny(in);
        readDoubleQuotedString(value, in);
        skipWhitespaceIfAny(in);
        if (name == "uuid")
        {
            res.uuid = parse<UUID>(value);
        }
        else
        {
            ViewTarget::Kind kind;
            parseFromString(kind, name);
            res.setTargetInnerUUID(kind, parse<UUID>(value));
        }
        if (in.peek(c) && c == ',')
        {
            in.ignore(1);
            skipWhitespaceIfAny(in);
        }
    }
    in >> "}";
    return res;
}

void CreateQueryUUIDs::setTargetInnerUUID(ViewTarget::Kind kind, const UUID & new_inner_uuid)
{
    for (auto & pair : targets_inner_uuids)
    {
        if (pair.first == kind)
        {
            pair.second = new_inner_uuid;
            return;
        }
    }
    if (new_inner_uuid != UUIDHelpers::Nil)
        targets_inner_uuids.emplace_back(kind, new_inner_uuid);
}

UUID CreateQueryUUIDs::getTargetInnerUUID(ViewTarget::Kind kind) const
{
    for (const auto & pair : targets_inner_uuids)
    {
        if (pair.first == kind)
            return pair.second;
    }
    return UUIDHelpers::Nil;
}

void CreateQueryUUIDs::copyToQuery(ASTCreateQuery & query) const
{
    query.uuid = uuid;

    if (query.targets)
        query.targets->resetInnerUUIDs();

    if (!targets_inner_uuids.empty())
    {
        if (!query.targets)
            query.set(query.targets, std::make_shared<ASTViewTargets>());

        for (const auto & [kind, inner_uuid] : targets_inner_uuids)
        {
            if (inner_uuid != UUIDHelpers::Nil)
                query.targets->setInnerUUID(kind, inner_uuid);
        }
    }
}

}
