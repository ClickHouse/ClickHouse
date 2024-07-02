#include <Parsers/CreateQueryUUIDs.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>


namespace DB
{

CreateQueryUUIDs::CreateQueryUUIDs(const ASTCreateQuery & create_query)
{
    uuid = create_query.uuid;
    if (create_query.targets)
    {
        for (const auto & target : create_query.targets->targets)
            setTargetInnerUUID(target.kind, target.inner_uuid);
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
        String uuid_value_str;
        readDoubleQuotedString(name, in);
        skipWhitespaceIfAny(in);
        in >> ":";
        skipWhitespaceIfAny(in);
        readDoubleQuotedString(uuid_value_str, in);
        skipWhitespaceIfAny(in);
        UUID uuid_value = parse<UUID>(uuid_value_str);
        if (name == "uuid")
        {
            res.uuid = uuid_value;
        }
        else
        {
            ViewTarget::Kind kind;
            parseFromString(kind, name);
            res.setTargetInnerUUID(kind, uuid_value);
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

void ASTCreateQuery::setUUIDs(const CreateQueryUUIDs & uuids)
{
    uuid = uuids.uuid;

    if (targets)
        targets->resetInnerUUIDs();

    if (!uuids.targets_inner_uuids.empty())
    {
        if (!targets)
            set(targets, std::make_shared<ASTViewTargets>());

        for (const auto & [kind, inner_uuid] : uuids.targets_inner_uuids)
        {
            if (inner_uuid != UUIDHelpers::Nil)
                targets->setInnerUUID(kind, inner_uuid);
        }
    }
}

void ASTCreateQuery::resetUUIDs()
{
    setUUIDs({});
}

CreateQueryUUIDs ASTCreateQuery::generateRandomUUIDs(bool always_generate_new_uuids) const
{
    CreateQueryUUIDs res;

    if (!always_generate_new_uuids)
        res = CreateQueryUUIDs{*this};

    if (res.uuid == UUIDHelpers::Nil)
        res.uuid = UUIDHelpers::generateV4();

    auto generate_uuid = [&](ViewTarget::Kind kind)
    {
        if (const auto * target = tryGetTarget(kind))
        {
            if (!target->table_id.empty())
                return;
            if ((target->inner_uuid != UUIDHelpers::Nil) && !always_generate_new_uuids)
                return;
        }
        res.setTargetInnerUUID(kind, UUIDHelpers::generateV4());
    };

    if (!attach)
    {
        /// If destination table (to_table_id) is not specified for materialized view,
        /// then MV will create inner table. We should generate UUID of inner table here.
        if (is_materialized_view)
            generate_uuid(ViewTarget::Kind::Default);

        if (is_time_series_table)
        {
            generate_uuid(ViewTarget::Kind::Data);
            generate_uuid(ViewTarget::Kind::Tags);
            generate_uuid(ViewTarget::Kind::Metrics);
        }
    }

    return res;
}

}
