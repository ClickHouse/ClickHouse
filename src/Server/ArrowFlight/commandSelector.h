#pragma once

#include <Interpreters/Context_fwd.h>

#include <arrow/flight/types.h>
#include <arrow/type.h>
#include <google/protobuf/any.pb.h>


namespace DB
{

namespace ArrowFlight
{

using SchemaModifier = std::function<arrow::Result<std::shared_ptr<arrow::Schema>>(std::shared_ptr<arrow::Schema>)>;
using BlockModifier = std::function<void(ContextPtr, Block &)>;

struct SQLSet
{
    std::string sql;
    SchemaModifier schema_modifier;
    BlockModifier block_modifier;
};

struct CommandSelectorResult : private std::variant<SQLSet, arrow::Result<std::shared_ptr<arrow::Table>>>
{
    // NOLINTNEXTLINE(google-explicit-constructor)
    CommandSelectorResult(const SQLSet & sql_set) : std::variant<SQLSet, arrow::Result<std::shared_ptr<arrow::Table>>>(sql_set) {}
    // NOLINTNEXTLINE(google-explicit-constructor)
    CommandSelectorResult(const arrow::Result<std::shared_ptr<arrow::Table>> & table) : std::variant<SQLSet, arrow::Result<std::shared_ptr<arrow::Table>>>(table) {}
    // NOLINTNEXTLINE(google-explicit-constructor)
    CommandSelectorResult(const arrow::Status & status) : std::variant<SQLSet, arrow::Result<std::shared_ptr<arrow::Table>>>(status) {}
    // NOLINTNEXTLINE(google-explicit-constructor)
    CommandSelectorResult(std::shared_ptr<arrow::Table> table) : std::variant<SQLSet, arrow::Result<std::shared_ptr<arrow::Table>>>(table) {}

    SQLSet * getSQLSet()
    {
        return std::get_if<SQLSet>(this);
    }

    arrow::Result<std::shared_ptr<arrow::Table>> * getTable()
    {
        return std::get_if<arrow::Result<std::shared_ptr<arrow::Table>>>(this);
    }
};

inline bool isArrowFlightSql(const google::protobuf::Any & any_msg)
{
    const auto & type_url = any_msg.type_url();
    const auto slash_pos = type_url.find_last_of('/');
    const auto type_name = (slash_pos == std::string::npos) ? type_url : type_url.substr(slash_pos + 1);

    return type_name.starts_with("arrow.flight.protocol.sql.");
}

inline bool cmdIsArrowFlightSql(const std::string & cmd)
{
    if (cmd.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
        return false;
    google::protobuf::Any any_msg;
    if (!any_msg.ParseFromArray(cmd.data(), static_cast<int>(cmd.size())))
        return false;
    return isArrowFlightSql(any_msg);
}

inline bool flightDescriptorIsArrowFlightSqlCommand(const arrow::flight::FlightDescriptor & descriptor)
{
    if (descriptor.type != arrow::flight::FlightDescriptor::CMD)
        return false;
    return cmdIsArrowFlightSql(descriptor.cmd);
}

/// commandSelector accepts arrow flight sql command buffer and produces either resulting arrow::Table
/// (and if schema_only == true then table can be empty - only schema is requested) or set of sql query - which will be executed,
/// and, if resulting table requires modification, possible schema_modifier and block_modifier - they should consistently
/// manipulate schema and blocks to produce compatible results. In case command is invalid, an error should be returned
/// in arrow::Result<std::shared_ptr<arrow::Table>> variant - in practice, return arrow::Status::<...> -
/// CommandSelectorResult has an implicit constructor for arrow::Status.
CommandSelectorResult commandSelector(const std::string & cmd, bool schema_only = false);

}

}
