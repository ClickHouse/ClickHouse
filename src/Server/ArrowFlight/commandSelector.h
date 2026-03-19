#pragma once

#include <Interpreters/Context_fwd.h>

#include <arrow/type.h>
#include <google/protobuf/any.pb.h>


namespace DB
{

namespace ArrowFlight
{

struct SQLSet
{
    std::string sql;
    std::function<arrow::Result<std::shared_ptr<arrow::Schema>>(std::shared_ptr<arrow::Schema>)> schema_modifier;
    std::function<void(ContextPtr, Block &)> block_modifier;
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

/// commandSelector accepts arrow flight sql command in protobuf any-message and produces either resulting arrow::Table
/// (and if schema_only == true then table can be empty - only schema is requested) or set of sql query - which will be executed,
/// and, if resulting table requires modification, possible schema_modifier and block_modifier - they should consistently
/// manipulate schema and blocks to produce compatible results. In case if command is invalid error should be returned
/// in arrow::Result<std::shared_ptr<arrow::Table>> variant - on practice just return arrow::Status::<whatever error> -
/// CommandSelectorResult has implicit constructor for arrow::Status
CommandSelectorResult commandSelector(const google::protobuf::Any & any_msg, bool schema_only = false);

}

}
