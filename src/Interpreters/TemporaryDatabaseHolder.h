#pragma once
#include <Interpreters/DatabaseCatalog.h>
//#include <Parsers/ASTDropQuery.h>
namespace DB
{

struct TemporaryDatabaseHolder : boost::noncopyable, WithContext
{
    TemporaryDatabaseHolder(ContextPtr context_, String temp_name, String global_name);
    TemporaryDatabaseHolder(TemporaryDatabaseHolder && rhs) noexcept;
    TemporaryDatabaseHolder & operator=(TemporaryDatabaseHolder && rhs) noexcept;

    ~TemporaryDatabaseHolder();

    // void executeToTableImpl(ContextPtr context_, ASTDropQuery & query, DatabasePtr & db, UUID & uuid_to_wait);
    String getTemporaryDatabaseName() const {return temporary_database_name;}
    String getGlobalDatabaseName() const {return global_database_name;}
    operator bool () const { return global_database_name != "";} /// NOLINT


    String temporary_database_name;
    String global_database_name;
};

using TemporaryDatabasesMapping = std::map<String, std::shared_ptr<TemporaryDatabaseHolder>>;
}
