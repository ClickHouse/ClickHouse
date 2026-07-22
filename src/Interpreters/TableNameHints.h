#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Common/NamePrompter.h>

#include <memory>

namespace DB
{

class IDatabase;
using ConstDatabasePtr = std::shared_ptr<const IDatabase>;

class TableNameHints : public IHints<>
{
public:
    TableNameHints(ConstDatabasePtr database_, ContextPtr context_) : context(context_), database(database_) { }

    /// getHintForTable tries to get a hint for the provided table_name in the provided
    /// database. If the results are empty, it goes for extended hints for the table
    /// with getExtendedHintForTable which looks for the table name in every database that's
    /// available in the database catalog. It finally returns a single hint which is the database
    /// name and table_name pair which is similar to the table_name provided. Perhaps something to
    /// consider is should we return more than one pair of hint?
    std::pair<String, String> getHintForTable(const String & table_name) const;

    /// getExtendedHintsForTable tries to get hint for the given table_name across all
    /// the databases that are available in the database catalog.
    std::pair<String, String> getExtendedHintForTable(const String & table_name) const;

    Names getAllRegisteredNames() const override;

private:
    ContextPtr context;
    ConstDatabasePtr database;
};

}
