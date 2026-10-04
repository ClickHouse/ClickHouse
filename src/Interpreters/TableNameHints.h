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

    VectorWithMemoryTracking<String> getAllRegisteredNames() const override;

private:
    /// Whether `name` (a candidate produced by `getAllRegisteredNames`) may be suggested to the
    /// current user. Cheap for users with `SHOW_TABLES`; for a name visible only through a
    /// `SHOW_DICTIONARIES` grant it verifies, with a single table load, that the object really is a
    /// dictionary - so a dictionary-only grant cannot leak the names of similarly-named tables, and
    /// a single typo cannot foreground-load every table in the database.
    bool isHintNameVisible(const String & name) const;

    /// How many ranked candidates the hint search considers before applying the visibility check.
    /// The user-facing message still shows a single suggestion; several candidates are examined only
    /// so that a closer hidden table cannot mask a farther but visible dictionary for a
    /// `SHOW_DICTIONARIES`-only user (see `getHintForTable`). Kept small so that, in the worst case,
    /// only a handful of tables are loaded by `isHintNameVisible` while formatting one hint.
    static constexpr size_t max_hint_candidates = 10;

    ContextPtr context;
    ConstDatabasePtr database;
};

}
