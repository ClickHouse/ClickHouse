#pragma once

#include <Columns/IColumn_fwd.h>
#include <base/defines.h>
#include <base/types.h>

#include <memory>

namespace DB
{

class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;
class IDatabaseTablesIterator;
using DatabaseTablesIteratorPtr = std::unique_ptr<IDatabaseTablesIterator>;

/// Cursor over a filtered list of database names for system-table block sources:
/// the position among the databases and the per-database tables iterator are owned
/// together, and every advance of the position drops the iterator. This enforces the
/// invariant that an exhausted iterator is consumed by exactly one advance. Keeping
/// the two as independent members allowed a code path to advance the position while
/// leaving a stale exhausted iterator behind, which double-advanced on the next
/// round and silently skipped databases.
class DatabaseTablesCursor
{
public:
    explicit DatabaseTablesCursor(ColumnPtr database_names_);
    ~DatabaseTablesCursor();

    /// Move to the database to process next: consume an exhausted tables iterator
    /// (stepping past its database), then skip names that no longer resolve because
    /// the database was dropped concurrently. Stays put when the tables iterator is
    /// still valid, i.e. the previous block ended mid-database. Returns false when
    /// no databases remain; on a `true` return the tables iterator is either absent
    /// or valid, never exhausted.
    bool advanceToNextDatabase();

    /// Skip the current database without reading its tables (e.g. when a fast path
    /// emitted them through its own local iterator). Only valid once the current
    /// database is done - i.e. the tables iterator is absent or exhausted.
    void skipCurrentDatabase();

    /// The accessors below are valid only after `advanceToNextDatabase` returned true.
    const String & getDatabaseName() const
    {
        chassert(database);
        return database_name;
    }

    const DatabasePtr & getDatabase() const
    {
        chassert(database);
        return database;
    }

    bool hasTablesIterator() const { return tables_it != nullptr; }
    void setTablesIterator(DatabaseTablesIteratorPtr tables_it_);
    IDatabaseTablesIterator & getTablesIterator() const;

private:
    ColumnPtr database_names;
    size_t database_idx = 0;
    DatabasePtr database;
    String database_name;
    DatabaseTablesIteratorPtr tables_it;
};

}
