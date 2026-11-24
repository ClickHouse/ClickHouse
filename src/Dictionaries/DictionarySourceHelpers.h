#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/ISimpleTransform.h>
#include <base/types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

struct DictionaryStructure;
class NamedCollection;
class SettingsChanges;

/// For simple key

Block blockForIds(const DictionaryStructure & dict_struct, const VectorWithMemoryTracking<UInt64> & ids);

/// For composite key

Block blockForKeys(
    const DictionaryStructure & dict_struct, const Columns & key_columns, const VectorWithMemoryTracking<size_t> & requested_rows);

/// Used for applying settings to copied context in some register[...]Source functions
SettingsChanges readSettingsFromDictionaryConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);
ContextMutablePtr copyContextAndApplySettingsFromDictionaryConfig(const ContextPtr & context, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

/// The query text comes from the dictionary definition (possibly a `CREATE DICTIONARY` written by a user
/// who has no other privileges) and is executed with privileges that user may not have: as an `internal`
/// query for a local ClickHouse source, or with the credentials of the configured account for an external
/// database (possibly taken from a named collection). So only a `SELECT` is allowed. Returns the parsed query.
ASTPtr checkDictionaryQueryIsSelect(const String & query, const String & description, const char * error_message);

/// Whether the dictionary source of this type sends queries to an external database: MySQL, PostgreSQL, ODBC.
bool isExternalDatabaseDictionarySource(const String & source_type);

/// Checks the queries that a source of an external database (MySQL, PostgreSQL, ODBC) sends to it: the load query
/// (`query`, or `table` with `where` and the attribute expressions), the update query (`update_field`) and `invalidate_query`.
/// They must be `SELECT` without `INTO OUTFILE` in the ClickHouse syntax, without comments, heredocs, typographic quotes and backslashes,
/// which the database may read differently, and for ODBC, whose dialect is unknown, without square brackets.
/// For MySQL and PostgreSQL, the queries can come from the named collection that the source refers to, with the keys of the
/// definition replacing its keys; `NAMED COLLECTION` on it is checked in `context` before it is read.
/// `config_prefix` is the dictionary configuration ("dictionary"). Does nothing for other sources.
/// It is used for a `CREATE DICTIONARY` query of a user and for `RESTORE`, whose definitions come from a backup: the configuration files
/// and the definitions of existing dictionaries (`ATTACH` at startup, replicas) are not checked, so that they keep loading.
void checkExternalDictionarySourceQueries(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context);

/// Checks the queries of a source of an external database as `checkExternalDictionarySourceQueries` above, from their values.
/// Used by the MySQL and PostgreSQL sources of a dictionary created with a DDL query, when a query comes from a named collection:
/// the collection is read again at each load, and it could be changed after the check of `CREATE DICTIONARY`.
void checkExternalDictionarySourceQueries(
    const DictionaryStructure & dict_struct,
    const String & source_type,
    const String & query,
    const String & where,
    const String & update_field,
    const String & invalidate_query);

/// Whether `query`, `where`, `update_field` or `invalidate_query` of the source comes from the named collection, not from the definition.
/// `collection` is the collection with the keys of the definition applied, `source_config_prefix` is "<dict_root>.source.<type>".
bool hasExternalDictionarySourceQueryFromNamedCollection(
    const Poco::Util::AbstractConfiguration & config, const String & source_config_prefix, const NamedCollection & collection);

/** A stream, adds additional columns to each block that it will read from inner stream.
     *
     *  block_to_add rows size must be equal to final sum rows size of all inner stream blocks.
     */
class TransformWithAdditionalColumns final : public ISimpleTransform
{
public:
    TransformWithAdditionalColumns(SharedHeader block_to_add_, SharedHeader header);

    void transform(Chunk & chunk) override;

    String getName() const override;

private:
    SharedHeader block_to_add;
    size_t current_range_index = 0;
};

}
