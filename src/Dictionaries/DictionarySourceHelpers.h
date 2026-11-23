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
/// `config_prefix` is the dictionary configuration ("dictionary"). Does nothing for other sources.
/// It is used only for a `CREATE DICTIONARY` query of a user: the configuration files and the definitions of existing dictionaries
/// (`ATTACH` at startup, `RESTORE`, replicas) are not checked, so that they keep loading.
void checkExternalDictionarySourceQueries(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

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
