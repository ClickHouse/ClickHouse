#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
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
/// query for a local ClickHouse source, or with the credentials of the source for an external database
/// (possibly taken from a named collection). So only a `SELECT` is allowed.
void checkDictionaryQueryIsSelect(const String & query, const String & description, const char * error_message);

/// The same for a query sent to an external database (MySQL, PostgreSQL, ODBC, JDBC). It is parsed by ClickHouse,
/// so what the database can read differently is rejected too: comments (MySQL executes `/*! ... */`), heredocs,
/// typographic quotes, backslashes in quoted tokens (PostgreSQL and SQL Server read them literally), and `INTO OUTFILE`,
/// `FORMAT` and `SETTINGS`, which are a part of a `SELECT` only for ClickHouse.
void checkExternalDictionaryQueryIsSelect(const String & query, const String & description, const char * error_message);

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
