#pragma once

#include <Core/QueryProcessingStage.h>
#include <DataStreams/BlockIO.h>

#include <Processors/QueryPipeline.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;
class Context;


/// Parse and execute a query.
void executeQuery(
    ReadBuffer & istr,                  /// Where to read query from (and data for INSERT, if present).
    WriteBuffer & ostr,                 /// Where to write query output to.
    bool allow_into_outfile,            /// If true and the query contains INTO OUTFILE section, redirect output to that file.
    Context & context,                  /// DB, tables, data types, storage engines, functions, aggregate functions...
    std::function<void(const String &, const String &, const String &, const String &)> set_result_details /// If a non-empty callback is passed, it will be called with the query id, the content-type, the format, and the timezone.
);


/// More low-level function for server-to-server interaction.
/// Prepares a query for execution but doesn't execute it.
/// Returns a pair of block streams which, when used, will result in query execution.
/// This means that the caller can to the extent control the query execution pipeline.
///
/// To execute:
/// * if present, write INSERT data into BlockIO::out
/// * then read the results from BlockIO::in.
///
/// If the query doesn't involve data insertion or returning of results, out and in respectively
/// will be equal to nullptr.
///
/// Correctly formatting the results (according to INTO OUTFILE and FORMAT sections)
/// must be done separately.
BlockIO executeQuery(
    const String & query,     /// Query text without INSERT data. The latter must be written to BlockIO::out.
    Context & context,        /// DB, tables, data types, storage engines, functions, aggregate functions...
    bool internal = false,    /// If true, this query is caused by another query and thus needn't be registered in the ProcessList.
    QueryProcessingStage::Enum stage = QueryProcessingStage::Complete,    /// To which stage the query must be executed.
    bool may_have_embedded_data = false /// If insert query may have embedded data
);

/// Old interface with allow_processors flag. For compatibility.
BlockIO executeQuery(
    const String & query,
    Context & context,
    bool internal,
    QueryProcessingStage::Enum stage,
    bool may_have_embedded_data,
    bool allow_processors /// If can use processors pipeline
);

}
