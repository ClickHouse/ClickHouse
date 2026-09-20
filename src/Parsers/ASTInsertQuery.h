#pragma once

#include <Interpreters/StorageID.h>
#include <Parsers/IAST.h>
#include <IO/ReadBuffer.h>
#include <IO/CompressionMethod.h>

class SipHash;

namespace Poco::JSON { class Object; }

namespace DB
{

class ReadBuffer;

/// Whether `select` (the SELECT part of an INSERT ... SELECT) reads inline insert data through
/// the `input` table function. Shared by the parser (to decide whether a clause like COMPRESSION
/// has a real data stream to apply to) and by ASTInsertQuery's own JSON (de)serialization.
bool selectReadsInlineDataViaInputFunction(const ASTPtr & select);

/// INSERT query
class ASTInsertQuery : public IAST
{
public:
    StorageID table_id = StorageID::createEmpty();

    ASTPtr database;
    ASTPtr table;

    ASTPtr columns;
    String format;
    ASTPtr table_function;
    ASTPtr partition_by;
    ASTPtr settings_ast;

    ASTPtr select;
    ASTPtr infile;
    ASTPtr compression;

    /// Data inlined into query
    const char * data = nullptr;
    const char * end = nullptr;

    /// Data from buffer to insert after inlined one - may be nullptr.
    mutable ReadBufferPtr tail = nullptr;

    bool async_insert_flush = false;

    String getDatabase() const;
    String getTable() const;

    void setDatabase(const String & name);
    void setTable(const String & name);

    bool hasInlinedData() const { return data || tail; }

    /// Whether `compression` resolves to an actual compression method (as opposed to 'none', or
    /// 'auto' with nothing to detect a method from). The server cannot decompress data itself, so
    /// this is what call sites check before rejecting a query that reached the server with a still
    /// -compressed data stream instead of a client-side-decompressed one.
    bool isCompressionEffective() const;

    /// Resolves `compression` to a CompressionMethod, without a file path to detect an extension
    /// from (there isn't one for stdin-piped data). `'auto'` resolves to `auto_fallback`, which the
    /// caller has typically already determined by sniffing its own real stdin descriptor (e.g. the
    /// backing file name, if any) -- `chooseCompressionMethod` has nothing to detect from here.
    /// No `compression` clause resolves to `CompressionMethod::None`.
    CompressionMethod resolveCompressionMethod(CompressionMethod auto_fallback) const;

    /// Try to find table function input() in SELECT part
    void tryFindInputFunction(ASTPtr & input_function) const;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "InsertQuery" + (delim + table_id.database_name) + delim + table_id.table_name; }

    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    ASTPtr clone() const override
    {
        auto res = make_intrusive<ASTInsertQuery>(*this);
        res->children.clear();

        if (database) { res->database = database->clone(); res->children.push_back(res->database); }
        if (table) { res->table = table->clone(); res->children.push_back(res->table); }
        if (columns) { res->columns = columns->clone(); res->children.push_back(res->columns); }
        if (table_function) { res->table_function = table_function->clone(); res->children.push_back(res->table_function); }
        if (partition_by) { res->partition_by = partition_by->clone(); res->children.push_back(res->partition_by); }
        if (settings_ast) { res->settings_ast = settings_ast->clone(); res->children.push_back(res->settings_ast); }
        if (select) { res->select = select->clone(); res->children.push_back(res->select); }
        if (infile) { res->infile = infile->clone(); res->children.push_back(res->infile); }
        if (compression) { res->compression = compression->clone(); res->children.push_back(res->compression); }

        return res;
    }

    QueryKind getQueryKind() const override { return async_insert_flush ? QueryKind::AsyncInsertFlush : QueryKind::Insert; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
};

}
