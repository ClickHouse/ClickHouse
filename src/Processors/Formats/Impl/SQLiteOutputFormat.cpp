#include <Processors/Formats/Impl/SQLiteOutputFormat.h>

#if USE_SQLITE

#    include <iostream>
#    include <Interpreters/ProcessList.h>
#    include <sqlite3.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SQLITE_ENGINE_ERROR;
extern const int UNKNOWN_TABLE;
}

SQLiteOutputFormat::SQLiteOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & settings_)
    : IOutputFormat(header_, out_), format_settings(settings_)
{
}

void SQLiteOutputFormat::writePrefix()
{
    if (db)
    {
        return;
    }
    auto * fd_in = dynamic_cast<WriteBufferFromFileDescriptor *>(&out);
    int fd = 0;
    String uri;
    if (fd_in)
    {
        fd = fd_in->getFD();
        if (fd <= 2)
        {
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Can't use non-seekable WriteBuffer");
        }
        uri = fmt::format("file:///dev/fd/{}", fd);
    }

    sqlite3 * db_ptr;

    int status = sqlite3_open_v2(uri.c_str(), &db_ptr, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, nullptr);

    if (status != SQLITE_OK)
    {
        throw Exception::createDeprecated(
            fmt::format("Cannot open sqlite database. Error status: {}. Message: {}", status, sqlite3_errstr(status)),
            ErrorCodes::SQLITE_ENGINE_ERROR);
    }
    db.reset(db_ptr, sqlite3_close_v2);


    auto names_and_types = getPort(PortKind::Main).getHeader().getNamesAndTypes();
    String names_and_types_sub_query = "";
    for (size_t i = 0; i < names_and_types.size(); i++)
    {
        if (i != 0)
        {
            names_and_types_sub_query += ", ";
        }
        names_and_types_sub_query += (names_and_types[i].name + " " + names_and_types[i].type->getName());
        serializations.emplace_back(names_and_types[i].type->getDefaultSerialization());
    }
    String sql = fmt::format("CREATE TABLE if not exists result({})", names_and_types_sub_query);
    status = sqlite3_exec(db.get(), sql.c_str(), nullptr, nullptr, nullptr);

    if (status != SQLITE_OK)
    {
        throw Exception::createDeprecated(
            fmt::format("Cannot create SQLite table. Error status: {}. Message: {}", status, sqlite3_errstr(status)),
            ErrorCodes::SQLITE_ENGINE_ERROR);
    }
}

void SQLiteOutputFormat::consume(Chunk chunk)
{
    String values = "";
    for (size_t i = 0; i != chunk.getNumRows(); ++i)
    {
        const Columns & columns = chunk.getColumns();

        if (i != 0)
        {
            values += ",";
        }
        values += "(";

        for (size_t j = 0; j != chunk.getNumColumns(); ++j)
        {
            WriteBufferFromOwnString ostr;
            serializations[j]->serializeTextQuoted(*columns[j], i, ostr, format_settings);

            if (j != 0)
            {
                values += ",";
            }
            values += ostr.str();
        }

        values += ")";
    }

    String sql = fmt::format("INSERT INTO result VALUES {}", values);
    int status = sqlite3_exec(db.get(), sql.c_str(), nullptr, nullptr, nullptr);

    if (status != SQLITE_OK)
    {
        throw Exception::createDeprecated(
            fmt::format("Cannot insert into SQLite table. Error status: {}. Message: {}", status, sqlite3_errstr(status)),
            ErrorCodes::SQLITE_ENGINE_ERROR);
    }
}

void SQLiteOutputFormat::flush()
{
}

void registerOutputFormatSQLite(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "SQLite",
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & settings)
        { return std::make_shared<SQLiteOutputFormat>(buf, sample, settings); });
}
}

#endif
