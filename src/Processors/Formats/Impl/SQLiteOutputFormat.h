#pragma once

#include "config.h"

#if USE_SQLITE

#include <Processors/Formats/IOutputFormat.h>
#include <Core/Block.h>

#include <Formats/FormatSettings.h>
#include <Databases/SQLite/SQLiteUtils.h>

struct sqlite3_stmt;

namespace DB
{

class SQLiteOutputFormat final : public IOutputFormat
{
public:
    SQLiteOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_);

    String getName() const override { return "SQLiteOutputFormat"; }

private:
    void writePrefix() override;
    void consume(Chunk) override;
    void writeSuffix() override;

    FormatSettings format_settings;
    Serializations serializations;
    SQLitePtr db;
    std::shared_ptr<sqlite3_stmt> insert_stmt;
};

}

#endif
