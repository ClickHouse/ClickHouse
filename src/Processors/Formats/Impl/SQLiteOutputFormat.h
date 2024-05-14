#pragma once

#include "config.h"

#if USE_SQLITE

#include <Processors/Formats/IRowOutputFormat.h>
#include <Core/Block.h>

#include <Formats/FormatSettings.h>
#include <Databases/SQLite/SQLiteUtils.h>


namespace DB
{

class SQLiteOutputFormat final : public IOutputFormat
{
public:
    SQLiteOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & settings_);

    String getName() const override {return "SQLiteOutputFormat";}

    void flush() override;

private:
    void writePrefix() override;
    void consume(Chunk) override;

    FormatSettings format_settings;
    Serializations serializations;
    SQLitePtr db;
};

}

#endif
