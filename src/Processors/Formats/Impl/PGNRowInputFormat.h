#pragma once

#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

namespace DB
{

class ReadBuffer;

/** Format for reading Portable Game Notation (PGN) files.
  * Each game is represented as one row with columns for game metadata and moves.
  * Uses a custom PGN parser to extract game metadata and moves.
  */
class PGNRowInputFormat : public IRowInputFormat
{
public:
    PGNRowInputFormat(SharedHeader header_, ReadBuffer & in_, Params params_);

    String getName() const override { return "PGNRowInputFormat"; }
    void resetParser() override;

private:
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;

    bool readGame(MutableColumns & columns);
    void skipWhitespaceAndComments();
    bool readTag(String & key, String & value);
    bool readMoves(String & moves_str);

    bool eof_reached = false;
};

class PGNSchemaReader : public IExternalSchemaReader
{
public:
    NamesAndTypesList readSchema() override;
};

}
