#include <Processors/Formats/Impl/PGNRowInputFormat.h>
#include <IO/ReadHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/FormatFactory.h>
#include <base/find_symbols.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
}

/** PGN Parser - parses Portable Game Notation format.
  * Each game consists of:
  * 1. Tags in format [TagName "TagValue"]
  * 2. Move text section with moves in algebraic notation
  * 3. Result at the end (like 1-0, 0-1, 1/2-1/2, *)
  */
class PGNParser
{
public:
    struct Game
    {
        String event;
        String site;
        String date;
        String round;
        String white;
        String black;
        String result;
        Int32 white_elo = 0;
        Int32 black_elo = 0;
        String moves;
    };

    static bool parseGame(ReadBuffer & in, Game & game)
    {
        game = Game();

        // Skip leading whitespace
        skipWhitespaceAndComments(in);

        if (in.eof())
            return false;

        // Read tags
        while (!in.eof() && *in.position() == '[')
        {
            String tag_name;
            String tag_value;
            if (!readTag(in, tag_name, tag_value))
                break;

            if (tag_name == "Event")
                game.event = tag_value;
            else if (tag_name == "Site")
                game.site = tag_value;
            else if (tag_name == "Date")
                game.date = tag_value;
            else if (tag_name == "Round")
                game.round = tag_value;
            else if (tag_name == "White")
                game.white = tag_value;
            else if (tag_name == "Black")
                game.black = tag_value;
            else if (tag_name == "Result")
                game.result = tag_value;
            else if (tag_name == "WhiteElo")
            {
                try
                {
                    game.white_elo = std::stoi(tag_value);
                }
                catch (...)  // NOLINT(bugprone-empty-catch)
                {
                    // Invalid Elo value, keep default of 0
                }
            }
            else if (tag_name == "BlackElo")
            {
                try
                {
                    game.black_elo = std::stoi(tag_value);
                }
                catch (...)  // NOLINT(bugprone-empty-catch)
                {
                    // Invalid Elo value, keep default of 0
                }
            }

            skipWhitespaceAndComments(in);
        }

        // Read moves section
        if (!readMoves(in, game.moves))
            return false;

        return true;
    }

private:
    static void skipWhitespaceAndComments(ReadBuffer & in)
    {
        while (!in.eof())
        {
            char c = *in.position();
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r')
            {
                in.ignore();
            }
            else if (c == ';')
            {
                // Skip comment until end of line
                while (!in.eof() && *in.position() != '\n')
                    in.ignore();
            }
            else if (c == '{')
            {
                // Skip block comment
                in.ignore();
                while (!in.eof() && *in.position() != '}')
                    in.ignore();
                if (!in.eof())
                    in.ignore(); // skip closing }
            }
            else
            {
                break;
            }
        }
    }

    static bool readTag(ReadBuffer & in, String & tag_name, String & tag_value)
    {
        if (in.eof() || *in.position() != '[')
            return false;

        in.ignore(); // skip [

        skipWhitespaceAndComments(in);

        // Read tag name
        tag_name.clear();
        while (!in.eof() && *in.position() != ' ' && *in.position() != '\t')
        {
            tag_name += *in.position();
            in.ignore();
        }

        skipWhitespaceAndComments(in);

        if (in.eof() || *in.position() != '"')
            return false;

        in.ignore(); // skip "

        // Read tag value
        tag_value.clear();
        while (!in.eof() && *in.position() != '"')
        {
            if (*in.position() == '\\' && !in.eof())
            {
                in.ignore();
                if (!in.eof())
                {
                    tag_value += *in.position();
                    in.ignore();
                }
            }
            else
            {
                tag_value += *in.position();
                in.ignore();
            }
        }

        if (in.eof() || *in.position() != '"')
            return false;

        in.ignore(); // skip "

        skipWhitespaceAndComments(in);

        if (!in.eof() && *in.position() == ']')
        {
            in.ignore(); // skip ]
            skipWhitespaceAndComments(in);
            return true;
        }

        return false;
    }

    static bool readMoves(ReadBuffer & in, String & moves_str)
    {
        moves_str.clear();

        skipWhitespaceAndComments(in);

        // Read move text until end of game (next [ or EOF or blank line followed by [)
        while (!in.eof() && *in.position() != '[')
        {
            if (*in.position() == '(' || *in.position() == '{' || *in.position() == ';')
            {
                // Skip variations and comments
                if (*in.position() == ';')
                {
                    while (!in.eof() && *in.position() != '\n')
                        in.ignore();
                }
                else if (*in.position() == '{')
                {
                    in.ignore();
                    while (!in.eof() && *in.position() != '}')
                        in.ignore();
                    if (!in.eof())
                        in.ignore();
                }
                else if (*in.position() == '(')
                {
                    int depth = 1;
                    in.ignore();
                    while (!in.eof() && depth > 0)
                    {
                        if (*in.position() == '(')
                            depth++;
                        else if (*in.position() == ')')
                            depth--;
                        in.ignore();
                    }
                }
            }
            else if (*in.position() == ' ' || *in.position() == '\t' || *in.position() == '\n' || *in.position() == '\r')
            {
                in.ignore();
            }
            else
            {
                // Read move or move number
                char c = *in.position();
                if ((c >= 'a' && c <= 'h') || (c >= 'A' && c <= 'H') || c == 'K' || c == 'Q' || c == 'R' || c == 'B' || c == 'N' || c == 'O')
                {
                    // Move found
                    while (!in.eof() && *in.position() != ' ' && *in.position() != '\t' && *in.position() != '\n' && *in.position() != '\r')
                    {
                        moves_str += *in.position();
                        in.ignore();
                    }
                    moves_str += ' ';
                }
                else if (c >= '0' && c <= '9')
                {
                    // Move number or result - skip to next space
                    while (!in.eof() && *in.position() != ' ' && *in.position() != '\t' && *in.position() != '\n' && *in.position() != '\r')
                        in.ignore();
                }
                else
                {
                    in.ignore();
                }
            }
        }

        return !moves_str.empty() || true; // Allow empty move sections for malformed games
    }
};

PGNRowInputFormat::PGNRowInputFormat(SharedHeader header_, ReadBuffer & in_, Params params_)
    : IRowInputFormat(header_, in_, std::move(params_))
{
    const auto & header = getPort().getHeader();
    // Validate that we have the expected columns
    if (header.columns() == 0)
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "PGN format requires at least one column");
    }
}

void PGNRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    eof_reached = false;
}

bool PGNRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    if (eof_reached)
        return false;

    PGNParser::Game game;
    if (!readGame(columns))
    {
        eof_reached = true;
        return false;
    }

    return true;
}

bool PGNRowInputFormat::readGame(MutableColumns & columns)
{
    PGNParser::Game game;

    if (!PGNParser::parseGame(*in, game))
        return false;

    const auto & header = getPort().getHeader();

    // Fill columns based on header column names
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const String & column_name = header.getByPosition(i).name;

        try
        {
            if (column_name == "event")
                columns[i]->insertData(game.event.data(), game.event.size());
            else if (column_name == "site")
                columns[i]->insertData(game.site.data(), game.site.size());
            else if (column_name == "date")
                columns[i]->insertData(game.date.data(), game.date.size());
            else if (column_name == "round")
                columns[i]->insertData(game.round.data(), game.round.size());
            else if (column_name == "white")
                columns[i]->insertData(game.white.data(), game.white.size());
            else if (column_name == "black")
                columns[i]->insertData(game.black.data(), game.black.size());
            else if (column_name == "result")
                columns[i]->insertData(game.result.data(), game.result.size());
            else if (column_name == "white_elo")
            {
                auto & col = assert_cast<ColumnInt32 &>(*columns[i]);
                col.insertValue(game.white_elo);
            }
            else if (column_name == "black_elo")
            {
                auto & col = assert_cast<ColumnInt32 &>(*columns[i]);
                col.insertValue(game.black_elo);
            }
            else if (column_name == "moves")
                columns[i]->insertData(game.moves.data(), game.moves.size());
            else
            {
                // Unknown column, fill with default
                columns[i]->insertDefault();
            }
        }
        catch (const Exception &)
        {
            // If type conversion fails, insert default value
            columns[i]->insertDefault();
        }
    }

    return true;
}

NamesAndTypesList PGNSchemaReader::readSchema()
{
    return {
        {"event", std::make_shared<DataTypeString>()},
        {"site", std::make_shared<DataTypeString>()},
        {"date", std::make_shared<DataTypeString>()},
        {"round", std::make_shared<DataTypeString>()},
        {"white", std::make_shared<DataTypeString>()},
        {"black", std::make_shared<DataTypeString>()},
        {"result", std::make_shared<DataTypeString>()},
        {"white_elo", std::make_shared<DataTypeInt32>()},
        {"black_elo", std::make_shared<DataTypeInt32>()},
        {"moves", std::make_shared<DataTypeString>()}
    };
}

void registerInputFormatPGN(FormatFactory & factory)
{
    factory.registerInputFormat("PGN", [](
        ReadBuffer & buf,
        const Block & sample,
        const RowInputFormatParams & params,
        const FormatSettings &)
    {
        return std::make_shared<PGNRowInputFormat>(std::make_shared<const Block>(sample), buf, params);
    });
}

static std::pair<bool, size_t> pgnSegmentationEngine(ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
{
    char * pos = in.position();
    bool need_more_data = true;
    size_t number_of_rows = 0;

    while (loadAtPosition(in, memory, pos) && need_more_data)
    {
        // In PGN format, each game starts with a '[' character for tags
        // We count complete games when we encounter a new '[' after moves
        bool in_game = false;

        while (pos < in.buffer().end())
        {
            if (*pos == '[')
            {
                if (in_game)
                {
                    // Found start of next game
                    ++number_of_rows;

                    if ((memory.size() + static_cast<size_t>(pos - in.position()) >= min_bytes) || (number_of_rows == max_rows))
                    {
                        need_more_data = false;
                        break;
                    }
                }
                in_game = true;
            }

            ++pos;

            if (pos >= in.buffer().end())
                break;
        }

        if (pos > in.buffer().end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");

        if (pos == in.buffer().end())
        {
            if (!need_more_data)
                break;
            if (!loadAtPosition(in, memory, pos))
                break;
        }
    }

    saveUpToPosition(in, memory, pos);

    return {loadAtPosition(in, memory, pos), number_of_rows};
}

void registerFileSegmentationEnginePGN(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("PGN", &pgnSegmentationEngine);
}

void registerPGNSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("PGN", [](const FormatSettings &)
    {
        return std::make_shared<PGNSchemaReader>();
    });
}

}
