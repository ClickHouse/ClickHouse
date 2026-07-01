#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBuffer.h>
#include <Processors/Formats/Impl/PGNRowInputFormat.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
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
        bool has_event = false;
        bool has_site = false;
        bool has_date = false;
        bool has_round = false;
        bool has_white = false;
        bool has_black = false;
        bool has_result = false;
        bool has_white_elo = false;
        bool has_black_elo = false;
        bool has_moves = false;
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
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid PGN tag");

            if (tag_name == "Event")
            {
                game.event = tag_value;
                game.has_event = true;
            }
            else if (tag_name == "Site")
            {
                game.site = tag_value;
                game.has_site = true;
            }
            else if (tag_name == "Date")
            {
                game.date = tag_value;
                game.has_date = true;
            }
            else if (tag_name == "Round")
            {
                game.round = tag_value;
                game.has_round = true;
            }
            else if (tag_name == "White")
            {
                game.white = tag_value;
                game.has_white = true;
            }
            else if (tag_name == "Black")
            {
                game.black = tag_value;
                game.has_black = true;
            }
            else if (tag_name == "Result")
            {
                game.result = tag_value;
                game.has_result = true;
            }
            else if (tag_name == "WhiteElo")
            {
                game.white_elo = parseElo(tag_value);
                game.has_white_elo = true;
            }
            else if (tag_name == "BlackElo")
            {
                game.black_elo = parseElo(tag_value);
                game.has_black_elo = true;
            }

            skipWhitespaceAndComments(in);
        }

        // Read moves section
        if (!readMoves(in, game))
            return false;

        return true;
    }

private:
    static Int32 parseElo(const String & tag_value)
    {
        try
        {
            size_t pos = 0;
            Int32 value = std::stoi(tag_value, &pos);
            if (pos == tag_value.size())
                return value;
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
        }

        return 0;
    }

    static void skipLineComment(ReadBuffer & in)
    {
        while (!in.eof() && *in.position() != '\n')
            in.ignore();
    }

    static void skipBlockComment(ReadBuffer & in)
    {
        in.ignore();
        while (!in.eof() && *in.position() != '}')
            in.ignore();

        if (in.eof())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid PGN: unterminated comment");

        in.ignore();
    }

    static void skipVariation(ReadBuffer & in)
    {
        int depth = 1;
        in.ignore();

        while (!in.eof() && depth > 0)
        {
            if (*in.position() == '{')
                skipBlockComment(in);
            else if (*in.position() == ';')
                skipLineComment(in);
            else
            {
                if (*in.position() == '(')
                    ++depth;
                else if (*in.position() == ')')
                    --depth;
                in.ignore();
            }
        }

        if (depth != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid PGN: unterminated variation");
    }

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
                skipLineComment(in);
            }
            else if (c == '{')
            {
                // Skip block comment
                skipBlockComment(in);
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
        while (!in.eof() && *in.position() != ' ' && *in.position() != '\t' && *in.position() != '\n' && *in.position() != '\r'
               && *in.position() != '"' && *in.position() != ']')
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

    static bool isWhitespace(char c) { return c == ' ' || c == '\t' || c == '\n' || c == '\r'; }

    static bool isResultToken(const String & token) { return token == "1-0" || token == "0-1" || token == "1/2-1/2" || token == "*"; }

    static bool isMoveNumberToken(const String & token)
    {
        if (token.empty())
            return false;

        for (char c : token)
        {
            if (!((c >= '0' && c <= '9') || c == '.'))
                return false;
        }

        return true;
    }

    static String extractMoveToken(String token)
    {
        if (token.empty() || token[0] == '$')
            return {};

        /// Compact PGN can write a move number and SAN without whitespace: 1.e4 or 1...e5.
        /// Keep the SAN part instead of dropping the whole digit-prefixed token.
        if (token[0] >= '0' && token[0] <= '9')
        {
            if (token.size() >= 3 && token.compare(0, 3, "0-0") == 0)
                return token;

            const size_t last_dot = token.find_last_of('.');
            if (last_dot == String::npos)
                return {};

            token = token.substr(last_dot + 1);
        }

        if (token.empty() || isMoveNumberToken(token) || isResultToken(token))
            return {};

        return token;
    }

    static void appendMove(String & moves, const String & move)
    {
        if (move.empty())
            return;

        if (!moves.empty())
            moves += ' ';
        moves += move;
    }

    static bool readMoves(ReadBuffer & in, Game & game)
    {
        game.moves.clear();

        skipWhitespaceAndComments(in);

        // Read move text until end of game (next [ or EOF or blank line followed by [)
        while (!in.eof() && *in.position() != '[')
        {
            if (*in.position() == '(' || *in.position() == '{' || *in.position() == ';')
            {
                // Skip variations and comments
                if (*in.position() == ';')
                    skipLineComment(in);
                else if (*in.position() == '{')
                    skipBlockComment(in);
                else if (*in.position() == '(')
                    skipVariation(in);
            }
            else if (isWhitespace(*in.position()))
            {
                in.ignore();
            }
            else
            {
                String token;
                while (!in.eof() && !isWhitespace(*in.position()) && *in.position() != '[' && *in.position() != '(' && *in.position() != '{'
                       && *in.position() != ';')
                {
                    token += *in.position();
                    in.ignore();
                }

                if (isResultToken(token))
                {
                    if (game.result.empty())
                    {
                        game.result = token;
                        game.has_result = true;
                    }
                }
                else
                    appendMove(game.moves, extractMoveToken(token));
            }
        }

        game.has_moves = !game.moves.empty();

        if (game.moves.empty() && game.result.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid PGN game: missing moves and result");

        return true;
    }
};

static void insertString(MutableColumnPtr & column, const String & value, const String & column_name)
{
    auto * string_column = typeid_cast<ColumnString *>(column.get());
    if (!string_column)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Column '{}' must have type String for PGN format", column_name);

    string_column->insertData(value.data(), value.size());
}

static void insertInt32(MutableColumnPtr & column, Int32 value, const String & column_name)
{
    auto * int_column = typeid_cast<ColumnInt32 *>(column.get());
    if (!int_column)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Column '{}' must have type Int32 for PGN format", column_name);

    int_column->insertValue(value);
}

PGNRowInputFormat::PGNRowInputFormat(SharedHeader header_, ReadBuffer & in_, Params params_)
    : IRowInputFormat(header_, in_, std::move(params_))
{
}

void PGNRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    eof_reached = false;
}

bool PGNRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (eof_reached)
        return false;

    if (!readGame(columns, ext))
    {
        eof_reached = true;
        return false;
    }

    return true;
}

bool PGNRowInputFormat::readGame(MutableColumns & columns, RowReadExtension & ext)
{
    PGNParser::Game game;

    if (!PGNParser::parseGame(*in, game))
        return false;

    const auto & header = getPort().getHeader();
    ext.read_columns.assign(header.columns(), false);

    // Fill columns based on header column names
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const String & column_name = header.getByPosition(i).name;

        if (column_name == "event")
        {
            insertString(columns[i], game.event, column_name);
            ext.read_columns[i] = game.has_event;
        }
        else if (column_name == "site")
        {
            insertString(columns[i], game.site, column_name);
            ext.read_columns[i] = game.has_site;
        }
        else if (column_name == "date")
        {
            insertString(columns[i], game.date, column_name);
            ext.read_columns[i] = game.has_date;
        }
        else if (column_name == "round")
        {
            insertString(columns[i], game.round, column_name);
            ext.read_columns[i] = game.has_round;
        }
        else if (column_name == "white")
        {
            insertString(columns[i], game.white, column_name);
            ext.read_columns[i] = game.has_white;
        }
        else if (column_name == "black")
        {
            insertString(columns[i], game.black, column_name);
            ext.read_columns[i] = game.has_black;
        }
        else if (column_name == "result")
        {
            insertString(columns[i], game.result, column_name);
            ext.read_columns[i] = game.has_result;
        }
        else if (column_name == "white_elo")
        {
            insertInt32(columns[i], game.white_elo, column_name);
            ext.read_columns[i] = game.has_white_elo;
        }
        else if (column_name == "black_elo")
        {
            insertInt32(columns[i], game.black_elo, column_name);
            ext.read_columns[i] = game.has_black_elo;
        }
        else if (column_name == "moves")
        {
            insertString(columns[i], game.moves, column_name);
            ext.read_columns[i] = game.has_moves;
        }
        else
        {
            // Unknown column, fill with default and mark as not read so table DEFAULT expressions can be applied.
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
        {"moves", std::make_shared<DataTypeString>()}};
}

void registerInputFormatPGN(FormatFactory & factory)
{
    factory.registerInputFormat(
        "PGN",
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams & params, const FormatSettings &)
        { return std::make_shared<PGNRowInputFormat>(std::make_shared<const Block>(sample), buf, params); });
}

void registerPGNSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("PGN", [](const FormatSettings &) { return std::make_shared<PGNSchemaReader>(); });
}

}
