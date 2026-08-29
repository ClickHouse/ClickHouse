#include <charconv>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
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
        /// Every game after the first begins at a '[' (readMoves stops only at '[' or at the end
        /// of the input), so the line-start state does not need to survive between games.
        return PGNParser().parseGameImpl(in, game);
    }

private:
    bool parseGameImpl(ReadBuffer & in, Game & game)
    {
        game = Game();

        /// Skip leading whitespace
        skipWhitespaceAndComments(in);

        if (in.eof())
            return false;

        /// Read tags
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
                if (!isResultToken(tag_value))
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA,
                        "Invalid PGN: tag 'Result' has value '{}', expected '1-0', '0-1', '1/2-1/2' or '*'",
                        tag_value);
                game.result = tag_value;
                game.has_result = true;
            }
            else if (tag_name == "WhiteElo")
                game.has_white_elo = parseElo(tag_name, tag_value, game.white_elo);
            else if (tag_name == "BlackElo")
                game.has_black_elo = parseElo(tag_name, tag_value, game.black_elo);

            skipWhitespaceAndComments(in);
        }

        /// Read moves section
        if (!readMoves(in, game))
            return false;

        return true;
    }

    /// The previously consumed byte. It is tracked explicitly to detect the beginning of a line:
    /// the byte before the current position cannot be read from the buffer itself, because at
    /// a refill boundary the current position is the very beginning of the buffer. The initial
    /// value makes the beginning of the input the beginning of a line.
    char last_consumed = '\n';

    /// Consume one byte, remembering it. Must not be called at the end of the input.
    void ignoreOne(ReadBuffer & in)
    {
        last_consumed = *in.position();
        in.ignore();
    }

    /// The standard spells an unknown rating as an empty value or a question mark;
    /// such a tag is reported as absent, so that a table `DEFAULT` expression can be applied.
    /// Anything else that is not a number is a malformed file, and it is not silently ignored.
    static bool parseElo(const String & tag_name, const String & tag_value, Int32 & value)
    {
        if (tag_value.empty() || tag_value == "?" || tag_value == "-")
            return false;

        const char * begin = tag_value.data();
        const char * end = begin + tag_value.size();
        const auto result = std::from_chars(begin, end, value);

        if (result.ec != std::errc{} || result.ptr != end)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid PGN: tag '{}' has a non-numeric value '{}'", tag_name, tag_value);

        return true;
    }

    void skipLineComment(ReadBuffer & in)
    {
        while (!in.eof() && *in.position() != '\n')
            ignoreOne(in);
    }

    void skipBlockComment(ReadBuffer & in)
    {
        ignoreOne(in);
        while (!in.eof() && *in.position() != '}')
            ignoreOne(in);

        if (in.eof())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid PGN: unterminated comment");

        ignoreOne(in);
    }

    /// The escape mechanism of the standard: a percent sign in the first column of a line
    /// means that the rest of the line is ignored.
    bool isEscapeLine(ReadBuffer & in) const
    {
        return !in.eof() && *in.position() == '%' && last_consumed == '\n';
    }

    void skipVariation(ReadBuffer & in)
    {
        int depth = 1;
        ignoreOne(in);

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
                ignoreOne(in);
            }
        }

        if (depth != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid PGN: unterminated variation");
    }

    void skipWhitespaceAndComments(ReadBuffer & in)
    {
        while (!in.eof())
        {
            char c = *in.position();
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r')
            {
                ignoreOne(in);
            }
            else if (c == ';')
            {
                /// Skip comment until end of line
                skipLineComment(in);
            }
            else if (c == '{')
            {
                /// Skip block comment
                skipBlockComment(in);
            }
            else if (c == '%' && isEscapeLine(in))
            {
                /// Skip escape line
                skipLineComment(in);
            }
            else
            {
                break;
            }
        }
    }

    bool readTag(ReadBuffer & in, String & tag_name, String & tag_value)
    {
        if (in.eof() || *in.position() != '[')
            return false;

        ignoreOne(in); /// skip [

        skipWhitespaceAndComments(in);

        /// Read tag name
        tag_name.clear();
        while (!in.eof() && *in.position() != ' ' && *in.position() != '\t' && *in.position() != '\n' && *in.position() != '\r'
               && *in.position() != '"' && *in.position() != ']')
        {
            tag_name += *in.position();
            ignoreOne(in);
        }

        skipWhitespaceAndComments(in);

        if (in.eof() || *in.position() != '"')
            return false;

        ignoreOne(in); /// skip "

        /// Read tag value
        tag_value.clear();
        while (!in.eof() && *in.position() != '"')
        {
            if (*in.position() == '\\' && !in.eof())
            {
                ignoreOne(in);
                if (!in.eof())
                {
                    tag_value += *in.position();
                    ignoreOne(in);
                }
            }
            else
            {
                tag_value += *in.position();
                ignoreOne(in);
            }
        }

        if (in.eof() || *in.position() != '"')
            return false;

        ignoreOne(in); /// skip "

        skipWhitespaceAndComments(in);

        if (!in.eof() && *in.position() == ']')
        {
            ignoreOne(in); /// skip ]
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
            if (token.starts_with("0-0"))
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

    bool readMoves(ReadBuffer & in, Game & game)
    {
        game.moves.clear();

        skipWhitespaceAndComments(in);

        /// Read move text until end of game (next [ or EOF or blank line followed by [)
        while (!in.eof() && *in.position() != '[')
        {
            if (*in.position() == '(' || *in.position() == '{' || *in.position() == ';')
            {
                /// Skip variations and comments
                if (*in.position() == ';')
                    skipLineComment(in);
                else if (*in.position() == '{')
                    skipBlockComment(in);
                else if (*in.position() == '(')
                    skipVariation(in);
            }
            else if (isWhitespace(*in.position()))
            {
                ignoreOne(in);
            }
            else if (isEscapeLine(in))
            {
                skipLineComment(in);
            }
            else
            {
                String token;
                while (!in.eof() && !isWhitespace(*in.position()) && *in.position() != '[' && *in.position() != '(' && *in.position() != '{'
                       && *in.position() != ';')
                {
                    token += *in.position();
                    ignoreOne(in);
                }

                if (isResultToken(token))
                {
                    if (game.result.empty())
                    {
                        game.result = token;
                        game.has_result = true;
                    }
                    else if (game.result != token)
                        throw Exception(
                            ErrorCodes::INCORRECT_DATA,
                            "Invalid PGN: the game termination marker '{}' contradicts the game result '{}'",
                            token,
                            game.result);
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

void PGNRowInputFormat::readPrefix()
{
    skipBOMIfExists(*in);
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

    /// Fill columns based on header column names
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
            /// Unknown column, fill with default and mark as not read so table DEFAULT expressions can be applied.
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

void registerInputFormatPGN(FormatFactory & factory);
void registerInputFormatPGN(FormatFactory & factory)
{
    factory.registerInputFormat(
        "PGN",
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams & params, const FormatSettings &)
        { return std::make_shared<PGNRowInputFormat>(std::make_shared<const Block>(sample), buf, params); });
    factory.setDocumentation("PGN", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |   ✗   |

## Description {#description}

[Portable Game Notation](https://en.wikipedia.org/wiki/Portable_Game_Notation) (`PGN`) is the standard text
format for recording chess games. A `PGN` file is a sequence of games, and every game consists of:

1. **Tags** — the metadata of the game, one `[TagName "TagValue"]` pair per line;
2. **Move text** — the moves of the game in standard algebraic notation, optionally interleaved with move
   numbers, comments, variations and numeric annotation glyphs, and terminated by the result.

Every game is read as a single row.

## Columns {#columns}

| Column      | Type     | Description                                                        |
|-------------|----------|--------------------------------------------------------------------|
| `event`     | `String` | The name of the event, e.g. the name of the tournament.             |
| `site`      | `String` | The place where the game was played.                                |
| `date`      | `String` | The date when the game was played, as written in the file.          |
| `round`     | `String` | The round of the tournament.                                        |
| `white`     | `String` | The name of the player playing white.                               |
| `black`     | `String` | The name of the player playing black.                               |
| `result`    | `String` | The result of the game: `1-0`, `0-1`, `1/2-1/2` or `*`.             |
| `white_elo` | `Int32`  | The Elo rating of the player playing white.                         |
| `black_elo` | `Int32`  | The Elo rating of the player playing black.                         |
| `moves`     | `String` | The moves of the game, separated by spaces.                         |

The columns are matched by name, and a column may be omitted from the requested structure. A column that is
listed with a type other than the type in the table above raises an exception. A requested column with a name
that is not in the table above is filled with the default value.

Only the mainline moves are put into `moves`: move numbers, comments (`; ...` until the end of the line and
`{ ... }`), variations in parentheses, and numeric annotation glyphs (`$1`) are skipped, and so is the result
token at the end of the move text. A move number written together with the move, as in `1.e4`, is understood.
Escape lines (a line whose first character is `%`) and a byte order mark at the beginning of the file are
skipped as well.

When a tag is not present in a game, the corresponding column is reported as absent, so that a `DEFAULT`
expression of the target table is applied to it. An Elo rating that the file spells as unknown (an empty value,
`?` or `-`) is reported as absent as well; a rating that is neither a number nor one of those is an error.
When the `Result` tag is missing, the result is taken from the game termination marker of the move text.
A `Result` tag whose value is not one of `1-0`, `0-1`, `1/2-1/2` or `*` is an error, and so is a game
termination marker that contradicts the game result.

## Example usage {#example-usage}

### Reading a PGN file {#reading-a-pgn-file}

```sql title="Query"
SELECT * FROM file('games.pgn', PGN);
```

```response title="Response"
   ┌─event────────────────────┬─site─────────────┬─date───────┬─round─┬─white───────────┬─black──────────────┬─result──┬─white_elo─┬─black_elo─┬─moves─────────────────────────────────────────┐
1. │ World Chess Championship │ Moscow, Russia   │ 2023.11.15 │ 1     │ Magnus Carlsen  │ Ian Nepomniachtchi │ 1-0     │      2859 │      2793 │ e4 c5 Nf3 d6 d4 cxd4 Nxd4 Nf6 Nc3 a6          │
2. │ London Chess Classic     │ London, England  │ 2023.11.20 │ 2     │ Fabiano Caruana │ Praggnanandhaa R   │ 0-1     │      2770 │      2709 │ d4 Nf6 c4 e6 Nc3 Bb4 Qc2 O-O a3 Bxc3+ Qxc3 b6 │
3. │ Sinquefield Cup          │ Saint Louis, USA │ 2023.11.25 │ 3     │ Ding Liren      │ Alireza Firouzja   │ 1/2-1/2 │      2787 │      2703 │ e4 c5 Nf3 d6 d4 cxd4 Nxd4 Nf6                 │
   └──────────────────────────┴──────────────────┴────────────┴───────┴─────────────────┴────────────────────┴─────────┴───────────┴───────────┴───────────────────────────────────────────────┘
```

### Creating a table from a PGN file {#creating-a-table-from-a-pgn-file}

The structure of the table does not have to be written out; it can be taken from the format:

```sql title="Query"
CREATE TABLE chess_games
ENGINE = MergeTree
ORDER BY (white, black)
AS SELECT * FROM file('games.pgn', PGN);

SELECT event, date, white, black, result
FROM chess_games
WHERE white = 'Ding Liren' OR black = 'Ding Liren';
```

```response title="Response"
   ┌─event───────────┬─date───────┬─white──────┬─black────────────┬─result──┐
1. │ Sinquefield Cup │ 2023.11.25 │ Ding Liren │ Alireza Firouzja │ 1/2-1/2 │
   └─────────────────┴────────────┴────────────┴──────────────────┴─────────┘
```

### Inserting into an existing table {#inserting-into-an-existing-table}

A table can also declare only the columns that are of interest:

```sql title="Query"
CREATE TABLE top_games
(
    white String,
    black String,
    result String,
    white_elo Int32,
    black_elo Int32
)
ENGINE = MergeTree
ORDER BY (white, black);

INSERT INTO top_games FROM INFILE 'games.pgn' FORMAT PGN;

SELECT * FROM top_games WHERE white_elo > 2800;
```

```response title="Response"
   ┌─white──────────┬─black──────────────┬─result─┬─white_elo─┬─black_elo─┐
1. │ Magnus Carlsen │ Ian Nepomniachtchi │ 1-0    │      2859 │      2793 │
   └────────────────┴────────────────────┴────────┴───────────┴───────────┘
```

## Format settings {#format-settings}

There are no settings specific to the `PGN` format.
)DOCS_MD"});
}

void registerPGNSchemaReader(FormatFactory & factory);
void registerPGNSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("PGN", [](const FormatSettings &) { return std::make_shared<PGNSchemaReader>(); });
}

}
