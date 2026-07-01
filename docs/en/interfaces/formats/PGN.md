---
alias: []
description: 'Documentation for the PGN format'
input_format: true
keywords: ['PGN', 'Portable Game Notation']
output_format: false
sidebar_label: 'PGN'
sidebar_position: 2
slug: /interfaces/formats/PGN
title: 'PGN'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |       |

## Description {#description}

Portable Game Notation (PGN) is a standard format for recording chess games. The PGN format is widely used for storing and exchanging chess game data.

The PGN format consists of two main sections:
1. **Tags**: Metadata about the game in the format `[TagName "TagValue"]`
2. **Moves**: The sequence of moves in algebraic notation

## Input Format {#input-format}

When reading PGN files, each game is parsed into a single row with the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `event` | String | The event name (e.g., tournament name) |
| `site` | String | The site where the game was played |
| `date` | String | The date the game was played |
| `round` | String | The round number in the tournament |
| `white` | String | The name of the white player |
| `black` | String | The name of the black player |
| `result` | String | The result of the game (1-0, 0-1, 1/2-1/2, or *) |
| `white_elo` | Int32 | The Elo rating of the white player |
| `black_elo` | Int32 | The Elo rating of the black player |
| `moves` | String | The moves in algebraic notation |

The parser extracts mainline move tokens only. Move numbers, comments, variations, NAGs, and result tokens are not included in the `moves` column. If the `Result` tag is missing, the result is parsed from the move text when present.

Missing or invalid Elo ratings are inserted as `0`. Recognized columns must use the types shown above; incompatible requested types raise an error. Unknown requested columns are filled with default values, and table `DEFAULT` expressions are applied when inserting into columns that are not present in the PGN data.

## Example Usage {#example-usage}

### Reading a PGN file {#reading-a-pgn-file}

```sql
SELECT *
FROM file('/path/to/games.pgn', PGN);
```

```text
┌─event────────────────────┬─site────────────┬─date───────┬─round─┬─white───────────┬─black───────────────┬─result─┬─white_elo─┬─black_elo─┬─moves───────────────┐
│ World Championship Match │ Dubai, UAE      │ 2021.12.03 │ 6     │ Magnus Carlsen │ Ian Nepomniachtchi │ 1-0    │      2855 │      2782 │ d4 Nf6 Nf3 d5 g3  │
│ Candidates Tournament    │ Madrid, Spain   │ 2022.06.17 │ 1     │ Jan-Krzysztof Duda │ Richard Rapport │ 1/2-1/2 │ 2750 │ 2764 │ d4 Nf6 c4 e6 Nf3  │
└──────────────────────────┴─────────────────┴────────────┴───────┴─────────────────┴─────────────────────┴────────┴───────────┴───────────┴─────────────────────┘
```

You can also create a table directly from a PGN file:

```sql
CREATE TABLE pgn_tab
ENGINE = MergeTree()
ORDER BY tuple()
AS SELECT * FROM file('/path/to/games.pgn', PGN);

SELECT * FROM pgn_tab FORMAT PRETTY;
```

```text
   ┏━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┓
   ┃ event                    ┃ site          ┃ date       ┃ round ┃ white               ┃ black               ┃ result  ┃ white_elo ┃ black_elo ┃ moves              ┃
   ┡━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━┩
1. │ World Championship Match │ Dubai, UAE    │ 2021.12.03 │ 6     │ Magnus Carlsen      │ Ian Nepomniachtchi  │ 1-0     │      2855 │      2782 │ d4 Nf6 Nf3 d5 g3  │
2. │ Candidates Tournament    │ Madrid, Spain │ 2022.06.17 │ 1     │ Jan-Krzysztof Duda  │ Richard Rapport     │ 1/2-1/2 │      2750 │      2764 │ d4 Nf6 c4 e6 Nf3  │
   └──────────────────────────┴───────────────┴────────────┴───────┴─────────────────────┴─────────────────────┴─────────┴───────────┴───────────┴────────────────────┘
```

Or insert into an existing table:

```sql
CREATE TABLE chess_games
(
    event String,
    site String,
    date String,
    round String,
    white String,
    black String,
    result String,
    white_elo Int32,
    black_elo Int32,
    moves String
) ENGINE = Memory;

INSERT INTO chess_games FROM INFILE '/path/to/games.pgn' FORMAT PGN
```

### Querying PGN data {#querying-pgn-data}

```sql
-- Find all games where Kasparov played
SELECT event, date, white, black, result FROM chess_games
WHERE white = 'Kasparov' OR black = 'Kasparov';
```

```text
┌─event────────────┬─date───────┬─white──────────────┬─black──────────────┬─result─┐
│ Linares          │ 1993.03.01 │ Garry Kasparov    │ Anatoly Karpov    │ 1-0    │
│ Wijk aan Zee     │ 1999.01.20 │ Veselin Topalov   │ Garry Kasparov    │ 0-1    │
└──────────────────┴────────────┴────────────────────┴────────────────────┴────────┘
```

```sql
-- Find games by Elo rating
SELECT white, black, result, white_elo, black_elo FROM chess_games
WHERE white_elo > 2700 AND black_elo > 2700;
```

```text
┌─white────────────┬─black───────────────┬─result──┬─white_elo─┬─black_elo─┐
│ Magnus Carlsen  │ Ian Nepomniachtchi  │ 1-0     │      2855 │      2782 │
│ Jan-Krzysztof Duda │ Richard Rapport   │ 1/2-1/2 │      2750 │      2764 │
└─────────────────┴─────────────────────┴─────────┴───────────┴───────────┘
```

## Format settings {#format-settings}

There are no PGN-specific format settings.
