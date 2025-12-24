---
alias: []
description: 'Documentation for the PGN format'
input_format: true
keywords: ['PGN', 'Portable Game Notation']
output_format: false
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

## Example Usage {#example-usage}

### Reading a PGN file {#reading-a-pgn-file}

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
) ENGINE = Memory

INSERT INTO chess_games FROM INFILE '/path/to/games.pgn' FORMAT PGN
```

### Querying PGN data {#querying-pgn-data}

```sql
-- Find all games where Kasparov played
SELECT event, date, white, black, result FROM chess_games 
WHERE white = 'Kasparov' OR black = 'Kasparov'

-- Find games by Elo rating
SELECT * FROM chess_games 
WHERE white_elo > 2700 AND black_elo > 2700
```

## Format settings {#format-settings}
