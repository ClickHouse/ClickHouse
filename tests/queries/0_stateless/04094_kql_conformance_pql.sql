-- Tests adapted from https://github.com/runreveal/pql
-- Copyright (c) RunReveal Inc. Licensed under the Apache License, Version 2.0.
-- Source: testdata/Goldens/

-- Create test tables for PQL tests
DROP TABLE IF EXISTS StormEvents;
CREATE TABLE StormEvents (
    EventId Int32,
    State String,
    EventType String,
    DamageProperty Int64
) ENGINE = Memory;
INSERT INTO StormEvents VALUES
    (11032, 'ATLANTIC SOUTH', 'Waterspout', 0),
    (11098, 'FLORIDA', 'Heavy Rain', 0),
    (60913, 'FLORIDA', 'Tornado', 6200000),
    (11503, 'GEORGIA', 'Thunderstorm Wind', 2000),
    (13913, 'MISSISSIPPI', 'Thunderstorm Wind', 20000);

DROP TABLE IF EXISTS SourceFiles;
CREATE TABLE SourceFiles (
    Directory String,
    FileName String,
    LineCount Int32
) ENGINE = Memory;
INSERT INTO SourceFiles VALUES
    ('.', 'pql.go', 78),
    ('.', 'clickhouse_test.go', 51),
    ('.', 'golden_test.go', 115),
    ('.', 'pql_test.go', 40),
    ('parser', 'parser.go', 878),
    ('parser', 'lex.go', 681),
    ('parser', 'parser_test.go', 1108),
    ('parser', 'lex_test.go', 383),
    ('parser', 'ast.go', 174),
    ('parser', 'tokenkind_string.go', 24);

DROP TABLE IF EXISTS Tokens;
CREATE TABLE Tokens (
    Kind Nullable(Int32),
    TokenConstant String
) ENGINE = Memory;
INSERT INTO Tokens VALUES
    (1, 'TokenIdentifier'),
    (2, 'TokenQuotedIdentifier'),
    (3, 'TokenNumber'),
    (4, 'TokenString'),
    (5, 'TokenAnd'),
    (6, 'TokenOr'),
    (7, 'TokenPipe'),
    (-1, 'TokenError');

DROP TABLE IF EXISTS StateCapitals;
CREATE TABLE StateCapitals (
    State String,
    StateCapital String
) ENGINE = Memory;
INSERT INTO StateCapitals VALUES
    ('Alabama', 'Montgomery'),
    ('Alaska', 'Juneau'),
    ('Arizona', 'Phoenix'),
    ('Florida', 'Tallahassee'),
    ('Georgia', 'Atlanta'),
    ('Mississippi', 'Jackson');


set allow_experimental_kusto_dialect=1;
set dialect='kusto';

print '-- pql::Count --';
StormEvents
| count;
print '-- pql::DoubleCount --';
StormEvents
| count
| count;
print '-- pql::Extend --';
StormEvents
| project State, EventType, DamageProperty
| extend foo=1;
print '-- pql::ExtendExprOnly --';
StormEvents
| project State, EventType, DamageProperty
| extend 42;
print '-- pql::If --';
SourceFiles
| sort by LineCount desc, FileName asc
| project FileName, Size = iff(LineCount >= 1000, "Large", "Smol");
print '-- pql::In --';
StormEvents
| where State in ("GEORGIA", "MISSISSIPPI");
print '-- pql::InAnd --';
StormEvents
| where State in ("GEORGIA", "MISSISSIPPI") and DamageProperty > 10000;
print '-- pql::Let --';
let n = 3;
StateCapitals
| top n by State asc;
print '-- pql::Limit --';
StormEvents
| take 3;
print '-- pql::OpParens --';
Tokens
| where Kind - 3 > 0
| sort by Kind asc;
print '-- pql::Project --';
StormEvents
| project State, EventType, DamageProperty;
print '-- pql::ProjectAfterTake --';
StormEvents
| sort by EventId asc
| take 3
| project State, EventType, DamageProperty;
print '-- pql::ProjectAssignment --';
StormEvents
| project Id = EventId, DamagePropertyK = DamageProperty / 1000;
print '-- pql::ProjectStrcat --';
StormEvents
| project Description = strcat(EventType, " in ", State);
print '-- pql::Sort --';
StormEvents
| sort by DamageProperty, State asc;
print '-- pql::SortLimit --';
StormEvents
| sort by DamageProperty, State asc
| take 3;
print '-- pql::Summarize --';
SourceFiles
| summarize TotalLines=sum(LineCount) by Directory, IsTest=endsWith(FileName, "_test.go")
| sort by Directory asc, IsTest asc;
print '-- pql::SummarizeBy --';
SourceFiles
| summarize by Directory
| sort by Directory asc;
print '-- pql::SummarizeCount --';
SourceFiles
| summarize Files=count() by Directory, IsTest=endsWith(FileName, "_test.go")
| sort by Directory asc, IsTest asc;
print '-- pql::SummarizeCountIf --';
SourceFiles
| summarize NonTestFiles=countif(not(endsWith(FileName, "_test.go"))) by Directory
| sort by Directory asc;
print '-- pql::SummarizeMinMax --';
SourceFiles
| summarize Min = min(LineCount), Max = max(LineCount);
print '-- pql::SummarizeMinMaxUnnamed --';
SourceFiles
| summarize min(LineCount), max(LineCount);
print '-- pql::Table --';
StormEvents;
print '-- pql::Top --';
SourceFiles
| top 3 by LineCount;
print '-- pql::Where --';
StormEvents
| where DamageProperty > 5000
  and EventType == "Thunderstorm Wind";
print '-- pql::WhereCaseInsensitiveEq --';
StormEvents
| where State =~ "georgia";
print '-- pql::WhereCaseInsensitiveNE --';
StormEvents
| where State !~ "georgia";
print '-- pql::WhereFunc --';
Tokens
| where abs(Kind) == 1;
print '-- pql::WhereIsNotNull --';
Tokens
| where isnotnull(Kind);
print '-- pql::WhereNot --';
Tokens
| where not(Kind > 0);
print '-- pql::WhereNotEqualsNegative --';
Tokens
| where Kind != -1;
print '-- pql::WhereToLower --';
StormEvents
| where tolower(EventType) == "tornado";
print '-- pql::WhereToUpper --';
StormEvents
| where toupper(EventType) == "TORNADO";
print '-- pql::WhereTrue --';
StormEvents
| where true;

set dialect='clickhouse';
DROP TABLE IF EXISTS StormEvents;
DROP TABLE IF EXISTS SourceFiles;
DROP TABLE IF EXISTS Tokens;
DROP TABLE IF EXISTS StateCapitals;
