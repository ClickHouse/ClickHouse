parser grammar ClickHouseParser;

options {
    tokenVocab = ClickHouseLexer;
}

// Top-level statements

queryList: queryStmt (SEMICOLON queryStmt)* SEMICOLON? EOF;

queryStmt: query (INTO OUTFILE STRING_LITERAL)? (FORMAT identifier)?;

query
    : checkStmt
    | describeStmt
    | distributedStmt
    | insertStmt
    | selectUnionStmt
    | setStmt
    | showTablesStmt
    | showCreateTableStmt
    | useStmt
    ;

// CHECK statement

checkStmt: CHECK TABLE tableIdentifier;

// DESCRIBE statement

describeStmt: (DESCRIBE | DESC) TABLE tableIdentifier;

// DDL statement

distributedStmt:
    ( createDatabaseStmt
    | createTableStmt
    | dropStmt
    | optimizeStmt
    )
    (ON CLUSTER identifier)?
    ;

// CREATE statement

createDatabaseStmt: CREATE DATABASE (IF NOT EXISTS)? databaseIdentifier engineExpr?;
createTableStmt: CREATE TEMPORARY? TABLE (IF NOT EXISTS)? tableIdentifier schemaClause;

/* FIXME: if not a backward-compatibility it could look like:
 *
 *    createTableStmt: CREATE TEMPORARY? TABLE (IF NOT EXISTS)? tableIdentifier schemaClause engineClause;
 *    schemaClause: LPAREN tableElementExpr (COMMA tableElementExpr)* RPAREN | AS tableExpr;
 *    engineClause: … ; // same
 *
 */
schemaClause
    : LPAREN tableElementExpr (COMMA tableElementExpr)* RPAREN engineClause?  # SchemaDescriptionClause
    | engineClause? AS selectUnionStmt                                        # SchemaAsSubqueryClause
    | AS tableIdentifier engineClause?                                        # SchemaAsTableClause
    | AS identifier LPAREN tableArgList? RPAREN                               # SchemaAsFunctionClause
    ;
engineClause:
    engineExpr
    orderByClause?
    partitionByClause?
    primaryKeyClause?
    sampleByClause?
    ttlClause?
    settingsClause?
    ;
partitionByClause: PARTITION BY columnExpr;
primaryKeyClause: PRIMARY KEY columnExpr;
sampleByClause: SAMPLE BY columnExpr;
ttlClause: TTL ttlExpr (COMMA ttlExpr)*;

engineExpr: ENGINE EQ_SINGLE? identifier (LPAREN columnExprList? RPAREN)?;
tableElementExpr
    : identifier columnTypeExpr tableColumnPropertyExpr? /*TODO: codecExpr?*/ (TTL columnExpr)?  # TableElementExprColumnWithType
    | identifier columnTypeExpr? tableColumnPropertyExpr /*TODO: codexExpr?*/ (TTL columnExpr)?  # TableElementExprColumnWithDefault
    // TODO: INDEX
    // TODO: CONSTRAINT
    ;
tableColumnPropertyExpr: (DEFAULT | MATERIALIZED | ALIAS) columnExpr;
ttlExpr: columnExpr (DELETE | TO DISK STRING_LITERAL | TO VOLUME STRING_LITERAL)?;

// DROP statement

dropStmt
    : DROP DATABASE (IF EXISTS)? databaseIdentifier       # DropDatabaseStmt
    | DROP TEMPORARY? TABLE (IF EXISTS)? tableIdentifier  # DropTableStmt
    ;

// INSERT statement

insertStmt: INSERT INTO tableIdentifier (LPAREN identifier (COMMA identifier)* RPAREN)? valuesClause;

valuesClause
    : VALUES valueTupleExpr (COMMA? valueTupleExpr)*
    | selectUnionStmt
    ;

valueTupleExpr: LPAREN valueExprList RPAREN;  // same as ValueExprTuple

// OPTIMIZE statement

optimizeStmt: OPTIMIZE TABLE tableIdentifier partitionClause? FINAL? DEDUPLICATE?;

partitionClause
    : PARTITION columnExpr // actually we expect here any form of tuple of literals
    | PARTITION ID STRING_LITERAL
    ;

// SELECT statement

selectUnionStmt: selectStmt (UNION ALL selectStmt)*;
selectStmt:
    withClause?
    SELECT DISTINCT? columnExprList
    fromClause?
    sampleClause?
    arrayJoinClause?
    prewhereClause?
    whereClause?
    groupByClause?
    havingClause?
    orderByClause?
    limitByClause?
    limitClause?
    settingsClause?
    ;

withClause: WITH columnExprList;
fromClause: FROM joinExpr FINAL?;
sampleClause: SAMPLE ratioExpr (OFFSET ratioExpr)?;
arrayJoinClause: LEFT? ARRAY JOIN columnExprList;
prewhereClause: PREWHERE columnExpr;
whereClause: WHERE columnExpr;
groupByClause: GROUP BY columnExprList (WITH TOTALS)?;
havingClause: HAVING columnExpr;
orderByClause: ORDER BY orderExprList;
limitByClause: LIMIT limitExpr BY columnExprList;
limitClause: LIMIT limitExpr;
settingsClause: SETTINGS settingExprList;

joinExpr
    : tableExpr                                                           # JoinExprTable
    | LPAREN joinExpr RPAREN                                              # JoinExprParens
    | joinExpr (GLOBAL|LOCAL)? joinOp JOIN joinExpr joinConstraintClause  # JoinExprOp
    | joinExpr joinOpCross joinExpr                                       # JoinExprCrossOp
    ;
joinOp
    : (ANY? INNER | INNER ANY?)                                                                                  # JoinOpInner
    | ((OUTER | SEMI | ANTI | ANY | ASOF)? (LEFT | RIGHT) | (LEFT | RIGHT) (OUTER | SEMI | ANTI | ANY | ASOF)?)  # JoinOpLeftRight
    | ((OUTER | ANY)? FULL | FULL (OUTER | ANY)?)                                                                # JoinOpFull
    ;
joinOpCross
    : (GLOBAL|LOCAL)? CROSS JOIN
    | COMMA
    ;
joinConstraintClause
    : ON columnExprList
    | USING LPAREN columnExprList RPAREN
    | USING columnExprList
    ;

limitExpr: INTEGER_LITERAL ((COMMA | OFFSET) INTEGER_LITERAL)?;
orderExprList: orderExpr (COMMA orderExpr)*;
orderExpr: columnExpr (ASCENDING | DESCENDING | DESC)? (NULLS (FIRST | LAST))? (COLLATE STRING_LITERAL)?;
ratioExpr: INTEGER_LITERAL (SLASH INTEGER_LITERAL); // TODO: not complete!
settingExprList: settingExpr (COMMA settingExpr)*;
settingExpr: identifier EQ_SINGLE literal;

// SET statement

setStmt: SET settingExprList;

// SHOW statements

showCreateTableStmt: SHOW CREATE TEMPORARY? TABLE tableIdentifier;
showTablesStmt: SHOW TEMPORARY? TABLES ((FROM | IN) databaseIdentifier)? (LIKE STRING_LITERAL | whereClause)? limitClause?;

// USE statement

useStmt: USE databaseIdentifier;

// Values

valueExprList: valueExpr (COMMA valueExpr)*;
valueExpr
    : literal                           # ValueExprLiteral
    | valueTupleExpr                    # ValueExprTuple
    | LBRACKET valueExprList? RBRACKET  # ValueExprArray
    ;

// Columns

columnTypeExpr
    : identifier                                                                             # ColumnTypeExprSimple   // UInt64
    | identifier LPAREN columnParamList RPAREN                                               # ColumnTypeExprParam    // FixedString(N)
    | identifier LPAREN enumValue (COMMA enumValue)* RPAREN                                  # ColumnTypeExprEnum     // Enum
    | identifier LPAREN columnTypeExpr (COMMA columnTypeExpr)* RPAREN                        # ColumnTypeExprComplex  // Array, Tuple
    | identifier LPAREN identifier columnTypeExpr (COMMA identifier columnTypeExpr)* RPAREN  # ColumnTypeExprNested   // Nested
    ;
columnExprList: columnsExpr (COMMA columnsExpr)*;
columnsExpr
    : (tableIdentifier DOT)? ASTERISK  # ColumnsExprAsterisk
    | LPAREN selectUnionStmt RPAREN    # ColumnsExprSubquery
    // NOTE: asterisk and subquery goes before |columnExpr| so that we can mark them as multi-column expressions.
    | columnExpr                       # ColumnsExprColumn
    ;
columnExpr
    : literal                                                                        # ColumnExprLiteral
    | (tableIdentifier DOT)? ASTERISK                                                # ColumnExprAsterisk // single-column only
    | LPAREN selectUnionStmt RPAREN                                                  # ColumnExprSubquery // single-column only
    | LPAREN columnExpr RPAREN                                                       # ColumnExprParens   // single-column only
    | LPAREN columnExprList RPAREN                                                   # ColumnExprTuple
    | LBRACKET columnExprList? RBRACKET                                              # ColumnExprArray
    | CASE columnExpr? (WHEN columnExpr THEN columnExpr)+ (ELSE columnExpr)? END     # ColumnExprCase
    // TODO: | CAST LPAREN columnExpr AS columnTypeExpr RPAREN                       # ColumnExprCast
    | EXTRACT LPAREN INTERVAL_TYPE FROM columnExpr RPAREN                            # ColumnExprExtract
    | TRIM LPAREN (BOTH | LEADING | TRAILING) STRING_LITERAL FROM columnExpr RPAREN  # ColumnExprTrim
    | INTERVAL columnExpr INTERVAL_TYPE                                              # ColumnExprInterval
    | columnIdentifier                                                               # ColumnExprIdentifier
    | identifier (LPAREN columnParamList? RPAREN)? LPAREN columnArgList? RPAREN      # ColumnExprFunction
    | columnExpr LBRACKET columnExpr RBRACKET                                        # ColumnExprArrayAccess
    | columnExpr DOT INTEGER_LITERAL                                                 # ColumnExprTupleAccess
    | unaryOp columnExpr                                                             # ColumnExprUnaryOp
    | columnExpr IS NOT? NULL_SQL                                                    # ColumnExprIsNull
    | columnExpr binaryOp columnExpr                                                 # ColumnExprBinaryOp
    | columnExpr QUERY columnExpr COLON columnExpr                                   # ColumnExprTernaryOp
    | columnExpr NOT? BETWEEN columnExpr AND columnExpr                              # ColumnExprBetween
    | columnExpr AS identifier                                                       # ColumnExprAlias
    ;
columnParamList: literal (COMMA literal)*;
columnArgList: columnArgExpr (COMMA columnArgExpr)*;
columnArgExpr: columnLambdaExpr | columnExpr;
columnLambdaExpr:
    ( LPAREN identifier (COMMA identifier)* RPAREN
    |        identifier (COMMA identifier)*
    )
    ARROW columnExpr
    ;
columnIdentifier: (tableIdentifier DOT)? identifier (DOT identifier)?;

// Tables

tableExpr
    : tableIdentifier                                       # TableExprIdentifier
    | identifier LPAREN tableArgList? RPAREN                # TableExprFunction
    | LPAREN selectUnionStmt RPAREN                         # TableExprSubquery
    | tableExpr AS? identifier                              # TableExprAlias
    ;
tableIdentifier: (databaseIdentifier DOT)? identifier;
tableArgList: tableArgExpr (COMMA tableArgExpr)*;
tableArgExpr
    : literal
    | tableIdentifier
    ;

// Databases

databaseIdentifier: identifier;

// Basics

literal
    : (PLUS | DASH)? (FLOATING_LITERAL | HEXADECIMAL_LITERAL | INTEGER_LITERAL | INF | NAN_SQL)
    | STRING_LITERAL
    | NULL_SQL
    | identifier LPAREN RPAREN
    ;
keyword  // except NULL_SQL, SELECT, INF, NAN, USING, FROM, WHERE
    : ALIAS | ALL | AND | ANTI | ANY | ARRAY | AS | ASCENDING | ASOF | BETWEEN | BOTH | BY | CASE | CAST | CHECK | CLUSTER | COLLATE
    | CREATE | CROSS | DATABASE | DAY | DEDUPLICATE | DEFAULT | DELETE | DESC | DESCENDING | DESCRIBE | DISK | DISTINCT | DROP | ELSE | END
    | ENGINE | EXISTS | EXTRACT | FINAL | FIRST | FORMAT | FULL | GLOBAL | GROUP | HAVING | HOUR | ID | IF | IN | INNER | INSERT | INTERVAL
    | INTO | IS | JOIN | KEY | LAST | LEADING | LEFT | LIKE | LIMIT | LOCAL | MATERIALIZED | MINUTE | MONTH | NOT | NULLS | OFFSET | ON
    | OPTIMIZE | OR | ORDER | OUTER | OUTFILE | PARTITION | PREWHERE | PRIMARY | QUARTER | RIGHT | SAMPLE | SECOND | SEMI | SET | SETTINGS
    | SHOW | TABLE | TABLES | TEMPORARY | THEN | TOTALS | TRAILING | TRIM | TO | TTL | UNION | USE | VALUES | VOLUME | WEEK | WHEN | WITH
    | YEAR
    ;
identifier: IDENTIFIER | INTERVAL_TYPE | keyword; // TODO: not complete!
unaryOp: DASH | NOT;
binaryOp
    : CONCAT
    | ASTERISK
    | SLASH
    | PLUS
    | DASH
    | PERCENT
    | EQ_DOUBLE
    | EQ_SINGLE
    | NOT_EQ
    | LE
    | GE
    | LT
    | GT
    | AND
    | OR
    | NOT? LIKE
    | GLOBAL? NOT? IN
    ;
enumValue: STRING_LITERAL EQ_SINGLE INTEGER_LITERAL;
