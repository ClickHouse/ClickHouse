parser grammar ClickHouseParser;

options {
    tokenVocab = ClickHouseLexer;
}

// Top-level statements

queryList: queryStmt (SEMICOLON queryStmt)* SEMICOLON? EOF;

queryStmt: query (INTO OUTFILE STRING_LITERAL)? (FORMAT (identifier | NULL_SQL))?;

query
    : alterStmt     // DDL
    | checkStmt
    | createStmt    // DDL
    | describeStmt
    | dropStmt      // DDL
    | existsStmt
    | insertStmt
    | optimizeStmt  // DDL
    | renameStmt    // DDL
    | selectUnionStmt
    | setStmt
    | showStmt
    | useStmt
    ;

// ALTER statement

alterStmt
    : ALTER TABLE tableIdentifier alterTableClause (COMMA alterTableClause)*  # AlterTableStmt
    | ALTER TABLE tableIdentifier alterPartitionClause                        # AlterPartitionStmt
    ;

alterTableClause
    : ADD COLUMN (IF NOT EXISTS)? tableColumnDfnt (AFTER nestedIdentifier)?  # AlterTableAddClause
    | COMMENT COLUMN (IF EXISTS)? nestedIdentifier STRING_LITERAL            # AlterTableCommentClause
    | DROP COLUMN (IF EXISTS)? nestedIdentifier                              # AlterTableDropClause
    | MODIFY COLUMN (IF EXISTS)? tableColumnDfnt                             # AlterTableModifyClause
    ;
alterPartitionClause
    : DROP partitionClause  # AlterPartitionDropClause
    ;

// CHECK statement

checkStmt: CHECK TABLE tableIdentifier;

// CREATE statement

createStmt
    : (ATTACH | CREATE) DATABASE (IF NOT EXISTS)? databaseIdentifier engineExpr?                                                 # CreateDatabaseStmt
    | (ATTACH | CREATE) MATERIALIZED VIEW (IF NOT EXISTS)? tableIdentifier schemaClause? engineClause? POPULATE? subqueryClause  # CreateMaterializedViewStmt
    | (ATTACH | CREATE) TEMPORARY? TABLE (IF NOT EXISTS)? tableIdentifier schemaClause? engineClause? subqueryClause?            # CreateTableStmt
    | (ATTACH | CREATE) VIEW (IF NOT EXISTS)? tableIdentifier subqueryClause                                                     # CreateViewStmt
    ;

subqueryClause: AS selectUnionStmt;
schemaClause
    : LPAREN tableElementExpr (COMMA tableElementExpr)* RPAREN  # SchemaDescriptionClause
    | AS tableIdentifier                                        # SchemaAsTableClause
    | AS identifier LPAREN tableArgList? RPAREN                 # SchemaAsFunctionClause
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

engineExpr: ENGINE EQ_SINGLE? (identifier | NULL_SQL) (LPAREN columnExprList? RPAREN)?;
tableElementExpr
    : tableColumnDfnt  # TableElementExprColumn
    // TODO: INDEX
    // TODO: CONSTRAINT
    ;
tableColumnDfnt
    : nestedIdentifier columnTypeExpr tableColumnPropertyExpr? /*TODO: codecExpr?*/ (TTL columnExpr)?
    | nestedIdentifier columnTypeExpr? tableColumnPropertyExpr /*TODO: codexExpr?*/ (TTL columnExpr)?
    ;
tableColumnPropertyExpr: (DEFAULT | MATERIALIZED | ALIAS) columnExpr;
ttlExpr: columnExpr (DELETE | TO DISK STRING_LITERAL | TO VOLUME STRING_LITERAL)?;

// DESCRIBE statement

describeStmt: (DESCRIBE | DESC) TABLE tableIdentifier;

// DROP statement

dropStmt
    : (DETACH | DROP) DATABASE (IF EXISTS)? databaseIdentifier                   # DropDatabaseStmt
    | (DETACH | DROP) TEMPORARY? TABLE (IF EXISTS)? tableIdentifier (NO DELAY)?  # DropTableStmt
    ;

// EXISTS statement

existsStmt: EXISTS TEMPORARY? TABLE tableIdentifier;

// INSERT statement

insertStmt: INSERT INTO tableIdentifier (LPAREN nestedIdentifier (COMMA nestedIdentifier)* RPAREN)? valuesClause;

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

// RENAME statement

renameStmt: RENAME TABLE tableIdentifier TO tableIdentifier (COMMA tableIdentifier TO tableIdentifier)*;

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
limitClause: LIMIT limitExpr (WITH TIES)?;
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
ratioExpr: numberLiteral (SLASH numberLiteral)?;
settingExprList: settingExpr (COMMA settingExpr)*;
settingExpr: identifier EQ_SINGLE literal;

// SET statement

setStmt: SET settingExprList;

// SHOW statements

showStmt
    : SHOW CREATE TEMPORARY? TABLE tableIdentifier                                                                # showCreateTableStmt
    | SHOW TEMPORARY? TABLES ((FROM | IN) databaseIdentifier)? (LIKE STRING_LITERAL | whereClause)? limitClause?  # showTablesStmt
    ;

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
    : CASE columnExpr? (WHEN columnExpr THEN columnExpr)+ (ELSE columnExpr)? END     # ColumnExprCase
    | CAST LPAREN columnExpr AS columnTypeExpr RPAREN                                # ColumnExprCast
    | EXTRACT LPAREN INTERVAL_TYPE FROM columnExpr RPAREN                            # ColumnExprExtract
    | INTERVAL columnExpr INTERVAL_TYPE                                              # ColumnExprInterval
    | TRIM LPAREN (BOTH | LEADING | TRAILING) STRING_LITERAL FROM columnExpr RPAREN  # ColumnExprTrim
    | identifier (LPAREN columnParamList? RPAREN)? LPAREN columnArgList? RPAREN      # ColumnExprFunction
    | columnExpr LBRACKET columnExpr RBRACKET                                        # ColumnExprArrayAccess
    | columnExpr DOT INTEGER_LITERAL                                                 # ColumnExprTupleAccess
    | unaryOp columnExpr                                                             # ColumnExprUnaryOp
    | columnExpr IS NOT? NULL_SQL                                                    # ColumnExprIsNull
    | columnExpr binaryOp columnExpr                                                 # ColumnExprBinaryOp
    | columnExpr QUERY columnExpr COLON columnExpr                                   # ColumnExprTernaryOp
    | columnExpr NOT? BETWEEN columnExpr AND columnExpr                              # ColumnExprBetween
    | columnExpr AS? identifier                                                      # ColumnExprAlias
    | (tableIdentifier DOT)? ASTERISK                                                # ColumnExprAsterisk  // single-column only
    | LPAREN selectUnionStmt RPAREN                                                  # ColumnExprSubquery  // single-column only
    | LPAREN columnExpr RPAREN                                                       # ColumnExprParens    // single-column only
    | LPAREN columnExprList RPAREN                                                   # ColumnExprTuple
    | LBRACKET columnExprList? RBRACKET                                              # ColumnExprArray
    | columnIdentifier                                                               # ColumnExprIdentifier
    | literal                                                                        # ColumnExprLiteral
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
columnIdentifier: (tableIdentifier DOT)? nestedIdentifier;
nestedIdentifier: identifier (DOT identifier)?;

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
    : tableIdentifier
    | literal
    ;

// Databases

databaseIdentifier: identifier;

// Basics

floatingLiteral
    : FLOATING_LITERAL
    | INTEGER_LITERAL DOT INTEGER_LITERAL?
    | DOT INTEGER_LITERAL
    ;
numberLiteral: (PLUS | DASH)? (floatingLiteral | HEXADECIMAL_LITERAL | INTEGER_LITERAL | INF | NAN_SQL);
literal
    : numberLiteral
    | STRING_LITERAL
    | NULL_SQL
    | identifier LPAREN RPAREN  // TODO: expand to LiteralExpr
    ;
keyword  // except NULL_SQL, SELECT, INF, NAN, USING, FROM, WHERE, POPULATE
    : AFTER | ALIAS | ALL | ALTER | AND | ANTI | ANY | ARRAY | AS | ASCENDING | ASOF | ATTACH | BETWEEN | BOTH | BY | CASE | CAST | CHECK
    | CLUSTER | COLLATE | COMMENT | CREATE | CROSS | DATABASE | DAY | DEDUPLICATE | DEFAULT | DELAY | DELETE | DESC | DESCENDING | DESCRIBE
    | DETACH | DISK | DISTINCT | DROP | ELSE | END | ENGINE | EXISTS | EXTRACT | FINAL | FIRST | FORMAT | FULL | GLOBAL | GROUP | HAVING
    | HOUR | ID | IF | IN | INNER | INSERT | INTERVAL | INTO | IS | JOIN | KEY | LAST | LEADING | LEFT | LIKE | LIMIT | LOCAL
    | MATERIALIZED | MINUTE | MODIFY | MONTH | NO | NOT | NULLS | OFFSET | ON | OPTIMIZE | OR | ORDER | OUTER | OUTFILE | PARTITION
    | PREWHERE | PRIMARY | QUARTER | RENAME | RIGHT | SAMPLE | SECOND | SEMI | SET | SETTINGS | SHOW | TABLE | TABLES | TEMPORARY | THEN
    | TIES | TOTALS | TRAILING | TRIM | TO | TTL | UNION | USE | VALUES | VIEW | VOLUME | WEEK | WHEN | WITH | YEAR
    ;
identifier: IDENTIFIER | INTERVAL_TYPE | keyword;
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
enumValue: STRING_LITERAL EQ_SINGLE numberLiteral;
