parser grammar ClickHouseParser;

options {
    tokenVocab = ClickHouseLexer;
}

// Top-level statements

queryStmt: query (INTO OUTFILE STRING_LITERAL)? (FORMAT identifierOrNull)? (SEMICOLON)? | insertStmt;
query
    : alterStmt     // DDL
    | analyzeStmt
    | checkStmt
    | createStmt    // DDL
    | describeStmt
    | dropStmt      // DDL
    | existsStmt
    | optimizeStmt  // DDL
    | renameStmt    // DDL
    | selectUnionStmt
    | setStmt
    | showStmt
    | systemStmt
    | truncateStmt  // DDL
    | useStmt
    ;

// ALTER statement

alterStmt
    : ALTER TABLE tableIdentifier alterTableClause (COMMA alterTableClause)*  # AlterTableStmt
    ;

alterTableClause
    : ADD COLUMN (IF NOT EXISTS)? tableColumnDfnt (AFTER nestedIdentifier)?        # AlterTableClauseAdd
    | ATTACH partitionClause (FROM tableIdentifier)?                               # AlterTableClauseAttach
    | CLEAR COLUMN (IF EXISTS)? nestedIdentifier (IN partitionClause)?             # AlterTableClauseClear
    | COMMENT COLUMN (IF EXISTS)? nestedIdentifier STRING_LITERAL                  # AlterTableClauseComment
    | DELETE WHERE columnExpr                                                      # AlterTableClauseDelete
    | DETACH partitionClause                                                       # AlterTableClauseDetach
    | DROP COLUMN (IF EXISTS)? nestedIdentifier                                    # AlterTableClauseDropColumn
    | DROP partitionClause                                                         # AlterTableClauseDropPartition
    | MODIFY COLUMN (IF EXISTS)? tableColumnDfnt                                   # AlterTableClauseModify
    | MODIFY ORDER BY columnExpr                                                   # AlterTableClauseOrderBy
    | MODIFY COLUMN (IF EXISTS)? nestedIdentifier REMOVE tableColumnPropertyType   # AlterTableClauseRemove
    | MODIFY TTL columnExpr                                                        # AlterTableClauseTTL
    | REMOVE TTL                                                                   # AlterTableClauseRemoveTTL
    | RENAME COLUMN (IF EXISTS)? nestedIdentifier TO nestedIdentifier              # AlterTableClauseRename
    | REPLACE partitionClause FROM tableIdentifier                                 # AlterTableClauseReplace
    ;

tableColumnPropertyType: ALIAS | CODEC | COMMENT | DEFAULT | MATERIALIZED | TTL;

partitionClause
    : PARTITION columnExpr         // actually we expect here any form of tuple of literals
    | PARTITION ID STRING_LITERAL
    ;

// ANALYZE statement

analyzeStmt: ANALYZE query;

// CHECK statement

checkStmt: CHECK TABLE tableIdentifier;

// CREATE statement

createStmt
    : (ATTACH | CREATE) DATABASE (IF NOT EXISTS)? databaseIdentifier engineExpr?                                                                      # CreateDatabaseStmt
    | (ATTACH | CREATE) MATERIALIZED VIEW (IF NOT EXISTS)? tableIdentifier schemaClause? (destinationClause | engineClause POPULATE?) subqueryClause  # CreateMaterializedViewStmt
    | (ATTACH | CREATE) TEMPORARY? TABLE (IF NOT EXISTS)? tableIdentifier schemaClause? engineClause? subqueryClause?                                 # CreateTableStmt
    | (ATTACH | CREATE) VIEW (IF NOT EXISTS)? tableIdentifier schemaClause? subqueryClause                                                            # CreateViewStmt
    ;

destinationClause: TO tableIdentifier;
subqueryClause: AS selectUnionStmt;
schemaClause
    : LPAREN tableElementExpr (COMMA tableElementExpr)* RPAREN  # SchemaDescriptionClause
    | AS tableIdentifier                                        # SchemaAsTableClause
    | AS tableFunctionExpr                                      # SchemaAsFunctionClause
    ;
engineClause
locals [std::set<std::string> clauses]:
    engineExpr
    ( {!$clauses.count("orderByClause")}?     orderByClause     {$clauses.insert("orderByClause");}
    | {!$clauses.count("partitionByClause")}? partitionByClause {$clauses.insert("partitionByClause");}
    | {!$clauses.count("primaryKeyClause")}?  primaryKeyClause  {$clauses.insert("primaryKeyClause");}
    | {!$clauses.count("sampleByClause")}?    sampleByClause    {$clauses.insert("sampleByClause");}
    | {!$clauses.count("ttlClause")}?         ttlClause         {$clauses.insert("ttlClause");}
    | {!$clauses.count("settingsClause")}?    settingsClause    {$clauses.insert("settingsClause");}
    )*
    ;
partitionByClause: PARTITION BY columnExpr;
primaryKeyClause: PRIMARY KEY columnExpr;
sampleByClause: SAMPLE BY columnExpr;
ttlClause: TTL ttlExpr (COMMA ttlExpr)*;

engineExpr: ENGINE EQ_SINGLE? identifierOrNull (LPAREN columnExprList? RPAREN)?;
tableElementExpr
    : tableColumnDfnt                                                              # TableElementExprColumn
    | CONSTRAINT identifier CHECK columnExpr                                       # TableElementExprConstraint
    | INDEX identifier columnExpr TYPE columnTypeExpr GRANULARITY DECIMAL_LITERAL  # TableElementExprIndex
    ;
tableColumnDfnt
    : nestedIdentifier columnTypeExpr tableColumnPropertyExpr? (COMMENT STRING_LITERAL)? codecExpr? (TTL columnExpr)?
    | nestedIdentifier columnTypeExpr? tableColumnPropertyExpr (COMMENT STRING_LITERAL)? codecExpr? (TTL columnExpr)?
    ;
tableColumnPropertyExpr: (DEFAULT | MATERIALIZED | ALIAS) columnExpr;
codecExpr: CODEC LPAREN identifier (LPAREN columnExprList? RPAREN)? RPAREN;
ttlExpr: columnExpr (DELETE | TO DISK STRING_LITERAL | TO VOLUME STRING_LITERAL)?;

// DESCRIBE statement

describeStmt: (DESCRIBE | DESC) TABLE? tableExpr;

// DROP statement

dropStmt
    : (DETACH | DROP) DATABASE (IF EXISTS)? databaseIdentifier                   # DropDatabaseStmt
    | (DETACH | DROP) TEMPORARY? TABLE (IF EXISTS)? tableIdentifier (NO DELAY)?  # DropTableStmt
    ;

// EXISTS statement

existsStmt: EXISTS TEMPORARY? TABLE tableIdentifier;

// INSERT statement

insertStmt: INSERT INTO TABLE? (tableIdentifier | FUNCTION tableFunctionExpr) columnsClause? dataClause;

columnsClause: LPAREN nestedIdentifier (COMMA nestedIdentifier)* RPAREN;
dataClause
    : FORMAT identifier  # DataClauseFormat
    | VALUES             # DataClauseValues
    | selectUnionStmt    # DataClauseSelect
    ;

// OPTIMIZE statement

optimizeStmt: OPTIMIZE TABLE tableIdentifier partitionClause? FINAL? DEDUPLICATE?;

// RENAME statement

renameStmt: RENAME TABLE tableIdentifier TO tableIdentifier (COMMA tableIdentifier TO tableIdentifier)*;

// SELECT statement

selectUnionStmt: selectStmtWithParens (UNION ALL selectStmtWithParens)*;
selectStmtWithParens: selectStmt | LPAREN selectUnionStmt RPAREN;
selectStmt:
    withClause?
    SELECT DISTINCT? columnExprList
    fromClause?
    arrayJoinClause?
    prewhereClause?
    whereClause?
    groupByClause? (WITH TOTALS)?
    havingClause?
    orderByClause?
    limitByClause?
    limitClause?
    settingsClause?
    ;

withClause: WITH columnExprList;
fromClause: FROM joinExpr;
arrayJoinClause: (LEFT | INNER)? ARRAY JOIN columnExprList;
prewhereClause: PREWHERE columnExpr;
whereClause: WHERE columnExpr;
groupByClause: GROUP BY columnExprList;
havingClause: HAVING columnExpr;
orderByClause: ORDER BY orderExprList;
limitByClause: LIMIT limitExpr BY columnExprList;
limitClause: LIMIT limitExpr (WITH TIES)?;
settingsClause: SETTINGS settingExprList;

joinExpr
    : joinExpr (GLOBAL | LOCAL)? joinOp? JOIN joinExpr joinConstraintClause  # JoinExprOp
    | joinExpr joinOpCross joinExpr                                          # JoinExprCrossOp
    | tableExpr FINAL? sampleClause?                                         # JoinExprTable
    | LPAREN joinExpr RPAREN                                                 # JoinExprParens
    ;
joinOp
    : ((ALL | ANY | ASOF)? INNER | INNER (ALL | ANY | ASOF)? | (ALL | ANY | ASOF))  # JoinOpInner
    | ( (SEMI | ALL | ANTI | ANY | ASOF)? (LEFT | RIGHT) OUTER?
      | (LEFT | RIGHT) OUTER? (SEMI | ALL | ANTI | ANY | ASOF)?
      )                                                                             # JoinOpLeftRight
    | ((ALL | ANY)? FULL OUTER? | FULL OUTER? (ALL | ANY)?)                         # JoinOpFull
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

sampleClause: SAMPLE ratioExpr (OFFSET ratioExpr)?;
limitExpr: DECIMAL_LITERAL ((COMMA | OFFSET) DECIMAL_LITERAL)?;
orderExprList: orderExpr (COMMA orderExpr)*;
orderExpr: columnExpr (ASCENDING | DESCENDING | DESC)? (NULLS (FIRST | LAST))? (COLLATE STRING_LITERAL)?;
ratioExpr: numberLiteral (SLASH numberLiteral)?;
settingExprList: settingExpr (COMMA settingExpr)*;
settingExpr: identifier EQ_SINGLE literal;

// SET statement

setStmt: SET settingExprList;

// SHOW statements

showStmt
    : SHOW CREATE DATABASE databaseIdentifier                                                                     # showCreateDatabaseStmt
    | SHOW CREATE TEMPORARY? TABLE? tableIdentifier                                                               # showCreateTableStmt
    | SHOW TEMPORARY? TABLES ((FROM | IN) databaseIdentifier)? (LIKE STRING_LITERAL | whereClause)? limitClause?  # showTablesStmt
    | SHOW DATABASES                                                                                              # showDatabasesStmt
    ;

// SYSTEM statements

systemStmt
    : SYSTEM FLUSH DISTRIBUTED tableIdentifier
    | SYSTEM FLUSH LOGS
    | SYSTEM (START | STOP) (DISTRIBUTED SENDS | FETCHES | MERGES) tableIdentifier
    | SYSTEM SYNC REPLICA tableIdentifier
    ;

// TRUNCATE statements

truncateStmt: TRUNCATE TEMPORARY? TABLE (IF EXISTS)? tableIdentifier;

// USE statement

useStmt: USE databaseIdentifier;



// Columns

columnTypeExpr
    : identifier                                                                             # ColumnTypeExprSimple   // UInt64
    | identifier LPAREN identifier columnTypeExpr (COMMA identifier columnTypeExpr)* RPAREN  # ColumnTypeExprNested   // Nested
    | identifier LPAREN enumValue (COMMA enumValue)* RPAREN                                  # ColumnTypeExprEnum     // Enum
    | identifier LPAREN columnTypeExpr (COMMA columnTypeExpr)* RPAREN                        # ColumnTypeExprComplex  // Array, Tuple
    | identifier LPAREN columnExprList? RPAREN                                               # ColumnTypeExprParam    // FixedString(N)
    ;
columnExprList: columnsExpr (COMMA columnsExpr)*;
columnsExpr
    : (tableIdentifier DOT)? ASTERISK  # ColumnsExprAsterisk
    | LPAREN selectUnionStmt RPAREN    # ColumnsExprSubquery
    // NOTE: asterisk and subquery goes before |columnExpr| so that we can mark them as multi-column expressions.
    | columnExpr                       # ColumnsExprColumn
    ;
columnExpr
    : CASE columnExpr? (WHEN columnExpr THEN columnExpr)+ (ELSE columnExpr)? END          # ColumnExprCase
    | CAST LPAREN columnExpr AS columnTypeExpr RPAREN                                     # ColumnExprCast
    | DATE STRING_LITERAL                                                                 # ColumnExprDate
    | EXTRACT LPAREN interval FROM columnExpr RPAREN                                      # ColumnExprExtract
    | INTERVAL columnExpr interval                                                        # ColumnExprInterval
    | SUBSTRING LPAREN columnExpr FROM columnExpr (FOR columnExpr)? RPAREN                # ColumnExprSubstring
    | TIMESTAMP STRING_LITERAL                                                            # ColumnExprTimestamp
    | TRIM LPAREN (BOTH | LEADING | TRAILING) STRING_LITERAL FROM columnExpr RPAREN       # ColumnExprTrim
    | identifier (LPAREN columnExprList? RPAREN)? LPAREN DISTINCT? columnArgList? RPAREN  # ColumnExprFunction
    | literal                                                                             # ColumnExprLiteral

    // FIXME(ilezhankin): this part looks very ugly, maybe there is another way to express it
    | columnExpr LBRACKET columnExpr RBRACKET                                             # ColumnExprArrayAccess
    | columnExpr DOT DECIMAL_LITERAL                                                      # ColumnExprTupleAccess
    | unaryOp columnExpr                                                                  # ColumnExprUnaryOp
    | columnExpr ( ASTERISK                                                               // multiply
                 | SLASH                                                                  // divide
                 | PERCENT                                                                // modulo
                 ) columnExpr                                                             # ColumnExprPrecedence1
    | columnExpr ( PLUS                                                                   // plus
                 | DASH                                                                   // minus
                 | CONCAT                                                                 // concat
                 ) columnExpr                                                             # ColumnExprPrecedence2
    | columnExpr ( EQ_DOUBLE                                                              // equals
                 | EQ_SINGLE                                                              // equals
                 | NOT_EQ                                                                 // notEquals
                 | LE                                                                     // lessOrEquals
                 | GE                                                                     // greaterOrEquals
                 | LT                                                                     // less
                 | GT                                                                     // greater
                 | GLOBAL? NOT? IN                                                        // in, notIn, globalIn, globalNotIn
                 | NOT? LIKE                                                              // like, notLike
                 ) columnExpr                                                             # ColumnExprPrecedence3
    | columnExpr IS NOT? NULL_SQL                                                         # ColumnExprIsNull
    | columnExpr AND columnExpr                                                           # ColumnExprAnd
    | columnExpr OR columnExpr                                                            # ColumnExprOr
    | columnExpr NOT? BETWEEN columnExpr AND columnExpr                                   # ColumnExprBetween
    | <assoc=right> columnExpr QUERY columnExpr COLON columnExpr                          # ColumnExprTernaryOp
    | columnExpr (alias | AS identifier)                                                  # ColumnExprAlias

    | (tableIdentifier DOT)? ASTERISK                                                     # ColumnExprAsterisk  // single-column only
    | LPAREN selectUnionStmt RPAREN                                                       # ColumnExprSubquery  // single-column only
    | LPAREN columnExpr RPAREN                                                            # ColumnExprParens    // single-column only
    | LPAREN columnExprList RPAREN                                                        # ColumnExprTuple
    | LBRACKET columnExprList? RBRACKET                                                   # ColumnExprArray
    | columnIdentifier                                                                    # ColumnExprIdentifier
    ;
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
    : tableIdentifier                    # TableExprIdentifier
    | tableFunctionExpr                  # TableExprFunction
    | LPAREN selectUnionStmt RPAREN      # TableExprSubquery
    | tableExpr (alias | AS identifier)  # TableExprAlias
    ;
tableFunctionExpr: identifier LPAREN tableArgList? RPAREN;
tableIdentifier: (databaseIdentifier DOT)? identifier;
tableArgList: tableArgExpr (COMMA tableArgExpr)*;
tableArgExpr
    : tableIdentifier
    | tableFunctionExpr
    | literal
    ;

// Databases

databaseIdentifier: identifier;

// Basics

floatingLiteral
    : FLOATING_LITERAL
    | DOT DECIMAL_LITERAL
    ;
numberLiteral: (PLUS | DASH)? (floatingLiteral | OCTAL_LITERAL | DECIMAL_LITERAL | HEXADECIMAL_LITERAL | INF | NAN_SQL);
literal
    : numberLiteral
    | STRING_LITERAL
    | NULL_SQL
    ;
interval: SECOND | MINUTE | HOUR | DAY | WEEK | MONTH | QUARTER | YEAR;
keyword
    // except NULL_SQL, INF, NAN_SQL
    : AFTER | ALIAS | ALL | ALTER | ANALYZE | AND | ANTI | ANY | ARRAY | AS | ASCENDING | ASOF | ATTACH | BETWEEN | BOTH | BY | CASE | CAST
    | CHECK | CLEAR | CLUSTER | CODEC | COLLATE | COLUMN | COMMENT | CONSTRAINT | CREATE | CROSS | DATABASE | DATABASES | DATE | DAY
    | DEDUPLICATE | DEFAULT | DELAY | DELETE | DESCRIBE | DESC | DESCENDING | DETACH | DISK | DISTINCT | DISTRIBUTED | DROP | ELSE | END
    | ENGINE | EXISTS | EXTRACT | FETCHES | FINAL | FIRST | FLUSH | FOR | FORMAT | FROM | FULL | FUNCTION | GLOBAL | GRANULARITY | GROUP
    | HAVING | HOUR | ID | IF | IN | INDEX | INNER | INSERT | INTERVAL | INTO | IS | JOIN | JSON_FALSE | JSON_TRUE | KEY | LAST | LEADING
    | LEFT | LIKE | LIMIT | LOCAL | LOGS | MATERIALIZED | MERGES | MINUTE | MODIFY | MONTH | NO | NOT | NULLS | OFFSET | ON | OPTIMIZE | OR
    | ORDER | OUTER | OUTFILE | PARTITION | POPULATE | PREWHERE | PRIMARY | QUARTER | REMOVE | RENAME | REPLACE | REPLICA | RIGHT | SAMPLE
    | SECOND | SELECT | SEMI | SENDS | SET | SETTINGS | SHOW | START | STOP | SUBSTRING | SYNC | SYSTEM | TABLE | TABLES | TEMPORARY | THEN
    | TIES | TIMESTAMP | TOTALS | TRAILING | TRIM | TRUNCATE | TO | TTL | TYPE | UNION | USE | USING | VALUES | VIEW | VOLUME | WEEK | WHEN
    | WHERE | WITH | YEAR
    ;
keywordForAlias
    : ID | KEY
    ;
alias: IDENTIFIER | keywordForAlias;
identifier: IDENTIFIER | interval | keyword;
identifierOrNull: identifier | NULL_SQL;  // NULL_SQL can be only 'Null' here.
unaryOp: DASH | NOT;
enumValue: STRING_LITERAL EQ_SINGLE numberLiteral;
