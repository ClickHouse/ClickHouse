parser grammar ClickHouseParser;

options {
    tokenVocab = ClickHouseLexer;
}

// Top-level statements

queryList: queryStmt (SEMICOLON queryStmt)* SEMICOLON?;

queryStmt: selectUnionStmt;

// SELECT statement

selectUnionStmt:
    selectStmt (UNION ALL selectStmt)*
    (INTO OUTFILE STRING_LITERAL)?
    (FORMAT identifier)?
    ;
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

joinExpr: tableIdentifier; // TODO: not complete!
limitExpr: NUMBER_LITERAL (COMMA NUMBER_LITERAL)? | NUMBER_LITERAL OFFSET NUMBER_LITERAL;
orderExprList: orderExpr (COMMA orderExpr)*;
orderExpr: columnExpr (ASCENDING | DESCENDING)? (NULLS (FIRST | LAST))? (COLLATE STRING_LITERAL)?;
ratioExpr: NUMBER_LITERAL (SLASH NUMBER_LITERAL); // TODO: not complete!
settingExprList: settingExpr (COMMA settingExpr)*;
settingExpr: identifier EQ_SINGLE LITERAL;

// Columns

columnExprList: columnExpr (COMMA columnExpr)*;
columnExpr
    : LITERAL                                   // literal
    | ASTERISK                                  // * (all columns)
    | columnIdentifier                         // identifier
    | LPAREN columnExpr RPAREN                 // tuple
    | LPAREN selectStmt RPAREN                 // subquery
    | LBRACKET columnExprList? RBRACKET       // array

    // NOTE: rules below are sorted according to operators' priority - the most priority on top.
    | columnExpr LBRACKET columnExpr RBRACKET // array access
    | columnExpr DOT NUMBER_LITERAL            // tuple access
    | unaryOp columnExpr
    | columnExpr IS NOT? NULL_SQL
    | columnExpr binaryOp columnExpr
    | columnExpr QUERY columnExpr COLON columnExpr
    | columnExpr NOT? BETWEEN columnExpr AND columnExpr
    | CASE columnExpr? (WHEN columnExpr THEN columnExpr)+ (ELSE columnExpr)? END
    | INTERVAL columnExpr INTERVAL_TYPE
    | columnFunctionExpr                      // function call
    | columnExpr AS identifier                 // alias
    ;
columnFunctionExpr
    : identifier (LPAREN (LITERAL (COMMA LITERAL)*)? RPAREN)? LPAREN columnArgList? RPAREN
    // TODO: do we really need this misc parsing rules?
    | EXTRACT LPAREN INTERVAL_TYPE FROM columnExpr RPAREN
    | CAST LPAREN columnExpr AS identifier RPAREN
    | TRIM LPAREN (BOTH | LEADING | TRAILING) STRING_LITERAL FROM columnExpr RPAREN
    ;
columnArgList: columnArgExpr (COMMA columnArgExpr)*;
columnArgExpr: columnExpr | columnLambdaExpr;
columnLambdaExpr:
    ( LPAREN identifier (COMMA identifier)* RPAREN
    |        identifier (COMMA identifier)*
    )
    ARROW columnExpr
    ;
columnIdentifier: (tableIdentifier DOT)? identifier; // TODO: don't forget compound identifier.

// Tables

tableIdentifier: (databaseIdentifier DOT)? identifier;

// Databases

databaseIdentifier: identifier;

// Basics

identifier: IDENTIFIER; // TODO: not complete!
unaryOp: DASH | NOT;
binaryOp
    : ASTERISK | SLASH | PERCENT | PLUS | DASH | EQ | NOT_EQ | LE | GE | LT | GT | CONCAT // signs
    | AND | OR | NOT? LIKE | GLOBAL? NOT? IN                                              // keywords
    ;
