parser grammar ClickHouseParser;

options {
    tokenVocab = ClickHouseLexer;
}

// Top-level statements

queryList: queryStmt (SEMICOLON queryStmt)* SEMICOLON?;

queryStmt:  // NOTE: |ParserTreeVisitor::visitQueryStmtAsParent()| callers depend on this rule
    ( selectUnionStmt | insertStmt )
    (INTO OUTFILE STRING_LITERAL)?
    (FORMAT identifier)?
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

joinExpr: tableIdentifier; // TODO: not complete!
limitExpr: NUMBER_LITERAL ((COMMA | OFFSET) NUMBER_LITERAL)?;
orderExprList: orderExpr (COMMA orderExpr)*;
orderExpr: columnExpr (ASCENDING | DESCENDING)? (NULLS (FIRST | LAST))? (COLLATE STRING_LITERAL)?;
ratioExpr: NUMBER_LITERAL (SLASH NUMBER_LITERAL); // TODO: not complete!
settingExprList: settingExpr (COMMA settingExpr)*;
settingExpr: identifier EQ_SINGLE LITERAL;

// INSERT statement

insertStmt:
    INSERT INTO  // TODO: not complete!
    ;

// Columns

columnExprList: columnExpr (COMMA columnExpr)*;
columnExpr
    : LITERAL                                                                     # Literal
    | ASTERISK                                                                    # Asterisk
    | columnIdentifier                                                            # Id
    | LPAREN columnExpr RPAREN                                                    # Tuple
    | LPAREN selectStmt RPAREN                                                    # Subquery
    | LBRACKET columnExprList? RBRACKET                                           # Array

    // NOTE: rules below are sorted according to operators' priority - the most priority on top.
    | columnExpr LBRACKET columnExpr RBRACKET                                     # ArrayAccess
    | columnExpr DOT NUMBER_LITERAL                                               # TupleAccess
    | unaryOp columnExpr                                                          # Unary
    | columnExpr IS NOT? NULL_SQL                                                 # IsNull
    | columnExpr binaryOp columnExpr                                              # Binary
    | columnExpr QUERY columnExpr COLON columnExpr                                # Ternary
    | columnExpr NOT? BETWEEN columnExpr AND columnExpr                           # Between
    | CASE columnExpr? (WHEN columnExpr THEN columnExpr)+ (ELSE columnExpr)? END  # Case
    | INTERVAL columnExpr INTERVAL_TYPE                                           # Interval
    | columnFunctionExpr                                                          # FunctionCall
    | columnExpr AS identifier                                                    # Alias
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
