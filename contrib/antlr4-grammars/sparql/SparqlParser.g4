/*
 * Copyright 2007 the original author or authors.
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 * disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 * disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * Neither the name of the author or authors nor the names of its contributors may be used to endorse or promote
 * products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * @author Simone Tripodi   (simone)
 * @author Michele Mostarda (michele)
 * @version $Id: Sparql.g 5 2007-10-30 17:20:36Z simone $
 */
/*
 * Ported to Antlr4 by Tom Everett
 */

// $antlr-format alignTrailingComments true, columnLimit 150, minEmptyLines 1, maxEmptyLinesToKeep 1, reflowComments false, useTab false
// $antlr-format allowShortRulesOnASingleLine false, allowShortBlocksOnASingleLine true, alignSemicolons hanging, alignColons hanging

parser grammar SparqlParser;

options {
    tokenVocab = SparqlLexer;
}

query
    : prologue (selectQuery | constructQuery | describeQuery | askQuery) EOF
    ;

prologue
    : baseDecl? prefixDecl*
    ;

baseDecl
    : BASE IRI_REF
    ;

prefixDecl
    : PREFIX PNAME_NS IRI_REF
    ;

selectQuery
    : SELECT (DISTINCT | REDUCED)? (var_+ | '*') datasetClause* whereClause solutionModifier
    ;

constructQuery
    : CONSTRUCT constructTemplate datasetClause* whereClause solutionModifier
    ;

describeQuery
    : DESCRIBE (varOrIRIref+ | '*') datasetClause* whereClause? solutionModifier
    ;

askQuery
    : ASK datasetClause* whereClause
    ;

datasetClause
    : FROM (defaultGraphClause | namedGraphClause)
    ;

defaultGraphClause
    : sourceSelector
    ;

namedGraphClause
    : NAMED sourceSelector
    ;

sourceSelector
    : iriRef
    ;

whereClause
    : WHERE? groupGraphPattern
    ;

solutionModifier
    : orderClause? limitOffsetClauses?
    ;

limitOffsetClauses
    : limitClause offsetClause?
    | offsetClause limitClause?
    ;

orderClause
    : ORDER BY orderCondition+
    ;

orderCondition
    : (ASC | DESC) brackettedExpression
    | constraint
    | var_
    ;

limitClause
    : LIMIT INTEGER
    ;

offsetClause
    : OFFSET INTEGER
    ;

groupGraphPattern
    : '{' triplesBlock? ((graphPatternNotTriples | filter_) '.'? triplesBlock?)* '}'
    ;

triplesBlock
    : triplesSameSubject ('.' triplesBlock?)?
    ;

graphPatternNotTriples
    : optionalGraphPattern
    | groupOrUnionGraphPattern
    | graphGraphPattern
    ;

optionalGraphPattern
    : OPTIONAL groupGraphPattern
    ;

graphGraphPattern
    : GRAPH varOrIRIref groupGraphPattern
    ;

groupOrUnionGraphPattern
    : groupGraphPattern (UNION groupGraphPattern)*
    ;

filter_
    : FILTER constraint
    ;

constraint
    : brackettedExpression
    | builtInCall
    | functionCall
    ;

functionCall
    : iriRef argList
    ;

argList
    : NIL
    | '(' expression (',' expression)* ')'
    ;

constructTemplate
    : '{' constructTriples? '}'
    ;

constructTriples
    : triplesSameSubject ('.' constructTriples?)?
    ;

triplesSameSubject
    : varOrTerm propertyListNotEmpty
    | triplesNode propertyList
    ;

propertyListNotEmpty
    : verb objectList (';' (verb objectList)?)*
    ;

propertyList
    : propertyListNotEmpty?
    ;

objectList
    : object_ (',' object_)*
    ;

object_
    : graphNode
    ;

verb
    : varOrIRIref
    | A
    ;

triplesNode
    : collection
    | blankNodePropertyList
    ;

blankNodePropertyList
    : '[' propertyListNotEmpty ']'
    ;

collection
    : '(' graphNode+ ')'
    ;

graphNode
    : varOrTerm
    | triplesNode
    ;

varOrTerm
    : var_
    | graphTerm
    ;

varOrIRIref
    : var_
    | iriRef
    ;

var_
    : VAR1
    | VAR2
    ;

graphTerm
    : iriRef
    | rdfLiteral
    | numericLiteral
    | booleanLiteral
    | blankNode
    | NIL
    ;

expression
    : conditionalOrExpression
    ;

conditionalOrExpression
    : conditionalAndExpression ('||' conditionalAndExpression)*
    ;

conditionalAndExpression
    : valueLogical ('&&' valueLogical)*
    ;

valueLogical
    : relationalExpression
    ;

relationalExpression
    : numericExpression (('=' | '!=' | '<' | '>' | '<=' | '>=') numericExpression)?
    ;

numericExpression
    : additiveExpression
    ;

additiveExpression
    : multiplicativeExpression (
        ('+' | '-') multiplicativeExpression
        | numericLiteralPositive
        | numericLiteralNegative
    )*
    ;

multiplicativeExpression
    : unaryExpression (('*' | '/') unaryExpression)*
    ;

unaryExpression
    : ('!' | '+' | '-')? primaryExpression
    ;

primaryExpression
    : brackettedExpression
    | builtInCall
    | iriRefOrFunction
    | rdfLiteral
    | numericLiteral
    | booleanLiteral
    | var_
    ;

brackettedExpression
    : '(' expression ')'
    ;

builtInCall
    : STR '(' expression ')'
    | LANG '(' expression ')'
    | LANGMATCHES '(' expression ',' expression ')'
    | DATATYPE '(' expression ')'
    | BOUND '(' var_ ')'
    | SAME_TERM '(' expression ',' expression ')'
    | IS_IRI '(' expression ')'
    | IS_URI '(' expression ')'
    | IS_BLANK '(' expression ')'
    | IS_LITERAL '(' expression ')'
    | regexExpression
    ;

regexExpression
    : REGEX '(' expression ',' expression (',' expression)? ')'
    ;

iriRefOrFunction
    : iriRef argList?
    ;

rdfLiteral
    : string_ (LANGTAG | '^^' iriRef)?
    ;

numericLiteral
    : numericLiteralUnsigned
    | numericLiteralPositive
    | numericLiteralNegative
    ;

numericLiteralUnsigned
    : INTEGER
    | DECIMAL
    | DOUBLE
    ;

numericLiteralPositive
    : INTEGER_POSITIVE
    | DECIMAL_POSITIVE
    | DOUBLE_POSITIVE
    ;

numericLiteralNegative
    : INTEGER_NEGATIVE
    | DECIMAL_NEGATIVE
    | DOUBLE_NEGATIVE
    ;

booleanLiteral
    : TRUE
    | FALSE
    ;

string_
    : STRING_LITERAL1
    | STRING_LITERAL2
    /* | STRING_LITERAL_LONG('0'..'9') | STRING_LITERAL_LONG('0'..'9')*/
    ;

iriRef
    : IRI_REF
    | prefixedName
    ;

prefixedName
    : PNAME_LN
    | PNAME_NS
    ;

blankNode
    : BLANK_NODE_LABEL
    | ANON
    ;